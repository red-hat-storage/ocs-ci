"""
This is plugin for all the plugins/hooks related to OCS-CI and its
configuration.

The basic configuration is done in run_ocsci.py module casue we need to load
all the config before pytest run. This run_ocsci.py is just a wrapper for
pytest which proccess config and passes all params to pytest.
"""
import logging
import os
from getpass import getuser

import pytest

from ocs_ci.framework import config as ocsci_config
from ocs_ci.framework.exceptions import ClusterPathNotProvidedError
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import (
    get_cluster_version,
    get_ceph_version,
    get_rook_version,
    get_csi_versions,
    get_testrun_name,
)
from ocs_ci.ocs.utils import collect_ocs_logs

__all__ = [
    "pytest_addoption",
]

log = logging.getLogger(__name__)


def pytest_addoption(parser):
    """
    Add necessary options to initialize OCS CI library.
    """
    parser.addoption(
        '--ocsci-conf',
        dest='ocsci_conf',
        help="Path to config file of OCS CI",
    )
    parser.addoption(
        '--cluster-conf',
        dest='cluster_conf',
        help="Path to cluster configuration yaml file",
    )
    parser.addoption(
        '--cluster-path',
        dest='cluster_path',
        help="Path to cluster directory",
    )
    parser.addoption(
        '--cluster-name',
        dest='cluster_name',
        help="Name of cluster",
    )
    parser.addoption(
        '--teardown',
        dest='teardown',
        action="store_true",
        default=False,
        help="If provided the test cluster will be destroyed after tests complete",
    )
    parser.addoption(
        '--deploy',
        dest='deploy',
        action="store_true",
        default=False,
        help="If provided a test cluster will be deployed on AWS to use for testing",
    )
    parser.addoption(
        '--email',
        dest='email',
        help="Email ID to send results",
    )
    parser.addoption(
        '--collect-logs',
        dest='collect-logs',
        action="store_true",
        default=False,
        help="Collect OCS logs when test case failed",
    )
    parser.addoption(
        '--io_in_bg',
        dest='io_in_bg',
        action="store_true",
        default=False,
        help="Run IO in the background",
    )


def pytest_configure(config):
    """
    Load config files, and initialize ocs-ci library.

    Args:
        config (pytest.config): Pytest config object

    """
    if not (config.getoption("--help") or config.getoption("collectonly")):
        process_cluster_cli_params(config)
        # Add OCS related versions to the html report and remove extraneous metadata
        markers_arg = config.getoption('-m')
        if ocsci_config.RUN['cli_params'].get('teardown') or (
            "deployment" in markers_arg
            and ocsci_config.RUN['cli_params'].get('deploy')
        ):
            log.info(
                "Skiping versions collecting because: Deploy or destroy of "
                "cluster is performed."
            )
            return
        print("Collecting Cluster versions")
        # remove extraneous metadata
        del config._metadata['Python']
        del config._metadata['Packages']
        del config._metadata['Plugins']
        del config._metadata['Platform']

        config._metadata['Test Run Name'] = get_testrun_name()

        try:
            # add cluster version
            clusterversion = get_cluster_version()
            config._metadata['Cluster Version'] = clusterversion

            # add ceph version
            ceph_version = get_ceph_version()
            config._metadata['Ceph Version'] = ceph_version

            # add rook version
            rook_version = get_rook_version()
            config._metadata['Rook Version'] = rook_version

            # add csi versions
            csi_versions = get_csi_versions()
            config._metadata['csi-provisioner'] = csi_versions.get('csi-provisioner')
            config._metadata['cephfsplugin'] = csi_versions.get('cephfsplugin')
            config._metadata['rbdplugin'] = csi_versions.get('rbdplugin')
        except (FileNotFoundError, CommandFailed):
            pass


def get_cli_param(config, name_of_param, default=None):
    """
    This is helper function which store cli parameter in RUN section in
    cli_params

    Args:
        config (pytest.config): Pytest config object
        name_of_param (str): cli parameter name
        default (any): default value of parameter (default: None)

    Returns:
        any: value of cli parameter or default value

    """
    cli_param = config.getoption(name_of_param, default=default)
    ocsci_config.RUN['cli_params'][name_of_param] = cli_param
    return cli_param


def process_cluster_cli_params(config):
    """
    Process cluster related cli parameters

    Args:
        config (pytest.config): Pytest config object

    """
    cluster_path = get_cli_param(config, 'cluster_path')
    if not cluster_path:
        raise ClusterPathNotProvidedError()
    cluster_path = os.path.expanduser(cluster_path)
    if not os.path.exists(cluster_path):
        os.makedirs(cluster_path)
    # Importing here cause once the function is invoked we have already config
    # loaded, so this is OK to import once you sure that config is loaded.
    from ocs_ci.ocs.openshift_ops import OCP
    OCP.set_kubeconfig(
        os.path.join(cluster_path, ocsci_config.RUN['kubeconfig_location'])
    )
    cluster_name = get_cli_param(config, 'cluster_name')
    if not cluster_name:
        cluster_name = f"ocs-ci-{getuser()[:8]}"
    ocsci_config.RUN['cli_params']['teardown'] = get_cli_param(config, "teardown", default=False)
    ocsci_config.RUN['cli_params']['deploy'] = get_cli_param(config, "deploy", default=False)
    ocsci_config.RUN['cli_params']['io_in_bg'] = get_cli_param(config, "io_in_bg", default=False)
    ocsci_config.ENV_DATA['cluster_name'] = cluster_name
    ocsci_config.ENV_DATA['cluster_path'] = cluster_path
    get_cli_param(config, 'collect-logs')
    if get_cli_param(config, 'email') and not get_cli_param(config, '--html'):
        pytest.exit("--html option must be provided to send email reports")
    get_cli_param(config, '-m')


def pytest_collection_modifyitems(session, config, items):
    """
    Add Polarion ID property to test cases that are marked with one.
    """
    for item in items:
        try:
            marker = item.get_closest_marker(name="polarion_id")
            if marker:
                polarion_id = marker.args[0]
                item.user_properties.append(
                    ("polarion-testcase-id", polarion_id)
                )
        except IndexError:
            log.warning(
                f"polarion_id marker found with no value for "
                f"{item.name} in {item.fspath}",
                exc_info=True
            )


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    # we only look at actual failing test calls, not setup/teardown
    if (
        rep.when == "call"
        and rep.failed
        and ocsci_config.RUN.get('cli_params').get('collect-logs')
    ):
        test_case_name = item.name
        collect_ocs_logs(test_case_name)
