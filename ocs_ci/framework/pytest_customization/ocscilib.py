"""
This is plugin for all the plugins/hooks related to OCS-CI and its
configuration.

The basic configuration is done in run_ocsci.py module casue we need to load
all the config before pytest run. This run_ocsci.py is just a wrapper for
pytest which proccess config and passes all params to pytest.
"""
import logging
import os

import pytest

from ocs_ci.framework import config as ocsci_config
from ocs_ci.framework.exceptions import (
    ClusterNameLengthError,
    ClusterNameNotProvidedError,
    ClusterPathNotProvidedError
)
from ocs_ci.ocs.constants import (
    CLUSTER_NAME_MAX_CHARACTERS,
    CLUSTER_NAME_MIN_CHARACTERS,
    OCP_VERSION_CONF_DIR,
)
from ocs_ci.ocs.exceptions import CommandFailed, ResourceNotFoundError
from ocs_ci.ocs.resources.ocs import get_ocs_csv, get_version_info
from ocs_ci.ocs.utils import collect_ocs_logs, collect_prometheus_metrics
from ocs_ci.utility.utils import (
    dump_config_to_file, get_ceph_version, get_cluster_name,
    get_cluster_version, get_csi_versions, get_ocp_version,
    get_ocs_build_number, get_testrun_name, load_config_file
)

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
        action="append",
        help="Path to config file of OCS CI",
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
        '--live-deploy',
        dest='live_deploy',
        action="store_true",
        default=False,
        help="Deploy OCS from live registry like a customer",
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
        '--io-in-bg',
        dest='io_in_bg',
        action="store_true",
        default=False,
        help="Run IO in the background",
    )
    parser.addoption(
        '--io-load',
        dest='io_load',
        help="IOs throughput target percentage. Value should be between 0 to 100",
    )
    parser.addoption(
        '--log-cluster-utilization',
        dest='log_cluster_utilization',
        action="store_true",
        help="Enable logging of cluster utilization metrics every 10 seconds"
    )
    parser.addoption(
        '--ocs-version',
        dest='ocs_version',
        help="ocs version for which ocs-ci to be run"
    )
    parser.addoption(
        '--upgrade-ocs-version',
        dest='upgrade_ocs_version',
        help="ocs version to upgrade (e.g. 4.3)"
    )
    parser.addoption(
        '--ocp-version',
        dest='ocp_version',
        help="""
        OCP version to be used for deployment. This version will be used for
        load file from conf/ocp_version/ocp-VERSION-config.yaml. You can use
        for example those values:
        4.2: for nightly 4.2 OCP build
        4.2-ga: for latest GAed 4.2 OCP build
        4.2-ga-minus1: for latest GAed 4.2 build - 1
        """
    )
    parser.addoption(
        '--ocs-registry-image',
        dest='ocs_registry_image',
        help=(
            "ocs registry image to be used for deployment "
            "(e.g. quay.io/rhceph-dev/ocs-olm-operator:latest-4.2)"
        )
    )
    parser.addoption(
        '--upgrade-ocs-registry-image',
        dest='upgrade_ocs_registry_image',
        help=(
            "ocs registry image to be used for upgrade "
            "(e.g. quay.io/rhceph-dev/ocs-olm-operator:latest-4.3)"
        )
    )
    parser.addoption(
        '--osd-size',
        dest='osd_size',
        type=int,
        help="OSD size in GB - for 2TB pass 2048, for 0.5TB pass 512 and so on."
    )
    parser.addoption(
        '--flexy-env-file',
        dest='flexy_env_file',
        help="Path to flexy environment file"
    )
    parser.addoption(
        '--csv-change',
        dest='csv_change',
        help=(
            "Pattern or string to change in the CSV. Should contain the value to replace "
            "from and the value to replace to, separated by '::'"
        )
    )


def pytest_configure(config):
    """
    Load config files, and initialize ocs-ci library.

    Args:
        config (pytest.config): Pytest config object

    """
    # Somewhat hacky but this lets us differentiate between run-ci executions
    # and plain pytest unit test executions
    ocscilib_module = 'ocs_ci.framework.pytest_customization.ocscilib'
    if ocscilib_module not in config.getoption('-p'):
        return
    if not (config.getoption("--help") or config.getoption("collectonly")):
        process_cluster_cli_params(config)
        config_file = os.path.expanduser(
            os.path.join(
                ocsci_config.RUN['log_dir'],
                f"run-{ocsci_config.RUN['run_id']}-config.yaml",
            )
        )
        dump_config_to_file(config_file)
        log.info(
            f"Dump of the consolidated config file is located here: "
            f"{config_file}"
        )
        set_report_portal_tags(config)
        # Add OCS related versions to the html report and remove
        # extraneous metadata
        markers_arg = config.getoption('-m')
        if ocsci_config.RUN['cli_params'].get('teardown') or (
            "deployment" in markers_arg
            and ocsci_config.RUN['cli_params'].get('deploy')
        ):
            log.info(
                "Skipping versions collecting because: Deploy or destroy of "
                "cluster is performed."
            )
            return
        elif ocsci_config.ENV_DATA['skip_ocs_deployment']:
            log.info(
                "Skipping version collection because we skipped "
                "the OCS deployment"
            )
            return
        print("Collecting Cluster versions")
        # remove extraneous metadata
        del config._metadata['Python']
        del config._metadata['Packages']
        del config._metadata['Plugins']
        del config._metadata['Platform']

        config._metadata['Test Run Name'] = get_testrun_name()
        gather_version_info_for_report(config)

        ocs_csv = get_ocs_csv()
        ocs_csv_version = ocs_csv.data['spec']['version']
        config.addinivalue_line(
            "rp_launch_tags", f"ocs_csv_version:{ocs_csv_version}"
        )


def gather_version_info_for_report(config):
    """
    This function gather all version related info used for report.

    Args:
        config (pytest.config): Pytest config object
    """
    gather_version_completed = False
    try:
        # add cluster version
        clusterversion = get_cluster_version()
        config._metadata['Cluster Version'] = clusterversion

        # add ceph version
        ceph_version = get_ceph_version()
        config._metadata['Ceph Version'] = ceph_version

        # add csi versions
        csi_versions = get_csi_versions()
        config._metadata['cephfsplugin'] = csi_versions.get('csi-cephfsplugin')
        config._metadata['rbdplugin'] = csi_versions.get('csi-rbdplugin')

        # add ocs operator version
        if ocsci_config.REPORTING['us_ds'] == 'DS':
            config._metadata['OCS operator'] = (
                get_ocs_build_number()
            )
        mods = {}
        mods = get_version_info(
            namespace=ocsci_config.ENV_DATA['cluster_namespace']
        )
        skip_list = ['ocs-operator']
        for key, val in mods.items():
            if key not in skip_list:
                config._metadata[key] = val.rsplit('/')[-1]
        gather_version_completed = True
    except ResourceNotFoundError as ex:
        log.error(
            "Problem occurred when looking for some resource! Error: %s",
            ex
        )
    except FileNotFoundError as ex:
        log.error("File not found! Error: %s", ex)
    except CommandFailed as ex:
        log.error("Failed to execute command! Error: %s", ex)
    except Exception as ex:
        log.error("Failed to gather version info! Error: %s", ex)
    finally:
        if not gather_version_completed:
            log.warning(
                "Failed to gather version details! The report of version might"
                "not be complete!"
            )


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

    Raises:
        ClusterPathNotProvidedError: If a cluster path is missing
        ClusterNameNotProvidedError: If a cluster name is missing
        ClusterNameLengthError: If a cluster name is too short or too long
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
    ocsci_config.RUN['cli_params']['teardown'] = get_cli_param(config, "teardown", default=False)
    ocsci_config.RUN['cli_params']['deploy'] = get_cli_param(config, "deploy", default=False)
    live_deployment = get_cli_param(config, "live_deploy", default=False)
    ocsci_config.DEPLOYMENT['live_deployment'] = live_deployment or (
        ocsci_config.DEPLOYMENT.get('live_deployment', False)
    )
    io_in_bg = get_cli_param(config, 'io_in_bg')
    if io_in_bg:
        ocsci_config.RUN['io_in_bg'] = True
        io_load = get_cli_param(config, 'io_load')
        if io_load:
            ocsci_config.RUN['io_load'] = io_load
    log_utilization = get_cli_param(config, 'log_cluster_utilization')
    if log_utilization:
        ocsci_config.RUN['log_utilization'] = True
    upgrade_ocs_version = get_cli_param(config, "upgrade_ocs_version")
    if upgrade_ocs_version:
        ocsci_config.UPGRADE['upgrade_ocs_version'] = upgrade_ocs_version
    ocs_registry_image = get_cli_param(config, "ocs_registry_image")
    if ocs_registry_image:
        ocsci_config.DEPLOYMENT['ocs_registry_image'] = ocs_registry_image
    upgrade_ocs_registry_image = get_cli_param(config, "upgrade_ocs_registry_image")
    if upgrade_ocs_registry_image:
        ocsci_config.UPGRADE['upgrade_ocs_registry_image'] = upgrade_ocs_registry_image
    ocsci_config.ENV_DATA['cluster_name'] = cluster_name
    ocsci_config.ENV_DATA['cluster_path'] = cluster_path
    get_cli_param(config, 'collect-logs')
    if ocsci_config.RUN.get("cli_params").get("deploy"):
        if not cluster_name:
            raise ClusterNameNotProvidedError()
        if (
            len(cluster_name) < CLUSTER_NAME_MIN_CHARACTERS
            or len(cluster_name) > CLUSTER_NAME_MAX_CHARACTERS
        ):
            raise ClusterNameLengthError(cluster_name)
    elif not cluster_name:
        try:
            ocsci_config.ENV_DATA['cluster_name'] = get_cluster_name(
                cluster_path
            )
        except FileNotFoundError:
            raise ClusterNameNotProvidedError()
    if get_cli_param(config, 'email') and not get_cli_param(config, '--html'):
        pytest.exit("--html option must be provided to send email reports")
    get_cli_param(config, '-m')
    osd_size = get_cli_param(config, '--osd-size')
    if osd_size:
        ocsci_config.ENV_DATA['device_size'] = osd_size
    ocp_version = get_cli_param(config, '--ocp-version')
    if ocp_version:
        version_config_file = f"ocp-{ocp_version}-config.yaml"
        version_config_file_path = os.path.join(
            OCP_VERSION_CONF_DIR, version_config_file
        )
        load_config_file(version_config_file_path)
    csv_change = get_cli_param(config, '--csv-change')
    if csv_change:
        csv_change = csv_change.split("::")
        ocsci_config.DEPLOYMENT['csv_change_from'] = csv_change[0]
        ocsci_config.DEPLOYMENT['csv_change_to'] = csv_change[1]


def pytest_collection_modifyitems(session, config, items):
    """
    Add Polarion ID property to test cases that are marked with one.
    """
    for item in items:
        try:
            marker = item.get_closest_marker(name="polarion_id")
            if marker:
                polarion_id = marker.args[0]
                if polarion_id:
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
    if rep.failed and ocsci_config.RUN.get('cli_params').get('collect-logs'):
        test_case_name = item.name
        ocp_logs_collection = True if rep.when == "call" else False
        mcg = True if any(x in item.location[0] for x in ['mcg', 'ecosystem']) else False
        try:
            collect_ocs_logs(dir_name=test_case_name, ocp=ocp_logs_collection, mcg=mcg)
        except Exception:
            log.exception("Failed to collect OCS logs")

    # Collect Prometheus metrics if specified in gather_metrics_on_fail marker
    if (
        (rep.when == "setup" or rep.when == "call")
        and rep.failed
        and item.get_closest_marker('gather_metrics_on_fail')
    ):
        metrics = item.get_closest_marker('gather_metrics_on_fail').args
        try:
            collect_prometheus_metrics(
                metrics,
                f'{item.name}-{call.when}',
                call.start,
                call.stop
            )
        except Exception:
            log.exception("Failed to collect prometheus metrics")

    # Get the performance metrics when tests fails for scale or performance tag
    from tests.helpers import collect_performance_stats
    if (
        (rep.when == "setup" or rep.when == "call")
        and rep.failed
        and (item.get_closest_marker('scale') or item.get_closest_marker('performance'))
    ):
        test_case_name = item.name
        try:
            collect_performance_stats(test_case_name)
        except Exception:
            log.exception("Failed to collect performance stats")


def set_report_portal_tags(config):
    rp_tags = list()
    rp_tags.append(ocsci_config.ENV_DATA.get('platform'))
    rp_tags.append(ocsci_config.ENV_DATA.get('deployment_type'))
    if ocsci_config.REPORTING.get('us_ds') == 'us':
        rp_tags.append('upstream')
    else:
        rp_tags.append('downstream')
    worker_instance_type = ocsci_config.ENV_DATA.get('worker_instance_type')
    rp_tags.append(f"worker_instance_type:{worker_instance_type}")
    rp_tags.append(f"ocp_version:{get_ocp_version()}")
    rp_tags.append(
        f"ocs_version:{ocsci_config.ENV_DATA.get('ocs_version')}"
    )
    if ocsci_config.DEPLOYMENT.get('ocs_registry_image'):
        ocs_registry_image = ocsci_config.DEPLOYMENT.get('ocs_registry_image')
        rp_tags.append(f"ocs_registry_image:{ocs_registry_image}")
        rp_tags.append(f"ocs_registry_tag:{ocs_registry_image.split(':')[1]}")
    if ocsci_config.DEPLOYMENT.get('ui_deployment'):
        rp_tags.append('ui_deployment')
    if ocsci_config.DEPLOYMENT.get('live_deployment'):
        rp_tags.append('live_deployment')
    if ocsci_config.DEPLOYMENT.get('stage'):
        rp_tags.append('stage_deployment')
    if not ocsci_config.DEPLOYMENT.get('allow_lower_instance_requirements'):
        rp_tags.append("production")
    if ocsci_config.ENV_DATA.get('fips'):
        rp_tags.append("fips")

    for tag in rp_tags:
        if tag:
            config.addinivalue_line("rp_launch_tags", tag.lower())
