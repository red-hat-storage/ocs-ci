"""
This is plugin for all the plugins/hooks related to OCS-CI and its
configuration.

The basic configuration is done in run_ocsci.py module casue we need to load
all the config before pytest run. This run_ocsci.py is just a wrapper for
pytest which proccess config and passes all params to pytest.
"""
import logging
import os
import pandas as pd
import pytest
from junitparser import JUnitXml
import ocs_ci.utility.memory
from ocs_ci.framework import config as ocsci_config
from ocs_ci.framework.logger_factory import set_log_record_factory
from ocs_ci.framework.exceptions import (
    ClusterNameLengthError,
    ClusterNameNotProvidedError,
    ClusterPathNotProvidedError,
)
from ocs_ci.ocs.constants import (
    CLUSTER_NAME_MAX_CHARACTERS,
    CLUSTER_NAME_MIN_CHARACTERS,
    OCP_VERSION_CONF_DIR,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceNotFoundError,
)
from ocs_ci.ocs.cluster import check_clusters
from ocs_ci.ocs.resources.ocs import get_version_info
from ocs_ci.ocs.utils import collect_ocs_logs, collect_prometheus_metrics
from ocs_ci.utility.utils import (
    dump_config_to_file,
    get_ceph_version,
    get_cluster_name,
    get_cluster_version,
    get_csi_versions,
    get_ocs_build_number,
    get_testrun_name,
    load_config_file,
    create_stats_dir,
)

from ocs_ci.utility.memory import (
    get_consumed_ram,
    start_monitor_memory,
    stop_monitor_memory,
    get_peak_sum_mem,
)
from ocs_ci.ocs import constants
from psutil._common import bytes2human

__all__ = [
    "pytest_addoption",
]

current_factory = logging.getLogRecordFactory()
log = logging.getLogger(__name__)


def _pytest_addoption_cluster_specific(parser):
    """
    Handle multicluster options here
    We will add options which will have suffix number same as its cluster number
    i.e `--cluster1 --cluster-name xyz --cluster-path /a/b/c --ocsci-conf /path/to/c1
    --ocsci-conf /path/to/c2...` will get translated to `--cluster1 --cluster-name1 xyz
    --cluster-path1 /a/b/c --ocsci-conf1 /path/to/c1 --ocsci-conf1 /path/to/c2`

    Rest of the general run-ci options will be handled by pytest_addoption

    """

    add_common_ocsci_conf = False

    for i in range(ocsci_config.nclusters):
        # If it's not multicluster then no suffix will be added
        suffix = i + 1 if ocsci_config.multicluster else ""
        if not add_common_ocsci_conf and ocsci_config.multicluster:
            parser.addoption(
                "--ocsci-conf",
                dest="ocsci_conf",
                action="append",
                help="Path to config file of OCS CI",
            )
            add_common_ocsci_conf = True

        parser.addoption(
            f"--ocsci-conf{suffix}",
            dest=f"ocsci_conf{suffix}",
            action="append",
            help="Path to config file of OCS CI",
        )
        parser.addoption(
            f"--cluster-path{suffix}",
            dest=f"cluster_path{suffix}",
            help="Path to cluster directory",
        )
        parser.addoption(
            f"--cluster-name{suffix}",
            dest=f"cluster_name{suffix}",
            help="Name of cluster",
        )
        parser.addoption(
            f"--ocs-version{suffix}",
            dest=f"ocs_version{suffix}",
            help="ocs version for which ocs-ci to be run",
        )
        parser.addoption(
            f"--ocp-version{suffix}",
            dest=f"ocp_version{suffix}",
            help="""
             OCP version to be used for deployment. This version will be used for
             load file from conf/ocp_version/ocp-VERSION-config.yaml. You can use
             for example those values:
             4.2: for nightly 4.2 OCP build
             4.2-ga: for latest GAed 4.2 OCP build
             4.2-ga-minus1: for latest GAed 4.2 build - 1
             """,
        )
        parser.addoption(
            f"--ocs-registry-image{suffix}",
            dest=f"ocs_registry_image{suffix}",
            help=(
                "ocs registry image to be used for deployment "
                "(e.g. quay.io/rhceph-dev/ocs-olm-operator:latest-4.2)"
            ),
        )
        parser.addoption(
            f"--osd-size{suffix}",
            dest=f"osd_size{suffix}",
            type=int,
            help="OSD size in GB - for 2TB pass 2048, for 0.5TB pass 512 and so on.",
        )
        parser.addoption(
            f"--lvmo-disks{suffix}",
            dest=f"lvmo_disks{suffix}",
            type=int,
            help="Number of disks to add to node for LVMO",
        )
        parser.addoption(
            f"--lvmo-disks-size{suffix}",
            dest=f"lvmo_disks_size{suffix}",
            type=int,
            help="Size of the disks to add to lvmo in GB",
        )


def pytest_addoption(parser):
    """
    Add necessary options to initialize OCS CI library.
    """
    # Handle only cluster specific options from the below call
    # Rest of the options which are general, will be handled here itself
    _pytest_addoption_cluster_specific(parser)

    parser.addoption(
        "--teardown",
        dest="teardown",
        action="store_true",
        default=False,
        help="If provided the test cluster will be destroyed after tests complete",
    )
    parser.addoption(
        "--deploy",
        dest="deploy",
        action="store_true",
        default=False,
        help="If provided a test cluster will be deployed on AWS to use for testing",
    )
    parser.addoption(
        "--live-deploy",
        dest="live_deploy",
        action="store_true",
        default=False,
        help="Deploy OCS from live registry like a customer",
    )
    parser.addoption(
        "--email",
        dest="email",
        help="Email ID to send results",
    )
    parser.addoption(
        "--squad-analysis",
        dest="squad_analysis",
        action="store_true",
        default=False,
        help="Include Squad Analysis to email report.",
    )
    parser.addoption(
        "--collect-logs",
        dest="collect-logs",
        action="store_true",
        default=False,
        help="Collect OCS logs when test case failed",
    )
    parser.addoption(
        "--collect-logs-on-success-run",
        dest="collect_logs_on_success_run",
        action="store_true",
        default=False,
        help="Collect must gather logs at the end of the execution (also when no failure in the tests)",
    )
    parser.addoption(
        "--io-in-bg",
        dest="io_in_bg",
        action="store_true",
        default=False,
        help="Run IO in the background",
    )
    parser.addoption(
        "--io-load",
        dest="io_load",
        help="IOs throughput target percentage. Value should be between 0 to 100",
    )
    parser.addoption(
        "--log-cluster-utilization",
        dest="log_cluster_utilization",
        action="store_true",
        help="Enable logging of cluster utilization metrics every 10 seconds",
    )
    parser.addoption(
        "--upgrade-ocs-version",
        dest="upgrade_ocs_version",
        help="ocs version to upgrade (e.g. 4.3)",
    )
    parser.addoption(
        "--upgrade-ocp-version",
        dest="upgrade_ocp_version",
        help="""
        OCP version to upgrade to. This version will be used to
        load file from conf/ocp_version/ocp-VERSION-config.yaml.
        For example:
        4.5 (for nightly 4.5 OCP build)
        4.5-ga (for latest GAed 4.5 OCP build)
        """,
    )
    parser.addoption(
        "--upgrade-ocp-image",
        dest="upgrade_ocp_image",
        help="""
        OCP image to upgrade to. This image string will be split on ':' to
        determine the image source and the specified tag to use.
        (e.g. quay.io/openshift-release-dev/ocp-release:4.6.0-x86_64)
        """,
    )
    parser.addoption(
        "--ocp-installer-version",
        dest="ocp_installer_version",
        help="""
        Specific OCP installer version to be used for deployment. This option
        will generally be used for non-GA or nightly builds. (e.g. 4.5.5).
        This option will overwrite any values set via --ocp-version.
        """,
    )
    parser.addoption(
        "--upgrade-ocs-registry-image",
        dest="upgrade_ocs_registry_image",
        help=(
            "ocs registry image to be used for upgrade "
            "(e.g. quay.io/rhceph-dev/ocs-olm-operator:latest-4.3)"
        ),
    )
    parser.addoption(
        "--flexy-env-file", dest="flexy_env_file", help="Path to flexy environment file"
    )
    parser.addoption(
        "--csv-change",
        dest="csv_change",
        help=(
            "Pattern or string to change in the CSV. Should contain the value to replace "
            "from and the value to replace to, separated by '::'"
        ),
    )
    parser.addoption(
        "--dev-mode",
        dest="dev_mode",
        action="store_true",
        default=False,
        help=(
            "Runs in development mode. It skips few checks like collecting "
            "versions, collecting logs, etc"
        ),
    )
    parser.addoption(
        "--ceph-debug",
        dest="ceph_debug",
        action="store_true",
        default=False,
        help=(
            "For OCS cluster deployment with Ceph configured in debug mode. Available for OCS 4.7 and above"
        ),
    )
    parser.addoption(
        "--skip-download-client",
        dest="skip_download_client",
        action="store_true",
        default=False,
        help="Skip the openshift client download step or not",
    )
    parser.addoption(
        "--disable-components",
        dest="disable_components",
        action="append",
        choices=["rgw", "cephfs", "noobaa", "blockpools"],
        help=("disable deployment of ocs component:rgw, cephfs, noobaa, blockpools."),
    )
    parser.addoption(
        "--re-trigger-failed-tests",
        dest="re_trigger_failed_tests",
        help="""
        Path to the xunit file for xml junit report from the previous execution.
        If the file is provided, the execution will remove all the test cases
        which passed and will run only those test cases which were skipped /
        failed / or had error in the provided report.
        """,
    )
    parser.addoption(
        "--default-cluster-context-index",
        dest="default_cluster_context_index",
        default=0,
        help="""
        Sets the default index of the cluster whose context needs to be
        loaded when run-ci starts
        """,
    )
    parser.addoption(
        "--install-lvmo",
        dest="install_lvmo",
        action="store_true",
        default=False,
        help="""
            set for installing lvmo operator and lvmo cluster
            """,
    )
    parser.addoption(
        "--disable-environment-checker",
        dest="disable_environment_checker",
        action="store_true",
        default=False,
        help="Disable environment checks for test cases.",
    )
    parser.addoption(
        "--resource-checker",
        dest="resource_checker",
        action="store_true",
        default=False,
        help=(
            "Resource checker checks for the left over resources which are added while running the test cases."
        ),
    )
    parser.addoption(
        "--kubeconfig",
        dest="kubeconfig",
        help=("Kubeconfig location which will be loaded as environmental variable"),
    )


def pytest_configure(config):
    """
    Load config files, and initialize ocs-ci library.

    Args:
        config (pytest.config): Pytest config object

    """
    set_log_level(config)
    # Set the new factory for the logging of pytest
    set_log_record_factory()
    # Somewhat hacky but this lets us differentiate between run-ci executions
    # and plain pytest unit test executions
    ocscilib_module = "ocs_ci.framework.pytest_customization.ocscilib"
    if ocscilib_module not in config.getoption("-p"):
        return
    for i in range(ocsci_config.nclusters):
        log.info(f"Pytest configure switching to: cluster={i}")
        ocsci_config.switch_ctx(i)

        if not (config.getoption("--help") or config.getoption("collectonly")):
            process_cluster_cli_params(config)
            config_file = os.path.expanduser(
                os.path.join(
                    ocsci_config.RUN["log_dir"],
                    f"run-{ocsci_config.RUN['run_id']}-cl{i}-config.yaml",
                )
            )
            if ocsci_config.DEPLOYMENT.get("multi_storagecluster"):
                ocsci_config.DEPLOYMENT["external_mode"] = False
                ocsci_config.ENV_DATA[
                    "storage_cluster_name"
                ] = constants.DEFAULT_STORAGE_CLUSTER
            dump_config_to_file(config_file)
            log.info(
                f"Dump of the consolidated config file is located here: "
                f"{config_file}"
            )

            # Add OCS related versions to the html report and remove
            # extraneous metadata
            markers_arg = config.getoption("-m")
            # add logs url
            logs_url = ocsci_config.RUN.get("logs_url")
            if logs_url:
                config._metadata["Logs URL"] = logs_url

            if ocsci_config.RUN["cli_params"].get("teardown") or (
                "deployment" in markers_arg
                and ocsci_config.RUN["cli_params"].get("deploy")
            ):
                log.info(
                    "Skipping versions collecting because: Deploy or destroy of "
                    "cluster is performed."
                )
                continue
            elif markers_arg == "acm_import":
                log.info(
                    "Skipping auto pytest executions and version collecting because "
                    "Import Clusters to ACM is performed."
                )
                continue
            elif ocsci_config.ENV_DATA["skip_ocs_deployment"]:
                log.info(
                    "Skipping version collection because we skipped "
                    "the OCS deployment"
                )
                continue
            elif ocsci_config.RUN["cli_params"].get("dev_mode"):
                log.info("Running in development mode")
                continue
            # remove extraneous metadata
            for extra_meta in ["Python", "Packages", "Plugins", "Platform"]:
                if config._metadata.get(extra_meta):
                    del config._metadata[extra_meta]

            config._metadata["Test Run Name"] = get_testrun_name()
            check_clusters()
            if ocsci_config.RUN.get("cephcluster"):
                gather_version_info_for_report(config)
    # switch the configuration context back to the default cluster
    ocsci_config.switch_default_cluster_ctx()


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
        config._metadata["Cluster Version"] = clusterversion

        # add ceph version
        if not ocsci_config.ENV_DATA["mcg_only_deployment"]:
            ceph_version = get_ceph_version()
            config._metadata["Ceph Version"] = ceph_version

            # add csi versions
            csi_versions = get_csi_versions()
            config._metadata["cephfsplugin"] = csi_versions.get("csi-cephfsplugin")
            config._metadata["rbdplugin"] = csi_versions.get("csi-rbdplugin")

        # add ocs operator version
        config._metadata["OCS operator"] = get_ocs_build_number()
        mods = {}
        mods = get_version_info(namespace=ocsci_config.ENV_DATA["cluster_namespace"])
        skip_list = ["ocs-operator"]
        for key, val in mods.items():
            if key not in skip_list:
                config._metadata[key] = val.rsplit("/")[-1]
        gather_version_completed = True
    except ResourceNotFoundError:
        log.exception("Problem occurred when looking for some resource!")
    except FileNotFoundError:
        log.exception("File not found!")
    except CommandFailed:
        log.exception("Failed to execute command!")
    except Exception:
        log.exception("Failed to gather version info!")
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
    ocsci_config.RUN["cli_params"][name_of_param] = cli_param
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
    suffix = ocsci_config.cur_index + 1 if ocsci_config.multicluster else ""
    cluster_path = get_cli_param(config, f"cluster_path{suffix}")
    if not cluster_path:
        raise ClusterPathNotProvidedError()
    cluster_path = os.path.expanduser(cluster_path)
    if not os.path.exists(cluster_path):
        os.makedirs(cluster_path)
    # Importing here cause once the function is invoked we have already config
    # loaded, so this is OK to import once you sure that config is loaded.
    from ocs_ci.ocs.openshift_ops import OCP

    OCP.set_kubeconfig(
        os.path.join(cluster_path, ocsci_config.RUN["kubeconfig_location"])
    )
    ocsci_config.RUN.update(
        {
            "kubeconfig": os.path.join(
                cluster_path, ocsci_config.RUN["kubeconfig_location"]
            )
        }
    )

    cluster_name = get_cli_param(config, f"cluster_name{suffix}")
    ocsci_config.RUN["cli_params"]["teardown"] = get_cli_param(
        config, "teardown", default=False
    )
    ocsci_config.RUN["cli_params"]["deploy"] = get_cli_param(
        config, "deploy", default=False
    )
    live_deployment = get_cli_param(
        config, "live_deploy", default=False
    ) or ocsci_config.DEPLOYMENT.get("live_deployment", False)
    ocsci_config.DEPLOYMENT["live_deployment"] = live_deployment

    io_in_bg = get_cli_param(config, "io_in_bg")
    if io_in_bg:
        ocsci_config.RUN["io_in_bg"] = True
        io_load = get_cli_param(config, "io_load")
        if io_load:
            ocsci_config.RUN["io_load"] = io_load
    log_utilization = get_cli_param(config, "log_cluster_utilization")
    if log_utilization:
        ocsci_config.RUN["log_utilization"] = True
    upgrade_ocs_version = get_cli_param(config, "upgrade_ocs_version")
    if upgrade_ocs_version:
        ocsci_config.UPGRADE["upgrade_ocs_version"] = upgrade_ocs_version
    ocs_registry_image = get_cli_param(config, f"ocs_registry_image{suffix}")
    if ocs_registry_image:
        ocsci_config.DEPLOYMENT["ocs_registry_image"] = ocs_registry_image
    install_lvmo = get_cli_param(config, "install_lvmo")
    if install_lvmo:
        ocsci_config.DEPLOYMENT["install_lvmo"] = True
        number_of_disks = get_cli_param(config, "lvmo_disks")
        if number_of_disks is None:
            ocsci_config.DEPLOYMENT["lvmo_disks"] = 3
        else:
            ocsci_config.DEPLOYMENT["lvmo_disks"] = number_of_disks
        disk_size = get_cli_param(config, "lvmo_disks_size")
        if disk_size is None:
            ocsci_config.DEPLOYMENT["lvmo_disks_size"] = 200
        else:
            ocsci_config.DEPLOYMENT["lvmo_disks_size"] = disk_size
    upgrade_ocs_registry_image = get_cli_param(config, "upgrade_ocs_registry_image")
    if upgrade_ocs_registry_image:
        ocsci_config.UPGRADE["upgrade_ocs_registry_image"] = upgrade_ocs_registry_image
    ocsci_config.ENV_DATA["cluster_name"] = cluster_name
    ocsci_config.ENV_DATA["cluster_path"] = cluster_path
    get_cli_param(config, "collect-logs")
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
            ocsci_config.ENV_DATA["cluster_name"] = get_cluster_name(cluster_path)
        except FileNotFoundError:
            raise ClusterNameNotProvidedError()
    if get_cli_param(config, "email") and not get_cli_param(config, "--html"):
        pytest.exit("--html option must be provided to send email reports")
    get_cli_param(config, "squad_analysis")
    get_cli_param(config, "-m")
    osd_size = get_cli_param(config, f"--osd-size{suffix}")
    if osd_size:
        ocsci_config.ENV_DATA["device_size"] = osd_size
    ocp_version = get_cli_param(config, "--ocp-version")
    if ocp_version:
        version_config_file = f"ocp-{ocp_version}-config.yaml"
        version_config_file_path = os.path.join(
            OCP_VERSION_CONF_DIR, version_config_file
        )
        load_config_file(version_config_file_path)
    upgrade_ocp_version = get_cli_param(config, "--upgrade-ocp-version")
    if upgrade_ocp_version:
        version_config_file = f"ocp-{upgrade_ocp_version}-upgrade.yaml"
        version_config_file_path = os.path.join(
            OCP_VERSION_CONF_DIR, version_config_file
        )
        load_config_file(version_config_file_path)
    upgrade_ocp_image = get_cli_param(config, "--upgrade-ocp-image")
    if upgrade_ocp_image:
        ocp_image = upgrade_ocp_image.rsplit(":", 1)
        ocsci_config.UPGRADE["ocp_upgrade_path"] = ocp_image[0]
        ocsci_config.UPGRADE["ocp_upgrade_version"] = ocp_image[1]
    ocp_installer_version = get_cli_param(config, "--ocp-installer-version")
    if ocp_installer_version:
        ocsci_config.DEPLOYMENT["installer_version"] = ocp_installer_version
        ocsci_config.RUN["client_version"] = ocp_installer_version
    csv_change = get_cli_param(config, "--csv-change")
    if csv_change:
        csv_change = csv_change.split("::")
        ocsci_config.DEPLOYMENT["csv_change_from"] = csv_change[0]
        ocsci_config.DEPLOYMENT["csv_change_to"] = csv_change[1]
    collect_logs_on_success_run = get_cli_param(config, "collect_logs_on_success_run")
    if collect_logs_on_success_run:
        ocsci_config.REPORTING["collect_logs_on_success_run"] = True
    get_cli_param(config, "dev_mode")
    ceph_debug = get_cli_param(config, "ceph_debug")
    if ceph_debug:
        ocsci_config.DEPLOYMENT["ceph_debug"] = True
    skip_download_client = get_cli_param(config, "skip_download_client")
    if skip_download_client:
        ocsci_config.DEPLOYMENT["skip_download_client"] = True
    re_trigger_failed_tests = get_cli_param(config, "--re-trigger-failed-tests")
    if re_trigger_failed_tests:
        ocsci_config.RUN["re_trigger_failed_tests"] = os.path.expanduser(
            re_trigger_failed_tests
        )
    disable_environment_checker = get_cli_param(config, "disable_environment_checker")
    ocsci_config.RUN["disable_environment_checker"] = disable_environment_checker
    resource_checker = get_cli_param(config, "resource_checker")
    ocsci_config.RUN["resource_checker"] = resource_checker
    custom_kubeconfig_location = get_cli_param(config, "kubeconfig")
    ocsci_config.RUN["custom_kubeconfig_location"] = custom_kubeconfig_location
    if custom_kubeconfig_location:
        os.environ["KUBECONFIG"] = custom_kubeconfig_location
        ocsci_config.RUN["kubeconfig"] = custom_kubeconfig_location


def pytest_collection_modifyitems(session, config, items):
    """
    Add Polarion ID property to test cases that are marked with one.
    """

    re_trigger_failed_tests = ocsci_config.RUN.get("re_trigger_failed_tests")
    if re_trigger_failed_tests:
        junit_report = JUnitXml.fromfile(re_trigger_failed_tests)
        cases_to_re_trigger = []
        for suite in junit_report:
            cases_to_re_trigger += [_case.name for _case in suite if _case.result]
    for item in items[:]:
        if re_trigger_failed_tests and item.name not in cases_to_re_trigger:
            log.info(
                f"Test case: {item.name} will be removed from execution, "
                "because of you provided --re-trigger-failed-tests parameter "
                "and this test passed in previous execution from the report!"
            )
            items.remove(item)
        try:
            marker = item.get_closest_marker(name="polarion_id")
            if marker:
                polarion_id = marker.args[0]
                if polarion_id:
                    item.user_properties.append(("polarion-testcase-id", polarion_id))
        except IndexError:
            log.warning(
                f"polarion_id marker found with no value for "
                f"{item.name} in {item.fspath}",
                exc_info=True,
            )


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    # we only look at actual failing test calls, not setup/teardown
    # Don't collect must-gather for deployment here since its already
    # handled in deployment
    if (
        rep.failed
        and ocsci_config.RUN.get("cli_params").get("collect-logs")
        and not ocsci_config.RUN.get("cli_params").get("deploy")
    ):
        test_case_name = item.name
        ocp_logs_collection = (
            True
            if any(
                x in item.location[0]
                for x in [
                    "ecosystem",
                    "e2e/performance",
                    "tests/functional/z_cluster",
                ]
            )
            else False
        )
        ocs_logs_collection = (
            False
            if any(x in item.location[0] for x in ["_ui", "must_gather"])
            else True
        )
        mcg_logs_collection = (
            True if any(x in item.location[0] for x in ["mcg", "ecosystem"]) else False
        )
        try:
            if not ocsci_config.RUN.get("is_ocp_deployment_failed"):
                collect_ocs_logs(
                    dir_name=test_case_name,
                    ocp=ocp_logs_collection,
                    ocs=ocs_logs_collection,
                    mcg=mcg_logs_collection,
                )
        except Exception:
            log.exception("Failed to collect OCS logs")

    # Collect Prometheus metrics if specified in gather_metrics_on_fail marker
    if (
        (rep.when == "setup" or rep.when == "call")
        and rep.failed
        and item.get_closest_marker("gather_metrics_on_fail")
    ):
        metrics = item.get_closest_marker("gather_metrics_on_fail").args
        try:
            threading_lock = call.getfixturevalue("threading_lock")
            collect_prometheus_metrics(
                metrics,
                f"{item.name}-{call.when}",
                call.start,
                call.stop,
                threading_lock=threading_lock,
            )
        except Exception:
            log.exception("Failed to collect prometheus metrics")

    # Get the performance metrics when tests fails for scale or performance tag
    from ocs_ci.helpers.helpers import collect_performance_stats

    if (
        (rep.when == "setup" or rep.when == "call")
        and rep.failed
        and (item.get_closest_marker("scale") or item.get_closest_marker("performance"))
    ):
        test_case_name = item.name
        try:
            collect_performance_stats(test_case_name)
        except Exception:
            log.exception("Failed to collect performance stats")

    if rep.failed:
        test_name = item.nodeid
        # Write the failure information to a file
        with open(
            f'{ocsci_config.ENV_DATA.get("cluster_path")}/failed_testcases.txt', "a"
        ) as file:
            file.write(f"{test_name}\n")

    if rep.passed and rep.when == "call":
        test_name = item.nodeid
        # Write the passed information to a file
        with open(
            f'{ocsci_config.ENV_DATA.get("cluster_path")}/passed_testcases.txt', "a"
        ) as file:
            file.write(f"{test_name}\n")

    if rep.skipped:
        test_name = item.nodeid
        # Write the skipped information to a file
        with open(
            f'{ocsci_config.ENV_DATA.get("cluster_path")}/skipped_testcases.txt', "a"
        ) as file:
            file.write(f"{test_name}\n")


def set_log_level(config):
    """
    Set the log level of this module based on the pytest.ini log_cli_level

    Args:
        config (pytest.config): Pytest config object

    """
    level = config.getini("log_cli_level") or "INFO"
    log.setLevel(logging.getLevelName(level))


global consumed_ram_start_test


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    try:
        start_monitor_memory()

        global consumed_ram_start_test
        consumed_ram_start_test = get_consumed_ram()

        log.debug(
            f"Consumed memory at the start of TC {item.nodeid}: {bytes2human(consumed_ram_start_test)}"
        )
    except Exception:
        log.exception("Got exception while start to monitor memory")


@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item):
    try:
        _, peak_rss_table, peak_vms_table = stop_monitor_memory(save_csv=False)
        log.info(
            f"Peak rss consumers during {item.nodeid}:\n"
            f"{peak_rss_table.to_markdown(headers='keys', index=False, tablefmt='grid')}"
        )
        log.info(
            f"Peak vms consumers during {item.nodeid}:\n"
            f"{peak_vms_table.to_markdown(headers='keys', index=False, tablefmt='grid')}"
        )

        if ocsci_config.REPORTING.get("save_mem_report"):
            stats_log_dir = create_stats_dir()

            peak_rss_file_detailed = os.path.join(
                stats_log_dir, f"{item.name}.peak_rss_table"
            )
            peak_vms_file_detailed = os.path.join(
                stats_log_dir, f"{item.name}.peak_vms_table"
            )
            peak_rss_table.to_csv(peak_rss_file_detailed)
            log.info(f"peak_rss_table saved to {peak_rss_file_detailed}")
            peak_vms_table.to_csv(peak_vms_file_detailed)
            log.info(f"peak_vms_table saved to {peak_vms_file_detailed}")

        global consumed_ram_start_test
        leaked_ram = consumed_ram_start_test - get_consumed_ram()
        # rss of the process is drastically changes each measurement, meaning that
        # processes may be closed immediately after measurement causing no leakage.
        # leaked_ram intentionally left without check on negative digit to record
        # gain of free memory
        if leaked_ram > 0:
            log.info(f"Leak RAM at the end of the test: {bytes2human(leaked_ram)}")
        else:
            log.info(
                f"Free RAM gain at the end of the test: {bytes2human(leaked_ram * -1)}"
            )

        df_ram_max, df_virt_max = get_peak_sum_mem()

        report_columns = [
            "TC name",
            "Peak total RAM consumed",
            "Peak total VMS consumed",
            "RAM leak",
        ]
        if "memory" not in ocsci_config.RUN:
            ocsci_config.RUN["memory"] = pd.DataFrame(data=[], columns=report_columns)
            ocsci_config.RUN["memory"].reset_index(drop=True, inplace=True)

        df_report_line = pd.DataFrame(
            [
                [
                    item.nodeid,
                    df_ram_max[constants.RAM].values[0],
                    df_virt_max[constants.VIRT].values[0],
                    leaked_ram,
                ]
            ],
            columns=report_columns,
        )
        df_report_line.reset_index(drop=True, inplace=True)
        ocsci_config.RUN["memory"] = pd.concat(
            [
                ocsci_config.RUN["memory"],
                df_report_line,
            ]
        )

        log.debug("Report Memory performance report: ")
        log.debug(
            "\n".join(
                ocsci_config.RUN["memory"].to_markdown(
                    headers="keys", index=False, tablefmt="grid"
                )
            )
        )
    except Exception:
        log.exception("Got exception while stop to monitor memory")
    finally:
        if hasattr(ocs_ci.utility.memory, "mon"):
            mon = ocs_ci.utility.memory.mon
            if mon and mon.is_alive():
                mon.cancel()
