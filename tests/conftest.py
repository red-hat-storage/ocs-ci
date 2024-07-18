import base64
import copy
import logging
import os
import pandas as pd
import random
import time
import tempfile
import threading
import json
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from math import floor
from shutil import copyfile, rmtree
from functools import partial

import boto3
import yaml
from botocore.exceptions import ClientError
import pytest
from collections import namedtuple

from ocs_ci.deployment import factory as dep_factory
from ocs_ci.deployment.helpers.hypershift_base import HyperShiftBase
from ocs_ci.deployment.hosted_cluster import HostedClients
from ocs_ci.framework import config as ocsci_config, Config
import ocs_ci.framework.pytest_customization.marks
from ocs_ci.framework.pytest_customization.marks import (
    deployment,
    ignore_leftovers,
    tier_marks,
    ignore_leftover_label,
    upgrade_marks,
    ignore_resource_not_found_error_label,
)
from ocs_ci.helpers.proxy import update_container_with_proxy_env
from ocs_ci.ocs import constants, defaults, fio_artefacts, node, ocp, platform_nodes
from ocs_ci.ocs.acm.acm import login_to_acm
from ocs_ci.ocs.awscli_pod import create_awscli_pod, awscli_pod_cleanup
from ocs_ci.ocs.benchmark_operator_fio import get_file_size, BenchmarkOperatorFIO
from ocs_ci.ocs.bucket_utils import (
    craft_s3_command,
    put_bucket_policy,
)
from ocs_ci.ocs.constants import FUSION_CONF_DIR
from ocs_ci.ocs.dr.dr_workload import BusyBox, BusyBox_AppSet, CnvWorkload
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    CephHealthException,
    ResourceWrongStatusException,
    UnsupportedPlatformError,
    PoolDidNotReachReadyState,
    StorageclassNotCreated,
    PoolNotDeletedFromUI,
    StorageClassNotDeletedFromUI,
    ResourceNotDeleted,
    MissingDecoratorError,
)
from ocs_ci.ocs.mcg_workload import mcg_job_factory as mcg_job_factory_implementation
from ocs_ci.ocs.node import get_node_objs, schedule_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.resources.bucket_policy import gen_bucket_policy
from ocs_ci.ocs.resources.mcg_replication_policy import AwsLogBasedReplicationPolicy
from ocs_ci.ocs.resources.mockup_bucket_logger import MockupBucketLogger
from ocs_ci.ocs.scale_lib import FioPodScale
from ocs_ci.ocs.utils import (
    setup_ceph_toolbox,
    collect_ocs_logs,
    collect_pod_container_rpm_package,
    get_dr_operator_versions,
)
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.ocs.resources.job import get_job_obj
from ocs_ci.ocs.resources.backingstore import (
    backingstore_factory as backingstore_factory_implementation,
    clone_bs_dict_from_backingstore,
)
from ocs_ci.ocs.cluster import check_clusters
from ocs_ci.ocs.resources.namespacestore import (
    namespace_store_factory as namespacestore_factory_implementation,
)
from ocs_ci.ocs.resources.bucketclass import (
    bucket_class_factory as bucketclass_factory_implementation,
)
from ocs_ci.ocs.resources.cloud_manager import CloudManager
from ocs_ci.ocs.resources.cloud_uls import (
    cloud_uls_factory as cloud_uls_factory_implementation,
)
from ocs_ci.ocs.node import check_nodes_specs
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.objectbucket import BUCKET_MAP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import (
    get_rgw_pods,
    delete_deploymentconfig_pods,
    get_pods_having_label,
    get_deployments_having_label,
    Pod,
    wait_for_pods_to_be_running,
    get_ceph_tools_pod,
    get_all_pods,
    verify_data_integrity_for_multi_pvc_objs,
    get_noobaa_pods,
    get_pod_count,
    wait_for_pods_by_label_count,
)
from ocs_ci.ocs.resources.pvc import PVC, create_restore_pvc, get_all_pvc_objs
from ocs_ci.ocs.version import get_ocs_version, get_ocp_version_dict, report_ocs_version
from ocs_ci.ocs.cluster_load import ClusterLoad, wrap_msg
from ocs_ci.utility import (
    aws,
    deployment_openshift_logging as ocp_logging_obj,
    ibmcloud,
    kms as KMS,
    pagerduty,
    reporting,
    templating,
    users,
    version,
)
from ocs_ci.utility.environment_check import (
    get_status_before_execution,
    get_status_after_execution,
)
from ocs_ci.utility.json import SetToListJSONEncoder
from ocs_ci.utility.resource_check import (
    create_resource_dct,
    get_environment_status_after_execution,
)
from ocs_ci.utility.flexy import load_cluster_info
from ocs_ci.utility.kms import is_kms_enabled, get_ksctl_cli
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.reporting import update_live_must_gather_image
from ocs_ci.utility.retry import retry
from ocs_ci.utility.uninstall_openshift_logging import uninstall_cluster_logging
from ocs_ci.utility.utils import (
    ceph_health_check,
    get_default_if_keyval_empty,
    get_ocs_build_number,
    get_openshift_client,
    get_random_str,
    get_testrun_name,
    load_auth_config,
    ocsci_log_path,
    skipif_ocp_version,
    skipif_ocs_version,
    TimeoutSampler,
    skipif_upgraded_from,
    update_container_with_mirrored_image,
    skipif_ui_not_support,
    run_cmd,
    ceph_health_check_multi_storagecluster_external,
    clone_repo,
)
from ocs_ci.helpers import helpers, dr_helpers
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    create_ocs_object_from_kind_and_name,
    setup_pod_directories,
    get_current_test_name,
    modify_deployment_replica_count,
    modify_statefulset_replica_count,
)
from ocs_ci.ocs.ceph_debug import CephObjectStoreTool, MonStoreTool, RookCephPlugin
from ocs_ci.ocs.bucket_utils import get_rgw_restart_counts
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.elasticsearch import ElasticSearch
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
from ocs_ci.ocs.ui.block_pool import BlockPoolUI
from ocs_ci.ocs.ui.storageclass import StorageClassUI
from ocs_ci.ocs.couchbase import CouchBase
from ocs_ci.helpers.longevity_helpers import (
    _multi_pvc_pod_lifecycle_factory,
    _multi_obc_lifecycle_factory,
)
from ocs_ci.ocs.longevity import start_app_workload
from ocs_ci.utility.decorators import switch_to_default_cluster_index_at_last


log = logging.getLogger(__name__)


class OCSLogFormatter(logging.Formatter):
    def __init__(self):
        fmt = (
            "%(asctime)s - %(threadName)s - %(levelname)s -"
            " %(name)s.%(funcName)s.%(lineno)d - %(message)s"
        )
        super(OCSLogFormatter, self).__init__(fmt)


def pytest_assertrepr_compare(config, op, left, right):
    """
    Log error message for a failed assert, so that it's possible to locate a
    moment of the failure in test logs. Returns None so that it won't actually
    change assert explanation.
    """
    log.error("'assert %s %s %s' failed", left, op, right)


def pytest_logger_config(logger_config):
    logger_config.add_loggers([""], stdout_level="info")
    logger_config.set_log_option_default("")
    logger_config.split_by_outcome()
    logger_config.set_formatter_class(OCSLogFormatter)


def verify_test_decorators_requirements(items):
    """
    Verify that all tests collected are decorated with a squad marker

    Args:
        items: list of collected tests

    """
    items_without_squad_marker = {}
    red_no_mcg_or_rgw_items = {}
    for item in items:
        base_dir = os.path.join(constants.TOP_DIR, "tests")
        ignored_markers = constants.SQUAD_CHECK_IGNORED_MARKERS
        if item.fspath.strpath.startswith(base_dir):
            item_markers = [marker.name for marker in item.iter_markers()]
            if any(marker in item_markers for marker in ignored_markers):
                log.debug(
                    "Ignoring test case %s as it has a marker in the ignore list",
                    item.name,
                )

            # Verify tests are decorated with the correct squad owner
            elif not any(["_squad" in marker for marker in item_markers]):
                log.debug("%s is missing a squad owner marker", item.name)
                items_without_squad_marker.update({item.name: item.fspath.strpath})

            # Verify red squad tests are decorated with either @mcg or @rgw
            elif (
                "red_squad" in item_markers
                and "mcg" not in item_markers
                and "rgw" not in item_markers
            ):
                log.debug("%s is a red_squad test without @mcg or @rgw", item.name)
                red_no_mcg_or_rgw_items.update({item.name: item.fspath.strpath})

    err_msg = ""
    if items_without_squad_marker:
        err_msg += f"""
Missing squad decorator for the following test items: {json.dumps(items_without_squad_marker, indent=4)}

Tests are required to be decorated with their squad owner. Please add the tests respective owner.

For example:

    @magenta_squad
    def test_name():

Test owner marks can be imported from `ocs_ci.framework.pytest_customization.marks`

            """
    if red_no_mcg_or_rgw_items:
        err_msg += f"""
The following tests are missing either the @mcg or @rgw decorators: {json.dumps(red_no_mcg_or_rgw_items, indent=4)}

Red squad tests are required to be decorated with either @mcg or @rgw. Please add either depending on the tests's focus.

                """
    if err_msg:
        raise MissingDecoratorError(err_msg)


def export_squad_marker_to_csv(items, filename=None):
    """
    Export data regarding tests that are missing squad markers to a CSV

    Args:
        items: list of collected tests
        filename: name of the file to export the data to

    """
    _filename = filename or "squad_decorator_data.csv"
    test_data = {"File": [], "Name": [], "Suggestions": []}
    ignored_markers = constants.SQUAD_CHECK_IGNORED_MARKERS
    for item in items:
        item_markers = [marker.name for marker in item.iter_markers()]
        if any(marker in item_markers for marker in ignored_markers):
            log.debug(
                "Ignoring test case %s as it has a marker in the ignore list", item.name
            )
        else:
            item_squad = None
            for marker in item_markers:
                if "_squad" in marker:
                    item_squad = marker.split("_")[0]
                    item_squad = item_squad.capitalize()
                    log.info("Test item %s has squad marker: %s", item.name, marker)
            if not item_squad:
                suggested_squads = []
                for squad, paths in constants.SQUADS.items():
                    for _path in paths:
                        test_path = os.path.relpath(
                            item.fspath.strpath, constants.TOP_DIR
                        )
                        if _path in test_path:
                            suggested_squads.append(squad)
                test_data["File"].append(item.fspath.strpath)
                test_data["Name"].append(item.name)
                test_data["Suggestions"].append(",".join(suggested_squads))

    df = pd.DataFrame(data=test_data)
    df.to_csv(
        _filename,
        header=["File ", "Test Name", "Squad Suggestions"],
        index=False,
        sep=",",
        mode="a",
    )
    num_tests = len(test_data["Name"])
    num_files = len(set(test_data["File"]))
    log.info("Exported squad marker info to %s", _filename)
    log.info("%s tests require action across %s files", num_tests, num_files)


def pytest_collection_modifyitems(session, config, items):
    """
    A pytest hook to filter out skipped tests satisfying
    skipif_ocs_version, skipif_upgraded_from or skipif_no_kms

    Args:
        session: pytest session
        config: pytest config object
        items: list of collected tests

    """
    teardown = ocsci_config.RUN["cli_params"].get("teardown")
    deploy = ocsci_config.RUN["cli_params"].get("deploy")
    skip_ocs_deployment = ocsci_config.ENV_DATA["skip_ocs_deployment"]

    if config.option.collectonly:
        verify_test_decorators_requirements(items)

    # Add squad markers to each test item based on filepath
    for item in items:
        # check, if test already have squad marker manually assigned
        for marker in item.iter_markers():
            if "_squad" in marker.name:
                squad = marker.name.split("_")[0]
                item.user_properties.append(("squad", squad.capitalize()))

    if not (teardown or deploy or (deploy and skip_ocs_deployment)):
        for item in items[:]:
            skipif_ocp_version_marker = item.get_closest_marker("skipif_ocp_version")
            skipif_ocs_version_marker = item.get_closest_marker("skipif_ocs_version")
            skipif_upgraded_from_marker = item.get_closest_marker(
                "skipif_upgraded_from"
            )
            skipif_no_kms_marker = item.get_closest_marker("skipif_no_kms")
            skipif_ui_not_support_marker = item.get_closest_marker(
                "skipif_ui_not_support"
            )
            skipif_lvm_not_installed_marker = item.get_closest_marker(
                "skipif_lvm_not_installed"
            )
            if skipif_lvm_not_installed_marker and "lvm" in ocsci_config.RUN:
                if not ocsci_config.RUN["lvm"]:
                    log.info(f"Test {item} will be removed due to lvm not installed")
                    items.remove(item)
                    continue
            if skipif_ocp_version_marker:
                skip_condition = skipif_ocp_version_marker.args
                # skip_condition will be a tuple
                # and condition will be first element in the tuple
                if skipif_ocp_version(skip_condition[0]):
                    log.debug(
                        f"Test: {item} will be skipped due to OCP {skip_condition}"
                    )
                    items.remove(item)
                    continue
            if skipif_ocs_version_marker:
                skip_condition = skipif_ocs_version_marker.args
                # skip_condition will be a tuple
                # and condition will be first element in the tuple
                if skipif_ocs_version(skip_condition[0]):
                    log.debug(f"Test: {item} will be skipped due to {skip_condition}")
                    items.remove(item)
                    continue
            if (
                skipif_upgraded_from_marker
                and ocsci_config.ENV_DATA.get("platform", "").lower()
                not in constants.HCI_PROVIDER_CLIENT_PLATFORMS
            ):
                skip_args = skipif_upgraded_from_marker.args
                if skipif_upgraded_from(skip_args[0]):
                    log.debug(
                        f"Test: {item} will be skipped because the OCS cluster is"
                        f" upgraded from one of these versions: {skip_args[0]}"
                    )
                    items.remove(item)
            if skipif_no_kms_marker:
                try:
                    if not is_kms_enabled(dont_raise=True):
                        log.debug(
                            f"Test: {item} it will be skipped because the OCS cluster"
                            " has not configured cluster-wide encryption with KMS"
                        )
                        items.remove(item)
                except KeyError:
                    log.warning(
                        "Cluster is not yet installed. Skipping skipif_no_kms check."
                    )
            if skipif_ui_not_support_marker:
                skip_condition = skipif_ui_not_support_marker
                if skipif_ui_not_support(skip_condition.args[0]):
                    log.debug(
                        f"Test: {item} will be skipped due to UI test"
                        f" {skip_condition.args} is not available"
                    )
                    items.remove(item)
                    continue
    # Skip UI test on openshift dedicated, ODF-MS, FaaS platform
    if ocsci_config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if "/ui/" in str(item.fspath):
                log.debug(
                    f"Test {item} is removed from the collected items"
                    f" UI is not supported on {ocsci_config.ENV_DATA['platform'].lower()}"
                )
                items.remove(item)


def pytest_collection_finish(session):
    """
    A pytest hook to get all collected tests post their collection modifications done in the varius
    pytest_collection_modifyitems hook functions

    Args:
        session: pytest session

    """
    ocsci_config.RUN["number_of_tests"] = len(session.items)


@pytest.fixture()
def supported_configuration():
    """
    Check that cluster nodes have enough CPU and Memory as described in:
    https://access.redhat.com/documentation/en-us/red_hat_openshift_container_storage/4.2/html-single/planning_your_deployment/index#infrastructure-requirements_rhocs
    This fixture is intended as a prerequisite for tests or fixtures that
    run flaky on configurations that don't meet minimal requirements.

    Minimum requirements for each starting node (OSD+MON):
        16 CPUs
        64 GB memory
    Last documentation check: 2020-02-21
    """
    if ocsci_config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        log.info("Check for supported configuration is not applied on Managed Service")
        return
    min_cpu = constants.MIN_NODE_CPU
    min_memory = constants.MIN_NODE_MEMORY

    log.info("Checking if system meets minimal requirements")
    if not check_nodes_specs(min_memory=min_memory, min_cpu=min_cpu):
        err_msg = (
            "At least one of the worker nodes doesn't meet the "
            f"required minimum specs of {min_cpu} vCPUs and {min_memory} RAM"
        )
        pytest.xfail(err_msg)


@pytest.fixture(scope="session")
def threading_lock():
    """
    threading.Lock object that can be used in threads across multiple tests.

    Returns:
        threading.Rlock: Reentrant Lock object. A reentrant lock (or RLock) is a type of lock that allows the same
        thread to acquire the lock multiple times without causing a deadlock
    """
    return threading.RLock()


@pytest.fixture(scope="session", autouse=True)
def auto_load_auth_config():
    try:
        auth_config = {"AUTH": load_auth_config()}
        ocsci_config.update(auth_config)
    except FileNotFoundError:
        pass  # If auth file doesn't exist we just ignore.


@pytest.fixture(scope="class")
def secret_factory_class(request):
    return secret_factory_fixture(request)


@pytest.fixture(scope="session")
def secret_factory_session(request):
    return secret_factory_fixture(request)


@pytest.fixture(scope="function")
def secret_factory(request):
    return secret_factory_fixture(request)


def secret_factory_fixture(request):
    """
    Secret factory. Calling this fixture creates a new secret.
    RBD based is default.
    ** This method should not be used anymore **
    ** This method is for internal testing only **
    """
    instances = []

    def factory(interface=constants.CEPHBLOCKPOOL):
        """
        Args:
            interface (str): CephBlockPool or CephFileSystem. This decides
                whether a RBD based or CephFS resource is created.
                RBD is default.
        """
        secret_obj = helpers.create_secret(interface_type=interface)
        assert secret_obj, "Failed to create a secret"
        instances.append(secret_obj)
        return secret_obj

    def finalizer():
        """
        Delete the RBD secrets
        """
        for instance in instances:
            instance.delete()
            instance.ocp.wait_for_delete(instance.name)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="session", autouse=True)
def log_ocs_version(cluster):
    """
    Fixture handling version reporting for OCS.

    This fixture handles alignment of the version reporting, so that we:

     * report version for each test run (no matter if just deployment, just
       test or both deployment and tests are executed)
     * prevent conflict of version reporting with deployment/teardown (eg. we
       should not run the version logging before actual deployment, or after
       a teardown)

    Version is reported in:

     * log entries of INFO log level during test setup phase
     * ocs_version file in cluster path directory (for copy pasting into bug
       reports)
    """
    teardown = ocsci_config.RUN["cli_params"].get("teardown")
    deploy = ocsci_config.RUN["cli_params"].get("deploy")
    dev_mode = ocsci_config.RUN["cli_params"].get("dev_mode")
    skip_ocs_deployment = ocsci_config.ENV_DATA["skip_ocs_deployment"]
    if teardown and not deploy:
        log.info("Skipping version reporting for teardown.")
        return
    elif dev_mode:
        log.info("Skipping version reporting for development mode.")
        return
    elif skip_ocs_deployment:
        log.info("Skipping version reporting since OCS deployment is skipped.")
        return
    cluster_version = retry(CommandFailed, tries=3, delay=15)(get_ocp_version_dict)()
    image_dict = retry(CommandFailed, tries=3, delay=15)(get_ocs_version)()
    file_name = os.path.join(
        ocsci_config.ENV_DATA["cluster_path"],
        "ocs_version." + datetime.now().isoformat(),
    )
    with open(file_name, "w") as file_obj:
        report_ocs_version(cluster_version, image_dict, file_obj)
    log.info("human readable ocs version info written into %s", file_name)


@pytest.fixture(scope="session")
def pagerduty_service(request):
    """
    Create a Service in PagerDuty service. The service represents a cluster instance.
    The service is deleted at the end of the test run.

    Returns:
        str: PagerDuty service json

    """
    if (
        ocsci_config.ENV_DATA["platform"].lower()
        not in constants.MANAGED_SERVICE_PLATFORMS
    ):
        log.info(
            "PagerDuty service is not created because "
            f"platform from {constants.MANAGED_SERVICE_PLATFORMS} "
            "is not used"
        )
        return None
    teardown = ocsci_config.RUN["cli_params"]["teardown"]
    skip_ocs_deployment = ocsci_config.ENV_DATA["skip_ocs_deployment"]
    ceph_cluster_installed = ocsci_config.RUN.get("cephcluster")
    if teardown or skip_ocs_deployment or not ceph_cluster_installed:
        log.info("CephCluster is not available. Skipping PagerDuty integration.")
        return None

    if ocsci_config.ENV_DATA.get("disable_pagerduty"):
        log.info(
            "PagerDuty service is not created because it was disabled "
            "with configuration"
        )
        return None

    pagerduty_api = pagerduty.PagerDutyAPI()
    payload = pagerduty_api.get_service_dict()
    service_response = pagerduty_api.create("services", payload=payload)
    msg = f"Request {service_response.request.url} failed: {service_response.text}"
    assert service_response.ok, msg
    service = service_response.json().get("service")
    ocsci_config.RUN["pagerduty_service_id"] = service["id"]

    def teardown():
        """
        Delete the service at the end of test run
        """
        service_id = service["id"]
        log.info(f"Deleting service with id {service_id}")
        delete_response = pagerduty_api.delete(f"services/{service_id}")
        msg = f"Deletion of service {service_id} failed"
        assert delete_response.ok, msg

    request.addfinalizer(teardown)
    return service


@pytest.fixture(scope="session", autouse=True)
def pagerduty_integration(request, pagerduty_service):
    """
    Create a new Pagerduty integration for service from pagerduty_service
    fixture if it doesn' exist. Update ocs-converged-pagerduty secret with
    correct integration key. This is currently applicable only for ODF
    Managed Service.

    """
    if not pagerduty_service:
        # this is used only for managed service platforms with configured PagerDuty
        # and installed CephCluster
        return

    service_id = pagerduty_service["id"]
    pagerduty_api = pagerduty.PagerDutyAPI()

    log.info(
        "Looking if Prometheus integration for pagerduty service with id "
        f"{service_id} exists"
    )
    integration_key = None
    for integration in pagerduty_service.get("integrations"):
        if integration["summary"] == "Prometheus":
            log.info(
                "Prometheus integration already exists. Skipping creation of new one."
            )
            integration_key = integration["integration_key"]
            break

    if not integration_key:
        payload = pagerduty_api.get_integration_dict("Prometheus")
        integration_response = pagerduty_api.create(
            f"services/{service_id}/integrations", payload=payload
        )
        msg = (
            f"Request {integration_response.request.url} failed: "
            f"{integration_response.text}"
        )
        assert integration_response.ok, msg
        integration = integration_response.json().get("integration")
        integration_key = integration["integration_key"]
        if ocsci_config.ENV_DATA["platform"] == constants.FUSIONAAS_PLATFORM:
            pagerduty.set_pagerduty_faas_secret(integration_key)
        else:
            pagerduty.set_pagerduty_integration_secret(integration_key)

    def update_pagerduty_integration_secret():
        """
        Make sure that pagerduty secret is updated with correct integration
        token. Check value of ocsci_config.RUN['thread_pagerduty_secret_update']:
            * required - secret is periodically updated to correct value
            * not required - secret is not updated
            * finished - thread is terminated

        """
        while ocsci_config.RUN["thread_pagerduty_secret_update"] != "finished":
            if ocsci_config.RUN["thread_pagerduty_secret_update"] == "required":
                if ocsci_config.ENV_DATA["platform"] == constants.FUSIONAAS_PLATFORM:
                    pagerduty.set_pagerduty_faas_secret(integration_key)
                else:
                    pagerduty.set_pagerduty_integration_secret(integration_key)
            time.sleep(60)

    ocsci_config.RUN["thread_pagerduty_secret_update"] = "not required"
    thread = threading.Thread(
        target=update_pagerduty_integration_secret,
        name="thread_pagerduty_secret_update",
    )

    def finalizer():
        """
        Stop the thread that executed update_pagerduty_integration_secret()
        """
        ocsci_config.RUN["thread_pagerduty_secret_update"] = "finished"
        if thread:
            thread.join()

    request.addfinalizer(finalizer)
    thread.start()


@pytest.fixture(scope="class")
def ceph_pool_factory_class(request, replica=3, compression=None):
    return ceph_pool_factory_fixture(request, replica=replica, compression=compression)


@pytest.fixture(scope="session")
def ceph_pool_factory_session(request, replica=3, compression=None):
    return ceph_pool_factory_fixture(request, replica=replica, compression=compression)


@pytest.fixture(scope="function")
def ceph_pool_factory(request, replica=3, compression=None):
    return ceph_pool_factory_fixture(request, replica=replica, compression=compression)


def ceph_pool_factory_fixture(request, replica=3, compression=None):
    """
    Create a Ceph pool factory.
    Calling this fixture creates new Ceph pool instance.
    ** This method should not be used anymore **
    ** This method is for internal testing only **
    """
    instances = []

    def factory(
        interface=constants.CEPHBLOCKPOOL, replica=replica, compression=compression
    ):
        if interface == constants.CEPHBLOCKPOOL:
            ceph_pool_obj = helpers.create_ceph_block_pool(
                replica=replica, compression=compression
            )
        elif interface == constants.CEPHFILESYSTEM:
            cfs = ocp.OCP(
                kind=constants.CEPHFILESYSTEM,
                namespace=ocsci_config.ENV_DATA["cluster_namespace"],
            ).get(defaults.CEPHFILESYSTEM_NAME)
            ceph_pool_obj = OCS(**cfs)
        assert ceph_pool_obj, f"Failed to create {interface} pool"
        if interface != constants.CEPHFILESYSTEM:
            instances.append(ceph_pool_obj)
        return ceph_pool_obj

    def finalizer():
        """
        Delete the Ceph block pool
        """
        skip_resource_not_found_error = None
        node = request.node
        for mark in node.iter_markers():
            if mark.name == ignore_resource_not_found_error_label.name:
                skip_resource_not_found_error = True

        for instance in instances:
            try:
                instance.delete()
            except CommandFailed as ex:
                if "NotFound" in str(ex) and skip_resource_not_found_error:
                    log.info(
                        f"Resource {instance.kind} {instance.name} not found in "
                        f"namespace {instance.namespace}, ignore_resource_not_found_error_label applied. "
                        "Skipping deletion"
                    )
                else:
                    raise
            instance.ocp.wait_for_delete(instance.name)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def storageclass_factory_class(request, ceph_pool_factory_class, secret_factory_class):
    return storageclass_factory_fixture(
        request, ceph_pool_factory_class, secret_factory_class
    )


@pytest.fixture(scope="session")
def storageclass_factory_session(
    request, ceph_pool_factory_session, secret_factory_session
):
    return storageclass_factory_fixture(
        request, ceph_pool_factory_session, secret_factory_session
    )


@pytest.fixture(scope="function")
def storageclass_factory(request, ceph_pool_factory, secret_factory):
    return storageclass_factory_fixture(request, ceph_pool_factory, secret_factory)


def storageclass_factory_fixture(
    request,
    ceph_pool_factory,
    secret_factory,
):
    """
    Create a storage class factory. Default is RBD based.
    Calling this fixture creates new storage class instance.

    ** This method should not be used anymore **
    ** This method is for internal testing only **

    """
    instances = []

    def factory(
        interface=constants.CEPHBLOCKPOOL,
        secret=None,
        custom_data=None,
        sc_name=None,
        reclaim_policy=constants.RECLAIM_POLICY_DELETE,
        replica=3,
        compression=None,
        new_rbd_pool=False,
        pool_name=None,
        rbd_thick_provision=False,
        encrypted=False,
        encryption_kms_id=None,
        volume_binding_mode="Immediate",
        allow_volume_expansion=True,
        kernelMountOptions=None,
    ):
        """
        Args:
            interface (str): CephBlockPool or CephFileSystem. This decides
                whether a RBD based or CephFS resource is created.
                RBD is default.
            secret (object): An OCS instance for the secret.
            custom_data (dict): If provided then storageclass object is created
                by using these data. Parameters `block_pool` and `secret`
                are not useds but references are set if provided.
            sc_name (str): Name of the storage class
            replica (int): Replica size for a pool
            compression (str): Compression type option for a pool
            new_rbd_pool (bool): True if user wants to create new rbd pool for SC
            pool_name (str): Existing pool name to create the storageclass other
                then the default rbd pool.
            rbd_thick_provision (bool): True to enable RBD thick provisioning.
                Applicable if interface is CephBlockPool
            encrypted (bool): True to enable RBD PV encryption
            encryption_kms_id (str): Key value of vault config to be used from
                    csi-kms-connection-details configmap
            volume_binding_mode (str): Can be "Immediate" or "WaitForFirstConsumer" which the PVC will be in pending
                till pod attachment.
            allow_volume_expansion (bool): True to Allows volume expansion
            kernelMountOptions (str): Mount option for security context

        Returns:
            object: helpers.create_storage_class instance with links to
                block_pool and secret.
        """
        if custom_data:
            sc_obj = helpers.create_resource(**custom_data)
        else:
            secret = secret or secret_factory(interface=interface)
            if interface == constants.CEPHBLOCKPOOL:
                if ocsci_config.ENV_DATA.get("new_rbd_pool") or new_rbd_pool:
                    pool_obj = ceph_pool_factory(
                        interface=interface,
                        replica=ocsci_config.ENV_DATA.get("replica") or replica,
                        compression=ocsci_config.ENV_DATA.get("compression")
                        or compression,
                    )
                    interface_name = pool_obj.name
                else:
                    if pool_name is None:
                        interface_name = helpers.default_ceph_block_pool()
                    else:
                        interface_name = pool_name
            elif interface == constants.CEPHFILESYSTEM:
                interface_name = helpers.get_cephfs_data_pool_name()

            sc_obj = helpers.create_storage_class(
                interface_type=interface,
                interface_name=interface_name,
                secret_name=secret.name,
                sc_name=sc_name,
                reclaim_policy=reclaim_policy,
                rbd_thick_provision=rbd_thick_provision,
                encrypted=encrypted,
                encryption_kms_id=encryption_kms_id,
                volume_binding_mode=volume_binding_mode,
                allow_volume_expansion=allow_volume_expansion,
                kernelMountOptions=kernelMountOptions,
            )
            assert sc_obj, f"Failed to create {interface} storage class"
            sc_obj.secret = secret
            sc_obj.interface_name = interface_name

        instances.append(sc_obj)
        return sc_obj

    def finalizer():
        """
        Delete the storageclass
        """
        for instance in instances:
            instance.delete()
            instance.ocp.wait_for_delete(instance.name)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def project_factory_class(request):
    return project_factory_fixture(request)


@pytest.fixture(scope="session")
def project_factory_session(request):
    return project_factory_fixture(request)


@pytest.fixture()
def project_factory(request):
    return project_factory_fixture(request)


@pytest.fixture()
def project(project_factory):
    """
    This fixture creates a single project instance.
    """
    project_obj = project_factory()
    return project_obj


def project_factory_fixture(request):
    """
    Create a new project factory.
    Calling this fixture creates new project.
    """
    instances = []

    def factory(project_name=None):
        """
        Args:
            project_name (str): The name for the new project

        Returns:
            object: ocs_ci.ocs.resources.ocs instance of 'Project' kind.
        """
        proj_obj = helpers.create_project(project_name=project_name)
        instances.append(proj_obj)
        return proj_obj

    def finalizer():
        delete_projects(instances)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="function")
def teardown_project_factory(request):
    return teardown_project_factory_fixture(request)


def teardown_project_factory_fixture(request):
    """
    Tearing down a project that was created during the test
    To use this factory, you'll need to pass 'teardown_project_factory' to your test
    function and call it in your test when a new project was created and you
    want it to be removed in teardown phase:
    def test_example(self, teardown_project_factory):
        project_obj = create_project(project_name="xyz")
        teardown_project_factory(project_obj)
    """
    instances = []

    def factory(resource_obj):
        """
        Args:
            resource_obj (OCP object or list of OCP objects) : Object to teardown after the test

        """
        if isinstance(resource_obj, list):
            instances.extend(resource_obj)
        else:
            instances.append(resource_obj)

    def finalizer():
        delete_projects(instances)

    request.addfinalizer(finalizer)
    return factory


def delete_projects(instances):
    """
    Delete the project

    instances (list): list of OCP objects (kind is Project)

    """
    for instance in instances:
        try:
            ocp_event = ocp.OCP(kind="Event", namespace=instance.namespace)
            events = ocp_event.get()
            event_count = len(events["items"])
            warn_event_count = 0
            for event in events["items"]:
                if event["type"] == "Warning":
                    warn_event_count += 1
            log.info(
                "There were %d events in %s namespace before it's"
                " removal (out of which %d were of type Warning)."
                " For a full dump of this event list, see DEBUG logs.",
                event_count,
                instance.namespace,
                warn_event_count,
            )
        except Exception:
            # we don't want any problem to disrupt the teardown itself
            log.exception("Failed to get events for project %s", instance.namespace)
        ocp.switch_to_default_rook_cluster_project()
        instance.delete(resource_name=instance.namespace)
        instance.wait_for_delete(instance.namespace, timeout=300)


@pytest.fixture(scope="class")
def pvc_factory_class(request, project_factory_class):
    return pvc_factory_fixture(request, project_factory_class)


@pytest.fixture(scope="session")
def pvc_factory_session(request, project_factory_session):
    return pvc_factory_fixture(request, project_factory_session)


@pytest.fixture(scope="function")
def pvc_factory(request, project_factory):
    return pvc_factory_fixture(
        request,
        project_factory,
    )


def pvc_factory_fixture(request, project_factory):
    """
    Create a persistent Volume Claim factory. Calling this fixture creates new
    PVC. For custom PVC provide 'storageclass' parameter.
    """
    instances = []
    active_project = None
    active_rbd_storageclass = None
    active_cephfs_storageclass = None

    def factory(
        interface=constants.CEPHBLOCKPOOL,
        project=None,
        storageclass=None,
        size=None,
        access_mode=constants.ACCESS_MODE_RWO,
        custom_data=None,
        status=constants.STATUS_BOUND,
        volume_mode=None,
        size_unit="Gi",
    ):
        """
        Args:
            interface (str): CephBlockPool or CephFileSystem. This decides
                whether a RBD based or CephFS resource is created.
                RBD is default.
            project (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'Project' kind.
            storageclass (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'StorageClass' kind.
            size (int): The requested size for the PVC
            access_mode (str): ReadWriteOnce, ReadOnlyMany or ReadWriteMany.
                This decides the access mode to be used for the PVC.
                ReadWriteOnce is default.
            custom_data (dict): If provided then PVC object is created
                by using these data. Parameters `project` and `storageclass`
                are not used but reference is set if provided.
            status (str): If provided then factory waits for object to reach
                desired state.
            volume_mode (str): Volume mode for PVC.
                eg: volume_mode='Block' to create rbd `block` type volume
            size_unit (str): PVC size unit, eg: "Mi"

        Returns:
            object: helpers.create_pvc instance.
        """
        if custom_data:
            pvc_obj = PVC(**custom_data)
            pvc_obj.create(do_reload=False)
        else:
            nonlocal active_project
            nonlocal active_rbd_storageclass
            nonlocal active_cephfs_storageclass

            project = project or active_project or project_factory()
            active_project = project
            if interface == constants.CEPHBLOCKPOOL:
                storageclass = storageclass or helpers.default_storage_class(
                    interface_type=interface
                )
                active_rbd_storageclass = storageclass
            elif interface == constants.CEPHFILESYSTEM:
                storageclass = storageclass or helpers.default_storage_class(
                    interface_type=interface
                )
                active_cephfs_storageclass = storageclass

            pvc_size = f"{size}{size_unit}" if size else None

            pvc_obj = helpers.create_pvc(
                sc_name=storageclass.name,
                namespace=project.namespace,
                size=pvc_size,
                do_reload=False,
                access_mode=access_mode,
                volume_mode=volume_mode,
            )
            assert pvc_obj, "Failed to create PVC"

        if status:
            helpers.wait_for_resource_state(pvc_obj, status)
        pvc_obj.storageclass = storageclass
        pvc_obj.project = project
        pvc_obj.access_mode = access_mode
        instances.append(pvc_obj)

        return pvc_obj

    def finalizer():
        """
        Delete the PVC
        """
        pv_objs = []

        # Get PV form PVC instances and delete PVCs
        for instance in instances:
            if not instance.is_deleted:
                pv_objs.append(instance.backed_pv_obj)
                instance.delete()
                instance.ocp.wait_for_delete(instance.name)

        # Wait for PVs to delete
        # If they have ReclaimPolicy set to Retain then change to Delete
        for pv_obj in pv_objs:
            if (
                pv_obj.data.get("spec", {}).get("persistentVolumeReclaimPolicy")
                == constants.RECLAIM_POLICY_RETAIN
                and pv_obj is not None
            ):
                helpers.wait_for_resource_state(pv_obj, constants.STATUS_RELEASED)
                patch_param = '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
                pv_obj.ocp.patch(resource_name=pv_obj.name, params=patch_param)

            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def pod_factory_class(request, pvc_factory_class):
    return pod_factory_fixture(request, pvc_factory_class)


@pytest.fixture(scope="session")
def pod_factory_session(request, pvc_factory_session):
    return pod_factory_fixture(request, pvc_factory_session)


@pytest.fixture(scope="function")
def pod_factory(request, pvc_factory):
    return pod_factory_fixture(request, pvc_factory)


def pod_factory_fixture(request, pvc_factory):
    """
    Create a Pod factory. Calling this fixture creates new Pod.
    For custom Pods provide 'pvc' parameter.
    """
    instances = []

    def factory(
        interface=constants.CEPHBLOCKPOOL,
        pvc=None,
        custom_data=None,
        status=constants.STATUS_RUNNING,
        node_name=None,
        pod_dict_path=None,
        raw_block_pv=False,
        deployment_config=False,
        service_account=None,
        security_context=None,
        replica_count=1,
        pod_name=None,
        command=None,
        command_args=None,
        subpath=None,
        deployment=False,
        pvc_read_only_mode=None,
        priorityClassName=None,
    ):
        """
        Args:
            interface (str): CephBlockPool or CephFileSystem. This decides
                whether a RBD based or CephFS resource is created.
                RBD is default.
            pvc (PVC object): ocs_ci.ocs.resources.pvc.PVC instance kind.
            custom_data (dict): If provided then Pod object is created
                by using these data. Parameter `pvc` is not used but reference
                is set if provided.
            status (str): If provided then factory waits for object to reach
                desired state.
            node_name (str): The name of specific node to schedule the pod
            pod_dict_path (str): YAML path for the pod.
            raw_block_pv (bool): True for creating raw block pv based pod,
                False otherwise.
            deployment_config (bool): True for DeploymentConfig creation,
                False otherwise
            service_account (OCS): Service account object, in case DeploymentConfig
                is to be created
            security_context (dict): security context in the form of dictionary
            replica_count (int): The replica count for deployment config
            pod_name (str): Name of the pod
            command (list): The command to be executed on the pod
            command_args (list): The arguments to be sent to the command running
                on the pod
            subpath (str): Value of subPath parameter in pod yaml
            deployment (bool): True for Deployment creation, False otherwise
            priorityClassName (str): Sets priority to the pod

        Returns:
            object: helpers.create_pod instance

        """
        sa_name = service_account.name if service_account else None
        if custom_data:
            pod_obj = helpers.create_resource(**custom_data)
        else:
            pvc = pvc or pvc_factory(interface=interface)
            pod_obj = helpers.create_pod(
                pvc_name=pvc.name,
                namespace=pvc.namespace,
                interface_type=interface,
                node_name=node_name,
                pod_dict_path=pod_dict_path,
                raw_block_pv=raw_block_pv,
                dc_deployment=deployment_config,
                sa_name=sa_name,
                replica_count=replica_count,
                pod_name=pod_name,
                security_context=security_context,
                command=command,
                command_args=command_args,
                subpath=subpath,
                deployment=deployment,
                pvc_read_only_mode=pvc_read_only_mode,
                priorityClassName=priorityClassName,
            )
            assert pod_obj, "Failed to create pod"

        if deployment_config or deployment:
            d_name = pod_obj.get_labels().get("name")
            d_ocp_dict = ocp.OCP(
                kind=(
                    constants.DEPLOYMENTCONFIG
                    if deployment_config
                    else constants.DEPLOYMENT
                ),
                namespace=pod_obj.namespace,
            ).get(resource_name=d_name)
            d_obj = OCS(**d_ocp_dict)
            instances.append(d_obj)

        else:
            instances.append(pod_obj)
        if status:
            helpers.wait_for_resource_state(pod_obj, status, timeout=300)
            pod_obj.reload()
        pod_obj.pvc = pvc
        if deployment_config or deployment:
            return d_obj
        return pod_obj

    def finalizer():
        """
        Delete the Pod or the DeploymentConfig
        """
        for instance in instances:
            instance.delete()
            instance.ocp.wait_for_delete(instance.name)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def teardown_factory_class(request):
    return teardown_factory_fixture(request)


@pytest.fixture(scope="session")
def teardown_factory_session(request):
    return teardown_factory_fixture(request)


@pytest.fixture(scope="function")
def teardown_factory(request):
    return teardown_factory_fixture(request)


def teardown_factory_fixture(request):
    """
    Tearing down a resource that was created during the test
    To use this factory, you'll need to pass 'teardown_factory' to your test
    function and call it in your test when a new resource was created and you
    want it to be removed in teardown phase:
    def test_example(self, teardown_factory):
        pvc_obj = create_pvc()
        teardown_factory(pvc_obj)

    """
    instances = []

    def factory(resource_obj):
        """
        Args:
            resource_obj (OCS object or list of OCS objects) : Object to teardown after the test

        """
        if isinstance(resource_obj, list):
            instances.extend(resource_obj)
        else:
            instances.append(resource_obj)

    def finalizer():
        """
        Delete the resources created in the test
        """
        for instance in instances[::-1]:
            if not instance.is_deleted:
                try:
                    if (instance.kind == constants.PVC) and (instance.reclaim_policy):
                        pass
                    reclaim_policy = (
                        instance.reclaim_policy
                        if instance.kind == constants.PVC
                        else None
                    )
                    instance.delete()
                    instance.ocp.wait_for_delete(instance.name)
                    if reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                        helpers.validate_pv_delete(instance.backed_pv)
                except CommandFailed as ex:
                    log.warning(
                        "Resource is already in deleted state, skipping this step"
                        f"Error: {ex}"
                    )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def service_account_factory_class(request):
    return service_account_factory_fixture(request)


@pytest.fixture(scope="session")
def service_account_factory_session(request):
    return service_account_factory_fixture(request)


@pytest.fixture(scope="function")
def service_account_factory(request):
    return service_account_factory_fixture(request)


def service_account_factory_fixture(request):
    """
    Create a service account
    """
    instances = []
    active_service_account_obj = None

    def factory(project=None, service_account=None):
        """
        Args:
            project (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'Project' kind.
            service_account (str): service_account_name

        Returns:
            object: serviceaccount instance.
        """
        nonlocal active_service_account_obj

        if active_service_account_obj and not service_account:
            return active_service_account_obj
        elif service_account:
            sa_obj = helpers.get_serviceaccount_obj(
                sa_name=service_account, namespace=project.namespace
            )
            if not helpers.validate_scc_policy(
                sa_name=service_account, namespace=project.namespace
            ):
                helpers.add_scc_policy(
                    sa_name=service_account, namespace=project.namespace
                )
            sa_obj.project = project
            active_service_account_obj = sa_obj
            instances.append(sa_obj)
            return sa_obj
        else:
            sa_obj = helpers.create_serviceaccount(
                namespace=project.namespace,
            )
            sa_obj.project = project
            active_service_account_obj = sa_obj
            helpers.add_scc_policy(sa_name=sa_obj.name, namespace=project.namespace)
            assert sa_obj, "Failed to create serviceaccount"
            instances.append(sa_obj)
            return sa_obj

    def finalizer():
        """
        Delete the service account
        """
        for instance in instances:
            original_cluster = None
            if instance.ocp.cluster_context:
                original_cluster = ocsci_config.cluster_ctx.MULTICLUSTER.get(
                    "multicluster_index"
                )
                ocsci_config.switch_ctx(instance.ocp.cluster_context)
            helpers.remove_scc_policy(
                sa_name=instance.name, namespace=instance.namespace
            )
            instance.delete()
            instance.ocp.wait_for_delete(resource_name=instance.name)
            if original_cluster:
                ocsci_config.switch_ctx(original_cluster)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def dc_pod_factory(request, pvc_factory, service_account_factory):
    """
    Create deploymentconfig pods
    """
    instances = []

    def factory(
        interface=constants.CEPHBLOCKPOOL,
        pvc=None,
        access_mode=constants.ACCESS_MODE_RWO,
        service_account=None,
        size=None,
        custom_data=None,
        node_name=None,
        node_selector=None,
        replica_count=1,
        raw_block_pv=False,
        sa_obj=None,
        wait=True,
    ):
        """
        Args:
            interface (str): CephBlockPool or CephFileSystem. This decides
                whether a RBD based or CephFS resource is created.
                RBD is default.
            pvc (PVC object): ocs_ci.ocs.resources.pvc.PVC instance kind.
            access_mode (str): ReadWriteOnce, ReadOnlyMany or ReadWriteMany.
                This decides the access mode to be used for the PVC.
                ReadWriteOnce is default.
            service_account (str): service account name for dc_pods
            size (int): The requested size for the PVC
            custom_data (dict): If provided then Pod object is created
                by using these data. Parameter `pvc` is not used but reference
                is set if provided.
            node_name (str): The name of specific node to schedule the pod
            node_selector (dict): dict of key-value pair to be used for nodeSelector field
                eg: {'nodetype': 'app-pod'}
            replica_count (int): Replica count for deployment config
            raw_block_pv (str): True if pod with raw block pvc
            sa_obj (object) : If specific service account is needed

        """
        if custom_data:
            dc_pod_obj = helpers.create_resource(**custom_data)
        else:
            pvc = pvc or pvc_factory(
                interface=interface, size=size, access_mode=access_mode
            )
            sa_obj = sa_obj or service_account_factory(
                project=pvc.project, service_account=service_account
            )
            dc_pod_obj = helpers.create_pod(
                interface_type=interface,
                pvc_name=pvc.name,
                do_reload=False,
                namespace=pvc.namespace,
                sa_name=sa_obj.name,
                dc_deployment=True,
                replica_count=replica_count,
                node_name=node_name,
                node_selector=node_selector,
                raw_block_pv=raw_block_pv,
                pod_dict_path=constants.FEDORA_DC_YAML,
            )
        instances.append(dc_pod_obj)
        log.info(dc_pod_obj.name)
        if wait:
            helpers.wait_for_resource_state(
                dc_pod_obj, constants.STATUS_RUNNING, timeout=180
            )
        dc_pod_obj.pvc = pvc
        return dc_pod_obj

    def finalizer():
        """
        Delete dc pods
        """
        for instance in instances:
            delete_deploymentconfig_pods(instance)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="session", autouse=True)
def polarion_testsuite_properties(record_testsuite_property, pytestconfig):
    """
    Configures polarion testsuite properties for junit xml
    """
    polarion_project_id = ocsci_config.REPORTING["polarion"]["project_id"]
    record_testsuite_property("polarion-project-id", polarion_project_id)
    jenkins_build_url = ocsci_config.RUN.get("jenkins_build_url")
    if jenkins_build_url:
        record_testsuite_property("polarion-custom-description", jenkins_build_url)
    polarion_testrun_name = get_testrun_name()
    record_testsuite_property("polarion-testrun-id", polarion_testrun_name)
    record_testsuite_property("polarion-testrun-status-id", "inprogress")
    record_testsuite_property("polarion-custom-isautomated", "True")


@pytest.fixture(scope="session", autouse=True)
def additional_testsuite_properties(record_testsuite_property, pytestconfig):
    """
    Configures additional custom testsuite properties for junit xml
    """
    # add logs url
    logs_url = ocsci_config.RUN.get("logs_url")
    if logs_url:
        record_testsuite_property("logs-url", logs_url)

    # add run_id
    record_testsuite_property("run_id", ocsci_config.RUN["run_id"])

    # add cluster dir full path (on NFS share, if configured, it should contain
    # full path to cluster dir on NFS share, starting with `/mnt/`)
    if ocsci_config.RUN.get("cluster_dir_full_path"):
        record_testsuite_property(
            "cluster_dir_full_path", ocsci_config.RUN.get("cluster_dir_full_path")
        )

    # Report Portal
    launch_name = reporting.get_rp_launch_name()
    record_testsuite_property("rp_launch_name", launch_name)
    launch_description = reporting.get_rp_launch_description()
    record_testsuite_property("rp_launch_description", launch_description)
    attributes = reporting.get_rp_launch_attributes()
    for key, value in attributes.items():
        # Prefix with `rp_` so the rp_preproc upload script knows to use the property
        record_testsuite_property(f"rp_{key}", value)
    launch_url = ocsci_config.REPORTING.get("rp_launch_url")
    if launch_url:
        record_testsuite_property("rp_launch_url", launch_url)
    # add markers as separated property
    markers = ocsci_config.RUN["cli_params"].get("-m", "").replace(" ", "-")
    record_testsuite_property("rp_markers", markers)
    dr_operator_versions = get_dr_operator_versions()
    for dr_operator_name, dr_operator_version in dr_operator_versions.items():
        record_testsuite_property(f"rp_{dr_operator_name}", dr_operator_version)


@pytest.fixture(scope="session")
def tier_marks_name():
    """
    Gets the tier mark names

    Returns:
        list: list of tier mark names

    """
    tier_marks_name = []
    for each_tier in tier_marks:
        try:
            tier_marks_name.append(each_tier.name)
        except AttributeError:
            tier_marks_name.append(each_tier().args[0].name)
    return tier_marks_name


@pytest.fixture(scope="session")
def upgrade_marks_name():
    """
    Gets the upgrade mark names

    Returns:
        list: list of upgrade mark names

    """
    upgrade_marks_name = []
    for upgrade_mark in upgrade_marks:
        try:
            upgrade_marks_name.append(upgrade_mark().args[0].name)
        except AttributeError:
            log.error("upgrade mark does not exist")
    return upgrade_marks_name


@pytest.fixture(scope="function", autouse=True)
def health_checker(request, tier_marks_name, upgrade_marks_name):
    skipped = False
    dev_mode = ocsci_config.RUN["cli_params"].get("dev_mode")
    mcg_only_deployment = ocsci_config.ENV_DATA["mcg_only_deployment"]
    if mcg_only_deployment:
        log.info("Skipping health checks for MCG only mode")
        return
    if dev_mode:
        log.info("Skipping health checks for development mode")
        return

    if ocsci_config.multicluster:
        if (
            ocsci_config.cluster_ctx.MULTICLUSTER["multicluster_index"]
            == ocsci_config.get_active_acm_index()
        ):
            return

    node = request.node

    # ignore ceph health check for the TestFailurePropagator test cases
    if "FailurePropagator" in str(node.cls):
        return

    def finalizer():
        if not skipped:
            multi_storagecluster_external_health_passed = False
            try:
                teardown = ocsci_config.RUN["cli_params"]["teardown"]
                skip_ocs_deployment = ocsci_config.ENV_DATA["skip_ocs_deployment"]
                ceph_cluster_installed = ocsci_config.RUN.get("cephcluster")
                if not (
                    teardown
                    or skip_ocs_deployment
                    or mcg_only_deployment
                    or not ceph_cluster_installed
                ):
                    # We are allowing 20 re-tries for health check, to avoid teardown failures for cases like:
                    # "flip-flopping ceph health OK and warn because of:
                    # HEALTH_WARN Reduced data availability: 2 pgs peering
                    ceph_health_check(
                        namespace=ocsci_config.ENV_DATA["cluster_namespace"]
                    )
                    log.info("Ceph health check passed at teardown!")
                    if ocsci_config.DEPLOYMENT.get("multi_storagecluster"):
                        ceph_health_check_multi_storagecluster_external()
                        log.info(
                            "Ceph health check for multi-storagecluster external cluster passed at teardown!"
                        )
                        multi_storagecluster_external_health_passed = True

            except CephHealthException:
                if not ocsci_config.RUN["skip_reason_test_found"]:
                    squad_name = None
                    for marker in node.iter_markers():
                        if "_squad" in marker.name:
                            squad_name = marker.name
                            break
                    ocsci_config.RUN["skip_reason_test_found"] = {
                        "test_name": node.name,
                        "squad": squad_name,
                    }
                log.info("Ceph health check failed at teardown")
                # Retrying to increase the chance the cluster health will be OK
                # for next test
                ceph_health_check(namespace=ocsci_config.ENV_DATA["cluster_namespace"])

                if (
                    not multi_storagecluster_external_health_passed
                    and ocsci_config.DEPLOYMENT.get("multi_storagecluster")
                ):
                    ceph_health_check_multi_storagecluster_external()
                raise

    request.addfinalizer(finalizer)
    for mark in node.iter_markers():
        if mark.name in tier_marks_name + upgrade_marks_name and ocsci_config.RUN.get(
            "cephcluster"
        ):
            log.info("Checking for Ceph Health OK ")
            external_multi_storagecluster_status = False
            try:
                status = ceph_health_check(
                    namespace=ocsci_config.ENV_DATA["cluster_namespace"],
                    tries=10,
                    delay=15,
                )
                if not ocsci_config.DEPLOYMENT.get("multi_storagecluster"):
                    if status:
                        log.info("Ceph health check passed at setup")
                        return
                else:
                    external_multi_storagecluster_status = (
                        ceph_health_check_multi_storagecluster_external()
                    )
                    if status and external_multi_storagecluster_status:
                        log.info(
                            "Ceph health check passed for internal and multi-storagecluster external at setup"
                        )
                        return
            except CephHealthException:
                ocsci_config.RUN["skipped_tests_ceph_health"] += 1
                skipped = True
                # skip because ceph is not in good health
                pytest.skip("Ceph health check failed at setup")


@pytest.fixture(scope="session", autouse=True)
def cluster(
    request, log_cli_level, record_testsuite_property, set_live_must_gather_images
):
    """
    This fixture initiates deployment for both OCP and OCS clusters.
    Specific platform deployment classes will handle the fine details
    of action
    """
    log.info(f"All logs located at {ocsci_log_path()}")

    teardown = ocsci_config.RUN["cli_params"]["teardown"]
    deploy = ocsci_config.RUN["cli_params"]["deploy"]
    if teardown or deploy:
        factory = dep_factory.DeploymentFactory()
        deployer = factory.get_deployment()

    # Add a finalizer to teardown the cluster after test execution is finished
    if teardown:

        def cluster_teardown_finalizer():
            # If KMS is configured, clean up the backend resources
            # we are doing it before OCP cleanup
            if ocsci_config.DEPLOYMENT.get("kms_deployment"):
                try:
                    kms = KMS.get_kms_deployment()
                    kms.cleanup()
                except Exception as ex:
                    log.error(f"Failed to cleanup KMS. Exception is: {ex}")
            deployer.destroy_cluster(log_cli_level)

        request.addfinalizer(cluster_teardown_finalizer)
        log.info("Will teardown cluster because --teardown was provided")

    # Download client
    if ocsci_config.DEPLOYMENT["skip_download_client"]:
        log.info("Skipping client download")
    else:
        force_download = (
            ocsci_config.RUN["cli_params"].get("deploy")
            and ocsci_config.DEPLOYMENT["force_download_client"]
        )
        get_openshift_client(force_download=force_download)

    # set environment variable for early testing of RHCOS
    if ocsci_config.ENV_DATA.get("early_testing"):
        release_img = ocsci_config.ENV_DATA["RELEASE_IMG"]
        log.info(f"Running early testing of RHCOS with release image: {release_img}")
        os.environ["RELEASE_IMG"] = release_img
        os.environ["OPENSHIFT_INSTALL_RELEASE_IMAGE_OVERRIDE"] = release_img

    if deploy:
        # Deploy cluster
        deployer.deploy_cluster(log_cli_level)
    else:
        if ocsci_config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
            ibmcloud.set_region()
            ibmcloud.login()
    if "cephcluster" not in ocsci_config.RUN.keys():
        check_clusters()
    if not ocsci_config.ENV_DATA["skip_ocs_deployment"] and ocsci_config.RUN.get(
        "cephcluster"
    ):
        record_testsuite_property("rp_ocs_build", get_ocs_build_number())


@pytest.fixture(scope="class")
def environment_checker(request):
    if ocsci_config.RUN["disable_environment_checker"]:
        log.debug("Skipping environment checker")
        return

    node = request.node
    # List of marks for which we will ignore the leftover checker
    marks_to_ignore = [m.mark for m in [deployment, ignore_leftovers]]
    # app labels of resources to be excluded for leftover check
    exclude_labels = [
        constants.must_gather_pod_label,
        constants.S3CLI_APP_LABEL,
        constants.MUST_GATHER_HELPER_LABEL,
    ]
    for mark in node.iter_markers():
        if mark in marks_to_ignore:
            return
        if mark.name == ignore_leftover_label.name:
            exclude_labels.extend(list(mark.args))
    if ocsci_config.ENV_DATA["platform"] in {
        constants.FUSIONAAS_PLATFORM,
        constants.HCI_BAREMETAL,
        constants.HCI_VSPHERE,
    }:
        log.error(
            "Environment checker is NOT IMPLEMENTED for Fusion service and provider/client hci setup."
            "This needs to be updated"
        )
    else:
        request.addfinalizer(
            partial(get_status_after_execution, exclude_labels=exclude_labels)
        )
        get_status_before_execution(exclude_labels=exclude_labels)


@pytest.fixture(scope="class")
def resource_checker(request):
    if not ocsci_config.RUN["resource_checker"]:
        log.debug("Skipping resource checker")
        return

    create_resource_dct()
    node = request.node
    # List of marks for which we will ignore the leftover checker
    marks_to_ignore = [m.mark for m in [deployment, ignore_leftovers]]
    # app labels of resources to be excluded for leftover check
    exclude_labels = [
        constants.must_gather_pod_label,
        constants.S3CLI_APP_LABEL,
        constants.MUST_GATHER_HELPER_LABEL,
    ]
    for mark in node.iter_markers():
        if mark in marks_to_ignore:
            return
        if mark.name == ignore_leftover_label.name:
            exclude_labels.extend(list(mark.args))
    if ocsci_config.ENV_DATA["platform"] in {
        constants.FUSIONAAS_PLATFORM,
        constants.HCI_BAREMETAL,
        constants.HCI_VSPHERE,
    }:
        log.error(
            "Resource checker is NOT IMPLEMENTED for Fusion service and provider/client hci setup."
            "This needs to be updated"
        )
    else:
        request.addfinalizer(
            lambda: get_environment_status_after_execution(
                exclude_labels=exclude_labels
            )
        )


@pytest.fixture(scope="session")
def log_cli_level(pytestconfig):
    """
    Retrieves the log_cli_level set in pytest.ini

    Returns:
        str: log_cli_level set in pytest.ini or DEBUG if not set

    """
    return pytestconfig.getini("log_cli_level") or "DEBUG"


@pytest.fixture(scope="session", autouse=True)
def cluster_load(
    request,
    project_factory_session,
    pvc_factory_session,
    service_account_factory_session,
    pod_factory_session,
    threading_lock,
):
    """
    Run IO during the test execution
    """
    cl_load_obj = None
    io_in_bg = ocsci_config.RUN.get("io_in_bg")
    log_utilization = ocsci_config.RUN.get("log_utilization")
    io_load = ocsci_config.RUN.get("io_load")
    cluster_load_error = None
    cluster_load_error_msg = (
        "Cluster load might not work correctly during this run, because "
        "it failed with an exception: %s"
    )

    # IO load should not happen during deployment
    deployment_test = (
        True if ("deployment" in request.node.items[0].location[0]) else False
    )
    if io_in_bg and not deployment_test:
        io_load = int(io_load) * 0.01
        log.info(wrap_msg("Tests will be running while IO is in the background"))
        log.info(
            "Start running IO in the background. The amount of IO that "
            "will be written is going to be determined by the cluster "
            "capabilities according to its limit"
        )
        try:
            cl_load_obj = ClusterLoad(
                project_factory=project_factory_session,
                sa_factory=service_account_factory_session,
                pvc_factory=pvc_factory_session,
                pod_factory=pod_factory_session,
                target_percentage=io_load,
                threading_lock=threading_lock,
            )
            cl_load_obj.reach_cluster_load_percentage()
        except Exception as ex:
            log.error(cluster_load_error_msg, ex)
            cluster_load_error = ex

    if (log_utilization or io_in_bg) and not deployment_test:
        if not cl_load_obj:
            try:
                cl_load_obj = ClusterLoad(threading_lock=threading_lock)
            except Exception as ex:
                log.error(cluster_load_error_msg, ex)
                cluster_load_error = ex

        ocsci_config.RUN["load_status"] = "running"

        def finalizer():
            """
            Stop the thread that executed watch_load()
            """
            ocsci_config.RUN["load_status"] = "finished"
            if thread:
                thread.join()
            if cluster_load_error:
                raise cluster_load_error

        request.addfinalizer(finalizer)

        def watch_load():
            """
            Watch the cluster load by monitoring the cluster latency.
            Print the cluster utilization metrics every 15 seconds.

            If IOs are running in the test background, dynamically adjust
            the IO load based on the cluster latency.

            """
            while ocsci_config.RUN["load_status"] != "finished":
                time.sleep(20)
                try:
                    cl_load_obj.print_metrics(mute_logs=True)
                    if io_in_bg:
                        if ocsci_config.RUN["load_status"] == "running":
                            cl_load_obj.adjust_load_if_needed()
                        elif ocsci_config.RUN["load_status"] == "to_be_paused":
                            cl_load_obj.reduce_load(pause=True)
                            ocsci_config.RUN["load_status"] = "paused"
                        elif ocsci_config.RUN["load_status"] == "to_be_reduced":
                            cl_load_obj.reduce_load(pause=False)
                            ocsci_config.RUN["load_status"] = "reduced"
                        elif ocsci_config.RUN["load_status"] == "to_be_resumed":
                            cl_load_obj.resume_load()
                            ocsci_config.RUN["load_status"] = "running"

                # Any type of exception should be caught and we should continue.
                # We don't want any test to fail
                except Exception:
                    continue

        thread = threading.Thread(target=watch_load)
        thread.start()


def resume_cluster_load_implementation():
    """
    Resume cluster load implementation

    """
    ocsci_config.RUN["load_status"] = "to_be_resumed"
    try:
        for load_status in TimeoutSampler(300, 3, ocsci_config.RUN.get, "load_status"):
            if load_status == "running":
                break
    except TimeoutExpiredError:
        log.error("Cluster load was not resumed successfully")


def reduce_cluster_load_implementation(request, pause, resume=True):
    """
    Pause/reduce the background cluster load

    Args:
        pause (bool): True for completely pausing the cluster load, False for reducing it by 50%
        resume (bool): True for resuming the cluster load upon teardown, False for not resuming

    """
    if ocsci_config.RUN.get("io_in_bg"):

        def finalizer():
            """
            Resume the cluster load

            """
            if resume:
                resume_cluster_load_implementation()

        request.addfinalizer(finalizer)

        ocsci_config.RUN["load_status"] = "to_be_paused" if pause else "to_be_reduced"
        try:
            for load_status in TimeoutSampler(
                300, 3, ocsci_config.RUN.get, "load_status"
            ):
                if load_status in ["paused", "reduced"]:
                    # Wait for 45 seconds for cluster load to pause/reduce effectively
                    wait_time = 45
                    log.info(
                        f"Waiting for {wait_time} seconds for cluster load to"
                        " pause/reduce..."
                    )
                    time.sleep(wait_time)
                    break
        except TimeoutExpiredError:
            log.error(
                f"Cluster load was not {'paused' if pause else 'reduced'} successfully"
            )


@pytest.fixture()
def pause_cluster_load(request):
    """
    Pause the background cluster load without resuming it

    """
    reduce_cluster_load_implementation(request=request, pause=True, resume=False)


@pytest.fixture()
def resume_cluster_load(request):
    """
    Resume the background cluster load

    """
    if ocsci_config.RUN.get("io_in_bg"):

        def finalizer():
            """
            Resume the cluster load

            """
            resume_cluster_load_implementation()

        request.addfinalizer(finalizer)


@pytest.fixture()
def pause_and_resume_cluster_load(request):
    """
    Pause the background cluster load and resume it in teardown to the original load value

    """
    reduce_cluster_load_implementation(request=request, pause=True)


@pytest.fixture()
def reduce_and_resume_cluster_load(request):
    """
    Reduce the background cluster load to be 50% of what it is and resume the load in teardown
    to the original load value

    """
    reduce_cluster_load_implementation(request=request, pause=False)


@pytest.fixture(
    params=[
        pytest.param({"interface": constants.CEPHBLOCKPOOL}),
        pytest.param({"interface": constants.CEPHFILESYSTEM}),
    ],
    ids=["RBD", "CephFS"],
)
def interface_iterate(request):
    """
    Iterate over interfaces - CephBlockPool and CephFileSystem

    """
    return request.param["interface"]


@pytest.fixture(scope="class")
def multi_pvc_factory_class(project_factory_class, pvc_factory_class):
    return multi_pvc_factory_fixture(project_factory_class, pvc_factory_class)


@pytest.fixture(scope="session")
def multi_pvc_factory_session(project_factory_session, pvc_factory_session):
    return multi_pvc_factory_fixture(project_factory_session, pvc_factory_session)


@pytest.fixture(scope="function")
def multi_pvc_factory(project_factory, pvc_factory):
    return multi_pvc_factory_fixture(project_factory, pvc_factory)


def multi_pvc_factory_fixture(project_factory, pvc_factory):
    """
    Create a Persistent Volume Claims factory. Calling this fixture creates a
    set of new PVCs. Options for PVC creation based on provided assess modes:
    1. For each PVC, choose random value from the list of access modes
    2. Create PVCs based on the specified distribution number of access modes.
       Create sets of PVCs based on the order of access modes.
    3. Create PVCs based on the specified distribution number of access modes.
       The order of PVC creation is independent of access mode.
    """

    def factory(
        interface=constants.CEPHBLOCKPOOL,
        project=None,
        storageclass=None,
        size=None,
        access_modes=None,
        access_modes_selection="distribute_sequential",
        access_mode_dist_ratio=None,
        status=constants.STATUS_BOUND,
        num_of_pvc=1,
        wait_each=False,
        timeout=60,
    ):
        """
        Args:
            interface (str): CephBlockPool or CephFileSystem. This decides
                whether a RBD based or CephFS resource is created.
                RBD is default.
            project (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'Project' kind.
            storageclass (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'StorageClass' kind.
            size (int): The requested size for the PVC
            access_modes (list): List of access modes. One of the access modes
                will be chosen for creating each PVC. If not specified,
                ReadWriteOnce will be selected for all PVCs. To specify
                volume mode, append volume mode in the access mode name
                separated by '-'.
                eg: ['ReadWriteOnce', 'ReadOnlyMany', 'ReadWriteMany',
                'ReadWriteMany-Block']
            access_modes_selection (str): Decides how to select accessMode for
                each PVC from the options given in 'access_modes' list.
                Values are 'select_random', 'distribute_random'
                'select_random' : While creating each PVC, one access mode will
                    be selected from the 'access_modes' list.
                'distribute_random' : The access modes in the list
                    'access_modes' will be distributed based on the values in
                    'distribute_ratio' and the order in which PVCs are created
                    will not be based on the access modes. For example, 1st and
                    6th PVC might have same access mode.
                'distribute_sequential' :The access modes in the list
                    'access_modes' will be distributed based on the values in
                    'distribute_ratio' and the order in which PVCs are created
                    will be as sets of PVCs of same assess mode. For example,
                    first set of 10 will be having same access mode followed by
                    next set of 13 with a different access mode.
            access_mode_dist_ratio (list): Contains the number of PVCs to be
                created for each access mode. If not specified, the given list
                of access modes will be equally distributed among the PVCs.
                eg: [10,12] for num_of_pvc=22 and
                access_modes=['ReadWriteOnce', 'ReadWriteMany']
            status (str): If provided then factory waits for object to reach
                desired state.
            num_of_pvc(int): Number of PVCs to be created
            wait_each(bool): True to wait for each PVC to be in status 'status'
                before creating next PVC, False otherwise
            timeout(int): Time in seconds to wait

        Returns:
            list: objects of PVC class.
        """
        pvc_list = []
        if wait_each:
            status_tmp = status
        else:
            status_tmp = ""

        project = project or project_factory()
        storageclass = storageclass or helpers.default_storage_class(
            interface_type=interface
        )

        access_modes = access_modes or [constants.ACCESS_MODE_RWO]

        access_modes_list = []
        if access_modes_selection == "select_random":
            for _ in range(num_of_pvc):
                mode = random.choice(access_modes)
                access_modes_list.append(mode)

        else:
            if not access_mode_dist_ratio:
                num_of_modes = len(access_modes)
                dist_val = floor(num_of_pvc / num_of_modes)
                access_mode_dist_ratio = [dist_val] * num_of_modes
                access_mode_dist_ratio[-1] = dist_val + (num_of_pvc % num_of_modes)
            zipped_share = list(zip(access_modes, access_mode_dist_ratio))
            for mode, share in zipped_share:
                access_modes_list.extend([mode] * share)

        if access_modes_selection == "distribute_random":
            random.shuffle(access_modes_list)

        for access_mode in access_modes_list:
            if "-" in access_mode:
                access_mode, volume_mode = access_mode.split("-")
            else:
                volume_mode = ""
            pvc_obj = pvc_factory(
                interface=interface,
                project=project,
                storageclass=storageclass,
                size=size,
                access_mode=access_mode,
                status=status_tmp,
                volume_mode=volume_mode,
            )
            pvc_list.append(pvc_obj)
            pvc_obj.project = project
        if status and not wait_each:
            for pvc_obj in pvc_list:
                helpers.wait_for_resource_state(pvc_obj, status, timeout=timeout)
        return pvc_list

    return factory


@pytest.fixture(scope="function")
def memory_leak_function(request):
    """
    Function to start Memory leak thread which will be executed parallel with test run
    Memory leak data will be captured in all worker nodes for ceph-osd process
    Data will be appended in /tmp/(worker)-top-output.txt file for each worker
    During teardown created tmp files will be deleted

    Usage:
        test_case(.., memory_leak_function):
            .....
            median_dict = helpers.get_memory_leak_median_value()
            .....
            TC execution part, memory_leak_fun will capture data
            ....
            helpers.memory_leak_analysis(median_dict)
            ....
    """

    def finalizer():
        """
        Finalizer to stop memory leak data capture thread and cleanup the files
        """
        set_flag_status("terminated")
        try:
            for status in TimeoutSampler(90, 3, get_flag_status):
                if status == "terminated":
                    break
        except TimeoutExpiredError:
            log.warning(
                "Background test execution still in progress before"
                "memory leak thread terminated"
            )
        if thread:
            thread.join()
        log_path = ocsci_log_path()
        for worker in node.get_worker_nodes():
            if os.path.exists(f"/tmp/{worker}-top-output.txt"):
                copyfile(
                    f"/tmp/{worker}-top-output.txt",
                    f"{log_path}/{worker}-top-output.txt",
                )
                os.remove(f"/tmp/{worker}-top-output.txt")
        log.info("Memory leak capture has stopped")

    request.addfinalizer(finalizer)

    temp_file = tempfile.NamedTemporaryFile(
        mode="w+", prefix="test_status", delete=False
    )

    def get_flag_status():
        with open(temp_file.name, "r") as t_file:
            return t_file.readline()

    def set_flag_status(value):
        with open(temp_file.name, "w") as t_file:
            t_file.writelines(value)

    set_flag_status("running")

    def run_memory_leak_in_bg():
        """
        Function to run memory leak in background thread
        Memory leak data is written in below format
        date time PID USER PR NI VIRT RES SHR S %CPU %MEM TIME+ COMMAND
        """
        oc = ocp.OCP(namespace=ocsci_config.ENV_DATA["cluster_namespace"])
        while get_flag_status() == "running":
            for worker in node.get_worker_nodes():
                filename = f"/tmp/{worker}-top-output.txt"
                top_cmd = (
                    "debug"
                    f" nodes/{worker} --to-namespaces={ocsci_config.ENV_DATA['cluster_namespace']} --"
                    " chroot /host top -n 2 b"
                )
                with open("/tmp/file.txt", "w+") as temp:
                    temp.write(
                        str(oc.exec_oc_cmd(command=top_cmd, out_yaml_format=False))
                    )
                    temp.seek(0)
                    for line in temp:
                        if line.__contains__("ceph-osd"):
                            with open(filename, "a+") as f:
                                f.write(str(datetime.now()))
                                f.write(" ")
                                f.write(line)

    log.info("Start memory leak data capture in the test background")
    thread = threading.Thread(target=run_memory_leak_in_bg)
    thread.start()


@pytest.fixture()
def aws_obj():
    """
    Initialize AWS instance

    Returns:
        AWS: An instance of AWS class

    """
    aws_obj = aws.AWS()
    return aws_obj


@pytest.fixture()
def ec2_instances(request, aws_obj):
    """
    Get cluster instances

    Returns:
        dict: The ID keys and the name values of the instances

    """
    # Get all cluster nodes objects
    nodes = node.get_node_objs()

    # Get the cluster nodes ec2 instances
    ec2_instances = aws.get_instances_ids_and_names(nodes)
    assert (
        ec2_instances
    ), f"Failed to get ec2 instances for node {[n.name for n in nodes]}"

    def finalizer():
        """
        Make sure all instances are running
        """
        # Getting the instances that are in status 'stopping' (if there are any), to wait for them to
        # get to status 'stopped' so it will be possible to start them
        stopping_instances = {
            key: val
            for key, val in ec2_instances.items()
            if (aws_obj.get_instances_status_by_id(key) == constants.INSTANCE_STOPPING)
        }

        # Waiting fot the instances that are in status 'stopping'
        # (if there are any) to reach 'stopped'
        if stopping_instances:
            for stopping_instance in stopping_instances:
                instance = aws_obj.get_ec2_instance(stopping_instance.key())
                instance.wait_until_stopped()
        stopped_instances = {
            key: val
            for key, val in ec2_instances.items()
            if (aws_obj.get_instances_status_by_id(key) == constants.INSTANCE_STOPPED)
        }

        # Start the instances
        if stopped_instances:
            aws_obj.start_ec2_instances(instances=stopped_instances, wait=True)

    request.addfinalizer(finalizer)

    return ec2_instances


@pytest.fixture(scope="session")
def cld_mgr(request):
    """
    Returns a cloud manager instance that'll be used throughout the session

    Returns:
        CloudManager: A CloudManager resource

    """
    cld_mgr = CloudManager()

    def finalizer():
        for client in vars(cld_mgr):
            try:
                getattr(cld_mgr, client).secret.delete()
            except AttributeError:
                log.info(f"{client} secret not found")

    request.addfinalizer(finalizer)

    return cld_mgr


@pytest.fixture()
def rgw_obj(request):
    return rgw_obj_fixture(request)


@pytest.fixture(scope="session")
def rgw_obj_session(request):
    return rgw_obj_fixture(request)


def rgw_obj_fixture(request):
    """
    Returns an RGW resource that represents RGW in the cluster

    Returns:
        RGW: An RGW resource
    """
    rgw_deployments = get_deployments_having_label(
        label=constants.RGW_APP_LABEL,
        namespace=ocsci_config.ENV_DATA["cluster_namespace"],
    )
    try:
        storageclass = OCP(
            kind=constants.STORAGECLASS,
            namespace=ocsci_config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RGW,
        ).get()
    except CommandFailed:
        storageclass = None

    if rgw_deployments or storageclass:
        return RGW()
    else:
        return None


@pytest.fixture()
def rgw_deployments(request):
    """
    Return RGW deployments or skip the test.

    """
    rgw_deployments = get_deployments_having_label(
        label=constants.RGW_APP_LABEL,
        namespace=ocsci_config.ENV_DATA["cluster_namespace"],
    )
    if rgw_deployments:
        # Force-skipping in case of IBM Cloud -
        # https://github.com/red-hat-storage/ocs-ci/issues/3863
        if ocsci_config.ENV_DATA["platform"].lower() == constants.IBMCLOUD_PLATFORM:
            pytest.skip(
                "RGW deployments were found, but test will be skipped because of"
                " BZ1926831"
            )
        return rgw_deployments
    else:
        pytest.skip("There is no RGW deployment available for this test.")


@pytest.fixture()
def mcg_obj(request):
    return mcg_obj_fixture(request)


@pytest.fixture(scope="session")
def mcg_obj_session(request):
    return mcg_obj_fixture(request)


def mcg_obj_fixture(request, *args, **kwargs):
    """
    Returns an MCG resource that's connected to the S3 endpoint

    Returns:
        MCG: An MCG resource
    """
    if ocsci_config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        log.warning("As openshift dedicated is used, no MCG resource is returned")
        return None

    mcg_obj = MCG(*args, **kwargs)

    def finalizer():
        if ocsci_config.ENV_DATA["platform"].lower() == "aws":
            mcg_obj.cred_req_obj.delete()

    if kwargs.get("create_aws_creds"):
        request.addfinalizer(finalizer)

    return mcg_obj


@pytest.fixture(scope="session")
def awscli_pod_session(request):
    return awscli_pod_fixture(request, scope_name="session")


@pytest.fixture(scope="session")
def awscli_pod(request, awscli_pod_session):
    # Used as a legacy fixture for backwards compatibility
    # with tests who rely on the function-scope
    # without the need for a wide refactor
    return awscli_pod_session


@pytest.fixture(scope="session")
def awscli_pod_client_session(
    request, project_factory_session, service_account_factory_session
):
    """
    Creates a new AWSCLI pod for relaying commands on a client cluster.

    Args:
        scope_name (str): The name of the fixture's scope,
        used for giving a descriptive name to the pod and configmap

    Returns:
        pod: A pod running the AWS CLI
        int: Index of client cluster where the awscli pod is running

    """
    original_cluster = ocsci_config.cluster_ctx.MULTICLUSTER["multicluster_index"]
    ocsci_config.switch_to_consumer()
    log.info(
        f"Creating namespace {constants.AWSCLI_NAMESPACE} on client for aws cli pod"
    )
    project = project_factory_session(constants.AWSCLI_NAMESPACE)
    log.info("Creating service account on client for aws cli pod")
    sa = service_account_factory_session(project=project)
    client_cluster = ocsci_config.cluster_ctx.MULTICLUSTER["multicluster_index"]
    ocsci_config.switch_ctx(original_cluster)

    def _create_awscli_pod():
        ocsci_config.switch_ctx(client_cluster)
        log.info(f"Switched to client with index {client_cluster}")
        awscli_pod = create_awscli_pod(
            namespace=constants.AWSCLI_NAMESPACE, service_account=sa.name
        )
        ocsci_config.switch_ctx(original_cluster)
        log.info(f"Switched to provider with index {original_cluster}")
        return awscli_pod

    def _awscli_pod_cleanup():
        ocsci_config.switch_ctx(client_cluster)
        log.info(f"Switched to client with index {client_cluster}")
        awscli_pod_cleanup(namespace=constants.AWSCLI_NAMESPACE)
        ocsci_config.switch_ctx(original_cluster)
        log.info(f"Switched to provider with index {original_cluster}")

    request.addfinalizer(_awscli_pod_cleanup)

    log.info("Cleaning up any previous AWS CLI resources on client")
    _awscli_pod_cleanup()
    return _create_awscli_pod(), client_cluster


def awscli_pod_fixture(request, scope_name):
    """
    Creates a new AWSCLI pod for relaying commands

    Args:
        scope_name (str): The name of the fixture's scope,
        used for giving a descriptive name to the pod and configmap

    Returns:
        pod: A pod running the AWS CLI

    """
    project = f"s3cli-{get_random_str()}"
    ocp_obj = ocp.OCP(namespace=project)

    def delete_project(namespace):
        if "openshift" not in namespace:
            ocp_obj.delete_project(project)

    request.addfinalizer(lambda: delete_project(namespace=project))
    request.addfinalizer(lambda: awscli_pod_cleanup(namespace=project))

    ocp_obj.new_project(project)
    ocp.switch_to_default_rook_cluster_project()
    return create_awscli_pod(scope_name, project)


@pytest.fixture(scope="session")
def scale_cli_pod(request):
    return scale_cli_fixture(request, scope_name="session")


def scale_cli_fixture(request, scope_name):
    """
    Creates AWSCLI pod for relaying commands which consists of
    thousands of directories consisting millions of objects under large_objects/
    Args:
        scope_name (str): The name of the fixture's scope,
        used for giving a descriptive name to the pod and configmap

    Returns:
        pod: A pod running the AWS CLI and consisting of large number of objects
    """
    # Create the service-ca configmap to be mounted upon pod creation
    service_ca_data = templating.load_yaml(constants.SCALECLI_SERVICE_CA_YAML)
    service_ca_configmap_name = create_unique_resource_name(
        constants.SCALECLI_SERVICE_CA_CM_NAME, scope_name
    )
    service_ca_data["metadata"]["name"] = service_ca_configmap_name
    log.info("Trying to create the SCALE CLI service CA")
    service_ca_configmap = helpers.create_resource(**service_ca_data)

    # create scale-cli pod
    pod_dict_path = constants.SCALECLI_POD_YAML
    scalecli_pod_dict = templating.load_yaml(pod_dict_path)
    scalecli_pod_dict["spec"]["volumes"][0]["configMap"][
        "name"
    ] = service_ca_configmap_name
    scalecli_pod_name = create_unique_resource_name("scale-cli", scope_name)
    scalecli_pod_dict["metadata"]["name"] = scalecli_pod_name

    update_container_with_mirrored_image(scalecli_pod_dict)
    update_container_with_proxy_env(scalecli_pod_dict)

    scalecli_pod_obj = Pod(**scalecli_pod_dict)
    assert scalecli_pod_obj.create(
        do_reload=True
    ), f"Failed to create pod {scalecli_pod_name}"
    OCP(
        namespace=ocsci_config.ENV_DATA["cluster_namespace"], kind="ConfigMap"
    ).wait_for_resource(
        resource_name=service_ca_configmap.name, column="DATA", condition="1"
    )
    helpers.wait_for_resource_state(
        scalecli_pod_obj, constants.STATUS_RUNNING, timeout=600
    )

    def scalecli_pod_cleanup():
        scalecli_pod_obj.delete()
        service_ca_configmap.delete()

    request.addfinalizer(scalecli_pod_cleanup)

    return scalecli_pod_obj


@pytest.fixture(scope="session")
def javasdk_pod_session(request):
    return javasdk_pod_fixture(request, scope_name="session")


def javasdk_pod_fixture(request, scope_name):
    """
    Creates a new javasdk pod for executing s3 commands through java application
    """
    javas3_pod_dict = templating.load_yaml(constants.JAVA_SDK_S3_POD_YAML)
    javas3_pod_name = create_unique_resource_name(constants.JAVAS3_POD_NAME, scope_name)
    javas3_pod_dict["metadata"]["name"] = javas3_pod_name
    update_container_with_mirrored_image(javas3_pod_dict)
    update_container_with_proxy_env(javas3_pod_dict)
    javas3_pod_obj = Pod(**javas3_pod_dict)

    assert javas3_pod_obj.create(do_reload=True), f"Failed to create {javas3_pod_name}"
    helpers.wait_for_resource_state(javas3_pod_obj, constants.STATUS_RUNNING)

    # push java code to the pod created
    java_src_code_path = constants.JAVA_SRC_CODE_PATH
    target_path = "/app/"
    assert javas3_pod_obj.copy_to_pod_rsync(
        src_path=java_src_code_path, target_path=target_path
    ), "Failed to copy java source code!!"

    def _javas3_pod_cleanup():
        javas3_pod_obj.delete()

    request.addfinalizer(_javas3_pod_cleanup)

    return javas3_pod_obj


@pytest.fixture()
def test_directory_setup(request, awscli_pod_session):
    return test_directory_setup_fixture(request, awscli_pod_session)


def test_directory_setup_fixture(request, awscli_pod_session):
    origin_dir, result_dir = setup_pod_directories(
        awscli_pod_session, ["origin", "result"]
    )
    SetupDirs = namedtuple("SetupDirs", "origin_dir, result_dir")

    def dir_cleanup():
        test_name = get_current_test_name()
        awscli_pod_session.exec_cmd_on_pod(command=f"rm -rf {test_name}")

    request.addfinalizer(dir_cleanup)

    return SetupDirs(origin_dir=origin_dir, result_dir=result_dir)


@pytest.fixture()
def nodes():
    """
    Return an instance of the relevant platform nodes class
    (e.g. AWSNodes, VMWareNodes) to be later used in the test
    for nodes related operations, like nodes restart,
    detach/attach volume, etc.

    """
    factory = platform_nodes.PlatformNodesFactory()
    nodes = factory.get_nodes_platform()
    return nodes


@pytest.fixture()
def nodes_multicluster():
    """
    Return a list of instances of the relevant platform nodes class
    (e.g. AWSNodes, VMWareNodes) to be later used in the test
    for nodes related operations, like nodes restart,
    detach/attach volume, etc. Useful in multicluster scenarios.

    """
    factory = platform_nodes.PlatformNodesFactory()
    nodes_multicluster = []
    for cluster in range(ocsci_config.nclusters):
        ocsci_config.switch_ctx(cluster)
        nodes = factory.get_nodes_platform()
        nodes_multicluster.append(nodes)
    return nodes_multicluster


@pytest.fixture()
def uploaded_objects(request, mcg_obj, awscli_pod, verify_rgw_restart_count):
    return uploaded_objects_fixture(
        request, mcg_obj, awscli_pod, verify_rgw_restart_count
    )


@pytest.fixture(scope="session")
def uploaded_objects_session(
    request, mcg_obj_session, awscli_pod_session, verify_rgw_restart_count_session
):
    return uploaded_objects_fixture(
        request, mcg_obj_session, awscli_pod_session, verify_rgw_restart_count_session
    )


def uploaded_objects_fixture(request, mcg_obj, awscli_pod, verify_rgw_restart_count):
    """
    Deletes all objects that were created as part of the test

    Args:
        mcg_obj (MCG): An MCG object containing the MCG S3 connection
            credentials
        awscli_pod (Pod): A pod running the AWSCLI tools

    Returns:
        list: An empty list of objects

    """

    uploaded_objects_paths = []

    def object_cleanup():
        for uploaded_filename in uploaded_objects_paths:
            log.info(f"Deleting object {uploaded_filename}")
            awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command("rm " + uploaded_filename, mcg_obj),
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_internal_endpoint,
                ],
            )

    request.addfinalizer(object_cleanup)
    return uploaded_objects_paths


@pytest.fixture()
def verify_rgw_restart_count(request):
    return verify_rgw_restart_count_fixture(request)


@pytest.fixture(scope="session")
def verify_rgw_restart_count_session(request):
    return verify_rgw_restart_count_fixture(request)


def verify_rgw_restart_count_fixture(request):
    """
    Verifies the RGW restart count at start and end of a test
    """
    if ocsci_config.ENV_DATA["platform"].lower() in constants.ON_PREM_PLATFORMS:
        log.info("Getting RGW pod restart count before executing the test")
        initial_counts = get_rgw_restart_counts()

        def finalizer():
            rgw_pods = get_rgw_pods()
            for rgw_pod in rgw_pods:
                rgw_pod.reload()
            log.info("Verifying whether RGW pods changed after executing the test")
            for rgw_pod in rgw_pods:
                assert rgw_pod.restart_count in initial_counts, "RGW pod restarted"

        request.addfinalizer(finalizer)


@pytest.fixture()
def rgw_bucket_factory(request, rgw_obj):
    if rgw_obj:
        return bucket_factory_fixture(request, rgw_obj=rgw_obj)
    else:
        return None


@pytest.fixture(scope="session")
def rgw_bucket_factory_session(request, rgw_obj_session):
    if rgw_obj_session:
        return bucket_factory_fixture(request, rgw_obj=rgw_obj_session)
    else:
        return None


@pytest.fixture()
def bucket_factory(request, bucket_class_factory, mcg_obj):
    """
    Returns an MCG bucket factory.
    If MCG object not found returns None
    """
    if mcg_obj:
        return bucket_factory_fixture(request, bucket_class_factory, mcg_obj)
    else:
        return None


@pytest.fixture(scope="session")
def bucket_factory_session(request, bucket_class_factory_session, mcg_obj_session):
    """
    Returns a session-scoped MCG bucket factory.
    If session-scoped MCG object not found returns None
    """
    if mcg_obj_session:
        return bucket_factory_fixture(
            request, bucket_class_factory_session, mcg_obj_session
        )
    else:
        return None


def bucket_factory_fixture(
    request, bucket_class_factory=None, mcg_obj=None, rgw_obj=None
):
    """
    Create a bucket factory. Calling this fixture creates a new bucket(s).
    For a custom amount, provide the 'amount' parameter.

    ***Please note***
    Creation of buckets by utilizing the S3 interface *does not* support bucketclasses.
    Only OC/CLI buckets can support different bucketclasses.
    By default, all S3 buckets utilize the default bucketclass.

    Args:
        bucket_class_factory: creates a new Bucket Class
        mcg_obj (MCG): An MCG object containing the MCG S3 connection
            credentials
        rgw_obj (RGW): An RGW object

    """
    created_buckets = []

    def _create_buckets(
        amount=1,
        interface="S3",
        verify_health=True,
        bucketclass=None,
        replication_policy=None,
        *args,
        **kwargs,
    ):
        """
        Creates and deletes all buckets that were created as part of the test

        Args:
            amount (int): The amount of buckets to create
            interface (str): The interface to use for creation of buckets.
                S3 | OC | CLI | NAMESPACE
            verify_Health (bool): Whether to verify the created bucket's health
                post-creation
            bucketclass (dict): A dictionary describing a new
                bucketclass to be created.
                When None, the default bucketclass is used.

        Returns:
            list: A list of s3.Bucket objects, containing all the created
                buckets

        """
        if bucketclass:
            interface = bucketclass["interface"]

        current_call_created_buckets = []
        if interface.lower() not in BUCKET_MAP:
            raise RuntimeError(
                f"Invalid interface type received: {interface}. "
                f'available types: {", ".join(BUCKET_MAP.keys())}'
            )

        bucketclass = (
            bucketclass if bucketclass is None else bucket_class_factory(bucketclass)
        )

        for _ in range(amount):
            bucket_name = helpers.create_unique_resource_name(
                resource_description="bucket", resource_type=interface.lower()
            )
            created_bucket = BUCKET_MAP[interface.lower()](
                bucket_name,
                mcg=mcg_obj,
                rgw=rgw_obj,
                bucketclass=bucketclass,
                replication_policy=replication_policy,
                *args,
                **kwargs,
            )
            current_call_created_buckets.append(created_bucket)
            created_buckets.append(created_bucket)
            if verify_health:
                created_bucket.verify_health(
                    timeout=kwargs.pop("timeout") if "timeout" in kwargs else 180,
                    **kwargs,
                )

        return current_call_created_buckets

    def bucket_cleanup():
        for bucket in created_buckets:
            log.info(f"Cleaning up bucket {bucket.name}")
            try:
                bucket.delete()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchBucket":
                    log.warning(f"{bucket.name} could not be found in cleanup")
                else:
                    raise

    request.addfinalizer(bucket_cleanup)

    return _create_buckets


@pytest.fixture(scope="class")
def cloud_uls_factory(request, cld_mgr):
    """
    Create an Underlying Storage factory.
    Calling this fixture creates a new underlying storage(s).

    Returns:
       func: Factory method - each call to this function creates
           an Underlying Storage factory

    """
    return cloud_uls_factory_implementation(request, cld_mgr)


@pytest.fixture(scope="session")
def cloud_uls_factory_session(request, cld_mgr):
    """
    Create an Underlying Storage factory.
    Calling this fixture creates a new underlying storage(s).

    Returns:
       func: Factory method - each call to this function creates
           an Underlying Storage factory

    """
    return cloud_uls_factory_implementation(request, cld_mgr)


@pytest.fixture(scope="function")
def mcg_job_factory(request, bucket_factory, project_factory, mcg_obj, tmp_path):
    """
    Create a Job factory.
    Calling this fixture creates a new Job(s) that utilize MCG bucket.

    Returns:
        func: Factory method - each call to this function creates
            a job

    """
    return mcg_job_factory_implementation(
        request, bucket_factory, project_factory, mcg_obj, tmp_path
    )


@pytest.fixture(scope="session")
def mcg_job_factory_session(
    request, bucket_factory_session, project_factory_session, mcg_obj_session, tmp_path
):
    """
    Create a Job factory.
    Calling this fixture creates a new Job(s) that utilize MCG bucket.

    Returns:
        func: Factory method - each call to this function creates
            a job

    """
    return mcg_job_factory_implementation(
        request,
        bucket_factory_session,
        project_factory_session,
        mcg_obj_session,
        tmp_path,
    )


@pytest.fixture()
def backingstore_factory(request, cld_mgr, mcg_obj, cloud_uls_factory):
    """
    Create a Backing Store factory.
    Calling this fixture creates a new Backing Store(s).

    Returns:
        func: Factory method - each call to this function creates
            a backingstore
        None: If MCG object not found

    """
    if mcg_obj:
        return backingstore_factory_implementation(
            request, cld_mgr, mcg_obj, cloud_uls_factory
        )
    else:
        return None


@pytest.fixture(scope="session")
def backingstore_factory_session(
    request, cld_mgr, mcg_obj_session, cloud_uls_factory_session
):
    """
    Create a Backing Store factory.
    Calling this fixture creates a new Backing Store(s).

    Returns:
        func: Factory method - each call to this function creates
            a backingstore
        None: If session-scoped MCG object not found

    """
    if mcg_obj_session:
        return backingstore_factory_implementation(
            request, cld_mgr, mcg_obj_session, cloud_uls_factory_session
        )
    else:
        return None


@pytest.fixture()
def bucket_class_factory(
    request, mcg_obj, backingstore_factory, namespace_store_factory
):
    """
    Create a Bucket Class factory.
    Calling this fixture creates a new Bucket Class.

    Returns:
        func: Factory method - each call to this function creates
            a bucketclass
        None: If MCG object not found

    """
    if mcg_obj:
        return bucketclass_factory_implementation(
            request, mcg_obj, backingstore_factory, namespace_store_factory
        )
    else:
        return None


@pytest.fixture(scope="session")
def bucket_class_factory_session(
    request,
    mcg_obj_session,
    backingstore_factory_session,
    namespace_store_factory_session,
):
    """
    Create a Bucket Class factory.
    Calling this fixture creates a new Bucket Class.

    Returns:
        func: Factory method - each call to this function creates
            a bucketclass
        None: If session-scoped MCG object not found

    """
    if mcg_obj_session:
        return bucketclass_factory_implementation(
            request,
            mcg_obj_session,
            backingstore_factory_session,
            namespace_store_factory_session,
        )
    else:
        return None


@pytest.fixture()
def multiregion_mirror_setup(bucket_factory):
    return multiregion_mirror_setup_fixture(bucket_factory)


@pytest.fixture(scope="session")
def multiregion_mirror_setup_session(bucket_factory_session):
    return multiregion_mirror_setup_fixture(bucket_factory_session)


def multiregion_mirror_setup_fixture(bucket_factory):
    # Setup
    # Todo:
    #  add region and amount parametrization - note that `us-east-1`
    #  will cause an error as it is the default region. If usage of `us-east-1`
    #  needs to be tested, keep the 'region' field out.

    bucketclass = {
        "interface": "CLI",
        "backingstore_dict": {"aws": [(1, "us-west-1"), (1, "us-east-2")]},
        "placement_policy": "Mirror",
    }

    # Create a NooBucket that'll use the bucket class in order to test
    # the mirroring policy
    bucket = bucket_factory(1, "OC", bucketclass=bucketclass)[0]

    return bucket, bucket.bucketclass.backingstores


@pytest.fixture(scope="session")
def default_storageclasses(request, teardown_factory_session):
    """
    Returns dictionary with storageclasses. Keys represent reclaim policy of
    storageclass. There are two storageclasses for each key. First is RBD based
    and the second one is CephFS based. Storageclasses with Retain Reclaim
    Policy are created from default storageclasses.
    """
    scs = {constants.RECLAIM_POLICY_DELETE: [], constants.RECLAIM_POLICY_RETAIN: []}

    # TODO(fbalak): Use proper constants after
    # https://github.com/red-hat-storage/ocs-ci/issues/1056
    # is resolved
    storage_classes = []
    if ocsci_config.ENV_DATA.get("custom_default_storageclass_names"):
        from ocs_ci.ocs.resources.storage_cluster import (
            get_storageclass_names_from_storagecluster_spec,
        )

        sc_from_spec = get_storageclass_names_from_storagecluster_spec()
        for component_key in [
            constants.OCS_COMPONENTS_MAP["blockpools"],
            constants.OCS_COMPONENTS_MAP["cephfs"],
        ]:
            sc_name = sc_from_spec.get(component_key)
            if sc_name:
                storage_classes.append(sc_name)
            else:
                raise ValueError(
                    f"Custome Storageclass is not defined from {component_key} in storagecluster spec."
                )
    else:
        storage_classes = [constants.CEPHBLOCKPOOL_SC, constants.CEPHFILESYSTEM_SC]

    for sc_name in storage_classes:
        sc = OCS(kind=constants.STORAGECLASS, metadata={"name": sc_name})
        sc.reload()
        scs[constants.RECLAIM_POLICY_DELETE].append(sc)
        sc.data["reclaimPolicy"] = constants.RECLAIM_POLICY_RETAIN
        sc.data["metadata"]["name"] += "-retain"
        sc._name = sc.data["metadata"]["name"]
        sc.create()
        teardown_factory_session(sc)
        scs[constants.RECLAIM_POLICY_RETAIN].append(sc)
    return scs


@pytest.fixture(scope="class")
def install_logging(request):
    """
    Setup and teardown
    * The setup will deploy openshift-logging in the cluster
    * The teardown will uninstall cluster-logging from the cluster

    """

    def finalizer():
        uninstall_cluster_logging()

    request.addfinalizer(finalizer)

    sub = ocp.OCP(
        kind=constants.SUBSCRIPTION,
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE,
    )
    logging_sub = sub.get().get("items")
    if logging_sub:
        log.info("Logging is already configured, Skipping Installation")
        return

    log.info("Configuring Openshift-logging")

    # Gets OCP version to align logging version to OCP version
    ocp_version = version.get_semantic_ocp_version_from_config()

    logging_channel = "stable" if ocp_version >= version.VERSION_4_7 else ocp_version

    # Creates namespace openshift-operators-redhat
    ocp_logging_obj.create_namespace(yaml_file=constants.EO_NAMESPACE_YAML)

    # Creates an operator-group for elasticsearch
    assert ocp_logging_obj.create_elasticsearch_operator_group(
        yaml_file=constants.EO_OG_YAML, resource_name="openshift-operators-redhat"
    )

    # Set RBAC policy on the project
    assert ocp_logging_obj.set_rbac(
        yaml_file=constants.EO_RBAC_YAML, resource_name="prometheus-k8s"
    )

    # Creates subscription for elastic-search operator
    subscription_yaml = templating.load_yaml(constants.EO_SUB_YAML)
    subscription_yaml["spec"]["channel"] = logging_channel
    helpers.create_resource(**subscription_yaml)
    assert ocp_logging_obj.get_elasticsearch_subscription()

    # Checks for Elasticsearch operator
    elastic_search_operator = OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE
    )
    elastic_search_operator.wait_for_resource(
        resource_count=1, condition=constants.STATUS_RUNNING, timeout=200, sleep=20
    )

    # Creates a namespace openshift-logging
    ocp_logging_obj.create_namespace(yaml_file=constants.CL_NAMESPACE_YAML)

    # Creates an operator-group for cluster-logging
    assert ocp_logging_obj.create_clusterlogging_operator_group(
        yaml_file=constants.CL_OG_YAML
    )

    # Creates subscription for cluster-logging
    cl_subscription = templating.load_yaml(constants.CL_SUB_YAML)
    cl_subscription["spec"]["channel"] = logging_channel
    helpers.create_resource(**cl_subscription)
    assert ocp_logging_obj.get_clusterlogging_subscription()

    # Creates instance in namespace openshift-logging
    cluster_logging_operator = OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    cluster_logging_operator.wait_for_resource(
        resource_count=1, condition=constants.STATUS_RUNNING, timeout=200, sleep=20
    )
    if cluster_logging_operator:
        log.info(f"The cluster-logging-operator {cluster_logging_operator.get()}")
        ocp_logging_obj.create_instance()
    else:
        log.error("The cluster logging operator pod is not created")


@pytest.fixture
def fio_pvc_dict():
    """
    PVC template for fio workloads.
    Note that all 'None' values needs to be defined before usage.

    """
    return fio_artefacts.get_pvc_dict()


@pytest.fixture(scope="session")
def fio_pvc_dict_session():
    """
    PVC template for fio workloads.
    Note that all 'None' values needs to be defined before usage.

    """
    return fio_artefacts.get_pvc_dict()


@pytest.fixture
def fio_configmap_dict():
    """
    ConfigMap template for fio workloads.
    Note that you need to add actual configuration to workload.fio file.

    """
    return fio_artefacts.get_configmap_dict()


@pytest.fixture(scope="session")
def fio_configmap_dict_session():
    """
    ConfigMap template for fio workloads.
    Note that you need to add actual configuration to workload.fio file.

    """
    return fio_artefacts.get_configmap_dict()


@pytest.fixture
def fio_job_dict():
    """
    Job template for fio workloads.

    """
    return fio_artefacts.get_job_dict()


@pytest.fixture(scope="session")
def fio_job_dict_session():
    """
    Job template for fio workloads.
    """
    return fio_artefacts.get_job_dict()


@pytest.fixture(scope="function")
def start_apps_workload(request):
    """
    Application workload fixture which reads the list of app workloads to run and
    starts running those iterating over the workloads in the list for a specified
    duration

    Usage:
    start_app_workload(workloads_list=['pgsql', 'couchbase', 'cosbench'], run_time=60,
    run_in_bg=True)
    """
    return start_app_workload(request)


@pytest.fixture(scope="function")
def pgsql_factory_fixture(request):
    """
    Pgsql factory fixture
    """
    pgsql = Postgresql()

    def factory(
        replicas,
        clients=None,
        threads=None,
        transactions=None,
        scaling_factor=None,
        samples=None,
        timeout=None,
        sc_name=None,
        wait_for_pgbench_to_complete=True,
    ):
        """
        Factory to start pgsql workload

        Args:
            replicas (int): Number of pgbench pods to be deployed
            clients (int): Number of clients
            threads (int): Number of threads
            transactions (int): Number of transactions
            scaling_factor (int): scaling factor
            samples (int): Number of samples to run
            timeout (int): Time in seconds to wait
            wait_for_pgbench_to_complete (bool): If set to True, the fixture will wait for the pgbench run to complete

        """
        # Setup postgres
        pgsql.setup_postgresql(replicas=replicas, sc_name=sc_name)

        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(
            replicas=replicas,
            clients=clients,
            threads=threads,
            transactions=transactions,
            scaling_factor=scaling_factor,
            samples=samples,
            timeout=timeout,
        )

        if wait_for_pgbench_to_complete:
            # Wait for pg_bench pod to initialized and complete
            pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

            # Get pgbench pods
            pgbench_pods = pgsql.get_pgbench_pods()

            # Validate pgbench run and parse logs
            pgsql.validate_pgbench_run(pgbench_pods)
        return pgsql

    def finalizer():
        """
        Clean up
        """
        pgsql.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="function")
def jenkins_factory_fixture(request):
    """
    Jenkins factory fixture
    """
    jenkins = Jenkins()

    def factory(num_projects=1, num_of_builds=1, wait_for_build_to_complete=True):
        """
        Factory to start jenkins workload

        Args:
            num_projects (int): Number of Jenkins projects
            num_of_builds (int): Number of builds per project
            wait_for_build_to_complete (bool): If set to True, the fixture will wait for the Jenkins build to complete

        """
        # Jenkins template
        jenkins.create_ocs_jenkins_template()
        # Init number of projects
        jenkins.number_projects = num_projects
        # Create app jenkins
        jenkins.create_app_jenkins()
        # Create jenkins pvc
        jenkins.create_jenkins_pvc()
        # Create jenkins build config
        jenkins.create_jenkins_build_config()
        # Wait jenkins deploy pod reach to completed state
        jenkins.wait_for_jenkins_deploy_status(status=constants.STATUS_COMPLETED)
        # Init number of builds per project
        jenkins.number_builds_per_project = num_of_builds
        # Start Builds
        jenkins.start_build()
        if wait_for_build_to_complete:
            # Wait build reach 'Complete' state
            jenkins.wait_for_build_to_complete()
            # Print table of builds
            jenkins.print_completed_builds_results()

        return jenkins

    def finalizer():
        """
        Clean up
        """
        jenkins.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="function")
def couchbase_factory_fixture(request):
    """
    Couchbase factory fixture using Couchbase operator
    """
    couchbase = CouchBase()

    def factory(
        replicas=3,
        run_in_bg=False,
        skip_analyze=True,
        sc_name=None,
        num_items=None,
        num_threads=None,
        wait_for_pillowfights_to_complete=True,
    ):
        """
        Factory to start couchbase workload

        Args:
            replicas (int): Number of couchbase workers to be deployed
            run_in_bg (bool): Run IOs in background as option
            skip_analyze (bool): Skip logs analysis as option
            wait_for_pillowfight_to_complete (bool): If set to True, the fixture will wait for the pillowfight
            workload to reach compelete state

        """
        # Create Couchbase subscription
        couchbase.couchbase_subscription()
        # Create Couchbase worker secrets
        couchbase.create_cb_secrets()
        # Create couchbase workers
        couchbase.create_cb_cluster(replicas=3, sc_name=sc_name)
        couchbase.create_data_buckets()
        # adding wait for the buckets created to be reconciled with the couchbase cluster
        time.sleep(10)
        # Run couchbase workload
        couchbase.run_workload(
            replicas=replicas,
            run_in_bg=run_in_bg,
            num_items=num_items,
            num_threads=num_threads,
        )
        if wait_for_pillowfights_to_complete:
            couchbase.wait_for_pillowfights_to_complete()
        # Run sanity check on data logs
        couchbase.analyze_run(skip_analyze=skip_analyze)

        return couchbase

    def finalizer():
        """
        Clean up
        """
        couchbase.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="function")
def amq_factory_fixture(request):
    """
    AMQ factory fixture
    """
    amq = AMQ()

    def factory(
        sc_name,
        kafka_namespace=constants.AMQ_NAMESPACE,
        size=100,
        replicas=3,
        topic_name="my-topic",
        user_name="my-user",
        partitions=1,
        topic_replicas=1,
        num_of_producer_pods=1,
        num_of_consumer_pods=1,
        value="10000",
        since_time=1800,
        run_in_bg=True,
        validate_messages=True,
    ):
        """
        Factory to start amq workload

        Args:
            sc_name (str): Name of storage clase
            kafka_namespace (str): Namespace where kafka cluster to be created
            size (int): Size of the storage
            replicas (int): Number of kafka and zookeeper pods to be created
            topic_name (str): Name of the topic to be created
            user_name (str): Name of the user to be created
            partitions (int): Number of partitions of topic
            topic_replicas (int): Number of replicas of topic
            num_of_producer_pods (int): Number of producer pods to be created
            num_of_consumer_pods (int): Number of consumer pods to be created
            value (str): Number of messages to be sent and received
            since_time (int): Number of seconds to required to sent the msg
            run_in_bg (bool): If set to True, validate of messages are done in a bg job
            validate_messages (bool): If set to True, the fixture will validate that all the messages are
            sent and recieved to Producer and Consumer Pods respectively.

        """
        if run_in_bg and not validate_messages:
            raise Exception(
                "run_in_bg is not allowed to call when validate_messages is set to"
                " False"
            )
        # Setup kafka cluster
        amq.setup_amq_cluster(
            sc_name=sc_name, namespace=kafka_namespace, size=size, replicas=replicas
        )

        # Run open messages
        amq.create_messaging_on_amq(
            topic_name=topic_name,
            user_name=user_name,
            partitions=partitions,
            replicas=topic_replicas,
            num_of_producer_pods=num_of_producer_pods,
            num_of_consumer_pods=num_of_consumer_pods,
            value=value,
        )

        # Wait for some time to generate msg
        waiting_time = 60
        log.info(f"Waiting for {waiting_time}sec to generate msg")
        time.sleep(waiting_time)

        if validate_messages:
            if run_in_bg:
                # Check messages are sent and received
                threads = amq.run_in_bg(
                    namespace=kafka_namespace, value=value, since_time=since_time
                )
                return amq, threads
            else:
                amq.validate_messages_are_produced()
                amq.validate_messages_are_consumed()

        return amq

    def finalizer():
        """
        Clean up

        """
        # Clean up
        amq.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture
def measurement_dir(tmp_path):
    """
    Returns directory path where should be stored all results related
    to measurement. If 'measurement_dir' is provided by config then use it,
    otherwise new directory is generated.

    Returns:
        str: Path to measurement directory
    """
    if ocsci_config.ENV_DATA.get("measurement_dir"):
        measurement_dir = ocsci_config.ENV_DATA.get("measurement_dir")
        log.info(f"Using measurement dir from configuration: {measurement_dir}")
    else:
        measurement_dir = os.path.join(os.path.dirname(tmp_path), "measurement_results")
    if not os.path.exists(measurement_dir):
        log.info(f"Measurement dir {measurement_dir} doesn't exist. Creating it.")
        os.mkdir(measurement_dir)
    return measurement_dir


@pytest.fixture()
def multi_dc_pod(multi_pvc_factory, dc_pod_factory, service_account_factory):
    """
    Prepare multiple dc pods for the test
    Returns:
        any: Pod instances
    """

    def factory(
        num_of_pvcs=1,
        pvc_size=100,
        project=None,
        access_mode="RWO",
        pool_type="rbd",
        timeout=60,
    ):
        dict_modes = {
            "RWO": "ReadWriteOnce",
            "RWX": "ReadWriteMany",
            "RWX-BLK": "ReadWriteMany-Block",
        }
        dict_types = {"rbd": "CephBlockPool", "cephfs": "CephFileSystem"}

        if access_mode in "RWX-BLK" and pool_type in "rbd":
            modes = dict_modes["RWX-BLK"]
            create_rbd_block_rwx_pod = True
        else:
            modes = dict_modes[access_mode]
            create_rbd_block_rwx_pod = False

        pvc_objs = multi_pvc_factory(
            interface=dict_types[pool_type],
            access_modes=[modes],
            size=pvc_size,
            num_of_pvc=num_of_pvcs,
            project=project,
            timeout=timeout,
        )
        dc_pods = []
        dc_pods_res = []
        sa_obj = service_account_factory(project=project)
        with ThreadPoolExecutor() as p:
            for pvc_obj in pvc_objs:
                if create_rbd_block_rwx_pod:
                    dc_pods_res.append(
                        p.submit(
                            dc_pod_factory,
                            interface=constants.CEPHBLOCKPOOL,
                            pvc=pvc_obj,
                            raw_block_pv=True,
                            sa_obj=sa_obj,
                        )
                    )
                else:
                    dc_pods_res.append(
                        p.submit(
                            dc_pod_factory,
                            interface=dict_types[pool_type],
                            pvc=pvc_obj,
                            sa_obj=sa_obj,
                        )
                    )

        for dc in dc_pods_res:
            pod_obj = dc.result()
            if create_rbd_block_rwx_pod:
                log.info(
                    "#### setting attribute pod_type since "
                    f"create_rbd_block_rwx_pod = {create_rbd_block_rwx_pod}"
                )
                setattr(pod_obj, "pod_type", "rbd_block_rwx")
            else:
                setattr(pod_obj, "pod_type", "")
            dc_pods.append(pod_obj)

        with ThreadPoolExecutor() as p:
            for dc in dc_pods:
                p.submit(
                    helpers.wait_for_resource_state,
                    resource=dc,
                    state=constants.STATUS_RUNNING,
                    timeout=120,
                )

        return dc_pods

    return factory


@pytest.fixture(scope="session")
def htpasswd_path(tmpdir_factory):
    """
    Returns:
        string: Path to HTPasswd file with additional usernames

    """
    return str(tmpdir_factory.mktemp("idp_data").join("users.htpasswd"))


@pytest.fixture(scope="session")
def htpasswd_identity_provider(request):
    """
    Creates HTPasswd Identity provider.

    Returns:
        object: OCS object representing OCP OAuth object with HTPasswd IdP

    """
    users.create_htpasswd_idp()
    cluster = OCS(kind=constants.OAUTH, metadata={"name": "cluster"})
    cluster.reload()

    def finalizer():
        """
        Remove HTPasswd IdP

        """
        # TODO(fbalak): remove HTPasswd identityProvider
        # cluster.ocp.patch(
        #     resource_name='cluster',
        #     params=f'[{ "op": "remove", "path": "/spec/identityProviders" }]'
        # )
        # users.delete_htpasswd_secret()

    request.addfinalizer(finalizer)
    return cluster


@pytest.fixture(scope="function")
def user_factory(request, htpasswd_identity_provider, htpasswd_path):
    return users.user_factory(request, htpasswd_path)


@pytest.fixture(scope="session")
def user_factory_session(request, htpasswd_identity_provider, htpasswd_path):
    return users.user_factory(request, htpasswd_path)


@pytest.fixture(autouse=True)
def log_alerts(request, threading_lock):
    """
    Log alerts at the beginning and end of each test case. At the end of test
    case print a difference: what new alerts are in place after the test is
    complete.

    """
    teardown = ocsci_config.RUN["cli_params"].get("teardown")
    dev_mode = ocsci_config.RUN["cli_params"].get("dev_mode")
    if teardown:
        return
    elif dev_mode:
        log.info("Skipping alert check for development mode")
        return

    alerts_before = []
    prometheus = None

    try:
        prometheus = PrometheusAPI(threading_lock=threading_lock)
    except Exception:
        log.exception("There was a problem with connecting to Prometheus")

    def _collect_alerts():
        try:
            alerts_response = prometheus.get(
                "alerts", payload={"silenced": False, "inhibited": False}
            )
            if alerts_response.ok:
                alerts = alerts_response.json().get("data").get("alerts")
                log.debug(f"Found alerts: {alerts}")
                return alerts
            else:
                log.warning(
                    "There was a problem with collecting alerts for analysis:"
                    f" {alerts_response.text}"
                )
                return False
        except Exception:
            log.exception("There was a problem with collecting alerts for analysis")
            return False

    def _print_diff():
        if alerts_before:
            alerts_after = _collect_alerts()
            if alerts_after:
                alerts_new = [
                    alert for alert in alerts_after if alert not in alerts_before
                ]
                if alerts_new:
                    log.warning("During test were raised new alerts")
                    log.warning(alerts_new)

    alerts_before = _collect_alerts()
    request.addfinalizer(_print_diff)


@pytest.fixture(scope="session", autouse=True)
def ceph_toolbox(request):
    """
    This fixture initiates ceph toolbox pod for manually created deployment
    and if it does not already exist.
    """
    teardown = ocsci_config.RUN["cli_params"].get("teardown")
    if "cephcluster" not in ocsci_config.RUN.keys() and not teardown:
        check_clusters()
    deploy = ocsci_config.RUN["cli_params"]["deploy"]
    skip_ocs = ocsci_config.ENV_DATA["skip_ocs_deployment"]
    ceph_cluster = ocsci_config.RUN.get("cephcluster")
    no_ocs = ceph_cluster or skip_ocs
    deploy_teardown = deploy or teardown
    managed_platform = (
        ocsci_config.ENV_DATA["platform"].lower()
        == constants.OPENSHIFT_DEDICATED_PLATFORM
        or ocsci_config.ENV_DATA["platform"].lower() == constants.ROSA_PLATFORM
        or ocsci_config.ENV_DATA["platform"].lower() == constants.FUSIONAAS_PLATFORM
    )
    if not (deploy_teardown or not no_ocs) or (
        managed_platform and not deploy_teardown
    ):
        try:
            # Creating toolbox pod
            setup_ceph_toolbox()
        except CommandFailed:
            log.info("Failed to create toolbox")


@pytest.fixture(scope="function")
def node_drain_teardown(request):
    """
    Tear down function after Node drain

    """

    def finalizer():
        """
        Make sure that all cluster's nodes are in 'Ready' state and if not,
        change them back to 'Ready' state by marking them as schedulable

        """
        scheduling_disabled_nodes = [
            n.name
            for n in get_node_objs()
            if n.ocp.get_resource_status(n.name)
            == constants.NODE_READY_SCHEDULING_DISABLED
        ]
        if scheduling_disabled_nodes:
            schedule_nodes(scheduling_disabled_nodes)
        ceph_health_check(tries=60)

    request.addfinalizer(finalizer)


@pytest.fixture(scope="function")
def node_restart_teardown(request, nodes_multicluster, nodes):
    """
    Make sure all nodes are up and in 'Ready' state and if not,
    try to make them 'Ready' by restarting the nodes.

    """
    cluster_node_objs = []
    for index in range(ocsci_config.nclusters):
        ocsci_config.switch_ctx(index)
        cluster_node_objs.append(get_node_objs())

    def finalizer():
        for index in range(ocsci_config.nclusters):
            ocsci_config.switch_ctx(index)
            # Start the powered off nodes
            try:
                nodes_multicluster[index].restart_nodes_by_stop_and_start_teardown()
            except CommandFailed:
                nodes_multicluster[index].start_nodes(cluster_node_objs[index])
                ocp.wait_for_cluster_connectivity()

            # Restart the nodes if in NotReady state
            try:
                node.wait_for_nodes_status(status=constants.NODE_READY)
            except ResourceWrongStatusException:
                not_ready_nodes = [
                    n
                    for n in node.get_node_objs()
                    if n.ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
                ]
                if not_ready_nodes:
                    log.info(
                        "Nodes in NotReady status found:"
                        f" {[n.name for n in not_ready_nodes]}"
                    )
                    nodes.restart_nodes_by_stop_and_start(not_ready_nodes)
                    node.wait_for_nodes_status(status=constants.NODE_READY)

    request.addfinalizer(finalizer)


@pytest.fixture()
def mcg_connection_factory(request, mcg_obj, cld_mgr):
    """
    Create a new MCG connection for given platform. If there already exists
    a connection for the platform then return this previously created
    connection.

    """
    created_connections = {}

    def _create_connection(platform=constants.AWS_PLATFORM, name=None):
        """
        Args:
            platform (str): Platform used for connection
            name (str): New connection name. If not provided then new name will
                be generated. New name will be used only if there is not
                existing connection for given platform

        Returns:
            str: connection name

        """
        if platform not in created_connections:
            connection_name = name or create_unique_resource_name(
                constants.MCG_CONNECTION, platform
            )
            mcg_obj.create_connection(cld_mgr, platform, connection_name)
            created_connections[platform] = connection_name
        return created_connections[platform]

    def _connections_cleanup():
        for platform in created_connections:
            mcg_obj.delete_ns_connection(created_connections[platform])

    request.addfinalizer(_connections_cleanup)

    return _create_connection


@pytest.fixture()
def ns_resource_factory(
    request, mcg_obj, cld_mgr, cloud_uls_factory, mcg_connection_factory
):
    """
    Create a namespace resource factory. Calling this fixture creates a new namespace resource.

    """
    created_ns_resources = []

    def _create_ns_resources(platform=constants.AWS_PLATFORM):
        # Create random connection_name
        rand_connection = mcg_connection_factory(platform)

        # Create the actual namespace resource
        rand_ns_resource = create_unique_resource_name(
            constants.MCG_NS_RESOURCE, platform
        )
        if platform == constants.RGW_PLATFORM:
            region = None
        else:
            # TODO: fix this when https://github.com/red-hat-storage/ocs-ci/issues/3338
            # is resolved
            region = "us-east-2"
        target_bucket_name = mcg_obj.create_namespace_resource(
            rand_ns_resource,
            rand_connection,
            region,
            cld_mgr,
            cloud_uls_factory,
            platform,
        )

        log.info(f"Check validity of NS resource {rand_ns_resource}")
        if platform == constants.AWS_PLATFORM:
            endpoint = constants.MCG_NS_AWS_ENDPOINT
        elif platform == constants.AZURE_PLATFORM:
            endpoint = constants.MCG_NS_AZURE_ENDPOINT
        elif platform == constants.RGW_PLATFORM:
            rgw_conn = RGW()
            endpoint, _, _ = rgw_conn.get_credentials()
        else:
            raise UnsupportedPlatformError(f"Unsupported Platform: {platform}")

        mcg_obj.check_ns_resource_validity(
            rand_ns_resource, target_bucket_name, endpoint
        )

        created_ns_resources.append(rand_ns_resource)
        return target_bucket_name, rand_ns_resource

    def ns_resources_cleanup():
        for ns_resource in created_ns_resources:
            mcg_obj.delete_ns_resource(ns_resource)

    request.addfinalizer(ns_resources_cleanup)

    return _create_ns_resources


@pytest.fixture()
def namespace_store_factory(request, cld_mgr, mcg_obj, cloud_uls_factory, pvc_factory):
    """
    Create a Namespace Store factory.
    Calling this fixture creates a new Namespace Store(s).

    Returns:
        func: Factory method - each call to this function creates
            a namespacestore

    """
    return namespacestore_factory_implementation(
        request, cld_mgr, mcg_obj, cloud_uls_factory, pvc_factory
    )


@pytest.fixture(scope="session")
def namespace_store_factory_session(
    request, cld_mgr, mcg_obj_session, cloud_uls_factory_session, pvc_factory_session
):
    """
    Create a Namespace Store factory.
    Calling this fixture creates a new Namespace Store(s).

    Returns:
        func: Factory method - each call to this function creates
            a namespacestore

    """
    return namespacestore_factory_implementation(
        request,
        cld_mgr,
        mcg_obj_session,
        cloud_uls_factory_session,
        pvc_factory_session,
    )


@pytest.fixture(scope="session")
def snapshot_factory_session(request):
    return snapshot_factory_fixture(request)


@pytest.fixture(scope="class")
def snapshot_factory_class(request):
    return snapshot_factory_fixture(request)


@pytest.fixture(scope="function")
def snapshot_factory(request):
    return snapshot_factory_fixture(request)


def snapshot_factory_fixture(request):
    """
    Snapshot factory. Calling this fixture creates a volume snapshot from the
    specified PVC

    """
    instances = []

    def factory(pvc_obj, wait=True, snapshot_name=None):
        """
        Args:
            pvc_obj (PVC): PVC object from which snapshot has to be created
            wait (bool): True to wait for snapshot to be ready, False otherwise
            snapshot_name (str): Name to be provided for snapshot

        Returns:
            OCS: OCS instance of kind VolumeSnapshot

        """
        snap_obj = pvc_obj.create_snapshot(snapshot_name=snapshot_name, wait=wait)
        instances.append(snap_obj)
        return snap_obj

    def finalizer():
        """
        Delete the snapshots

        """
        snapcontent_objs = []

        # Get VolumeSnapshotContent form VolumeSnapshots and delete
        # VolumeSnapshots
        for instance in instances:
            if not instance.is_deleted:
                snapcontent_objs.append(
                    helpers.get_snapshot_content_obj(snap_obj=instance)
                )
                instance.delete()
                instance.ocp.wait_for_delete(instance.name)

        # Wait for VolumeSnapshotContents to be deleted
        for snapcontent_obj in snapcontent_objs:
            snapcontent_obj.ocp.wait_for_delete(
                resource_name=snapcontent_obj.name, timeout=240
            )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def multi_snapshot_factory(snapshot_factory):
    """
    Snapshot factory. Calling this fixture creates volume snapshots of each
    PVC in the provided list

    """

    def factory(pvc_obj, wait=True, snapshot_name_suffix=None):
        """
        Args:
            pvc_obj (list): List PVC object from which snapshot has to be created
            wait (bool): True to wait for snapshot to be ready, False otherwise
            snapshot_name_suffix (str): Suffix to be added to snapshot

        Returns:
            OCS: List of OCS instances of kind VolumeSnapshot

        """
        snapshot = []

        for obj in pvc_obj:
            log.info(f"Creating snapshot of PVC {obj.name}")
            snapshot_name = (
                f"{obj.name}-{snapshot_name_suffix}" if snapshot_name_suffix else None
            )
            snap_obj = snapshot_factory(
                pvc_obj=obj, snapshot_name=snapshot_name, wait=wait
            )
            snapshot.append(snap_obj)
        return snapshot

    return factory


@pytest.fixture(scope="class")
def snapshot_restore_factory_class(request):
    return snapshot_restore_factory_fixture(request)


@pytest.fixture(scope="session")
def snapshot_restore_factory_session(request):
    return snapshot_restore_factory_fixture(request)


@pytest.fixture(scope="function")
def snapshot_restore_factory(request):
    return snapshot_restore_factory_fixture(request)


def snapshot_restore_factory_fixture(request):
    """
    Snapshot restore factory. Calling this fixture creates new PVC out of the
    specified VolumeSnapshot.

    """
    instances = []

    def factory(
        snapshot_obj,
        restore_pvc_name=None,
        storageclass=None,
        size=None,
        volume_mode=None,
        restore_pvc_yaml=None,
        access_mode=constants.ACCESS_MODE_RWO,
        status=constants.STATUS_BOUND,
        timeout=60,
    ):
        """
        Args:
            snapshot_obj (OCS): OCS instance of kind VolumeSnapshot which has
                to be restored to new PVC
            restore_pvc_name (str): Name to be provided for restored pvc
            storageclass (str): Name of storageclass
            size (str): Size of PVC being created. eg: 5Gi. Ideally, this
                should be same as the restore size of snapshot. Adding this
                parameter to consider negative test scenarios.
            volume_mode (str): Volume mode for PVC. This should match the
                volume mode of parent PVC.
            restore_pvc_yaml (str): The location of pvc-restore.yaml
            access_mode (str): This decides the access mode to be used for the
                PVC. ReadWriteOnce is default.
            status (str): If provided then factory waits for the PVC to reach
                desired state.
            timeout (int): Time in seconds to wait for the PVC to reach the desired status.

        Returns:
            PVC: Restored PVC object

        """
        no_interface = False
        snapshot_info = snapshot_obj.get()
        size = size or snapshot_info["status"]["restoreSize"]
        restore_pvc_name = restore_pvc_name or (
            helpers.create_unique_resource_name(snapshot_obj.name, "restore")
        )
        vol_snapshot_class = snapshot_info["spec"]["volumeSnapshotClassName"]
        log.info(f"Volume snapshot class name: {vol_snapshot_class}")

        if (
            vol_snapshot_class == constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD
            or vol_snapshot_class
            == constants.DEFAULT_EXTERNAL_MODE_VOLUMESNAPSHOTCLASS_RBD
            or vol_snapshot_class == constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD_MS_PC
        ):
            storageclass = (
                storageclass
                or helpers.default_storage_class(constants.CEPHBLOCKPOOL).name
            )
            restore_pvc_yaml = restore_pvc_yaml or constants.CSI_RBD_PVC_RESTORE_YAML
            interface = constants.CEPHBLOCKPOOL
        elif (
            vol_snapshot_class == constants.DEFAULT_VOLUMESNAPSHOTCLASS_CEPHFS
            or vol_snapshot_class
            == constants.DEFAULT_EXTERNAL_MODE_VOLUMESNAPSHOTCLASS_CEPHFS
            or vol_snapshot_class == constants.DEFAULT_VOLUMESNAPSHOTCLASS_CEPHFS_MS_PC
        ):
            storageclass = (
                storageclass
                or helpers.default_storage_class(constants.CEPHFILESYSTEM).name
            )
            restore_pvc_yaml = restore_pvc_yaml or constants.CSI_CEPHFS_PVC_RESTORE_YAML
            interface = constants.CEPHFILESYSTEM
        elif (
            snapshot_info["spec"]["volumeSnapshotClassName"]
            == constants.DEFAULT_VOLUMESNAPSHOTCLASS_LVM
            or snapshot_info["spec"]["volumeSnapshotClassName"]
            == constants.DEFAULT_VOLUMESNAPSHOTCLASS_LVMS
        ):
            restore_pvc_yaml = restore_pvc_yaml or constants.CSI_LVM_PVC_RESTORE_YAML
            no_interface = True
        elif (
            ocsci_config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM
        ):
            storageclass = storageclass or vol_snapshot_class
            storageclass_obj = OCP(kind="storageclass", resource_name=storageclass)
            sc_data = storageclass_obj.get(resource_name=storageclass)
            if sc_data["provisioner"] == constants.RBD_PROVISIONER:
                restore_pvc_yaml = (
                    restore_pvc_yaml or constants.CSI_RBD_PVC_RESTORE_YAML
                )
                interface = constants.CEPHBLOCKPOOL
            elif sc_data["provisioner"] == constants.CEPHFS_PROVISIONER:
                restore_pvc_yaml = (
                    restore_pvc_yaml or constants.CSI_CEPHFS_PVC_RESTORE_YAML
                )
                interface = constants.CEPHFILESYSTEM

        restored_pvc = create_restore_pvc(
            sc_name=storageclass,
            snap_name=snapshot_obj.name,
            namespace=snapshot_obj.namespace,
            size=size,
            pvc_name=restore_pvc_name,
            volume_mode=volume_mode,
            restore_pvc_yaml=restore_pvc_yaml,
            access_mode=access_mode,
        )
        instances.append(restored_pvc)
        restored_pvc.snapshot = snapshot_obj
        if not no_interface:
            restored_pvc.interface = interface
        if status:
            helpers.wait_for_resource_state(restored_pvc, status, timeout)
        return restored_pvc

    def finalizer():
        """
        Delete the PVCs

        """
        pv_objs = []

        # Get PV form PVC instances and delete PVCs
        for instance in instances:
            if not instance.is_deleted:
                pv_objs.append(instance.backed_pv_obj)
                instance.delete()
                instance.ocp.wait_for_delete(instance.name)

        # Wait for PVs to delete
        helpers.wait_for_pv_delete(pv_objs)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def multi_snapshot_restore_factory(snapshot_restore_factory):
    """
    Snapshot restore factory. Calling this fixture creates set of new PVC out of the
    each VolumeSnapshot provided in the list.

    """

    def factory(
        snapshot_obj,
        restore_pvc_suffix=None,
        storageclass=None,
        size=None,
        volume_mode=None,
        restore_pvc_yaml=None,
        access_mode=constants.ACCESS_MODE_RWO,
        status=constants.STATUS_BOUND,
        wait_each=False,
    ):
        """
        Args:
            snapshot_obj (list): List OCS instance of kind VolumeSnapshot which has
                to be restored to new PVC
            restore_pvc_suffix (str): Suffix to be added to pvc name
            storageclass (str): Name of storageclass
            size (str): Size of PVC being created. eg: 5Gi. Ideally, this
                should be same as the restore size of snapshot. Adding this
                parameter to consider negative test scenarios.
            volume_mode (str): Volume mode for PVC. This should match the
                volume mode of parent PVC.
            restore_pvc_yaml (str): The location of pvc-restore.yaml
            access_mode (str): This decides the access mode to be used for the
                PVC. ReadWriteOnce is default.
            status (str): If provided then factory waits for the PVC to reach
                desired state.
            wait_each(bool): True to wait for each PVC to be in status 'status'
                before creating next PVC, False otherwise

        Returns:
            PVC: List of restored PVC object

        """
        new_pvcs = []

        status_tmp = status if wait_each else ""

        for snap_obj in snapshot_obj:
            log.info(f"Creating a PVC from snapshot {snap_obj.name}")
            restore_pvc_name = (
                f"{snap_obj.name}-{restore_pvc_suffix}" if restore_pvc_suffix else None
            )
            restored_pvc = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                restore_pvc_name=restore_pvc_name,
                storageclass=storageclass,
                size=size,
                volume_mode=volume_mode,
                restore_pvc_yaml=restore_pvc_yaml,
                access_mode=access_mode,
                status=status_tmp,
            )
            restored_pvc.snapshot = snapshot_obj
            new_pvcs.append(restored_pvc)

        if status and not wait_each:
            for restored_pvc in new_pvcs:
                helpers.wait_for_resource_state(restored_pvc, status)

        return new_pvcs

    return factory


@pytest.fixture(scope="session", autouse=True)
def collect_logs_fixture(request):
    """
    This fixture collects ocs logs after tier execution and this will allow
    to see the cluster's status after the execution on all execution status options.
    """

    def finalizer():
        """
        Tracking both logs separately reduce changes of collision
        """
        if not ocsci_config.RUN["cli_params"].get("deploy") and not ocsci_config.RUN[
            "cli_params"
        ].get("teardown"):
            if ocsci_config.REPORTING["collect_logs_on_success_run"]:
                collect_ocs_logs("testcases", ocs=False, status_failure=False)
                collect_ocs_logs("testcases", ocp=False, status_failure=False)
                collect_pod_container_rpm_package("testcases")

    request.addfinalizer(finalizer)


def get_ready_noobaa_endpoint_count(namespace):
    """
    Get the number of ready nooobaa endpoints
    """
    pods_info = get_pods_having_label(
        label=constants.NOOBAA_ENDPOINT_POD_LABEL, namespace=namespace
    )
    ready_count = 0
    for ep_info in pods_info:
        container_statuses = ep_info.get("status", {}).get("containerStatuses")
        if container_statuses is not None and len(container_statuses) > 0:
            if container_statuses[0].get("ready"):
                ready_count += 1
    return ready_count


@pytest.fixture(scope="function")
def nb_ensure_endpoint_count(request):
    """
    Validate and ensure the number of running noobaa endpoints
    """
    cls = request.cls
    min_ep_count = cls.MIN_ENDPOINT_COUNT
    max_ep_count = cls.MAX_ENDPOINT_COUNT

    assert min_ep_count <= max_ep_count
    namespace = ocsci_config.ENV_DATA["cluster_namespace"]
    should_wait = False

    # prior to 4.6 we configured the ep count directly on the noobaa cr.
    if version.get_semantic_ocs_version_from_config() < version.VERSION_4_6:
        noobaa = OCP(kind="noobaa", namespace=namespace)
        resource = noobaa.get()["items"][0]
        endpoints = resource.get("spec", {}).get("endpoints", {})

        if endpoints.get("minCount", -1) != min_ep_count:
            log.info(f"Changing minimum Noobaa endpoints to {min_ep_count}")
            params = f'{{"spec":{{"endpoints":{{"minCount":{min_ep_count}}}}}}}'
            noobaa.patch(resource_name="noobaa", params=params, format_type="merge")
            should_wait = True

        if endpoints.get("maxCount", -1) != max_ep_count:
            log.info(f"Changing maximum Noobaa endpoints to {max_ep_count}")
            params = f'{{"spec":{{"endpoints":{{"maxCount":{max_ep_count}}}}}}}'
            noobaa.patch(resource_name="noobaa", params=params, format_type="merge")
            should_wait = True

    else:
        storage_cluster = OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
        resource = storage_cluster.get()["items"][0]
        resource_name = resource["metadata"]["name"]
        endpoints = (
            resource.get("spec", {}).get("multiCloudGateway", {}).get("endpoints", {})
        )

        if endpoints.get("minCount", -1) != min_ep_count:
            log.info(f"Changing minimum Noobaa endpoints to {min_ep_count}")
            params = f'{{"spec":{{"multiCloudGateway":{{"endpoints":{{"minCount":{min_ep_count}}}}}}}}}'
            storage_cluster.patch(
                resource_name=resource_name, params=params, format_type="merge"
            )
            should_wait = True

        if endpoints.get("maxCount", -1) != max_ep_count:
            log.info(f"Changing maximum Noobaa endpoints to {max_ep_count}")
            params = f'{{"spec":{{"multiCloudGateway":{{"endpoints":{{"maxCount":{max_ep_count}}}}}}}}}'
            storage_cluster.patch(
                resource_name=resource_name, params=params, format_type="merge"
            )
            should_wait = True

    if should_wait:
        # Wait for the NooBaa endpoint pods to stabilize
        try:
            for ready_nb_ep_count in TimeoutSampler(
                300, 30, get_ready_noobaa_endpoint_count, namespace
            ):
                if min_ep_count <= ready_nb_ep_count <= max_ep_count:
                    log.info(
                        "NooBaa endpoints stabilized. Ready endpoints:"
                        f" {ready_nb_ep_count}"
                    )
                    break
                log.info(
                    "Waiting for the NooBaa endpoints to stabilize. "
                    f"Current ready count: {ready_nb_ep_count}"
                )
        except TimeoutExpiredError:
            raise TimeoutExpiredError(
                "NooBaa endpoints did not stabilize in time.\nMin count:"
                f" {min_ep_count}, max count: {max_ep_count}, ready count:"
                f" {ready_nb_ep_count}"
            )


@pytest.fixture(scope="class")
def pvc_clone_factory_class(request):
    return pvc_clone_factory_fixture(request)


@pytest.fixture(scope="session")
def pvc_clone_factory_session(request):
    return pvc_clone_factory_fixture(request)


@pytest.fixture(scope="function")
def pvc_clone_factory(request):
    return pvc_clone_factory_fixture(request)


def pvc_clone_factory_fixture(request):
    """
    Calling this fixture creates a clone from the specified PVC

    """
    instances = []

    def factory(
        pvc_obj,
        status=constants.STATUS_BOUND,
        clone_name=None,
        storageclass=None,
        size=None,
        access_mode=None,
        volume_mode=None,
        timeout=120,
    ):
        """
        Args:
            pvc_obj (PVC): PVC object from which clone has to be created
            status (str): If provided then factory waits for cloned PVC to
                reach the desired state
            clone_name (str): Name to be provided for cloned PVC
            storageclass (str): storage class to be used for cloned PVC
            size (int): The requested size for the cloned PVC. This should
                be same as the size of parent PVC for a successful clone
            access_mode (str): This decides the access mode to be used for
                the cloned PVC. eg: ReadWriteOnce, ReadOnlyMany, ReadWriteMany
            volume_mode (str): Volume mode for PVC. This should match the
                volume mode of parent PVC
            timeout (int): Time in seconds to wait for the PVC to reach the desired status.

        Returns:
            PVC: PVC instance

        """
        assert (
            pvc_obj.provisioner in constants.OCS_PROVISIONERS
        ), f"Unknown provisioner in PVC {pvc_obj.name}"
        no_interface = False
        if "rbd.csi.ceph.com" in pvc_obj.provisioner:
            clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
            interface = constants.CEPHBLOCKPOOL
        elif "cephfs.csi.ceph.com" in pvc_obj.provisioner:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
            interface = constants.CEPHFILESYSTEM
        elif pvc_obj.provisioner in [
            constants.LVM_PROVISIONER_4_11,
            constants.LVM_PROVISIONER,
        ]:
            clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
            no_interface = True
        size = size or pvc_obj.get().get("spec").get("resources").get("requests").get(
            "storage"
        )
        storageclass = storageclass or pvc_obj.backed_sc
        access_mode = access_mode or pvc_obj.get_pvc_access_mode
        volume_mode = volume_mode or pvc_obj.get_pvc_vol_mode

        # Create clone
        clone_pvc_obj = pvc.create_pvc_clone(
            sc_name=storageclass,
            parent_pvc=pvc_obj.name,
            clone_yaml=clone_yaml,
            pvc_name=clone_name,
            namespace=pvc_obj.namespace,
            storage_size=size,
            access_mode=access_mode,
            volume_mode=volume_mode,
        )
        instances.append(clone_pvc_obj)
        clone_pvc_obj.parent = pvc_obj
        clone_pvc_obj.volume_mode = volume_mode
        if not no_interface:
            clone_pvc_obj.interface = interface
        if status:
            helpers.wait_for_resource_state(clone_pvc_obj, status, timeout=timeout)
        return clone_pvc_obj

    def finalizer():
        """
        Delete the cloned PVCs

        """
        pv_objs = []

        # Get PV form PVC instances and delete PVCs
        for instance in instances:
            if not instance.is_deleted:
                pv_objs.append(instance.backed_pv_obj)
                instance.delete()
                instance.ocp.wait_for_delete(instance.name)

        # Wait for PVs to delete
        helpers.wait_for_pv_delete(pv_objs)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="session", autouse=True)
def reportportal_customization(request):
    if ocsci_config.REPORTING.get("rp_launch_url"):
        request.config._metadata["RP Launch URL:"] = ocsci_config.REPORTING[
            "rp_launch_url"
        ]


@pytest.fixture(scope="function")
def login_factory(request):
    return login_factory_fixture(request)


@pytest.fixture(scope="class")
def login_factory_class(request):
    return login_factory_fixture(request)


@pytest.fixture(scope="session")
def login_factory_session(request):
    return login_factory_fixture(request)


def login_factory_fixture(request):
    """
    Calling this fixture will login into console using other user(user other than kubeadmin)
    """
    drivers = []

    def factory(username, password):
        driver = login_ui(username=username, password=password)
        drivers.append(driver)
        return driver

    def finalizer():
        for driver in drivers:
            close_browser()

    request.addfinalizer(finalizer)

    return factory


@pytest.fixture()
def multi_pvc_clone_factory(pvc_clone_factory, pod_factory):
    """
    Calling this fixture creates clone from each PVC in the provided list of PVCs

    """

    def factory(
        pvc_obj,
        status=constants.STATUS_BOUND,
        clone_name=None,
        storageclass=None,
        size=None,
        access_mode=None,
        volume_mode=None,
        wait_each=False,
        attach_pods=False,
        verify_data_integrity=False,
        file_name=None,
    ):
        """
        Args:
            pvc_obj (list): List PVC object from which clone has to be created
            status (str): If provided then factory waits for cloned PVC to
                reach the desired state
            clone_name (str): Name to be provided for cloned PVC
            storageclass (str): storage class to be used for cloned PVC
            size (int): The requested size for the cloned PVC. This should
                be same as the size of parent PVC for a successful clone
            access_mode (str): This decides the access mode to be used for
                the cloned PVC. eg: ReadWriteOnce, ReadOnlyMany, ReadWriteMany
            volume_mode (str): Volume mode for PVC. This should match the
                volume mode of parent PVC
            wait_each(bool): True to wait for each PVC to be in status 'status'
                before creating next PVC, False otherwise
            attach_pods(bool): True if we want to attach PODs to the cloned PVCs, False otherwise.
            verify_data_integrity(bool): True if we want to verify data integrity by checking the existence and md5sum
                                            of file in the cloned PVC, False otherwise.
            file_name(str): The name of the file for which data integrity is to be checked.

        Returns:
            PVC: List PVC instance

        """
        cloned_pvcs = []

        status_tmp = status if wait_each else ""

        log.info("Started creation of clones of the PVCs.")
        for obj in pvc_obj:
            # Create clone
            clone_pvc_obj = pvc_clone_factory(
                pvc_obj=obj,
                clone_name=clone_name,
                storageclass=storageclass,
                size=size,
                access_mode=access_mode,
                volume_mode=volume_mode,
                status=status_tmp,
            )
            cloned_pvcs.append(clone_pvc_obj)

        if status and not wait_each:
            for cloned_pvc in cloned_pvcs:
                helpers.wait_for_resource_state(cloned_pvc, status)

        log.info("Successfully created clones of the PVCs.")

        if attach_pods:
            # Attach PODs to cloned PVCs
            cloned_pod_objs = list()
            for cloned_pvc_obj in cloned_pvcs:
                if cloned_pvc_obj.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
                    cloned_pod_objs.append(
                        pod_factory(
                            pvc=cloned_pvc_obj,
                            raw_block_pv=True,
                            status=constants.STATUS_RUNNING,
                            pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
                        )
                    )
                else:
                    cloned_pod_objs.append(
                        pod_factory(pvc=cloned_pvc_obj, status=constants.STATUS_RUNNING)
                    )

            # Verify that the fio exists and md5sum matches
            if verify_data_integrity:
                verify_data_integrity_for_multi_pvc_objs(
                    cloned_pod_objs, pvc_obj, file_name
                )

            return cloned_pvcs, cloned_pod_objs

        return cloned_pvcs

    return factory


@pytest.fixture(scope="function")
def multiple_snapshot_and_clone_of_postgres_pvc_factory(
    request,
    multi_snapshot_factory,
    multi_snapshot_restore_factory,
    multi_pvc_clone_factory,
):
    """
    Calling this fixture creates multiple snapshots & clone of postgres PVC
    """
    instances = []

    def factory(pvc_size_new, pgsql, sc_name=None):
        """
        Args:
            pvc_size_new (int): Resize/Expand the pvc size
            pgsql (obj): Pgsql obj

        Returns:
            Postgres pod: Pod instances

        """

        # Get postgres pvc list obj
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        snapshots = multi_snapshot_factory(pvc_obj=postgres_pvcs_obj)
        log.info("Created snapshots from all the PVCs and snapshots are in Ready state")

        restored_pvc_objs = multi_snapshot_restore_factory(
            snapshot_obj=snapshots, storageclass=sc_name
        )
        log.info("Created new PVCs from all the snapshots")

        cloned_pvcs = multi_pvc_clone_factory(
            pvc_obj=restored_pvc_objs,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
            storageclass=sc_name,
        )
        log.info("Created new PVCs from all restored volumes")

        # Attach a new pgsql pod cloned pvcs
        sset_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=cloned_pvcs, postgres_name="postgres-clone", run_benchmark=False
        )
        instances.extend(sset_list)

        # Resize cloned PVCs
        for pvc_obj in cloned_pvcs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
            pvc_obj.resize_pvc(pvc_size_new, True)

        new_snapshots = multi_snapshot_factory(pvc_obj=cloned_pvcs)
        log.info(
            "Created snapshots from all the cloned PVCs"
            " and snapshots are in Ready state"
        )

        new_restored_pvc_objs = multi_snapshot_restore_factory(
            snapshot_obj=new_snapshots, storageclass=sc_name
        )
        log.info("Created new PVCs from all the snapshots and in Bound state")
        # Attach a new pgsql pod restored pvcs
        pgsql_obj_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=new_restored_pvc_objs,
            postgres_name="postgres-clone-restore",
            run_benchmark=False,
        )
        instances.extend(pgsql_obj_list)

        # Resize restored PVCs
        for pvc_obj in new_restored_pvc_objs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
            pvc_obj.resize_pvc(pvc_size_new, True)

        return instances

    def finalizer():
        """
        Delete the list of pod objects created

        """
        for instance in instances:
            if not instance.is_deleted:
                instance.delete()
                instance.ocp.wait_for_delete(instance.name)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def es(request):
    """
    Create In-cluster elastic-search deployment for benchmark-operator tests.

    using the name es - as shortcut for elastic-search for simplicity
    """

    def teardown():
        es.cleanup()

    request.addfinalizer(teardown)

    es = ElasticSearch()

    return es


@pytest.fixture(scope="session")
def setup_ui_session(request):
    return setup_ui_fixture(request)


@pytest.fixture(scope="class")
def setup_ui_class(request):
    return setup_ui_fixture(request)


@pytest.fixture(scope="class")
def setup_ui_class_factory(request):
    # The problem with class scope fixtures is that they are executed always the first, when the class is loaded.
    # This fixture is used to control fixture execution order, and call the fixture from within the test, after
    # switch_to_provider fixture with autouse=True will be executed (for example).
    # This way we can control the order of execution and perform login to management-console only after
    # switching the context.
    def factory():
        setup_ui_fixture(request)

    return factory


@pytest.fixture(scope="function")
def setup_ui(request):
    return setup_ui_fixture(request)


def setup_ui_fixture(request):
    driver = login_ui()

    def finalizer():
        close_browser()

    request.addfinalizer(finalizer)

    return driver


@pytest.fixture(scope="function")
def setup_acm_ui(request):
    return setup_acm_ui_fixture(request)


def setup_acm_ui_fixture(request):
    if not ocsci_config.RUN.get("dr_action_via_ui"):
        return
    restore_ctx_index = ocsci_config.cur_index
    ocsci_config.switch_acm_ctx()
    driver = login_to_acm()

    def finalizer():
        close_browser()
        log.info("Switching back to the initial cluster context")
        ocsci_config.switch_ctx(restore_ctx_index)

    request.addfinalizer(finalizer)

    return driver


@pytest.fixture(scope="session", autouse=True)
def use_client_proxy(request):
    """
    This fixture configure required env variables for using client http proxy
    if configured.
    """
    if (
        ocsci_config.DEPLOYMENT.get("proxy")
        or ocsci_config.DEPLOYMENT.get("disconnected")
        or ocsci_config.ENV_DATA.get("private_link")
    ) and ocsci_config.ENV_DATA.get("client_http_proxy"):
        log.info(
            f"Configuring client proxy: {ocsci_config.ENV_DATA['client_http_proxy']}"
        )
        os.environ["http_proxy"] = ocsci_config.ENV_DATA["client_http_proxy"]
        os.environ["https_proxy"] = ocsci_config.ENV_DATA["client_http_proxy"]


@pytest.fixture(scope="session", autouse=True)
def load_cluster_info_file(request):
    """
    This fixture tries to load cluster_info.json file if exists (on cluster
    installed via Flexy) and apply the information to the config object (for
    example related to disconnected cluster)
    """
    load_cluster_info()


@pytest.fixture(scope="function")
def pv_encryption_kms_setup_factory(request):
    """
    Create vault resources and setup csi-kms-connection-details configMap
    """

    # set the KMS provider based on KMS_PROVIDER env value.
    if ocsci_config.ENV_DATA["KMS_PROVIDER"].lower() == constants.HPCS_KMS_PROVIDER:
        return pv_encryption_hpcs_setup_factory(request)
    elif ocsci_config.ENV_DATA["KMS_PROVIDER"] == constants.AZURE_KV_PROVIDER_NAME:
        return pv_encryption_azure_kv_setup_factory(request)
    else:
        return pv_encryption_vault_setup_factory(request)


def pv_encryption_vault_setup_factory(request):
    """
    Create vault resources and setup csi-kms-connection-details configMap

    """
    vault = KMS.Vault()

    def factory(kv_version, use_vault_namespace=False):
        """
        Args:
            kv_version(str): KV version to be used, either v1 or v2
            use_vault_namespace (bool): True, to use vault namespace
        Returns:
            object: Vault(KMS) object
        """
        vault.gather_init_vault_conf()
        vault.update_vault_env_vars()

        # Check if cert secrets already exist, if not create cert resources
        ocp_obj = OCP(
            kind="secret", namespace=ocsci_config.ENV_DATA["cluster_namespace"]
        )
        try:
            ocp_obj.get_resource(resource_name="ocs-kms-ca-secret", column="NAME")
        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                vault.create_ocs_vault_cert_resources()

        # Create vault namespace, backend path and policy in vault
        vault_resource_name = create_unique_resource_name("test", "vault")
        if use_vault_namespace:
            vault.vault_create_namespace(namespace=vault_resource_name)
        vault.vault_create_backend_path(
            backend_path=vault_resource_name, kv_version=kv_version
        )
        vault.vault_create_policy(policy_name=vault_resource_name)

        # If csi-kms-connection-details exists, edit the configmap to add new vault config
        ocp_obj = OCP(
            kind="configmap", namespace=ocsci_config.ENV_DATA["cluster_namespace"]
        )

        try:
            ocp_obj.get_resource(
                resource_name="csi-kms-connection-details", column="NAME"
            )
            new_kmsid = vault_resource_name
            vdict = defaults.VAULT_CSI_CONNECTION_CONF
            for key in vdict.keys():
                old_key = key
            vdict[new_kmsid] = vdict.pop(old_key)
            vdict[new_kmsid][
                "VAULT_ADDR"
            ] = f"https://{vault.vault_server}:{vault.port}"
            vdict[new_kmsid]["VAULT_BACKEND_PATH"] = vault_resource_name
            if use_vault_namespace:
                vdict[new_kmsid]["VAULT_NAMESPACE"] = vault.vault_namespace
            vault.kmsid = vault_resource_name
            if kv_version == "v1":
                vdict[new_kmsid]["VAULT_BACKEND"] = "kv"
            else:
                vdict[new_kmsid]["VAULT_BACKEND"] = "kv-v2"
            KMS.update_csi_kms_vault_connection_details(vdict)

        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                vault.kmsid = "1-vault"
                vault.create_vault_csi_kms_connection_details(kv_version=kv_version)

        return vault

    def finalizer():
        """
        Remove the vault config from csi-kms-connection-details configMap

        """
        if len(KMS.get_encryption_kmsid()) > 1:
            KMS.remove_kmsid(vault.kmsid)
        if vault.vault_namespace:
            vault.remove_vault_backend_path(vault_namespace=vault.vault_namespace)
            vault.remove_vault_policy(vault_namespace=vault.vault_namespace)
        else:
            vault.remove_vault_backend_path()
            vault.remove_vault_policy()
        if vault.vault_namespace:
            vault.remove_vault_namespace()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="function")
def pv_encryption_kmip_setup_factory(request):
    """
    Create KMIP resources and setup csi-kms-connection-details configMap

    """
    kmip = KMS.KMIP()

    def factory():
        """
        Returns:
            object: KMIP (KMS) object
        """
        kmip.update_kmip_env_vars()
        get_ksctl_cli()
        kmip.kmsid = create_unique_resource_name("test", "kmip")
        kmip.kmip_secret_name = kmip.create_kmip_secret(type="csi")

        # If csi-kms-connection-details exists, edit the configmap to add new kmip config
        ocp_obj = OCP(
            kind="configmap", namespace=ocsci_config.ENV_DATA["cluster_namespace"]
        )

        try:
            ocp_obj.get_resource(
                resource_name="csi-kms-connection-details", column="NAME"
            )
            # new_kmsid = vault_resource_name
            vdict = defaults.KMIP_CSI_CONNECTION_CONF
            for key in vdict.keys():
                old_key = key
            vdict[kmip.kmsid] = vdict.pop(old_key)
            vdict[kmip.kmsid]["KMS_SERVICE_NAME"] = kmip.kmsid
            vdict[kmip.kmsid][
                "KMIP_ENDPOINT"
            ] = f"{kmip.kmip_endpoint}:{kmip.kmip_port}"
            vdict[kmip.kmsid]["KMIP_SECRET_NAME"] = kmip.kmip_secret_name
            vdict[kmip.kmsid]["TLS_SERVER_NAME"] = kmip.kmip_tls_server_name
            KMS.update_csi_kms_vault_connection_details(vdict)

        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                kmip.kmsid = "1-kmip"
                kmip.create_kmip_csi_kms_connection_details()

        return kmip

    def finalizer():
        """
        Remove the kmip config from csi-kms-connection-details configMap

        """
        if len(KMS.get_encryption_kmsid()) > 1:
            KMS.remove_kmsid(kmip.kmsid)
        if kmip.kmip_secret_name:
            run_cmd(
                f"oc delete secret {kmip.kmip_secret_name} -n"
                f" {ocsci_config.ENV_DATA['cluster_namespace']}"
            )
        if kmip.kmip_key_identifier:
            kmip.delete_ciphertrust_key(key_id=kmip.kmip_key_identifier)

    request.addfinalizer(finalizer)
    return factory


def pv_encryption_hpcs_setup_factory(request):
    """
    Create hpcs resources and setup csi-kms-connection-details configMap

    """
    hpcs = KMS.HPCS()

    def factory(kv_version):
        """
        Args:
            kv_version(str): KV version to be used
        Returns:
            object: HPCS(KMS) object
        Raises:
            CommandFailed: if fails to get csi-kms-connection-details configmap
        """
        hpcs.gather_init_hpcs_conf()

        # Create hpcs secret with a unique name otherwise raise error if it already exists.
        hpcs.ibm_kp_secret_name = hpcs.create_ibm_kp_kms_secret()

        # Create or update hpcs related confimap.
        hpcs_resource_name = create_unique_resource_name("test", "hpcs")
        ocp_obj = OCP(
            kind="configmap", namespace=ocsci_config.ENV_DATA["cluster_namespace"]
        )
        # If csi-kms-connection-details exists, edit the configmap to add new hpcs config
        try:
            ocp_obj.get_resource(
                resource_name="csi-kms-connection-details", column="NAME"
            )
            new_kmsid = hpcs_resource_name
            hdict = defaults.HPCS_CSI_CONNECTION_CONF
            for key in hdict.keys():
                old_key = key
            hdict[new_kmsid] = hdict.pop(old_key)
            hdict[new_kmsid][
                "IBM_KP_SERVICE_INSTANCE_ID"
            ] = hpcs.ibm_kp_service_instance_id
            hdict[new_kmsid]["IBM_KP_SECRET_NAME"] = hpcs.ibm_kp_secret_name
            hdict[new_kmsid]["IBM_KP_BASE_URL"] = hpcs.ibm_kp_base_url
            hdict[new_kmsid]["IBM_KP_TOKEN_URL"] = hpcs.ibm_kp_token_url
            hdict[new_kmsid]["KMS_SERVICE_NAME"] = new_kmsid
            hpcs.kmsid = hpcs_resource_name
            KMS.update_csi_kms_vault_connection_details(hdict)

        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                hpcs.kmsid = "1-hpcs"
                hpcs.create_hpcs_csi_kms_connection_details()

        return hpcs

    def finalizer():
        """
        Remove the hpcs config from csi-kms-connection-details configMap

        """
        if len(KMS.get_encryption_kmsid()) > 1:
            KMS.remove_kmsid(hpcs.kmsid)
        # remove the kms secret created to store hpcs creds
        hpcs.delete_resource(
            hpcs.ibm_kp_secret_name,
            "secret",
            ocsci_config.ENV_DATA["cluster_namespace"],
        )

    request.addfinalizer(finalizer)
    return factory


def pv_encryption_azure_kv_setup_factory(request):
    """
    Create a Azure KV resource and returh the azure KV Object.
    """
    kms = KMS.AzureKV()

    def factory():
        """
        Create a Azure KV resources in the cluster
        """
        # setup KMS connection details.
        kms.create_azure_kv_csi_kms_connection_details()
        return kms

    def finalizer():
        """
        Cleanup Azure KV resources from the cluster.
        """
        kms.remove_kmsid()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def cephblockpool_factory_ui_class(request, setup_ui_class):
    return cephblockpool_factory_ui_fixture(request, setup_ui_class)


@pytest.fixture(scope="session")
def cephblockpool_factory_ui_session(request, setup_ui_session):
    return cephblockpool_factory_ui_fixture(request, setup_ui_session)


@pytest.fixture(scope="function")
def cephblockpool_factory_ui(request, setup_ui):
    return cephblockpool_factory_ui_fixture(request, setup_ui)


def cephblockpool_factory_ui_fixture(request, setup_ui):
    """
    This funcion create new cephblockpool
    """
    instances = []

    def factory(
        replica=3,
        compression=False,
    ):
        """
        Args:
            replica (int): size of pool 2,3 supported for now
            compression (bool): True to enable compression otherwise False
        Return:
            (ocs_ci.ocs.resource.ocs) ocs object of the CephBlockPool.

        """
        blockpool_ui_object = BlockPoolUI()
        pool_name, pool_status = blockpool_ui_object.create_pool(
            replica=replica, compression=compression
        )
        if pool_status:
            log.info(
                f"Pool {pool_name} with replica {replica} and compression"
                f" {compression} was created and is in ready state"
            )
            ocs_blockpool_obj = create_ocs_object_from_kind_and_name(
                kind=constants.CEPHBLOCKPOOL,
                resource_name=pool_name,
            )
            instances.append(ocs_blockpool_obj)
            return ocs_blockpool_obj
        else:
            blockpool_ui_object.take_screenshot()
            if pool_name:
                instances.append(
                    create_ocs_object_from_kind_and_name(
                        kind=constants.CEPHBLOCKPOOL, resource_name=pool_name
                    )
                )
            raise PoolDidNotReachReadyState(
                f"Pool {pool_name} with replica {replica} and compression {compression}"
                " did not reach ready state"
            )

    def finalizer():
        """
        Delete the cephblockpool from ui and if fails from cli
        """

        for instance in instances:
            try:
                instance.get()
            except CommandFailed:
                log.warning("Pool is already deleted")
                continue
            blockpool_ui_obj = BlockPoolUI()
            if not blockpool_ui_obj.delete_pool(instance.name):
                instance.delete()
                raise PoolNotDeletedFromUI(
                    f"Could not delete block pool {instances.name} from UI."
                    " Deleted from CLI"
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def storageclass_factory_ui_class(
    request, cephblockpool_factory_ui_class, setup_ui_class
):
    return storageclass_factory_ui_fixture(
        request, cephblockpool_factory_ui_class, setup_ui_class
    )


@pytest.fixture(scope="session")
def storageclass_factory_ui_session(
    request, cephblockpool_factory_ui_session, setup_ui_session
):
    return storageclass_factory_ui_fixture(
        request, cephblockpool_factory_ui_session, setup_ui_session
    )


@pytest.fixture(scope="function")
def storageclass_factory_ui(request, cephblockpool_factory_ui, setup_ui):
    return storageclass_factory_ui_fixture(request, cephblockpool_factory_ui, setup_ui)


def storageclass_factory_ui_fixture(request, cephblockpool_factory_ui, setup_ui):
    """
    The function create new storage class without encryption and creates an encrypted storage class vi UI
    if the flag encryption is set to True
    """
    instances = []

    def factory(
        provisioner=constants.OCS_PROVISIONERS[0],
        compression=False,
        replica=3,
        create_new_pool=False,
        encryption=False,
        reclaim_policy=constants.RECLAIM_POLICY_DELETE,
        default_pool=constants.DEFAULT_BLOCKPOOL,
        existing_pool=None,
        backend_path=None,
        vault_namespace=None,
        vol_binding_mode="Immediate",
        service_name=None,
        kms_address=None,
        tls_server_name=None,
    ):
        """
        Args:
            provisioner (str): The name of the provisioner. Default is openshift-storage.rbd.csi.ceph.com
            compression (bool): if create_new_pool is True, compression will be set if True.
            replica (int): if create_new_pool is True, replica will be set.
            create_new_pool (bool): True to create new pool with factory.
            encryption (bool): enable PV encryption if True.
            reclaim_policy (str): Reclaim policy for the storageclass.
            existing_pool(str): Use pool name for storageclass.
        Return:
            (ocs_ci.ocs.resource.ocs) ocs object of the storageclass.

        """
        global sc_name
        storageclass_ui_object = StorageClassUI()
        if encryption:
            sc_name = storageclass_ui_object.create_encrypted_storage_class_ui(
                backend_path=backend_path,
                reclaim_policy=reclaim_policy,
                provisioner=provisioner,
                vol_binding_mode=vol_binding_mode,
                service_name=service_name,
                kms_address=kms_address,
                tls_server_name=tls_server_name,
            )
        else:
            if existing_pool is None and create_new_pool is False:
                pool_name = default_pool
            if create_new_pool is True:
                pool_ocs_obj = cephblockpool_factory_ui(
                    replica=replica, compression=compression
                )
                pool_name = pool_ocs_obj.name
            if existing_pool is not None:
                pool_name = existing_pool
            sc_name = storageclass_ui_object.create_storageclass(pool_name)
        if sc_name is None:
            log.error("Storageclass was not created")
            raise StorageclassNotCreated(
                "Storageclass is not found in storageclass list page"
            )
        else:
            log.info(f"Storageclass created with name {sc_name}")
            sc_obj = create_ocs_object_from_kind_and_name(
                resource_name=sc_name, kind=constants.STORAGECLASS
            )
            instances.append(sc_obj)
            log.info(f"{sc_obj.get()}")
            return sc_obj

    def finalizer():
        for instance in instances:
            try:
                instance.get()
            except CommandFailed:
                log.warning("Storageclass is already deleted")
                continue
            storageclass_ui_obj = StorageClassUI()
            if not storageclass_ui_obj.delete_rbd_storage_class(instance.name):
                instance.delete()
                raise StorageClassNotDeletedFromUI(
                    f"Could not delete storageclass {instances.name} from UI."
                    "Deleted from CLI"
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def vault_tenant_sa_setup_factory(request):
    """
    Create vault resources and setup csi-kms-connection-details configMap for
    vault tenant sa method of PV encryption

    """
    vault = KMS.Vault()

    def factory(
        kv_version,
        use_auth_path=True,
        use_vault_namespace=False,
        use_backend=False,
    ):
        """
        Args:
            kv_version (str): KV version to be used, either v1 or v2
            use_auth_path (bool): Use a non-default auth path (used with kubernetes auth method)
            use_vault_namespace (bool): Use namespace in Vault
            use_backend (bool): Specify VaultBackend variable in the configmap when set to True

        Returns:
            object: Vault(KMS) object

        """
        vault.gather_init_vault_conf()
        vault.update_vault_env_vars()

        # Check if cert secrets already exist, if not create cert resources
        ocp_obj = OCP(
            kind="secret", namespace=ocsci_config.ENV_DATA["cluster_namespace"]
        )
        try:
            ocp_obj.get_resource(resource_name="ocs-kms-ca-secret", column="NAME")
        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                vault.create_ocs_vault_cert_resources()

        # Create vault namespace, backend path and policy in vault
        vault_resource_name = create_unique_resource_name("test", "vault")

        if use_vault_namespace:
            vault.vault_create_namespace(namespace=vault_resource_name)

        vault.vault_create_backend_path(
            backend_path=vault_resource_name, kv_version=kv_version
        )
        vault.vault_create_policy(policy_name=vault_resource_name)
        vault.kmsid = vault_resource_name

        vault.create_token_reviewer_resources()
        if use_auth_path and use_vault_namespace:
            vault.vault_kube_auth_setup(
                auth_path=vault_resource_name, auth_namespace=vault.vault_namespace
            )
        elif use_auth_path:
            vault.vault_kube_auth_setup(auth_path=vault_resource_name)
        elif use_vault_namespace:
            vault.vault_kube_auth_setup(auth_namespace=vault.vault_namespace)
        else:
            vault.vault_kube_auth_setup()

        # If csi-kms-connection-details exists, edit the configmap to add new vault config
        ocp_obj = OCP(
            kind="configmap", namespace=ocsci_config.ENV_DATA["cluster_namespace"]
        )
        try:
            ocp_obj.get_resource(
                resource_name="csi-kms-connection-details", column="NAME"
            )
            vdict = copy.deepcopy(defaults.VAULT_TENANT_SA_CONNECTION_CONF)
            for key in vdict.keys():
                old_key = key
            vdict[vault.kmsid] = vdict.pop(old_key)
            vdict[vault.kmsid][
                "vaultAddress"
            ] = f"https://{vault.vault_server}:{vault.port}"
            vdict[vault.kmsid]["vaultBackendPath"] = vault_resource_name
            if not ocsci_config.ENV_DATA.get("VAULT_CA_ONLY", None):
                vdict[vault.kmsid][
                    "vaultClientCertFromSecret"
                ] = get_default_if_keyval_empty(
                    ocsci_config.ENV_DATA,
                    "VAULT_CLIENT_CERT",
                    defaults.VAULT_DEFAULT_CLIENT_CERT,
                )
                vdict[vault.kmsid][
                    "vaultClientCertKeyFromSecret"
                ] = get_default_if_keyval_empty(
                    ocsci_config.ENV_DATA,
                    "VAULT_CLIENT_KEY",
                    defaults.VAULT_DEFAULT_CLIENT_KEY,
                )
            else:
                vdict[vault.kmsid].pop("vaultClientCertFromSecret")
                vdict[vault.kmsid].pop("vaultClientCertKeyFromSecret")
            if use_vault_namespace:
                vdict[vault.kmsid]["vaultNamespace"] = vault.vault_namespace
                vdict[vault.kmsid]["vaultAuthNamespace"] = vault.vault_namespace
            else:
                vdict[vault.kmsid].pop("vaultNamespace")
                vdict[vault.kmsid].pop("vaultAuthNamespace")
            if use_auth_path:
                vdict[vault.kmsid][
                    "vaultAuthPath"
                ] = f"/v1/auth/{vault_resource_name}/login"
            else:
                vdict[vault.kmsid].pop("vaultAuthPath")
            if use_backend:
                if kv_version == "v1":
                    vdict[vault.kmsid]["vaultBackend"] = "kv"
                else:
                    vdict[vault.kmsid]["vaultBackend"] = "kv-v2"
            else:
                vdict[vault.kmsid].pop("vaultBackend")
            KMS.update_csi_kms_vault_connection_details(vdict)

        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                vault.kmsid = "vault-tenant-sa"
                vault.create_vault_csi_kms_connection_details(
                    kv_version=kv_version, vault_auth_method=constants.VAULT_TENANT_SA
                )
        return vault

    def finalizer():
        """
        Cleanup for vault resources and csi-kms-connection-details configMap

        """
        vault.remove_vault_backend_path()
        vault.remove_vault_policy()
        if "VAULT_NAMESPACE" in os.environ:
            vault.remove_vault_namespace()
        KMS.remove_token_reviewer_resources()
        if len(KMS.get_encryption_kmsid()) > 1:
            KMS.remove_kmsid(vault.kmsid)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="session")
def nsfs_interface_session(request):
    return nsfs_interface_fixture(request)


@pytest.fixture(scope="function")
def nsfs_interface(request):
    return nsfs_interface_fixture(request)


def nsfs_interface_fixture(request):
    created_deployments = []

    def nsfs_interface_deployment_factory(pvc_name, pvc_mount_path="/nsfs"):
        """
        A factory for creating an NSFS deployment whose pods can be used as a filesystem interface
        for the NSFS PVC and bucket.

        Args:
            pvc_name (str): The name of the PVC to mount
            pvc_mount_path (str, optional): The filesystem path in which the PVC should be mounted. Defaults to '/nsfs'.

        Returns:
            (OCS): The OCS object of the NSFS deployment

        """
        nsfs_deployment_data = templating.load_yaml(constants.NSFS_INTERFACE_YAML)
        nsfs_deployment_data["metadata"]["name"] = create_unique_resource_name(
            "nsfs-interface", "deployment"
        )
        uid = nsfs_deployment_data["metadata"]["name"].split("-")[-1]
        nsfs_deployment_data["spec"]["selector"]["matchLabels"]["app"] += f"-{uid}"
        nsfs_deployment_data["spec"]["template"]["metadata"]["labels"][
            "app"
        ] += f"-{uid}"
        vol_mnt = nsfs_deployment_data["spec"]["template"]["spec"]["containers"][0][
            "volumeMounts"
        ][0]
        vol_mnt["name"] = pvc_name
        vol_mnt["mountPath"] = pvc_mount_path
        volumes = nsfs_deployment_data["spec"]["template"]["spec"]["volumes"][0]
        volumes["name"] = pvc_name
        volumes["persistentVolumeClaim"]["claimName"] = pvc_name

        if ocsci_config.DEPLOYMENT.get("disconnected"):
            update_container_with_mirrored_image(nsfs_deployment_data)

        deployment_obj = helpers.create_resource(**nsfs_deployment_data)
        created_deployments.append(deployment_obj)
        return deployment_obj

    def nsfs_interface_deployment_cleanup():
        """
        Delete the deployment that was created for the test

        """
        for deploy in created_deployments:
            deploy.delete()
            deploy.ocp.wait_for_delete(deploy.name)

    request.addfinalizer(nsfs_interface_deployment_cleanup)
    return nsfs_interface_deployment_factory


@pytest.fixture(scope="function")
def mcg_account_factory(request, mcg_obj_session):
    return mcg_account_factory_fixture(request, mcg_obj_session)


def mcg_account_factory_fixture(request, mcg_obj_session):
    created_accounts = []

    def mcg_account_factory_implementation(
        name,
        default_resource=constants.DEFAULT_NOOBAA_BACKINGSTORE,
        nsfs_account_config=False,
        uid=-1,
        gid=-1,
        new_buckets_path="/",
        nsfs_only=False,
        allow_bucket_create=True,
        ssl=True,
    ):
        """
        Create a new MCG account with the given parameters

        Args:
            name (str): Name of the user; Has to be RFC 1123 compliant
            default_resource (str): Default resource for the user
            new_buckets_path (str): The FS path in which new buckets will be created
            nsfs_account_config (bool): Whether the user has an NSFS account config
            uid (str): UID of the user
            gid (str): GID of the user
            nsfs_only (bool): Whether the user has access to NSFS only
            allow_bucket_create (bool): Whether the user is allowed to create buckets
            ssl (bool): Whether to use SSL for the connection

        Returns:
            A dictionary containing the S3 credentials, with the following keys:
            access_key (str)
            access_key_id (str)
            endpoint (str)
            ssl (bool)

        """
        if uid == -1:
            uid = random.randint(1000, 10000)
        if gid == -1:
            gid = random.randint(1000, 10000)

        # Build the mcg-cli command for creating an account
        cli_cmd = (
            f"account create {name} "
            f"--allow_bucket_create={allow_bucket_create} "
            f"--default_resource {default_resource} "
            f"--gid {gid} "
            f"--new_buckets_path {new_buckets_path} "
            f"--nsfs_account_config={nsfs_account_config} "
            f"--nsfs_only={nsfs_only} "
            f"--uid {uid} "
        )

        # Create the account
        acc_creation_process_output = mcg_obj_session.exec_mcg_cmd(cli_cmd)
        created_accounts.append(name)

        # Verify that the account was created successfuly and that the response contains the needed data
        assert "access_key" in str(acc_creation_process_output).lower(), (
            "Did not find access_key in account creation response. Response:"
            f" {str(acc_creation_process_output)}"
        )

        # Prepare the credentials dict
        acc_secret_dict = OCP(
            kind="secret", namespace=ocsci_config.ENV_DATA["cluster_namespace"]
        ).get(f"noobaa-account-{name}")
        access_key_id = base64.b64decode(
            acc_secret_dict["data"]["AWS_ACCESS_KEY_ID"]
        ).decode()
        access_key = base64.b64decode(
            acc_secret_dict["data"]["AWS_SECRET_ACCESS_KEY"]
        ).decode()

        return {
            "access_key_id": access_key_id,
            "access_key": access_key,
            "endpoint": mcg_obj_session.s3_endpoint,
            "ssl": ssl,
        }

    def mcg_account_factory_cleanup():
        for acc_name in created_accounts:
            log.info(f"Deleting MCG account {acc_name}")
            deletion_process_output = mcg_obj_session.exec_mcg_cmd(
                f"account delete {acc_name}"
            )
            assert "Deleted" in str(deletion_process_output)

    request.addfinalizer(mcg_account_factory_cleanup)
    return mcg_account_factory_implementation


@pytest.fixture(scope="function")
def nsfs_bucket_factory(
    namespace_store_factory,
    nsfs_interface,
    mcg_obj_session,
    mcg_account_factory,
    bucket_factory,
):
    return nsfs_bucket_factory_fixture(
        namespace_store_factory,
        nsfs_interface,
        mcg_obj_session,
        mcg_account_factory,
        bucket_factory,
    )


def nsfs_bucket_factory_fixture(
    namespace_store_factory,
    nsfs_interface,
    mcg_obj_session,
    mcg_account_factory,
    bucket_factory,
):
    def nsfs_bucket_factory_implementation(nsfs_obj):
        """
        A factory for creating an NSFS bucket and setting up all required components.

        Args:
            nsfs_obj (NSFS): An NSFS parametrization object (please see `mcg_params.py`)

        """
        # Get the number of nooba-endpoint pods before creating the nsfs
        original_endpoint_pods_count = get_pod_count(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL
        )

        # Create a PVC and namespacestore for the bucket
        nsfs_obj.nss = namespace_store_factory(
            nsfs_obj.method,
            {
                "nsfs": [
                    (
                        nsfs_obj.pvc_name,
                        nsfs_obj.pvc_size,
                        nsfs_obj.sub_path,
                        nsfs_obj.fs_backend,
                    )
                ]
            },
        )[0]
        # Create a deployment for mounting the PVC and accessing its filesystem
        nsfs_deploy = nsfs_interface(nsfs_obj.nss.uls_name, nsfs_obj.mount_path)
        deployment_app_label = nsfs_deploy.data["spec"]["selector"]["matchLabels"][
            "app"
        ]
        nsfs_obj.interface_pod = Pod(
            **get_pods_having_label(
                f"app={deployment_app_label}",
                ocsci_config.ENV_DATA["cluster_namespace"],
            )[0]
        )
        wait_for_pods_to_be_running(
            pod_names=[nsfs_obj.interface_pod.name],
            timeout=60,
            sleep=10,
        )

        # Wait for the new noobaa-endpoint pods with the mount to be created
        # and for the obsolete noobaa-endpoint pods to be terminated
        wait_for_pods_by_label_count(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            exptected_count=original_endpoint_pods_count,
        )

        # Apply the necessary permissions on the filesystem
        retry(CommandFailed, tries=3, delay=15)(nsfs_obj.interface_pod.exec_cmd_on_pod)(
            "chmod -R 777 /nsfs"
        )

        # Create a new MCG account and get its credentials
        nsfs_obj.s3_creds = mcg_account_factory(
            name=f"nsfs-integrity-test-{random.randrange(100)}",
            default_resource=nsfs_obj.nss.name,
            nsfs_account_config=True,
            gid=nsfs_obj.gid,
            uid=nsfs_obj.uid,
            ssl=False,
        )
        nsfs_s3_resource = boto3.resource(
            "s3",
            verify=False,
            endpoint_url=nsfs_obj.s3_creds["endpoint"],
            aws_access_key_id=nsfs_obj.s3_creds["access_key_id"],
            aws_secret_access_key=nsfs_obj.s3_creds["access_key"],
        )
        nsfs_obj.s3_client = nsfs_s3_resource.meta.client
        # Let the account propagate through the system
        time.sleep(15)

        # Create a new NSFS bucket
        # Follow this flow if the bucket should be created on top of an existing directory
        if nsfs_obj.mount_existing_dir:
            new_dir_name = helpers.create_unique_resource_name(
                resource_description="nsfs-bucket", resource_type="dir"
            )
            nsfs_obj.interface_pod.exec_cmd_on_pod(
                "mkdir -m" f" {nsfs_obj.existing_dir_mode} /nsfs/{new_dir_name}"
            )
            new_dir_path = f"/{new_dir_name}"

            nsfs_obj.bucket_name = bucket_factory(
                amount=1,
                interface="mcg-namespace",
                write_ns_resource={
                    "resource": nsfs_obj.nss.name,
                    "path": new_dir_path,
                },
                read_ns_resources=[
                    {"resource": nsfs_obj.nss.name, "path": new_dir_path}
                ],
            )[0].name

            # Allow access to the export dir by adding a bucket policy
            bucket_policy = gen_bucket_policy(
                user_list="*",
                actions_list=["*"],
                resources_list=["*"],
            )
            bucket_policy = json.dumps(bucket_policy)
            put_bucket_policy(mcg_obj_session, nsfs_obj.bucket_name, bucket_policy)

            nsfs_obj.mounted_bucket_path = f"{nsfs_obj.mount_path}/{new_dir_name}"

        # Otherwise, the new bucket will create a directory for itself
        else:
            nsfs_obj.bucket_name = retry(CommandFailed, tries=4, delay=10)(
                bucket_factory
            )(s3resource=nsfs_s3_resource)[0].name
            nsfs_obj.mounted_bucket_path = (
                f"{nsfs_obj.mount_path}/{nsfs_obj.bucket_name}"
            )

    return nsfs_bucket_factory_implementation


@pytest.fixture(scope="class")
def revert_noobaa_endpoint_scc_class(request):
    """
    This fixture reverts the noobaa-endpoint SCC back to the way it was before ODF 4.12.
    See https://url.corp.redhat.com/b92fd1d for details.

    """
    return revert_noobaa_endpoint_scc_fixture(request)


def revert_noobaa_endpoint_scc_fixture(request):
    """
    This fixture reverts the noobaa-endpoint SCC back to the way it was before ODF 4.12.
    See https://url.corp.redhat.com/b92fd1d for details.

    """

    ocp_scc = ocp.OCP(
        kind=constants.SCC, namespace=ocsci_config.ENV_DATA["cluster_namespace"]
    )
    nb_endpoint_scc_name = constants.NOOBAA_ENDPOINT_SERVICE_ACCOUNT_NAME
    nb_endpoint_sa = constants.NOOBAA_ENDPOINT_SERVICE_ACCOUNT

    # Abort if the noobaa-endpoint SCC has already been modified
    scc_dict = ocp_scc.get(resource_name=nb_endpoint_scc_name)
    if scc_dict["seLinuxContext"]["type"] == "MustRunAs" or scc_dict["users"]:
        return

    def revert_endpoint_scc_implementation():
        """
        1. Modify the noobaa-endpoint scc via oc patch
        2. Verify that the changes were not reconciled

        """
        # Modify the noobaa-endpoint SCC
        json_payload = [
            {"op": "replace", "path": "/seLinuxContext/type", "value": "MustRunAs"},
            {"op": "add", "path": "/users/0", "value": f"{nb_endpoint_sa}"},
        ]

        ocp_scc.patch(
            resource_name=nb_endpoint_scc_name,
            params=json_payload,
            format_type="json",
        )

        # Verify the changes
        scc_dict = ocp_scc.get(resource_name=nb_endpoint_scc_name)
        assert (
            scc_dict["seLinuxContext"]["type"] == "MustRunAs"
        ), "Failed to modify the noobaa-db SCC seLinuxContext type"
        assert (
            constants.NOOBAA_ENDPOINT_SERVICE_ACCOUNT in scc_dict["users"]
        ), "The noobaa-endpoint SA wasn't added to the noobaa-endpoint SCC"

    def finalizer():
        """
        1. Restore the noobaa-endpoint SCC back to its default values
        2. Verify that the changes were not reconciled

        """

        # Restore the noobaa-endpoint SCC back to it's default state
        json_payload = [
            {"op": "replace", "path": "/seLinuxContext/type", "value": "RunAsAny"},
            {"op": "remove", "path": "/users/0", "value": f"{nb_endpoint_sa}"},
        ]

        ocp_scc.patch(
            resource_name=nb_endpoint_scc_name,
            params=json_payload,
            format_type="json",
        )

        # Verify the changes
        scc_dict = ocp_scc.get(resource_name=nb_endpoint_scc_name)
        assert (
            scc_dict["seLinuxContext"]["type"] == "RunAsAny"
        ), "Failed to restore the default noobaa-endpoint SCC seLinuxContext type"
        assert (
            constants.NOOBAA_ENDPOINT_SERVICE_ACCOUNT not in scc_dict["users"]
        ), "Failed to restore the default noobaa-endpoint SA status"

    request.addfinalizer(finalizer)
    revert_endpoint_scc_implementation()


@pytest.fixture(scope="session", autouse=True)
def patch_consumer_toolbox_with_secret():
    """
    Patch the rook-ceph-tools deployment with ceph.admin key. Applicable for MS platform only to enable rook-ceph-tools
    to run ceph commands until we have the fix for rook-ceph-tools in consumer cluster

    """
    # Get the secret from provider if MS multicluster run
    if not (
        ocsci_config.multicluster
        and ocsci_config.ENV_DATA.get("platform", "").lower()
        in constants.MANAGED_SERVICE_PLATFORMS
        and not ocsci_config.RUN["cli_params"].get("deploy")
        and ocsci_config.ENV_DATA.get("platform").lower()
        != constants.FUSIONAAS_PLATFORM
    ):
        return

    restore_ctx_index = ocsci_config.cur_index

    # Get the admin key if available
    ceph_admin_key = os.environ.get("CEPHADMINKEY") or ocsci_config.AUTH.get(
        "external", {}
    ).get("ceph_admin_key")

    if not ceph_admin_key:
        provider_cluster = ""

        # Identify the provider cluster
        for cluster in ocsci_config.clusters:
            if cluster.ENV_DATA.get("cluster_type") == "provider":
                provider_cluster = cluster
                break
        if not provider_cluster:
            log.warning(
                "Provider cluster not found to patch rook-ceph-tools deployment on"
                " consumers with ceph.admin key. Assuming the toolbox on consumers are"
                " already fixed to run ceph commands."
            )
            return

        # Switch context to provider cluster
        log.info("Switching to the provider cluster context")
        ocsci_config.switch_ctx(provider_cluster.MULTICLUSTER["multicluster_index"])

        # Get the key from provider cluster tools pod
        provider_tools_pod = get_ceph_tools_pod()
        ceph_admin_key = (
            provider_tools_pod.exec_cmd_on_pod("grep key /etc/ceph/keyring")
            .strip()
            .split()[-1]
        )

    # Patch the rook-ceph-tools deployment of all consumer clusters
    for cluster in ocsci_config.clusters:
        if cluster.ENV_DATA.get("cluster_type") == "consumer":
            ocsci_config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            consumer_tools_pod = get_ceph_tools_pod()

            # Check whether ceph command is working on tools pod.
            # Patch is needed only if the error is "RADOS permission error"
            try:
                consumer_tools_pod.exec_ceph_cmd("ceph health")
                continue
            except Exception as exc:
                if "RADOS permission error" not in str(exc):
                    raise

            consumer_tools_deployment = OCP(
                kind=constants.DEPLOYMENT,
                namespace=ocsci_config.ENV_DATA["cluster_namespace"],
                resource_name="rook-ceph-tools",
            )
            patch_value = (
                '[{"op": "replace", "path": "/spec/template/spec/containers/0/env", '
                '"value":[{"name": "ROOK_CEPH_USERNAME", "value": "client.admin"}, '
                f'{{"name": "ROOK_CEPH_SECRET", "value": "{ceph_admin_key}"}}]}}]'
            )
            assert consumer_tools_deployment.patch(
                params=patch_value, format_type="json"
            ), "Failed to patch rook-ceph-tools deployment in consumer cluster"

            # Wait for the existing tools pod to delete
            consumer_tools_pod.ocp.wait_for_delete(
                resource_name=consumer_tools_pod.name
            )

            # Wait for the new tools pod to reach Running state
            new_tools_pod_info = get_pods_having_label(
                label=constants.TOOL_APP_LABEL,
                namespace=ocsci_config.ENV_DATA["cluster_namespace"],
            )[0]
            new_tools_pod = Pod(**new_tools_pod_info)
            helpers.wait_for_resource_state(new_tools_pod, constants.STATUS_RUNNING)

    log.info("Switching back to the initial cluster context")
    ocsci_config.switch_ctx(restore_ctx_index)


@pytest.fixture(scope="session", autouse=True)
def toolbox_on_faas_consumer():
    """
    Create tools pod on FaaS consumer cluster at the start of a test run while using multicluster configuration

    """
    from ocs_ci.helpers.managed_services import create_toolbox_on_faas_consumer

    if not (
        ocsci_config.multicluster
        and ocsci_config.ENV_DATA.get("platform", "").lower()
        == constants.FUSIONAAS_PLATFORM
        and ocsci_config.ENV_DATA["cluster_type"].lower() == constants.MS_CONSUMER_TYPE
        and not ocsci_config.RUN["cli_params"].get("deploy")
    ):
        return

    tools_pod = get_pods_having_label(
        label=constants.TOOL_APP_LABEL,
        namespace=ocsci_config.ENV_DATA["cluster_namespace"],
    )
    if not tools_pod:
        create_toolbox_on_faas_consumer()


@pytest.fixture(scope="function", autouse=True)
def switch_to_provider_for_test(request):
    """
    Switch to provider cluster as required by the test. Applicable for Managed Services and HCI Provider-client only if
    the marker 'runs_on_provider' is added in the test.

    """
    switched_to_provider = False
    current_cluster = ocsci_config.cluster_ctx
    if (
        request.node.get_closest_marker("runs_on_provider")
        and ocsci_config.multicluster
        and (
            current_cluster.ENV_DATA.get("platform", "").lower()
            in constants.HCI_PC_OR_MS_PLATFORM
        )
    ):
        for cluster in ocsci_config.clusters:
            if cluster.ENV_DATA.get("cluster_type") == "provider":
                provider_cluster = cluster
                log.debug("Switching to the provider cluster context")
                # TODO: Use 'switch_to_provider' function introduced in PR 5541
                ocsci_config.switch_ctx(
                    provider_cluster.MULTICLUSTER["multicluster_index"]
                )
                switched_to_provider = True
                break

    def finalizer():
        """
        Switch context to the initial cluster

        """
        if switched_to_provider:
            log.debug("Switching back to the previous cluster context")
            ocsci_config.switch_ctx(current_cluster.MULTICLUSTER["multicluster_index"])

    request.addfinalizer(finalizer)


@pytest.fixture()
def create_pvcs_and_pods(multi_pvc_factory, pod_factory, service_account_factory):
    """
    Create rbd, cephfs PVCs and dc pods. To be used for test cases which need
    rbd and cephfs PVCs with different access modes.

    """

    def factory(
        pvc_size=3,
        pods_for_rwx=1,
        access_modes_rbd=None,
        access_modes_cephfs=None,
        num_of_rbd_pvc=None,
        num_of_cephfs_pvc=None,
        replica_count=1,
        deployment_config=False,
        sc_rbd=None,
        sc_cephfs=None,
        pod_dict_path=None,
    ):
        """
        Args:
            pvc_size (int): The requested size for the PVC in GB
            pods_for_rwx (int): Number of pods to be created if PVC
                access mode is RWX
            access_modes_rbd (list): List of access modes. One of the
                access modes will be chosen for creating each PVC. To specify
                volume mode, append volume mode in the access mode name
                separated by '-'. Default is set as
                ['ReadWriteOnce', 'ReadWriteOnce-Block', 'ReadWriteMany-Block']
            access_modes_cephfs (list): List of access modes.
                One of the access modes will be chosen for creating each PVC.
                Default is set as ['ReadWriteOnce', 'ReadWriteMany']
            num_of_rbd_pvc (int): Number of rbd PVCs to be created. Value
                should be greater than or equal to the number of elements in
                the list 'access_modes_rbd'. Pass 0 for not creating RBD PVC.
            num_of_cephfs_pvc (int): Number of cephfs PVCs to be created
                Value should be greater than or equal to the number of
                elements in the list 'access_modes_cephfs'. Pass 0 for not
                creating CephFS PVC
            replica_count (int): The replica count for deployment config
            deployment_config (bool): True for DeploymentConfig creation,
                False otherwise
            sc_rbd (OCS): RBD storage class. ocs_ci.ocs.resources.ocs.OCS instance
                of 'StorageClass' kind
            sc_cephfs (OCS): Cephfs storage class. ocs_ci.ocs.resources.ocs.OCS instance
                of 'StorageClass' kind
            pod_dict_path (str): YAML path for the pod.

        Returns:
            tuple: List of pvcs and pods
        """

        access_modes_rbd = access_modes_rbd or [
            constants.ACCESS_MODE_RWO,
            f"{constants.ACCESS_MODE_RWO}-Block",
            f"{constants.ACCESS_MODE_RWX}-Block",
        ]

        access_modes_cephfs = access_modes_cephfs or [
            constants.ACCESS_MODE_RWO,
            constants.ACCESS_MODE_RWX,
        ]

        num_of_rbd_pvc = (
            num_of_rbd_pvc if num_of_rbd_pvc is not None else len(access_modes_rbd)
        )
        num_of_cephfs_pvc = (
            num_of_cephfs_pvc
            if num_of_cephfs_pvc is not None
            else len(access_modes_cephfs)
        )

        pvcs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            storageclass=sc_rbd,
            size=pvc_size,
            access_modes=access_modes_rbd,
            status=constants.STATUS_BOUND,
            num_of_pvc=num_of_rbd_pvc,
            timeout=180,
        )
        for pvc_obj in pvcs_rbd:
            pvc_obj.interface = constants.CEPHBLOCKPOOL

        project = pvcs_rbd[0].project if pvcs_rbd else None

        pvcs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project,
            storageclass=sc_cephfs,
            size=pvc_size,
            access_modes=access_modes_cephfs,
            status=constants.STATUS_BOUND,
            num_of_pvc=num_of_cephfs_pvc,
            timeout=180,
        )
        for pvc_obj in pvcs_cephfs:
            pvc_obj.interface = constants.CEPHFILESYSTEM

        pvcs = pvcs_cephfs + pvcs_rbd

        # Set volume mode on PVC objects
        for pvc_obj in pvcs:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, "volume_mode", pvc_info["spec"]["volumeMode"])

        sa_obj = service_account_factory(project=project) if deployment_config else None

        pods_dc = []
        pods = []

        # Create pods
        for pvc_obj in pvcs:
            if constants.CEPHFS_INTERFACE in pvc_obj.storageclass.name:
                interface = constants.CEPHFILESYSTEM
            else:
                interface = constants.CEPHBLOCKPOOL

            if deployment_config:
                pod_dict_path = pod_dict_path or constants.FEDORA_DC_YAML
            elif pvc_obj.volume_mode == "Block":
                pod_dict_path = pod_dict_path or constants.CSI_RBD_RAW_BLOCK_POD_YAML
            else:
                pod_dict_path = pod_dict_path if pod_dict_path else ""

            num_pods = (
                pods_for_rwx if pvc_obj.access_mode == constants.ACCESS_MODE_RWX else 1
            )
            for _ in range(num_pods):
                # pod_obj will be a Pod instance if deployment_config=False,
                # otherwise an OCP instance of kind DC
                pod_obj = pod_factory(
                    interface=interface,
                    pvc=pvc_obj,
                    pod_dict_path=pod_dict_path,
                    raw_block_pv=pvc_obj.volume_mode == "Block",
                    deployment_config=deployment_config,
                    service_account=sa_obj,
                    replica_count=replica_count,
                )
                pod_obj.pvc = pvc_obj
                pods_dc.append(pod_obj) if deployment_config else pods.append(pod_obj)

        # Get pod objects if deployment_config is True
        # pods_dc will be an empty list if deployment_config is False
        for pod_dc in pods_dc:
            pod_objs = get_all_pods(
                namespace=pvcs[0].project.namespace,
                selector=[pod_dc.name],
                selector_label="name",
            )
            for pod_obj in pod_objs:
                pod_obj.pvc = pod_dc.pvc
            pods.extend(pod_objs)

        log.info(
            f"Created {len(pvcs_cephfs)} cephfs PVCs and {len(pvcs_rbd)} rbd "
            f"PVCs. Created {len(pods)} pods. "
        )
        return pvcs, pods

    return factory


@pytest.fixture()
def multi_pvc_pod_lifecycle_factory(
    project_factory, multi_pvc_factory, pod_factory, teardown_factory
):
    return _multi_pvc_pod_lifecycle_factory(
        project_factory, multi_pvc_factory, pod_factory, teardown_factory
    )


@pytest.fixture()
def multi_obc_lifecycle_factory(
    bucket_factory, mcg_obj, awscli_pod_session, mcg_obj_session, test_directory_setup
):
    return _multi_obc_lifecycle_factory(
        bucket_factory,
        mcg_obj,
        awscli_pod_session,
        mcg_obj_session,
        test_directory_setup,
    )


@pytest.fixture(scope="session", autouse=True)
def set_live_must_gather_images(pytestconfig):
    """
    Set live must gather images
    """
    live_deployment = ocsci_config.DEPLOYMENT["live_deployment"]
    managed_ibmcloud_platform = (
        ocsci_config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
        and ocsci_config.ENV_DATA["deployment_type"] == "managed"
    )
    # For ROSA platforms, we use upstream must gather image
    if ocsci_config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        log.debug(
            "Live must gather image is not supported in Managed Service platforms"
        )
        return
    # As we cannot use internal build of must gather for IBM Cloud platform
    # we will use live must gather image as a W/A.
    if live_deployment or managed_ibmcloud_platform:
        update_live_must_gather_image()
    # For non GAed version of ODF as a W/A we need to use upstream must gather image
    # for IBM Cloud platform
    if (
        managed_ibmcloud_platform
        and not live_deployment
        and (version.get_semantic_ocs_version_from_config() >= version.VERSION_4_13)
    ):
        ocsci_config.REPORTING[
            "default_ocs_must_gather_image"
        ] = defaults.MUST_GATHER_UPSTREAM_IMAGE
        ocsci_config.REPORTING[
            "default_ocs_must_gather_latest_tag"
        ] = defaults.MUST_GATHER_UPSTREAM_TAG


@pytest.fixture(scope="function")
def create_scale_pods_and_pvcs_using_kube_job(request):
    """
    Create scale pods and PVCs using a kube job fixture. This fixture makes use of the
    FioPodScale class to create the expected number of PODs+PVCs
    """

    orig_index = None
    fio_scale = None

    def factory(
        scale_count=None,
        pvc_per_pod_count=5,
        start_io=True,
        io_runtime=None,
        pvc_size=None,
        max_pvc_size=30,
    ):
        """
        Create a factory for creating resources using k8s fixture.

        Args:
            scale_count (int): No of PVCs to be Scaled. Should be one of the values in the dict
                "constants.SCALE_PVC_ROUND_UP_VALUE".
            pvc_per_pod_count (int): Number of PVCs to be attached to single POD
            Example, If 20 then 20 PVCs will be attached to single POD
            start_io (bool): Binary value to start IO default it's True
            io_runtime (seconds): Runtime in Seconds to continue IO
            pvc_size (int): Size of PVC to be created
            max_pvc_size (int): The max size of the pvc

        Returns:
            FioPodScale: The FioPodScale object

        """
        nonlocal orig_index
        nonlocal fio_scale

        if (
            ocsci_config.multicluster
            and ocsci_config.ENV_DATA.get("platform", "").lower()
            in constants.HCI_PC_OR_MS_PLATFORM
        ):
            orig_index = ocsci_config.cur_index

        # Scale FIO pods in the cluster
        scale_count = scale_count or min(constants.SCALE_PVC_ROUND_UP_VALUE)
        fio_scale = FioPodScale(
            kind=constants.DEPLOYMENTCONFIG, node_selector=constants.SCALE_NODE_SELECTOR
        )
        kube_pod_obj_list, kube_pvc_obj_list = fio_scale.create_scale_pods(
            scale_count=scale_count,
            pvc_per_pod_count=pvc_per_pod_count,
            start_io=start_io,
            io_runtime=io_runtime,
            pvc_size=pvc_size,
            max_pvc_size=max_pvc_size,
        )
        kube_pod_obj_list_names = [p.name for p in kube_pod_obj_list]
        kube_pvc_obj_list_names = [p.name for p in kube_pvc_obj_list]

        log.info(
            f"kube pod list = {kube_pod_obj_list_names}, kube pvc list ="
            f" {kube_pvc_obj_list_names}"
        )

        return fio_scale

    def finalizer():
        if orig_index is not None:
            ocsci_config.switch_ctx(orig_index)
        log.info("Cleaning the fio_scale instance")
        if fio_scale and not fio_scale.is_cleanup:
            fio_scale.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="function")
def create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers(
    request, create_scale_pods_and_pvcs_using_kube_job
):
    """
    Create scale pods and PVCs using a kube job on MS consumers fixture. This fixture makes use of the
    FioPodScale class to create the expected number of PODs+PVCs.
    This fixture is for Managed service when using MS consumers.
    """
    orig_index = None
    consumer_index_per_fio_scale_dict = {}

    def factory(
        scale_count=None,
        pvc_per_pod_count=5,
        start_io=True,
        io_runtime=None,
        pvc_size=None,
        max_pvc_size=30,
        consumer_indexes=None,
    ):
        """
        Create a factory for creating scale pods and PVCs using k8s on MS consumers fixture.

        Args:
            scale_count (int): No of PVCs to be Scaled. Should be one of the values in the dict
                "constants.SCALE_PVC_ROUND_UP_VALUE".
            pvc_per_pod_count (int): Number of PVCs to be attached to single POD
            Example, If 20 then 20 PVCs will be attached to single POD
            start_io (bool): Binary value to start IO default it's True
            io_runtime (seconds): Runtime in Seconds to continue IO
            pvc_size (int): Size of PVC to be created
            max_pvc_size (int): The max size of the pvc
            consumer_indexes (list): the list of the consumer indexes to create scale pods and PVCs.
                If not specified, it creates scale pods and PVCs on all the consumers.

        Returns:
            dict: Dictionary of the consumer index per fio_scale object associated with the consumer.

        """
        nonlocal orig_index
        orig_index = ocsci_config.cur_index

        scale_count = scale_count or min(constants.SCALE_PVC_ROUND_UP_VALUE)
        consumer_indexes = consumer_indexes or ocsci_config.get_consumer_indexes_list()
        for consumer_i in consumer_indexes:
            ocsci_config.switch_ctx(consumer_i)

            fio_scale = FioPodScale(
                kind=constants.DEPLOYMENTCONFIG,
                node_selector=constants.SCALE_NODE_SELECTOR,
            )
            kube_pod_obj_list, kube_pvc_obj_list = fio_scale.create_scale_pods(
                scale_count=scale_count,
                pvc_per_pod_count=pvc_per_pod_count,
                start_io=start_io,
                io_runtime=io_runtime,
                pvc_size=pvc_size,
                max_pvc_size=max_pvc_size,
                obj_name_prefix=f"obj_c{consumer_i}_",
            )
            kube_pod_obj_list_names = [p.name for p in kube_pod_obj_list]
            kube_pvc_obj_list_names = [p.name for p in kube_pvc_obj_list]

            log.info(
                f"kube pod list = {kube_pod_obj_list_names}, kube pvc list ="
                f" {kube_pvc_obj_list_names}"
            )

            consumer_index_per_fio_scale_dict[consumer_i] = fio_scale

        ocsci_config.switch_ctx(orig_index)
        return consumer_index_per_fio_scale_dict

    @switch_to_default_cluster_index_at_last
    def finalizer():
        log.info("Cleaning the fio_scale instances")
        for consumer_i, fio_scale in consumer_index_per_fio_scale_dict.items():
            ocsci_config.switch_ctx(consumer_i)
            if not fio_scale.is_cleanup:
                fio_scale.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def dr_workload(request):
    """
    Setup Busybox workload for DR setup

    """
    instances = []
    ctx = []

    def factory(
        num_of_subscription=1,
        num_of_appset=0,
        appset_model=None,
        pvc_interface=constants.CEPHBLOCKPOOL,
        switch_ctx=None,
    ):
        """
        Args:
            num_of_subscription (int): Number of Subscription type workload to be created
            num_of_appset (int): Number of ApplicationSet type workload to be created
            appset_model (str): GitOps ApplicationSet deployment model. Valid values include "pull" or "push".
                ODF 4.16 onwards, "pull" model is the default if not user-provided.
            pvc_interface (str): 'CephBlockPool' or 'CephFileSystem'.
                This decides whether a RBD based or CephFS based resource is created. RBD is default.
            switch_ctx (int): The cluster index by the cluster name

        Raises:
            ResourceNotDeleted: In case workload resources not deleted properly

        Returns:
            list: objects of workload class.

        """
        ctx.append(switch_ctx)
        total_pvc_count = 0

        if pvc_interface == constants.CEPHBLOCKPOOL:
            interface = constants.RBD_INTERFACE
        else:
            interface = constants.CEPHFS_INTERFACE

        if num_of_appset > 0 and appset_model is None:
            ocs_version = version.get_semantic_ocs_version_from_config()
            appset_model = "pull" if ocs_version >= version.VERSION_4_16 else "push"

        for index in range(num_of_subscription):
            workload_key = "dr_workload_subscription"
            if ocsci_config.MULTICLUSTER["multicluster_mode"] == constants.RDR_MODE:
                workload_key += f"_{interface}"
            workload_details = ocsci_config.ENV_DATA[workload_key][index]
            workload = BusyBox(
                workload_dir=workload_details["workload_dir"],
                workload_pod_count=workload_details["pod_count"],
                workload_pvc_count=workload_details["pvc_count"],
            )
            instances.append(workload)
            total_pvc_count += workload_details["pvc_count"]
            workload.deploy_workload()

        for index in range(num_of_appset):
            workload_key = "dr_workload_appset"
            if ocsci_config.MULTICLUSTER["multicluster_mode"] == constants.RDR_MODE:
                workload_key += f"_{interface}"
            workload_details = ocsci_config.ENV_DATA[workload_key][index]
            workload = BusyBox_AppSet(
                workload_dir=workload_details["workload_dir"],
                workload_pod_count=workload_details["pod_count"],
                workload_pvc_count=workload_details["pvc_count"],
                workload_placement_name=workload_details[
                    "dr_workload_app_placement_name"
                ],
                workload_pvc_selector=workload_details["dr_workload_app_pvc_selector"],
                appset_model=appset_model,
            )
            instances.append(workload)
            total_pvc_count += workload_details["pvc_count"]
            workload.deploy_workload()
        if (
            ocsci_config.MULTICLUSTER["multicluster_mode"] == constants.RDR_MODE
            and pvc_interface == constants.CEPHBLOCKPOOL
        ):
            dr_helpers.wait_for_mirroring_status_ok(replaying_images=total_pvc_count)
        return instances

    def teardown():
        failed_to_delete = False
        for instance in instances:
            try:
                instance.delete_workload(switch_ctx=ctx[0], force=True)
            except ResourceNotDeleted:
                failed_to_delete = True

        if failed_to_delete:
            raise ResourceNotDeleted(
                "Workload deletion was unsuccessful. Leftover resources were removed from the managed clusters."
            )

    request.addfinalizer(teardown)
    return factory


@pytest.fixture()
def cnv_dr_workload(request):
    """
    Deploys CNV based workload for DR setup

    """
    instances = []

    def factory(
        num_of_vm_subscription=1, num_of_vm_appset_push=0, num_of_vm_appset_pull=0
    ):
        """
        Args:
            num_of_vm_subscription (int): Number of Subscription type workload to be created
            num_of_vm_appset_push (int): Number of ApplicationSet Push type workload to be created
            num_of_vm_appset_pull (int): Number of ApplicationSet Pull type workload to be created

        Raises:
            ResourceNotDeleted: In case workload resources not deleted properly

        Returns:
            list: objects of workload class

        """
        total_pvc_count = 0
        workload_types = [
            (constants.SUBSCRIPTION, "dr_cnv_workload_sub", num_of_vm_subscription),
            (
                constants.APPLICATION_SET,
                "dr_cnv_workload_appset_push",
                num_of_vm_appset_push,
            ),
            (
                constants.APPLICATION_SET,
                "dr_cnv_workload_appset_pull",
                num_of_vm_appset_pull,
            ),
        ]

        for workload_type, data_key, num_of_vm in workload_types:
            for index in range(num_of_vm):
                workload_details = ocsci_config.ENV_DATA[data_key][index]
                workload = CnvWorkload(
                    workload_type=workload_type,
                    workload_dir=workload_details["workload_dir"],
                    vm_name=workload_details["vm_name"],
                    vm_secret=workload_details["vm_secret"],
                    vm_username=workload_details["vm_username"],
                    workload_name=workload_details["name"],
                    workload_pod_count=workload_details["pod_count"],
                    workload_pvc_count=workload_details["pvc_count"],
                    workload_placement_name=workload_details[
                        "dr_workload_app_placement_name"
                    ],
                    workload_pvc_selector=workload_details[
                        "dr_workload_app_pvc_selector"
                    ],
                    appset_model=(
                        workload_details["appset_model"]
                        if workload_type == constants.APPLICATION_SET
                        else None
                    ),
                )
                instances.append(workload)
                total_pvc_count += workload_details["pvc_count"]
                workload.deploy_workload()

        if ocsci_config.MULTICLUSTER["multicluster_mode"] == constants.RDR_MODE:
            dr_helpers.wait_for_mirroring_status_ok(replaying_images=total_pvc_count)

        return instances

    def teardown():
        for instance in instances:
            try:
                instance.delete_workload(force=True)
            except ResourceNotDeleted:
                raise ResourceNotDeleted("Workload deletion was unsuccessful")

    request.addfinalizer(teardown)
    return factory


@pytest.fixture(scope="class")
def lvm_storageclass_factory_class(request, storageclass_factory_class):
    return lvm_storageclass_factory_fixture(request, storageclass_factory_class)


@pytest.fixture(scope="session")
def lvm_storageclass_factory_session(request, storageclass_factory_session):
    return lvm_storageclass_factory_fixture(request, storageclass_factory_session)


@pytest.fixture(scope="function")
def lvm_storageclass_factory(request, storageclass_factory):
    return lvm_storageclass_factory_fixture(request, storageclass_factory)


def lvm_storageclass_factory_fixture(request, storageclass_factory):
    """
    Create lvm storageclass, if volume binding mode is wffc it stays with the default storageclass,
    if volume binding mode is Immediate it create a new storageclass

    """
    instances = []

    def factory(
        volume_binding=constants.IMMEDIATE_VOLUMEBINDINGMODE,
    ):
        """
        Args:
            volume_binding (str): The volume binding mode for the stoargeclass

        """
        sc_obj = None
        if volume_binding == constants.WFFC_VOLUMEBINDINGMODE:
            sc_ocp_obj = OCP(kind="StorageClass", resource_name=constants.LVM_SC)
            sc_obj = OCS(**sc_ocp_obj.data)
            log.info(f"Will return default storageclass {sc_obj.name}")
        elif volume_binding == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            if version.get_semantic_ocs_version_from_config() <= version.VERSION_4_11:
                sc_obj = templating.load_yaml(constants.CSI_LVM_STORAGECLASS_YAML_4_11)
            elif version.get_semantic_ocs_version_from_config() >= version.VERSION_4_12:
                sc_obj = templating.load_yaml(constants.CSI_LVM_STORAGECLASS_YAML)
            sc_obj["metadata"]["name"] = create_unique_resource_name(
                resource_description="immediate-test",
                resource_type="storageclass",
            )
            sc_obj["volumeBindingMode"] = constants.IMMEDIATE_VOLUMEBINDINGMODE
            sc_obj = storageclass_factory(custom_data=sc_obj)
            log.info(f"Will return newly created storageclass {sc_obj.name}")
            instances.append(sc_obj)
        if sc_obj is not None:
            return sc_obj
        raise StorageclassNotCreated(
            f"Could not create storageclass with {volume_binding}"
        )

    def finalizer():
        """
        Delete storageclass
        """

        for instance in instances:
            if not instance.is_deleted:
                instance.delete(wait=True)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="session")
def fedora_pod_session(request):
    return fedora_pod_fixture(request, scope_name="session")


def fedora_pod_fixture(request, scope_name):
    """
    Creates a new fedora pod containing ~500k files
    on it in the folder /home/linux_tar_dir/

    Args:
        scope_name (str): The name of the fixture's scope,
        used for giving a descriptive name to the pod and configmap

    Returns:
        pod: A fedora pod running

    """
    # Create the service-ca configmap to be mounted upon pod creation
    service_ca_data = templating.load_yaml(constants.FEDORA_SERVICE_CA_YAML)
    service_ca_configmap_name = create_unique_resource_name(
        "fedora-service-ca", scope_name
    )
    service_ca_data["metadata"]["name"] = service_ca_configmap_name
    log.info("Trying to create the fedora service CA")
    service_ca_configmap = helpers.create_resource(**service_ca_data)

    # Create fedora pod
    pod_dict_path = constants.FEDORA_WITH_LINUXTAR_FILES_YAML
    fedora_pod_dict = templating.load_yaml(pod_dict_path)
    fedora_pod_dict["spec"]["volumes"][0]["configMap"][
        "name"
    ] = service_ca_configmap_name
    fedora_pod_name = create_unique_resource_name("fedora-pod", scope_name)
    fedora_pod_dict["metadata"]["name"] = fedora_pod_name

    update_container_with_mirrored_image(fedora_pod_dict)
    update_container_with_proxy_env(fedora_pod_dict)

    fedora_pod_obj = Pod(**fedora_pod_dict)
    assert fedora_pod_obj.create(
        do_reload=True
    ), f"Failed to create Pod {fedora_pod_name}"
    OCP(
        namespace=ocsci_config.ENV_DATA["cluster_namespace"], kind="ConfigMap"
    ).wait_for_resource(
        resource_name=service_ca_configmap.name, column="DATA", condition="1"
    )
    helpers.wait_for_resource_state(
        fedora_pod_obj, constants.STATUS_RUNNING, timeout=240
    )

    def fedora_pod_cleanup():
        fedora_pod_obj.delete()
        service_ca_configmap.delete()

    request.addfinalizer(fedora_pod_cleanup)

    return fedora_pod_obj


@pytest.fixture()
def scc_factory(request):
    """
    creates SecurityContextConstraints using this factory
    """
    scc_objs = []

    def create_scc(scc_name=None, scc_dict=None, scc_dict_path=None):
        scc_obj = helpers.create_scc(
            scc_name=scc_name, scc_dict=scc_dict, scc_dict_path=scc_dict_path
        )
        scc_objs.append(scc_obj)
        return scc_obj

    def teardown():
        for obj in scc_objs:
            obj.delete()

    request.addfinalizer(teardown)
    return create_scc


@pytest.fixture(scope="session")
def krew_rook_ceph_install_factory(request):
    """
    Install rook-ceph plugin
    """
    RookCephPlugin()


@pytest.fixture()
def ceph_objectstore_factory(request, krew_rook_ceph_install_factory):
    """
    Setup CephObjectStoreTool instance
    """
    return ceph_objectstore_tool_fixture(request)


def ceph_objectstore_tool_fixture(request):
    """
    Implementation of ceph_objectstore_factory()
    """
    cot_obj = CephObjectStoreTool()

    def teardown():
        deployment_in_debug = cot_obj.deployment_in_debug
        for deployment_name in list(deployment_in_debug):
            cot_obj.debug_stop(deployment_name=deployment_name)

    request.addfinalizer(teardown)

    return cot_obj


@pytest.fixture()
def ceph_monstore_factory(request, krew_rook_ceph_install_factory):
    """
    Setup MonStoreTool instance
    """
    return ceph_monstore_tool_fixture(request)


def ceph_monstore_tool_fixture(request):
    """
    Implementation of ceph_monstore_factory()
    """
    mot_obj = MonStoreTool()

    def teardown():
        deployment_in_debug = mot_obj.deployment_in_debug
        for deployment_name in list(deployment_in_debug):
            mot_obj.debug_stop(deployment_name=deployment_name)

    request.addfinalizer(teardown)

    return mot_obj


@pytest.fixture()
def change_the_noobaa_log_level(request):
    """
    This fixture helps you set the noobaa log level to any of these ["all", "nsfs", "default_level"]
    """
    noobaa_cm = OCP(
        kind="configmap",
        resource_name="noobaa-config",
        namespace=ocsci_config.ENV_DATA["cluster_namespace"],
    )

    def factory(level="all"):
        assert level in ["all", "nsfs", "default_level"], "Invalid noobaa log level"
        noobaa_cm.patch(
            params=f'{{"data": {{"NOOBAA_LOG_LEVEL": "{level}"}}}}', format_type="merge"
        )
        wait_for_pods_to_be_running(pod_names=[pod.name for pod in get_noobaa_pods()])

    def finalizer():
        level = "default_level"
        noobaa_cm.patch(
            params=f'{{"data": {{"NOOBAA_LOG_LEVEL": "{level}"}}}}', format_type="merge"
        )
        wait_for_pods_to_be_running(pod_names=[pod.name for pod in get_noobaa_pods()])

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def add_env_vars_to_noobaa_core_class(request, mcg_obj_session):
    """
    Class-scoped fixture for adding env vars to the noobaa-core sts

    """
    return add_env_vars_to_noobaa_core_fixture(request, mcg_obj_session)


def add_env_vars_to_noobaa_core_fixture(request, mcg_obj_session):
    """
    Add env vars to the noobaa-core sts

    """
    sts_obj = OCP(
        kind="StatefulSet", namespace=ocsci_config.ENV_DATA["cluster_namespace"]
    )
    yaml_path_to_env_variables = "/spec/template/spec/containers/0/env"
    op_template_dict = {"op": "", "path": "", "value": {"name": "", "value": ""}}

    added_env_vars = []

    def add_env_vars_to_noobaa_core_implementation(new_env_vars_touples):
        """
        Implementation of add_env_vars_to_noobaa_core_fixture()

        Args:
            new_env_vars_touples (list): A list of touples, each containing the env var name and
                value to be added to the noobaa-core sts
                i.e. [("env_var_name_1", "env_var_value_1"), ("env_var_name_2", "env_var_value_2")]

        """

        nb_core_sts = sts_obj.get(resource_name=constants.NOOBAA_CORE_STATEFULSET)
        sts_env_vars = nb_core_sts["spec"]["template"]["spec"]["containers"][0]["env"]
        sts_env_vars = [env_var_in_sts["name"] for env_var_in_sts in sts_env_vars]

        patch_ops = []

        for env_var, value in new_env_vars_touples:
            if env_var in sts_env_vars:
                log.warning(f"Env var {env_var} already exists in the noobaa-core sts")
                continue

            # Copy and modify the template to create the required dict for the first addition
            add_env_var_op = copy.deepcopy(op_template_dict)
            add_env_var_op["op"] = "add"
            add_env_var_op["path"] = f"{yaml_path_to_env_variables}/-"
            add_env_var_op["value"] = {"name": env_var, "value": str(value)}

            patch_ops.append(copy.deepcopy(add_env_var_op))
            added_env_vars.append(env_var)

        log.info(
            f"Adding following new env vars to the noobaa-core sts: {added_env_vars}"
        )
        sts_obj.patch(
            resource_name=constants.NOOBAA_CORE_STATEFULSET,
            params=json.dumps(patch_ops),
            format_type="json",
        )

        # Reset the noobaa-core pod to apply the changes
        mcg_obj_session.reset_core_pod()

    def finalizer():
        """
        Remove any env vars that were added to the noobaa-core sts

        """
        log.info("Removing the added env vars from the noobaa-core statefulset:")

        # Adjust the template for removal ops
        remove_env_var_op = copy.deepcopy(op_template_dict)
        remove_env_var_op["op"] = "remove"
        remove_env_var_op["path"] = ""
        del remove_env_var_op["value"]

        for target_env_var in added_env_vars:
            # Fetch the target's index from the noobaa-core statefulset
            nb_core_sts = sts_obj.get(resource_name=constants.NOOBAA_CORE_STATEFULSET)
            env_vars_in_sts = nb_core_sts["spec"]["template"]["spec"]["containers"][0][
                "env"
            ]
            env_vars_names_in_sts = [
                env_var_in_sts["name"] for env_var_in_sts in env_vars_in_sts
            ]
            target_index = env_vars_names_in_sts.index(target_env_var)
            remove_env_var_op["path"] = f"{yaml_path_to_env_variables}/{target_index}"

            # Patch the noobaa-core sts to remove the env var
            sts_obj.patch(
                resource_name=constants.NOOBAA_CORE_STATEFULSET,
                params=json.dumps([remove_env_var_op]),
                format_type="json",
            )

        # Reset the noobaa-core pod to apply the changes
        mcg_obj_session.reset_core_pod()

    request.addfinalizer(finalizer)
    return add_env_vars_to_noobaa_core_implementation


@pytest.fixture()
def logwriter_cephfs_many_pvc_factory(request, pvc_factory):
    return logwriter_cephfs_many_pvc(request, pvc_factory)


@pytest.fixture(scope="class")
def logwriter_cephfs_many_pvc_class(request, pvc_factory_class):
    return logwriter_cephfs_many_pvc(request, pvc_factory_class)


def logwriter_cephfs_many_pvc(request, pvc_factory):
    """
    Fixture to create RWX cephfs volume

    """

    def factory(project_name):
        return pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_name,
            size="10",
            access_mode=constants.ACCESS_MODE_RWX,
        )

    return factory


@pytest.fixture(scope="session")
def setup_stretch_cluster_project(request, project_factory_session):
    """
    Session scoped fixture for creating stretch cluster project

    """
    return project_factory_session(constants.STRETCH_CLUSTER_NAMESPACE)


@pytest.fixture(scope="class")
def logwriter_workload_class(request, teardown_factory_class):
    return setup_logwriter_workload(request, teardown_factory_class)


@pytest.fixture()
def logwriter_workload_factory(request, teardown_factory):
    return setup_logwriter_workload(request, teardown_factory)


def setup_logwriter_workload(request, teardown_factory):
    """
    Fixture to create logwriter deployment

    """

    def factory(pvc, logwriter_path):
        """
        Args:
            pvc (PVC): PVC object
            logwriter_path (str): String representing logwriter yaml path

        Returns:
            OCS object: Lgwriter deployment object

        """
        dc_data = templating.load_yaml(logwriter_path)
        dc_data["metadata"]["namespace"] = pvc.namespace
        dc_data["spec"]["replicas"] = 4
        dc_data["spec"]["template"]["spec"]["containers"][0][
            "image"
        ] = "quay.io/ocsci/logwriter:latest"
        dc_data["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
            "claimName"
        ] = pvc.name
        logwriter_dc = helpers.create_resource(**dc_data)
        teardown_factory(logwriter_dc)

        logwriter_dc_obj = Deployment(
            **get_deployments_having_label(
                label="app=logwriter-cephfs", namespace=pvc.namespace
            )[0]
        )
        logwriter_dc_pods = [
            pod["metadata"]["name"]
            for pod in get_pods_having_label(
                label="app=logwriter-cephfs", namespace=pvc.namespace
            )
        ]
        wait_for_pods_to_be_running(
            namespace=pvc.namespace, pod_names=logwriter_dc_pods
        )

        return logwriter_dc_obj

    return factory


@pytest.fixture(scope="class")
def logreader_workload_class(request, teardown_factory_class):
    return setup_logreader_workload(request, teardown_factory_class)


@pytest.fixture()
def logreader_workload_factory(request, teardown_factory):
    return setup_logreader_workload(request, teardown_factory)


def setup_logreader_workload(request, teardown_factory):
    def factory(pvc, logreader_path, duration=30):
        """
        Args:
            pvc (PVC): PVC object
            logreader_path (str): String representing logreader yaml path
            duration (int): Time in minutes, representing read duration

        Retuns:
            OCS object: Logreader job object

        """

        job_data = templating.load_yaml(logreader_path)
        job_data["metadata"]["namespace"] = pvc.namespace
        job_data["spec"]["completions"] = 4
        job_data["spec"]["parallelism"] = 4
        job_data["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
            "claimName"
        ] = pvc.name
        job_data["spec"]["template"]["spec"]["containers"][0][
            "image"
        ] = "quay.io/ocsci/logwriter:latest"
        job_data["spec"]["template"]["spec"]["containers"][0]["command"][
            2
        ] = f"/opt/logreader.py -t {duration} *.log -d"
        logreader_job = helpers.create_resource(**job_data)
        teardown_factory(logreader_job)

        logreader_job_obj = get_job_obj(
            name="logreader-cephfs", namespace=pvc.namespace
        )
        logreader_job_pods = [
            pod["metadata"]["name"]
            for pod in get_pods_having_label(
                label="app=logreader-cephfs", namespace=pvc.namespace
            )
        ]
        wait_for_pods_to_be_running(
            namespace=pvc.namespace, pod_names=logreader_job_pods
        )

        return logreader_job_obj

    return factory


@pytest.fixture(scope="class")
def setup_logwriter_cephfs_workload_class(
    request,
    setup_stretch_cluster_project,
    pvc_factory_class,
    logwriter_cephfs_many_pvc_class,
    logwriter_workload_class,
    logreader_workload_class,
):

    return setup_logwriter_cephfs_workload(
        request,
        setup_stretch_cluster_project,
        pvc_factory_class,
        logwriter_cephfs_many_pvc_class,
        logwriter_workload_class,
        logreader_workload_class,
    )


@pytest.fixture()
def setup_logwriter_cephfs_workload_factory(
    request,
    setup_stretch_cluster_project,
    pvc_factory,
    logwriter_cephfs_many_pvc_factory,
    logwriter_workload_factory,
    logreader_workload_factory,
):

    return setup_logwriter_cephfs_workload(
        request,
        setup_stretch_cluster_project,
        pvc_factory,
        logwriter_cephfs_many_pvc_factory,
        logwriter_workload_factory,
        logreader_workload_factory,
    )


def setup_logwriter_cephfs_workload(
    request,
    setup_stretch_cluster_project,
    pvc_factory,
    logwriter_cephfs_many_pvc_factory,
    logwriter_workload_factory,
    logreader_workload_factory,
):
    """
    This fixture will create the RWX cephfs volume and call the logwriter, logreader fixture to do
    complete setup

    """

    def factory(read_duration=30):
        """
        Args:
            read_duration (int): Time duration in minutes

        Returns:
             OCS objects: Representing both logwriter and logreader objects

        """
        logwriter_path = constants.LOGWRITER_CEPHFS_WRITER
        logreader_path = constants.LOGWRITER_CEPHFS_READER
        pvc = logwriter_cephfs_many_pvc_factory(
            project_name=setup_stretch_cluster_project
        )
        logwriter_workload = logwriter_workload_factory(
            pvc=pvc, logwriter_path=logwriter_path
        )
        logreader_workload = logreader_workload_factory(
            pvc=pvc, logreader_path=logreader_path, duration=read_duration
        )
        return logwriter_workload, logreader_workload

    return factory


@pytest.fixture(scope="class")
def setup_logwriter_rbd_workload_class(
    request, setup_stretch_cluster_project, teardown_factory_class
):
    return setup_logwriter_rbd_workload(
        request, setup_stretch_cluster_project, teardown_factory_class
    )


@pytest.fixture()
def setup_logwriter_rbd_workload_factory(
    request, setup_stretch_cluster_project, teardown_factory
):
    return setup_logwriter_rbd_workload(
        request, setup_stretch_cluster_project, teardown_factory
    )


def setup_logwriter_rbd_workload(
    request, setup_stretch_cluster_project, teardown_factory
):
    """
    This fixture will create the RWO RBD volume, create logwriter sts using that volume

    Returns:
        OCS object: Logwriter sts object

    """

    logwriter_sts_path = constants.LOGWRITER_STS_PATH
    sts_data = templating.load_yaml(logwriter_sts_path)
    sts_data["metadata"]["namespace"] = setup_stretch_cluster_project.namespace
    logwriter_sts = helpers.create_resource(**sts_data)
    teardown_factory(logwriter_sts)
    logwriter_sts_pods = [
        pod["metadata"]["name"]
        for pod in get_pods_having_label(
            label="app=logwriter-rbd", namespace=setup_stretch_cluster_project.namespace
        )
    ]
    wait_for_pods_to_be_running(
        namespace=setup_stretch_cluster_project.namespace, pod_names=logwriter_sts_pods
    )

    return logwriter_sts


@pytest.fixture()
def reduce_expiration_interval(add_env_vars_to_noobaa_core_class):
    """
    Reduce the interval in which the lifecycle
    background worker is running

    """

    def factory(interval):
        """
        Args:
            interval (int): new interval in minutes

        """
        new_intervals_in_miliseconds = 60 * interval * 1000
        add_env_vars_to_noobaa_core_class(
            [(constants.LIFECYCLE_INTERVAL_PARAM, new_intervals_in_miliseconds)]
        )

    return factory


@pytest.fixture()
def reset_conn_score():
    """
    This is a fixture that will reset the connections scores for
    each mon's

    """
    from ocs_ci.ocs.resources.stretchcluster import StretchCluster

    return StretchCluster().reset_conn_score()


@pytest.fixture(scope="session")
def allow_default_backingstore_override(request):
    """
    Modify the noobaa CR to allow overriding the default backingstore

    """

    nb_ocp_obj = OCP(
        kind="noobaa",
        namespace=ocsci_config.ENV_DATA["cluster_namespace"],
        resource_name="noobaa",
    )

    def patch_allow_manual_default_backingstore():
        """
        Patch "manualDefaultBackingStore: true" to the noobaa CR

        """
        add_op = [
            {"op": "add", "path": "/spec/manualDefaultBackingStore", "value": True}
        ]
        nb_ocp_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(add_op),
            format_type="json",
        )

    def finalizer():
        """
        Remove "manualDefaultBackingStore: true" from the noobaa CR

        """
        remove_op = [
            {
                "op": "remove",
                "path": "/spec/manualDefaultBackingStore",
            }
        ]
        nb_ocp_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(remove_op),
            format_type="json",
        )

    request.addfinalizer(finalizer)
    patch_allow_manual_default_backingstore()


@pytest.fixture(scope="session")
def override_default_backingstore_session(
    request,
    mcg_obj_session,
    backingstore_factory_session,
    allow_default_backingstore_override,
):
    return override_default_backingstore_fixture(
        request, mcg_obj_session, backingstore_factory_session
    )


@pytest.fixture(scope="function")
def override_default_backingstore(
    request, mcg_obj_session, backingstore_factory, allow_default_backingstore_override
):
    return override_default_backingstore_fixture(
        request, mcg_obj_session, backingstore_factory
    )


def override_default_backingstore_fixture(
    request, mcg_obj_session, backingstore_factory
):
    """
    Returns a function that overrides the default backingstore with an alternative
    of the same type.

    """

    bucketclass_ocp_obj = OCP(
        kind=constants.BUCKETCLASS,
        namespace=ocsci_config.ENV_DATA["cluster_namespace"],
        resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
    )

    def _override_nb_default_backingstore_implementation(alt_bs_name=None):
        """
        1. If the name of an alternative backingstore is not provided,
            Create a new backingstore of the same type as the current default
        2. Update the new default resource of the admin account
        3. Patch the default bucketclass to use the new default backingstore

        Args:
            alternative_backingstore_name (str, optional): The name of an alternative backingstore

        """

        # 1. if the name of an alternative backingstore is not provided,
        # Create a new backingstore of the same type as the current default
        if alt_bs_name is None:
            bs_dict = clone_bs_dict_from_backingstore(
                protype_backingstore_name=constants.DEFAULT_NOOBAA_BACKINGSTORE,
            )
            alt_bs_name = backingstore_factory("oc", bs_dict)[0].name

        # 2. Update the new default resource of the admin account
        mcg_obj_session.exec_mcg_cmd(
            "".join(
                (
                    f"account update {mcg_obj_session.noobaa_user} ",
                    f"--new_default_resource={alt_bs_name}",
                )
            )
        )

        # 3. Patch the default bucketclass to use the new default backingstore
        update_op = [
            {
                "op": "replace",
                "path": "/spec/placementPolicy/tiers/0/backingStores/0",
                "value": alt_bs_name,
            }
        ]
        bucketclass_ocp_obj.patch(
            resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
            params=json.dumps(update_op),
            format_type="json",
        )

        return alt_bs_name

    def finalizer():
        """
        Change the default backingstore back to the original

        """
        _override_nb_default_backingstore_implementation(
            constants.DEFAULT_NOOBAA_BACKINGSTORE
        )

    request.addfinalizer(finalizer)
    return _override_nb_default_backingstore_implementation


@pytest.fixture(scope="session")
def scale_noobaa_resources_session(request):
    """
    Session scoped fixture to scale noobaa resources

    """
    scale_noobaa_resources(request)


@pytest.fixture()
def scale_noobaa_resources_fixture(request):
    """
    Fixture to scale noobaa resources

    """
    scale_noobaa_resources(request)


def scale_noobaa_resources(request):
    """
    Scale the noobaa pod resources and scale endpoint count

    """

    def factory(min_ep_count=3, max_ep_count=3, cpu=6, memory="10Gi"):
        storagecluster_obj = OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        scale_endpoint_pods_param = (
            f'{{"spec": {{"multiCloudGateway": '
            f'{{"endpoints": {{"minCount": {min_ep_count},"maxCount": {max_ep_count}}}}}}}}}'
        )
        scale_noobaa_resources_param = (
            f'{{"spec": {{"resources": {{"noobaa-core": {{"limits": {{"cpu": {cpu},"memory": {memory}}},'
            f'"requests": {{"cpu": {cpu},"memory": {memory}}}}},'
            f'"noobaa-db": {{"limits": {{"cpu": {cpu},"memory": {memory}}},'
            f'"requests": {{"cpu": {cpu},"memory": {memory}}}}},'
            f'"noobaa-endpoint": {{"limits": {{"cpu": {cpu},"memory": {memory}}},'
            f'"requests": {{"cpu": {cpu},"memory": "{memory}}}}}}}}}}}'
        )
        storagecluster_obj.patch(params=scale_endpoint_pods_param, format_type="merge")
        log.info("Scaled noobaa endpoint counts")
        storagecluster_obj.patch(
            params=scale_noobaa_resources_param, format_type="merge"
        )
        log.info("Scaled noobaa pod resources")
        time.sleep(60)

    return factory


@pytest.fixture(scope="function")
def create_scale_pods_and_pvcs_using_kube_job_on_hci_clients(request):
    """
    Create scale pods and PVCs using a kube job on HCI clients fixture. This fixture makes use of the
    FioPodScale class to create the expected number of PODs+PVCs.
    This fixture is for HCI Bare Metal when using HCI clients.

    """
    orig_index = None
    client_index_per_fio_scale_dict = {}

    def factory(
        scale_count=None,
        pvc_per_pod_count=5,
        start_io=True,
        io_runtime=None,
        pvc_size=None,
        max_pvc_size=30,
        client_indexes=None,
    ):
        """
        Create a factory for creating scale pods and PVCs using k8s on HCI clients fixture.

        Args:
            scale_count (int): No of PVCs to be Scaled. Should be one of the values in the dict
                "constants.SCALE_PVC_ROUND_UP_VALUE".
            pvc_per_pod_count (int): Number of PVCs to be attached to single POD
            Example, If 20 then 20 PVCs will be attached to single POD
            start_io (bool): Binary value to start IO default it's True
            io_runtime (seconds): Runtime in Seconds to continue IO
            pvc_size (int): Size of PVC to be created
            max_pvc_size (int): The max size of the pvc
            client_indexes (list): The list of the client indexes to create scale pods and PVCs.
                If not specified, it creates scale pods and PVCs on all the clients.

        Returns:
            dict: Dictionary of the client index per fio_scale object associated with the client.

        """
        nonlocal orig_index
        orig_index = ocsci_config.cur_index

        scale_count = scale_count or min(constants.SCALE_PVC_ROUND_UP_VALUE)
        client_indexes = client_indexes or ocsci_config.get_consumer_indexes_list()
        for client_i in client_indexes:
            ocsci_config.switch_ctx(client_i)

            fio_scale = FioPodScale(
                kind=constants.DEPLOYMENTCONFIG,
                node_selector=constants.SCALE_NODE_SELECTOR,
            )
            # Save the client index and fio_scale object in a dictionary to clean the
            # fio_scale pods and PVCs in the teardown
            client_index_per_fio_scale_dict[client_i] = fio_scale

            log.info("Start creating scale pods and PVCs using the 'fio_scale' object")
            kube_pod_obj_list, kube_pvc_obj_list = fio_scale.create_scale_pods(
                scale_count=scale_count,
                pvc_per_pod_count=pvc_per_pod_count,
                start_io=start_io,
                io_runtime=io_runtime,
                pvc_size=pvc_size,
                max_pvc_size=max_pvc_size,
                obj_name_prefix=f"obj_c{client_i}_",
            )
            kube_pod_obj_list_names = [p.name for p in kube_pod_obj_list]
            kube_pvc_obj_list_names = [p.name for p in kube_pvc_obj_list]

            log.info(
                f"kube pod list = {kube_pod_obj_list_names}, kube pvc list ="
                f" {kube_pvc_obj_list_names}"
            )

            client_index_per_fio_scale_dict[client_i] = fio_scale

        ocsci_config.switch_ctx(orig_index)
        return client_index_per_fio_scale_dict

    @switch_to_default_cluster_index_at_last
    def finalizer():
        log.info("Cleaning the fio_scale instances")
        for client_i, fio_scale in client_index_per_fio_scale_dict.items():
            ocsci_config.switch_ctx(client_i)
            if not fio_scale.is_cleanup:
                fio_scale.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def reduce_replication_delay_setup(add_env_vars_to_noobaa_core_class):
    """
    A fixture to reduce the replication delay to one minute.

    Args:
        new_delay_in_miliseconds (function): A function to add env vars to the noobaa-core pod

    """
    log.warning("Reducing replication delay")

    def factory(new_delay_in_miliseconds=60 * 1000):
        new_env_var_tuples = [
            (constants.BUCKET_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
            (constants.BUCKET_LOG_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
        ]
        add_env_vars_to_noobaa_core_class(new_env_var_tuples)

    return factory


@pytest.fixture()
def aws_log_based_replication_setup(
    awscli_pod_session, mcg_obj_session, bucket_factory, reduce_replication_delay_setup
):
    """
    A fixture to set up standard log-based replication with deletion sync.

    Args:
        awscli_pod_session(Pod): A pod running the AWS CLI
        mcg_obj_session(MCG): An MCG object
        bucket_factory: A bucket factory fixture

    Returns:
        MockupBucketLogger: A MockupBucketLogger object
        Bucket: The source bucket
        Bucket: The target bucket

    """

    reduce_replication_delay_setup()

    def factory(bucketclass_dict=None):
        log.info("Starting log-based replication setup")
        if bucketclass_dict is None:
            bucketclass_dict = {
                "interface": "OC",
                "namespace_policy_dict": {
                    "type": "Single",
                    "namespacestore_dict": {
                        constants.AWS_PLATFORM: [(1, constants.DEFAULT_AWS_REGION)]
                    },
                },
            }
        target_bucket = bucket_factory(bucketclass=bucketclass_dict)[0]

        mockup_logger = MockupBucketLogger(
            awscli_pod=awscli_pod_session,
            mcg_obj=mcg_obj_session,
            bucket_factory=bucket_factory,
            platform=constants.AWS_PLATFORM,
            region=constants.DEFAULT_AWS_REGION,
        )
        replication_policy = AwsLogBasedReplicationPolicy(
            destination_bucket=target_bucket.name,
            sync_deletions=True,
            logs_bucket=mockup_logger.logs_bucket_uls_name,
        )

        source_bucket = bucket_factory(
            1, bucketclass=bucketclass_dict, replication_policy=replication_policy
        )[0]

        log.info("log-based replication setup complete")

        return mockup_logger, source_bucket, target_bucket

    return factory


@pytest.fixture(autouse=True, scope="function")
def update_current_active_test_marks_global(request):
    """
    This fixture updates ocs_ci.framework.pytest_customization.marks::get_current_test_marks
    with the marks of the current test

    Returns:
        list: The marks of the current test

    """
    marks = [mark.name for mark in request.node.iter_markers()]
    ocs_ci.framework.pytest_customization.marks.current_test_marks = marks


@pytest.fixture(scope="function")
def benchmark_workload_storageutilization(request):
    """
    This fixture is for cluster storage utilization using the benchmark operator.

    """
    benchmark_obj = None

    def factory(
        target_percentage,
        jobs="read",
        read_runtime=30,
        bs="4096KiB",
        storageclass=constants.DEFAULT_STORAGECLASS_RBD,
        timeout_completed=2400,
    ):
        """
        Setup of benchmark fio

        Args:
            target_percentage (int): The number of percentage to fill up the cluster
            jobs (str): fio job types to run, for example the readwrite option
            read_runtime (int): Amount of time in seconds to run read workloads
            bs (str): the Block size that need to used for the prefill
            storageclass (str): StorageClass to use for PVC per server pod
            timeout_completed (int): timeout client pod move to completed state

        """
        nonlocal benchmark_obj

        size = get_file_size(target_percentage)
        benchmark_obj = BenchmarkOperatorFIO()
        benchmark_obj.setup_benchmark_fio(
            total_size=size,
            jobs=jobs,
            read_runtime=read_runtime,
            bs=bs,
            storageclass=storageclass,
            timeout_completed=timeout_completed,
        )
        benchmark_obj.run_fio_benchmark_operator(is_completed=True)

    def finalizer():
        if benchmark_obj is not None:
            benchmark_obj.cleanup()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def scale_noobaa_db_pod_pv_size(request):
    """
    This fixtue helps to scale the noobaa db pv size.
    follows KCS: https://access.redhat.com/solutions/6976547
    Note: Once the noobaa db pv is scaled it can't be reverted back to the
    original size

    """

    operators = [
        constants.OCS_SUBSCRIPTION,
        constants.ROOK_CEPH_OPERATOR,
        constants.NOOBAA_OPERATOR_DEPLOYMENT,
    ]
    labels = [
        constants.OCS_OPERATOR_LABEL,
        constants.OPERATOR_LABEL,
        constants.NOOBAA_OPERATOR_POD_LABEL,
        constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
    ]
    nb_pvc = get_all_pvc_objs(selector=constants.NOOBAA_DB_LABEL_47_AND_ABOVE)[0]

    def factory(pv_size="50"):
        """
        Args:
            pv_size(int): Size in GB

        """
        pods = []

        for operator in operators:
            modify_deployment_replica_count(deployment_name=operator, replica_count=0)
        log.info(f"Scaled down operators: {operators}")

        modify_statefulset_replica_count(
            statefulset_name=constants.NOOBAA_DB_STATEFULSET, replica_count=0
        )
        log.info("Scaled down noobaa db sts")

        nb_pvc.resize_pvc(new_size=pv_size)
        log.info(f"{nb_pvc.name} is resized to {pv_size}")

        modify_statefulset_replica_count(
            statefulset_name=constants.NOOBAA_DB_STATEFULSET, replica_count=1
        )
        log.info("Scaled up noobaa db sts")

        for operator in operators:
            modify_deployment_replica_count(deployment_name=operator, replica_count=1)
        log.info(f"Scaled up operators: {operators}")

        for label in labels:
            pods.extend(
                get_pods_having_label(
                    label=label,
                    retry=5,
                    namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                )
            )

        wait_for_pods_to_be_running(
            pod_names=[pod_obj["metadata"]["name"] for pod_obj in pods]
        )

    def finalizer():
        pods = []

        modify_statefulset_replica_count(
            statefulset_name=constants.NOOBAA_DB_STATEFULSET, replica_count=1
        )
        log.info("Scaled up noobaa db sts")

        for operator in operators:
            modify_deployment_replica_count(deployment_name=operator, replica_count=1)
        log.info(f"Scaled up operators: {operators}")

        for label in labels:
            pods.extend(
                get_pods_having_label(
                    label=label,
                    retry=5,
                    namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                )
            )

        wait_for_pods_to_be_running(
            pod_names=[pod_obj["metadata"]["name"] for pod_obj in pods]
        )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def create_hypershift_clusters():
    """
    Create hosted hyperhift clusters.

    Here we create cluster deployment configuration that was set in the Test. With this configuration we
    create a hosted cluster. After successful creation of the hosted cluster, we update the Multicluster Config,
    adding the new cluster configuration to the list of the clusters. Now we can operate with new and old clusters
    switching the context of Multicluster Config

    Following arguments are necessary to build the hosted cluster configuration:
    ENV_DATA:
        clusters:
            <cluster_name>:
                hosted_cluster_path: <path>
                ocp_version: <version>
                cpu_cores_per_hosted_cluster: <cores>
                memory_per_hosted_cluster: <memory>
                hosted_odf_registry: <registry>
                hosted_odf_version: <version>
                setup_storage_client: <bool>
                nodepool_replicas: <replicas>

    """

    def factory(
        cluster_names, ocp_version, odf_version, setup_storage_client, nodepool_replicas
    ):
        """
        Factory function implementing the fixture

        Args:
            cluster_names (list): List of cluster names
            ocp_version (str): OCP version
            odf_version (str): ODF version
            setup_storage_client (bool): Setup storage client
            nodepool_replicas (int): Nodepool replicas; supported values are 2,3

        """
        hosted_cluster_conf_on_provider = {"ENV_DATA": {"clusters": {}}}

        for cluster_name in cluster_names:
            hosted_cluster_conf_on_provider["ENV_DATA"]["clusters"][cluster_name] = {
                "hosted_cluster_path": f"~/clusters/{cluster_name}/openshift-cluster-dir",
                "ocp_version": ocp_version,
                "cpu_cores_per_hosted_cluster": 8,
                "memory_per_hosted_cluster": "12Gi",
                "hosted_odf_registry": "quay.io/rhceph-dev/ocs-registry",
                "hosted_odf_version": odf_version,
                "setup_storage_client": setup_storage_client,
                "nodepool_replicas": nodepool_replicas,
            }

        log.info(
            "Creating a hosted clusters with following deployment config: \n%s",
            json.dumps(
                hosted_cluster_conf_on_provider, indent=4, cls=SetToListJSONEncoder
            ),
        )
        ocsci_config.update(hosted_cluster_conf_on_provider)

        # During the initial deployment phase, we always deploy Hosting and specific Hosted clusters.
        # To distinguish between clusters intended for deployment on deployment CI stage and those intended for
        # deployment on the Test stage, we pass the names of the clusters to be deployed to the
        # HostedClients().do_deploy() method.
        hosted_clients_obj = HostedClients()
        deployed_hosted_cluster_objects = hosted_clients_obj.do_deploy(cluster_names)
        deployed_clusters = [obj.name for obj in deployed_hosted_cluster_objects]

        for cluster_name in deployed_clusters:

            client_conf_default_dir = os.path.join(
                FUSION_CONF_DIR, f"hypershift_client_bm_{nodepool_replicas}w.yaml"
            )
            if not os.path.exists(client_conf_default_dir):
                raise FileNotFoundError(f"File {client_conf_default_dir} not found")
            with open(client_conf_default_dir) as file_stream:
                def_client_config_dict = {
                    k: (v if v is not None else {})
                    for (k, v) in yaml.safe_load(file_stream).items()
                }
                def_client_config_dict.get("ENV_DATA").update(
                    {"cluster_name": cluster_name}
                )
                kubeconfig_path = hosted_clients_obj.get_kubeconfig_path(cluster_name)
                log.info(f"Kubeconfig path: {kubeconfig_path}")
                def_client_config_dict.setdefault("RUN", {}).update(
                    {"kubeconfig": kubeconfig_path}
                )
                cluster_config = Config()
                cluster_config.update(def_client_config_dict)

                log.debug(
                    "Inserting new hosted cluster config to Multicluster Config "
                    f"\n{json.dumps(vars(cluster_config), indent=4, cls=SetToListJSONEncoder)}"
                )
                ocsci_config.insert_cluster_config(
                    ocsci_config.nclusters, cluster_config
                )

    return factory


@pytest.fixture()
def destroy_hosted_cluster():
    def factory(cluster_name):
        ocsci_config.switch_to_provider()
        log.info("Destroying hosted cluster. OCS related leftovers are expected")
        hypershift_base_obj = HyperShiftBase()

        if not hypershift_base_obj.hcp_binary_exists():
            hypershift_base_obj.update_hcp_binary()

        destroy_res = HyperShiftBase().destroy_kubevirt_cluster(cluster_name)

        if destroy_res:
            log.info("Removing cluster from Multicluster Config")
            ocsci_config.remove_cluster_by_name(cluster_name)

        return destroy_res

    return factory


@pytest.fixture(scope="session")
def clone_upstream_ceph(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'upstream ceph' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("upstream_ceph_dir")

    def finalizer():
        rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(
        constants.CEPH_UPSTREAM_REPO, str(repo_dir), branch="main", tmp_repo=True
    )
    return repo_dir


@pytest.fixture(scope="session")
def clone_ocs_operator(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'ocs operator' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("ocs_operator_dir")

    def finalizer():
        rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(constants.OCS_OPERATOR_REPO, str(repo_dir), branch="main", tmp_repo=True)
    return repo_dir


@pytest.fixture(scope="session")
def clone_odf_monitoring_compare_tool(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'ODF monitor compare tool' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("monitor_tool_dir")

    def finalizer():
        rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(
        constants.ODF_MONITORING_TOOL_REPO, str(repo_dir), branch="main", tmp_repo=True
    )
    return repo_dir
