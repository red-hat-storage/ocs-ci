import logging
import os
import random
import time
import tempfile
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from math import floor
from shutil import copyfile
from functools import partial

from botocore.exceptions import ClientError
import pytest

from ocs_ci.deployment import factory as dep_factory
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    deployment,
    ignore_leftovers,
    tier_marks,
    ignore_leftover_label,
)
from ocs_ci.ocs import constants, defaults, fio_artefacts, node, ocp, platform_nodes
from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    CephHealthException,
    ResourceWrongStatusException,
    UnsupportedPlatformError,
)
from ocs_ci.ocs.mcg_workload import mcg_job_factory as mcg_job_factory_implementation
from ocs_ci.ocs.node import get_node_objs, schedule_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.utils import setup_ceph_toolbox, collect_ocs_logs
from ocs_ci.ocs.resources.backingstore import (
    backingstore_factory as backingstore_factory_implementation,
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
)
from ocs_ci.ocs.resources.pvc import PVC, create_restore_pvc
from ocs_ci.ocs.version import get_ocs_version, report_ocs_version
from ocs_ci.ocs.cluster_load import ClusterLoad, wrap_msg
from ocs_ci.utility import aws
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj
from ocs_ci.utility import templating
from ocs_ci.utility import users
from ocs_ci.utility.environment_check import (
    get_status_before_execution,
    get_status_after_execution,
)
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.uninstall_openshift_logging import uninstall_cluster_logging
from ocs_ci.utility.utils import (
    ceph_health_check,
    ceph_health_check_base,
    get_running_ocp_version,
    get_openshift_client,
    get_system_architecture,
    get_testrun_name,
    ocsci_log_path,
    skipif_ocp_version,
    skipif_ocs_version,
    TimeoutSampler,
    skipif_upgraded_from,
    update_container_with_mirrored_image,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.bucket_utils import get_rgw_restart_counts
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.couchbase import CouchBase
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.elasticsearch import ElasticSearch
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser

log = logging.getLogger(__name__)


class OCSLogFormatter(logging.Formatter):
    def __init__(self):
        fmt = (
            "%(asctime)s - %(threadName)s - %(levelname)s - %(name)s.%(funcName)s.%(lineno)d "
            "- %(message)s"
        )
        super(OCSLogFormatter, self).__init__(fmt)


def pytest_logger_config(logger_config):
    logger_config.add_loggers([""], stdout_level="info")
    logger_config.set_log_option_default("")
    logger_config.split_by_outcome()
    logger_config.set_formatter_class(OCSLogFormatter)


def pytest_collection_modifyitems(session, items):
    """
    A pytest hook to filter out skipped tests satisfying
    skipif_ocs_version or skipif_upgraded_from

    Args:
        session: pytest session
        config: pytest config object
        items: list of collected tests

    """
    teardown = config.RUN["cli_params"].get("teardown")
    deploy = config.RUN["cli_params"].get("deploy")
    if not (teardown or deploy):
        for item in items[:]:
            skipif_ocp_version_marker = item.get_closest_marker("skipif_ocp_version")
            skipif_ocs_version_marker = item.get_closest_marker("skipif_ocs_version")
            skipif_upgraded_from_marker = item.get_closest_marker(
                "skipif_upgraded_from"
            )
            if skipif_ocp_version_marker:
                skip_condition = skipif_ocp_version_marker.args
                # skip_condition will be a tuple
                # and condition will be first element in the tuple
                if skipif_ocp_version(skip_condition[0]):
                    log.info(
                        f"Test: {item} will be skipped due to OCP {skip_condition}"
                    )
                    items.remove(item)
                    continue
            if skipif_ocs_version_marker:
                skip_condition = skipif_ocs_version_marker.args
                # skip_condition will be a tuple
                # and condition will be first element in the tuple
                if skipif_ocs_version(skip_condition[0]):
                    log.info(f"Test: {item} will be skipped due to {skip_condition}")
                    items.remove(item)
                    continue
            if skipif_upgraded_from_marker:
                skip_args = skipif_upgraded_from_marker.args
                if skipif_upgraded_from(skip_args[0]):
                    log.info(
                        f"Test: {item} will be skipped because the OCS cluster is"
                        f" upgraded from one of these versions: {skip_args[0]}"
                    )
                    items.remove(item)


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
    min_cpu = constants.MIN_NODE_CPU
    min_memory = constants.MIN_NODE_MEMORY

    log.info("Checking if system meets minimal requirements")
    if not check_nodes_specs(min_memory=min_memory, min_cpu=min_cpu):
        err_msg = (
            f"At least one of the worker nodes doesn't meet the "
            f"required minimum specs of {min_cpu} vCPUs and {min_memory} RAM"
        )
        pytest.xfail(err_msg)


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
    teardown = config.RUN["cli_params"].get("teardown")
    deploy = config.RUN["cli_params"].get("deploy")
    dev_mode = config.RUN["cli_params"].get("dev_mode")
    skip_ocs_deployment = config.ENV_DATA["skip_ocs_deployment"]
    if teardown and not deploy:
        log.info("Skipping version reporting for teardown.")
        return
    elif dev_mode:
        log.info("Skipping version reporting for development mode.")
        return
    elif skip_ocs_deployment:
        log.info("Skipping version reporting since OCS deployment is skipped.")
        return
    cluster_version, image_dict = get_ocs_version()
    file_name = os.path.join(
        config.ENV_DATA["cluster_path"], "ocs_version." + datetime.now().isoformat()
    )
    with open(file_name, "w") as file_obj:
        report_ocs_version(cluster_version, image_dict, file_obj)
    log.info("human readable ocs version info written into %s", file_name)


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
                kind=constants.CEPHFILESYSTEM, namespace=defaults.ROOK_CLUSTER_NAMESPACE
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
        for instance in instances:
            instance.delete()
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

        Returns:
            object: helpers.create_storage_class instance with links to
                block_pool and secret.
        """
        if custom_data:
            sc_obj = helpers.create_resource(**custom_data)
        else:
            secret = secret or secret_factory(interface=interface)
            if interface == constants.CEPHBLOCKPOOL:
                if config.ENV_DATA.get("new_rbd_pool") or new_rbd_pool:
                    pool_obj = ceph_pool_factory(
                        interface=interface,
                        replica=config.ENV_DATA.get("replica") or replica,
                        compression=config.ENV_DATA.get("compression") or compression,
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
            )
            assert sc_obj, f"Failed to create {interface} storage class"
            sc_obj.secret = secret

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
        """
        Delete the project
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
                    (
                        "There were %d events in %s namespace before it's"
                        " removal (out of which %d were of type Warning)."
                        " For a full dump of this event list, see DEBUG logs."
                    ),
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

    request.addfinalizer(finalizer)
    return factory


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

            pvc_size = f"{size}Gi" if size else None

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
        # If they have ReclaimPolicy set to Retain then delete them manually
        for pv_obj in pv_objs:
            if (
                pv_obj.data.get("spec").get("persistentVolumeReclaimPolicy")
                == constants.RECLAIM_POLICY_RETAIN
            ):
                helpers.wait_for_resource_state(pv_obj, constants.STATUS_RELEASED)
                pv_obj.delete()
                pv_obj.ocp.wait_for_delete(pv_obj.name)
            else:
                # Workaround for bug 1915706, increasing timeout from 180 to 720
                timeout = (
                    720
                    if config.ENV_DATA["platform"].lower() == constants.AZURE_PLATFORM
                    else 180
                )
                pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=timeout)

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
        replica_count=1,
        command=None,
        command_args=None,
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
            replica_count (int): The replica count for deployment config
            command (list): The command to be executed on the pod
            command_args (list): The arguments to be sent to the command running
                on the pod

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
                command=command,
                command_args=command_args,
            )
            assert pod_obj, "Failed to create pod"
        if deployment_config:
            dc_name = pod_obj.get_labels().get("name")
            dc_ocp_dict = ocp.OCP(
                kind=constants.DEPLOYMENTCONFIG, namespace=pod_obj.namespace
            ).get(resource_name=dc_name)
            dc_obj = OCS(**dc_ocp_dict)
            instances.append(dc_obj)

        else:
            instances.append(pod_obj)
        if status:
            helpers.wait_for_resource_state(pod_obj, status)
            pod_obj.reload()
        pod_obj.pvc = pvc
        if deployment_config:
            return dc_obj
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
                reclaim_policy = (
                    instance.reclaim_policy if instance.kind == constants.PVC else None
                )
                instance.delete()
                instance.ocp.wait_for_delete(instance.name)
                if reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                    helpers.validate_pv_delete(instance.backed_pv)

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
            helpers.remove_scc_policy(
                sa_name=instance.name, namespace=instance.namespace
            )
            instance.delete()
            instance.ocp.wait_for_delete(resource_name=instance.name)

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
            pvc = pvc or pvc_factory(interface=interface, size=size)
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
    polarion_project_id = config.REPORTING["polarion"]["project_id"]
    record_testsuite_property("polarion-project-id", polarion_project_id)
    jenkins_build_url = config.RUN.get("jenkins_build_url")
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
    logs_url = config.RUN.get("logs_url")
    if logs_url:
        record_testsuite_property("logs-url", logs_url)


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


@pytest.fixture(scope="function", autouse=True)
def health_checker(request, tier_marks_name):
    skipped = False
    dev_mode = config.RUN["cli_params"].get("dev_mode")
    if dev_mode:
        log.info("Skipping health checks for development mode")
        return

    def finalizer():
        if not skipped:
            try:
                teardown = config.RUN["cli_params"]["teardown"]
                skip_ocs_deployment = config.ENV_DATA["skip_ocs_deployment"]
                if not (teardown or skip_ocs_deployment):
                    ceph_health_check_base()
                    log.info("Ceph health check passed at teardown")
            except CephHealthException:
                log.info("Ceph health check failed at teardown")
                # Retrying to increase the chance the cluster health will be OK
                # for next test
                ceph_health_check()
                raise

    node = request.node
    request.addfinalizer(finalizer)
    for mark in node.iter_markers():
        if mark.name in tier_marks_name:
            log.info("Checking for Ceph Health OK ")
            try:
                status = ceph_health_check_base()
                if status:
                    log.info("Ceph health check passed at setup")
                    return
            except CephHealthException:
                skipped = True
                # skip because ceph is not in good health
                pytest.skip("Ceph health check failed at setup")


@pytest.fixture(scope="session", autouse=True)
def cluster(request, log_cli_level):
    """
    This fixture initiates deployment for both OCP and OCS clusters.
    Specific platform deployment classes will handle the fine details
    of action
    """
    log.info(f"All logs located at {ocsci_log_path()}")

    teardown = config.RUN["cli_params"]["teardown"]
    deploy = config.RUN["cli_params"]["deploy"]
    if teardown or deploy:
        factory = dep_factory.DeploymentFactory()
        deployer = factory.get_deployment()

    # Add a finalizer to teardown the cluster after test execution is finished
    if teardown:

        def cluster_teardown_finalizer():
            deployer.destroy_cluster(log_cli_level)

        request.addfinalizer(cluster_teardown_finalizer)
        log.info("Will teardown cluster because --teardown was provided")

    # Download client
    force_download = (
        config.RUN["cli_params"].get("deploy")
        and config.DEPLOYMENT["force_download_client"]
    )
    get_openshift_client(force_download=force_download)

    if deploy:
        # Deploy cluster
        deployer.deploy_cluster(log_cli_level)


@pytest.fixture(scope="class")
def environment_checker(request):
    node = request.node
    # List of marks for which we will ignore the leftover checker
    marks_to_ignore = [m.mark for m in [deployment, ignore_leftovers]]

    # app labels of resources to be excluded for leftover check
    exclude_labels = [constants.must_gather_pod_label]
    for mark in node.iter_markers():
        if mark in marks_to_ignore:
            return
        if mark.name == ignore_leftover_label.name:
            exclude_labels.extend(list(mark.args))
    request.addfinalizer(
        partial(get_status_after_execution, exclude_labels=exclude_labels)
    )
    get_status_before_execution(exclude_labels=exclude_labels)


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
):
    """
    Run IO during the test execution
    """
    cl_load_obj = None
    io_in_bg = config.RUN.get("io_in_bg")
    log_utilization = config.RUN.get("log_utilization")
    io_load = config.RUN.get("io_load")
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
            )
            cl_load_obj.reach_cluster_load_percentage()
        except Exception as ex:
            log.error(cluster_load_error_msg, ex)
            cluster_load_error = ex

    if (log_utilization or io_in_bg) and not deployment_test:
        if not cl_load_obj:
            try:
                cl_load_obj = ClusterLoad()
            except Exception as ex:
                log.error(cluster_load_error_msg, ex)
                cluster_load_error = ex

        config.RUN["load_status"] = "running"

        def finalizer():
            """
            Stop the thread that executed watch_load()
            """
            config.RUN["load_status"] = "finished"
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
            while config.RUN["load_status"] != "finished":
                time.sleep(20)
                try:
                    cl_load_obj.print_metrics(mute_logs=True)
                    if io_in_bg:
                        if config.RUN["load_status"] == "running":
                            cl_load_obj.adjust_load_if_needed()
                        elif config.RUN["load_status"] == "to_be_paused":
                            cl_load_obj.reduce_load(pause=True)
                            config.RUN["load_status"] = "paused"
                        elif config.RUN["load_status"] == "to_be_reduced":
                            cl_load_obj.reduce_load(pause=False)
                            config.RUN["load_status"] = "reduced"
                        elif config.RUN["load_status"] == "to_be_resumed":
                            cl_load_obj.resume_load()
                            config.RUN["load_status"] = "running"

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
    config.RUN["load_status"] = "to_be_resumed"
    try:
        for load_status in TimeoutSampler(300, 3, config.RUN.get, "load_status"):
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
    if config.RUN.get("io_in_bg"):

        def finalizer():
            """
            Resume the cluster load

            """
            if resume:
                resume_cluster_load_implementation()

        request.addfinalizer(finalizer)

        config.RUN["load_status"] = "to_be_paused" if pause else "to_be_reduced"
        try:
            for load_status in TimeoutSampler(300, 3, config.RUN.get, "load_status"):
                if load_status in ["paused", "reduced"]:
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
    if config.RUN.get("io_in_bg"):

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
        oc = ocp.OCP(namespace=config.ENV_DATA["cluster_namespace"])
        while get_flag_status() == "running":
            for worker in node.get_worker_nodes():
                filename = f"/tmp/{worker}-top-output.txt"
                top_cmd = f"debug nodes/{worker} -- chroot /host top -n 2 b"
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
def cld_mgr(request, rgw_endpoint):
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
        label=constants.RGW_APP_LABEL, namespace=config.ENV_DATA["cluster_namespace"]
    )
    if rgw_deployments:
        return RGW()
    else:
        return None


@pytest.fixture()
def rgw_deployments(request):
    """
    Return RGW deployments or skip the test.

    """
    rgw_deployments = get_deployments_having_label(
        label=constants.RGW_APP_LABEL, namespace=config.ENV_DATA["cluster_namespace"]
    )
    if rgw_deployments:
        return rgw_deployments
    else:
        pytest.skip("There is no RGW deployment available for this test.")


@pytest.fixture(scope="session")
def rgw_endpoint(request):
    """
    Expose RGW service and return external RGW endpoint address if available.

    Returns:
        string: external RGW endpoint

    """
    log.info("Looking for RGW service to expose")
    oc = ocp.OCP(kind=constants.SERVICE, namespace=config.ENV_DATA["cluster_namespace"])
    rgw_service = oc.get(selector=constants.RGW_APP_LABEL)["items"]
    if rgw_service:
        if config.DEPLOYMENT["external_mode"]:
            rgw_service = constants.RGW_SERVICE_EXTERNAL_MODE
        else:
            rgw_service = constants.RGW_SERVICE_INTERNAL_MODE
        log.info(f"Service {rgw_service} found and will be exposed")
        # custom hostname is provided because default hostname from rgw service
        # is too long and OCP rejects it
        oc = ocp.OCP(
            kind=constants.ROUTE, namespace=config.ENV_DATA["cluster_namespace"]
        )
        route = oc.get(resource_name="noobaa-mgmt")
        router_hostname = route["status"]["ingress"][0]["routerCanonicalHostname"]
        rgw_hostname = f"rgw.{router_hostname}"
        oc.exec_oc_cmd(f"expose service/{rgw_service} --hostname {rgw_hostname}")
        # new route is named after service
        rgw_endpoint = oc.get(resource_name=rgw_service)
        endpoint_obj = OCS(**rgw_endpoint)

        def _finalizer():
            endpoint_obj.delete()

        request.addfinalizer(_finalizer)
        return f"http://{rgw_hostname}"
    else:
        log.info("RGW service is not available")


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

    mcg_obj = MCG(*args, **kwargs)

    def finalizer():
        if config.ENV_DATA["platform"].lower() == "aws":
            mcg_obj.cred_req_obj.delete()

    if kwargs.get("create_aws_creds"):
        request.addfinalizer(finalizer)

    return mcg_obj


@pytest.fixture()
def awscli_pod(request):
    return awscli_pod_fixture(request, scope_name="function")


@pytest.fixture(scope="session")
def awscli_pod_session(request):
    return awscli_pod_fixture(request, scope_name="session")


def awscli_pod_fixture(request, scope_name):
    """
    Creates a new AWSCLI pod for relaying commands
    Args:
        scope_name (str): The name of the fixture's scope,
        used for giving a descriptive name to the pod and configmap

    Returns:
        pod: A pod running the AWS CLI

    """
    # Create the service-ca configmap to be mounted upon pod creation
    service_ca_data = templating.load_yaml(constants.AWSCLI_SERVICE_CA_YAML)
    service_ca_configmap_name = create_unique_resource_name(
        constants.AWSCLI_SERVICE_CA_CONFIGMAP_NAME, scope_name
    )
    service_ca_data["metadata"]["name"] = service_ca_configmap_name
    log.info("Trying to create the AWS CLI service CA")
    service_ca_configmap = helpers.create_resource(**service_ca_data)

    arch = get_system_architecture()
    if arch.startswith("x86"):
        pod_dict_path = constants.AWSCLI_POD_YAML
    else:
        pod_dict_path = constants.AWSCLI_MULTIARCH_POD_YAML

    awscli_pod_dict = templating.load_yaml(pod_dict_path)
    awscli_pod_dict["spec"]["volumes"][0]["configMap"][
        "name"
    ] = service_ca_configmap_name
    awscli_pod_name = create_unique_resource_name(
        constants.AWSCLI_RELAY_POD_NAME, scope_name
    )
    awscli_pod_dict["metadata"]["name"] = awscli_pod_name

    update_container_with_mirrored_image(awscli_pod_dict)

    awscli_pod_obj = Pod(**awscli_pod_dict)
    assert awscli_pod_obj.create(
        do_reload=True
    ), f"Failed to create Pod {awscli_pod_name}"
    OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE, kind="ConfigMap").wait_for_resource(
        resource_name=service_ca_configmap.name, column="DATA", condition="1"
    )
    helpers.wait_for_resource_state(awscli_pod_obj, constants.STATUS_RUNNING)

    def _awscli_pod_cleanup():
        awscli_pod_obj.delete()
        service_ca_configmap.delete()

    request.addfinalizer(_awscli_pod_cleanup)

    return awscli_pod_obj


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
    if config.ENV_DATA["platform"].lower() in constants.ON_PREM_PLATFORMS:
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
    Returns an MCG bucket factory
    """
    return bucket_factory_fixture(request, bucket_class_factory, mcg_obj)


@pytest.fixture(scope="session")
def bucket_factory_session(request, bucket_class_factory_session, mcg_obj_session):
    """
    Returns a session-scoped MCG bucket factory
    """
    return bucket_factory_fixture(
        request, bucket_class_factory_session, mcg_obj_session
    )


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
        if interface.lower() not in BUCKET_MAP:
            raise RuntimeError(
                f"Invalid interface type received: {interface}. "
                f'available types: {", ".join(BUCKET_MAP.keys())}'
            )

        bucketclass = (
            bucketclass if bucketclass is None else bucket_class_factory(bucketclass)
        )

        for i in range(amount):
            bucket_name = helpers.create_unique_resource_name(
                resource_description="bucket", resource_type=interface.lower()
            )
            created_bucket = BUCKET_MAP[interface.lower()](
                bucket_name,
                mcg=mcg_obj,
                rgw=rgw_obj,
                bucketclass=bucketclass,
                *args,
                **kwargs,
            )
            created_buckets.append(created_bucket)
            if verify_health:
                created_bucket.verify_health()

        return created_buckets

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

    """
    return backingstore_factory_implementation(
        request, cld_mgr, mcg_obj, cloud_uls_factory
    )


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

    """
    return backingstore_factory_implementation(
        request, cld_mgr, mcg_obj_session, cloud_uls_factory_session
    )


@pytest.fixture()
def bucket_class_factory(request, mcg_obj, backingstore_factory):
    """
    Create a Bucket Class factory.
    Calling this fixture creates a new Bucket Class.

    Returns:
        func: Factory method - each call to this function creates
            a bucketclass

    """
    return bucketclass_factory_implementation(request, mcg_obj, backingstore_factory)


@pytest.fixture(scope="session")
def bucket_class_factory_session(
    request, mcg_obj_session, backingstore_factory_session
):
    """
    Create a Bucket Class factory.
    Calling this fixture creates a new Bucket Class.

    Returns:
        func: Factory method - each call to this function creates
            a bucketclass

    """
    return bucketclass_factory_implementation(
        request, mcg_obj_session, backingstore_factory_session
    )


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
    for sc_name in ("ocs-storagecluster-ceph-rbd", "ocs-storagecluster-cephfs"):
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

    csv = ocp.OCP(
        kind=constants.CLUSTER_SERVICE_VERSION,
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE,
    )
    logging_csv = csv.get().get("items")
    if logging_csv:
        log.info("Logging is already configured, Skipping Installation")
        return

    log.info("Configuring Openshift-logging")

    # Checks OCP version
    ocp_version = get_running_ocp_version()

    # Creates namespace opensift-operators-redhat
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
    subscription_yaml["spec"]["channel"] = ocp_version
    helpers.create_resource(**subscription_yaml)
    assert ocp_logging_obj.get_elasticsearch_subscription()

    # Creates a namespace openshift-logging
    ocp_logging_obj.create_namespace(yaml_file=constants.CL_NAMESPACE_YAML)

    # Creates an operator-group for cluster-logging
    assert ocp_logging_obj.create_clusterlogging_operator_group(
        yaml_file=constants.CL_OG_YAML
    )

    # Creates subscription for cluster-logging
    cl_subscription = templating.load_yaml(constants.CL_SUB_YAML)
    cl_subscription["spec"]["channel"] = ocp_version
    helpers.create_resource(**cl_subscription)
    assert ocp_logging_obj.get_clusterlogging_subscription()

    # Creates instance in namespace openshift-logging
    cluster_logging_operator = OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    log.info(f"The cluster-logging-operator {cluster_logging_operator.get()}")
    ocp_logging_obj.create_instance()


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
        timeout=None,
        sc_name=None,
    ):
        """
        Factory to start pgsql workload

        Args:
            replicas (int): Number of pgbench pods to be deployed
            clients (int): Number of clients
            threads (int): Number of threads
            transactions (int): Number of transactions
            scaling_factor (int): scaling factor
            timeout (int): Time in seconds to wait

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
            timeout=timeout,
        )

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

    def factory(num_projects=1, num_of_builds=1):
        """
        Factory to start jenkins workload

        Args:
            num_projects (int): Number of Jenkins projects
            num_of_builds (int): Number of builds per project

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
    Couchbase factory fixture
    """
    couchbase = CouchBase()

    def factory(replicas=3, run_in_bg=False, skip_analyze=True, sc_name=None):
        """
        Factory to start couchbase workload

        Args:
            replicas (int): Number of couchbase workers to be deployed
            run_in_bg (bool): Run IOs in background as option
            skip_analyze (bool): Skip logs analysis as option
        """
        # Setup couchbase
        couchbase.setup_cb()
        # Create couchbase workers
        couchbase.create_couchbase_worker(replicas=replicas, sc_name=sc_name)
        # Run couchbase workload
        couchbase.run_workload(replicas=replicas, run_in_bg=run_in_bg)
        # Run sanity check on data logs
        couchbase.analyze_run(skip_analyze=skip_analyze)

        return couchbase

    def finalizer():
        """
        Clean up
        """
        couchbase.teardown()

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

        """
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

        # Check messages are sent and received
        threads = amq.run_in_bg(
            namespace=kafka_namespace, value=value, since_time=since_time
        )

        return amq, threads

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
    if config.ENV_DATA.get("measurement_dir"):
        measurement_dir = config.ENV_DATA.get("measurement_dir")
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
        list: Pod instances
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
def log_alerts(request):
    """
    Log alerts at the beginning and end of each test case. At the end of test
    case print a difference: what new alerts are in place after the test is
    complete.

    """
    alerts_before = []
    prometheus = None

    try:
        prometheus = PrometheusAPI()
    except Exception:
        log.exception("There was a problem with connecting to Promeheus")

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
                    f"There was a problem with collecting alerts for analysis: {alerts_response.text}"
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
    deploy = config.RUN["cli_params"]["deploy"]
    teardown = config.RUN["cli_params"].get("teardown")
    skip_ocs = config.ENV_DATA["skip_ocs_deployment"]
    if not (deploy or teardown or skip_ocs):
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
def node_restart_teardown(request, nodes):
    """
    Make sure all nodes are up again
    Make sure that all cluster's nodes are in 'Ready' state and if not,
    change them back to 'Ready' state by restarting the nodes
    """

    def finalizer():
        # Start the powered off nodes
        nodes.restart_nodes_by_stop_and_start_teardown()
        try:
            node.wait_for_nodes_status(status=constants.NODE_READY)
        except ResourceWrongStatusException:
            # Restart the nodes if in NotReady state
            not_ready_nodes = [
                n
                for n in node.get_node_objs()
                if n.ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
            ]
            if not_ready_nodes:
                log.info(
                    f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
                )
                nodes.restart_nodes(not_ready_nodes)
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
def snapshot_factory(request):
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


@pytest.fixture()
def snapshot_restore_factory(request):
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

        Returns:
            PVC: Restored PVC object

        """
        snapshot_info = snapshot_obj.get()
        size = size or snapshot_info["status"]["restoreSize"]
        restore_pvc_name = restore_pvc_name or (
            helpers.create_unique_resource_name(snapshot_obj.name, "restore")
        )

        if snapshot_info["spec"]["volumeSnapshotClassName"] == (
            helpers.default_volumesnapshotclass(constants.CEPHBLOCKPOOL).name
        ):
            storageclass = (
                storageclass
                or helpers.default_storage_class(constants.CEPHBLOCKPOOL).name
            )
            restore_pvc_yaml = restore_pvc_yaml or constants.CSI_RBD_PVC_RESTORE_YAML
            interface = constants.CEPHBLOCKPOOL
        elif snapshot_info["spec"]["volumeSnapshotClassName"] == (
            helpers.default_volumesnapshotclass(constants.CEPHFILESYSTEM).name
        ):
            storageclass = (
                storageclass
                or helpers.default_storage_class(constants.CEPHFILESYSTEM).name
            )
            restore_pvc_yaml = restore_pvc_yaml or constants.CSI_CEPHFS_PVC_RESTORE_YAML
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
        restored_pvc.interface = interface
        if status:
            helpers.wait_for_resource_state(restored_pvc, status)
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
        if not config.RUN["cli_params"].get("deploy") and not config.RUN[
            "cli_params"
        ].get("teardown"):
            if config.REPORTING["collect_logs_on_success_run"]:
                collect_ocs_logs("testcases", ocs=False, status_failure=False)
                collect_ocs_logs("testcases", ocp=False, status_failure=False)

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
    namespace = defaults.ROOK_CLUSTER_NAMESPACE
    should_wait = False

    # prior to 4.6 we configured the ep count directly on the noobaa cr.
    if float(config.ENV_DATA["ocs_version"]) < 4.6:
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
                        f"NooBaa endpoints stabilized. Ready endpoints: {ready_nb_ep_count}"
                    )
                    break
                log.info(
                    f"Waiting for the NooBaa endpoints to stabilize. "
                    f"Current ready count: {ready_nb_ep_count}"
                )
        except TimeoutExpiredError:
            raise TimeoutExpiredError(
                "NooBaa endpoints did not stabilize in time.\n"
                f"Min count: {min_ep_count}, max count: {max_ep_count}, ready count: {ready_nb_ep_count}"
            )


@pytest.fixture()
def pvc_clone_factory(request):
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

        Returns:
            PVC: PVC instance

        """
        assert (
            pvc_obj.provisioner in constants.OCS_PROVISIONERS
        ), f"Unknown provisioner in PVC {pvc_obj.name}"
        if pvc_obj.provisioner == "openshift-storage.rbd.csi.ceph.com":
            clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
            interface = constants.CEPHBLOCKPOOL
        elif pvc_obj.provisioner == "openshift-storage.cephfs.csi.ceph.com":
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
            interface = constants.CEPHFILESYSTEM

        size = size or pvc_obj.get().get("spec").get("resources").get("requests").get(
            "storage"
        )
        storageclass = storageclass or pvc_obj.backed_sc
        access_mode = access_mode or pvc_obj.get_pvc_access_mode
        volume_mode = volume_mode or getattr(pvc_obj, "volume_mode", None)

        # Create clone
        clone_pvc_obj = pvc.create_pvc_clone(
            sc_name=storageclass,
            parent_pvc=pvc_obj.name,
            clone_yaml=clone_yaml,
            pvc_name=clone_name,
            storage_size=size,
            access_mode=access_mode,
            volume_mode=volume_mode,
        )
        instances.append(clone_pvc_obj)
        clone_pvc_obj.parent = pvc_obj
        clone_pvc_obj.volume_mode = volume_mode
        clone_pvc_obj.interface = interface

        if status:
            helpers.wait_for_resource_state(clone_pvc_obj, status)
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
    if hasattr(request.node.config, "py_test_service"):
        rp_service = request.node.config.py_test_service
        if not hasattr(rp_service.RP, "rp_client"):
            request.config._metadata[
                "RP Launch URL:"
            ] = "Problem with RP, launch URL is not available!"
            return
        launch_id = rp_service.RP.rp_client.launch_id
        project = rp_service.RP.rp_client.project
        endpoint = rp_service.RP.rp_client.endpoint
        launch_url = f"{endpoint}/ui/#{project}/launches/all/{launch_id}/{launch_id}"
        config.REPORTING["rp_launch_url"] = launch_url
        config.REPORTING["rp_launch_id"] = launch_id
        config.REPORTING["rp_endpoint"] = endpoint
        config.REPORTING["rp_project"] = project
        request.config._metadata["RP Launch URL:"] = launch_url


@pytest.fixture()
def multi_pvc_clone_factory(pvc_clone_factory):
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

        Returns:
            PVC: List PVC instance

        """
        cloned_pvcs = []

        status_tmp = status if wait_each else ""

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

    def factory(pvc_size_new, pgsql):
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

        restored_pvc_objs = multi_snapshot_restore_factory(snapshot_obj=snapshots)
        log.info("Created new PVCs from all the snapshots")

        cloned_pvcs = multi_pvc_clone_factory(
            pvc_obj=restored_pvc_objs, volume_mode=constants.VOLUME_MODE_FILESYSTEM
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
            snapshot_obj=new_snapshots
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


@pytest.fixture(scope="function")
def setup_ui(request):
    driver = login_ui()

    def finalizer():
        close_browser(driver)

    request.addfinalizer(finalizer)

    return driver
