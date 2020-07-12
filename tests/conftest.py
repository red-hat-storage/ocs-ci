import logging
import os
import random
import time
import tempfile
import textwrap
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from itertools import chain
from math import floor
from random import randrange
from time import sleep
from shutil import copyfile
from functools import partial

import pytest
import yaml
from botocore.exceptions import ClientError

from ocs_ci.deployment import factory as dep_factory
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    deployment, ignore_leftovers, tier_marks, ignore_leftover_label
)
from ocs_ci.ocs import constants, ocp, defaults, node, platform_nodes
from ocs_ci.ocs.exceptions import TimeoutExpiredError, CephHealthException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.utils import setup_ceph_toolbox
from ocs_ci.ocs.resources.cloud_manager import CloudManager
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.mcg_bucket import S3Bucket, OCBucket, CLIBucket
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import get_rgw_pods, delete_deploymentconfig_pods
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.ocs.version import get_ocs_version, report_ocs_version
from ocs_ci.ocs.cluster_load import ClusterLoad
from ocs_ci.utility import aws
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj
from ocs_ci.utility import templating
from ocs_ci.utility.environment_check import (
    get_status_before_execution, get_status_after_execution
)
from ocs_ci.utility.uninstall_openshift_logging import uninstall_cluster_logging
from ocs_ci.utility.utils import (
    ceph_health_check,
    get_rook_repo,
    get_ocp_version,
    TimeoutSampler,
    update_container_with_mirrored_image,
)
from ocs_ci.utility.utils import (
    get_openshift_client, ocsci_log_path, get_testrun_name,
    ceph_health_check_base, skipif_ocs_version
)
from tests import helpers
from tests.helpers import create_unique_resource_name
from tests.manage.mcg.helpers import get_rgw_restart_counts
from tests.manage.mcg.helpers import (
    oc_create_aws_backingstore, oc_create_google_backingstore, oc_create_azure_backingstore,
    oc_create_s3comp_backingstore, oc_create_pv_backingstore, cli_create_aws_backingstore,
    cli_create_google_backingstore, cli_create_azure_backingstore, cli_create_s3comp_backingstore,
    cli_create_pv_backingstore
)
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.couchbase import CouchBase
from ocs_ci.ocs.amq import AMQ

log = logging.getLogger(__name__)


class OCSLogFormatter(logging.Formatter):

    def __init__(self):
        fmt = (
            "%(asctime)s - %(threadName)s - %(levelname)s - %(name)s.%(funcName)s.%(lineno)d "
            "- %(message)s"
        )
        super(OCSLogFormatter, self).__init__(fmt)


def pytest_logger_config(logger_config):
    logger_config.add_loggers([''], stdout_level='info')
    logger_config.set_log_option_default('')
    logger_config.split_by_outcome()
    logger_config.set_formatter_class(OCSLogFormatter)


def pytest_collection_modifyitems(session, config, items):
    """
    A pytest hook to filter out skipped tests satisfying
    skipif_ocs_version

    Args:
        session: pytest session
        config: pytest config object
        items: list of collected tests

    """
    for item in items[:]:
        skip_marker = item.get_closest_marker("skipif_ocs_version")
        if skip_marker:
            skip_condition = skip_marker.args
            # skip_condition will be a tuple
            # and condition will be first element in the tuple
            if skipif_ocs_version(skip_condition[0]):
                log.info(
                    f'Test: {item} will be skipped due to {skip_condition}'
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
    min_cpu = 16
    min_memory = 64 * 10 ** 9

    node_obj = ocp.OCP(kind=constants.NODE)
    log.info('Checking if system meets minimal requirements')
    nodes = node_obj.get(selector=constants.WORKER_LABEL).get('items')
    log.info(
        f"Checking following nodes with worker selector (assuming that "
        f"this is ran in CI and there are no worker nodes without OCS):\n"
        f"{[item.get('metadata').get('name') for item in nodes]}"
    )
    for node_info in nodes:
        real_cpu = int(node_info['status']['capacity']['cpu'])
        real_memory = node_info['status']['capacity']['memory']
        if real_memory.endswith('Ki'):
            real_memory = int(real_memory[0:-2]) * 2 ** 10
        elif real_memory.endswith('Mi'):
            real_memory = int(real_memory[0:-2]) * 2 ** 20
        elif real_memory.endswith('Gi'):
            real_memory = int(real_memory[0:-2]) * 2 ** 30
        elif real_memory.endswith('Ti'):
            real_memory = int(real_memory[0:-2]) * 2 ** 40
        else:
            real_memory = int(real_memory)

        if (real_cpu < min_cpu or real_memory < min_memory):
            error_msg = (
                f"Node {node_info.get('metadata').get('name')} doesn't have "
                f"minimum of required reasources for running the test:\n"
                f"{min_cpu} CPU and {min_memory} Memory\nIt has:\n{real_cpu} "
                f"CPU and {real_memory} Memory"
            )
            log.error(error_msg)
            pytest.xfail(error_msg)


@pytest.fixture(scope='class')
def secret_factory_class(request):
    return secret_factory_fixture(request)


@pytest.fixture(scope='session')
def secret_factory_session(request):
    return secret_factory_fixture(request)


@pytest.fixture(scope='function')
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
        secret_obj = helpers.create_secret(
            interface_type=interface
        )
        assert secret_obj, "Failed to create a secret"
        instances.append(secret_obj)
        return secret_obj

    def finalizer():
        """
        Delete the RBD secrets
        """
        for instance in instances:
            instance.delete()
            instance.ocp.wait_for_delete(
                instance.name
            )

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
    teardown = config.RUN['cli_params'].get('teardown')
    deploy = config.RUN['cli_params'].get('deploy')
    if teardown and not deploy:
        log.info("Skipping version reporting for teardown.")
        return
    cluster_version, image_dict = get_ocs_version()
    file_name = os.path.join(
        config.ENV_DATA['cluster_path'],
        "ocs_version." + datetime.now().isoformat())
    with open(file_name, "w") as file_obj:
        report_ocs_version(cluster_version, image_dict, file_obj)
    log.info("human readable ocs version info written into %s", file_name)


@pytest.fixture(scope='class')
def ceph_pool_factory_class(request):
    return ceph_pool_factory_fixture(request)


@pytest.fixture(scope='session')
def ceph_pool_factory_session(request):
    return ceph_pool_factory_fixture(request)


@pytest.fixture(scope='function')
def ceph_pool_factory(request):
    return ceph_pool_factory_fixture(request)


def ceph_pool_factory_fixture(request):
    """
    Create a Ceph pool factory.
    Calling this fixture creates new Ceph pool instance.
    ** This method should not be used anymore **
    ** This method is for internal testing only **
    """
    instances = []

    def factory(interface=constants.CEPHBLOCKPOOL):
        if interface == constants.CEPHBLOCKPOOL:
            ceph_pool_obj = helpers.create_ceph_block_pool()
        elif interface == constants.CEPHFILESYSTEM:
            cfs = ocp.OCP(
                kind=constants.CEPHFILESYSTEM,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE
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
            instance.ocp.wait_for_delete(
                instance.name
            )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope='class')
def storageclass_factory_class(
    request,
    ceph_pool_factory_class,
    secret_factory_class
):
    return storageclass_factory_fixture(
        request,
        ceph_pool_factory_class,
        secret_factory_class
    )


@pytest.fixture(scope='session')
def storageclass_factory_session(
    request,
    ceph_pool_factory_session,
    secret_factory_session
):
    return storageclass_factory_fixture(
        request,
        ceph_pool_factory_session,
        secret_factory_session
    )


@pytest.fixture(scope='function')
def storageclass_factory(
    request,
    ceph_pool_factory,
    secret_factory
):
    return storageclass_factory_fixture(
        request,
        ceph_pool_factory,
        secret_factory
    )


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
        reclaim_policy=constants.RECLAIM_POLICY_DELETE
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

        Returns:
            object: helpers.create_storage_class instance with links to
                block_pool and secret.
        """
        if custom_data:
            sc_obj = helpers.create_resource(**custom_data)
        else:
            secret = secret or secret_factory(interface=interface)
            if interface == constants.CEPHBLOCKPOOL:
                interface_name = constants.DEFAULT_BLOCKPOOL
            elif interface == constants.CEPHFILESYSTEM:
                interface_name = helpers.get_cephfs_data_pool_name()

            sc_obj = helpers.create_storage_class(
                interface_type=interface,
                interface_name=interface_name,
                secret_name=secret.name,
                sc_name=sc_name,
                reclaim_policy=reclaim_policy
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
            instance.ocp.wait_for_delete(
                instance.name
            )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope='class')
def project_factory_class(request):
    return project_factory_fixture(request)


@pytest.fixture(scope='session')
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
                event_count = len(events['items'])
                warn_event_count = 0
                for event in events['items']:
                    if event['type'] == "Warning":
                        warn_event_count += 1
                log.info(
                    (
                        "There were %d events in %s namespace before it's"
                        " removal (out of which %d were of type Warning)."
                        " For a full dump of this event list, see DEBUG logs."
                    ),
                    event_count,
                    instance.namespace,
                    warn_event_count
                )
            except Exception:
                # we don't want any problem to disrupt the teardown itself
                log.exception(
                    "Failed to get events for project %s",
                    instance.namespace
                )
            ocp.switch_to_default_rook_cluster_project()
            instance.delete(resource_name=instance.namespace)
            instance.wait_for_delete(instance.namespace, timeout=300)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope='class')
def pvc_factory_class(
    request,
    project_factory_class
):
    return pvc_factory_fixture(
        request,
        project_factory_class
    )


@pytest.fixture(scope='session')
def pvc_factory_session(
    request,
    project_factory_session
):
    return pvc_factory_fixture(
        request,
        project_factory_session
    )


@pytest.fixture(scope='function')
def pvc_factory(
    request,
    project_factory
):
    return pvc_factory_fixture(
        request,
        project_factory,
    )


def pvc_factory_fixture(
    request,
    project_factory
):
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
                instance.ocp.wait_for_delete(
                    instance.name
                )

        # Wait for PVs to delete
        # If they have ReclaimPolicy set to Retain then delete them manually
        for pv_obj in pv_objs:
            if pv_obj.data.get('spec').get(
                'persistentVolumeReclaimPolicy'
            ) == constants.RECLAIM_POLICY_RETAIN:
                helpers.wait_for_resource_state(
                    pv_obj,
                    constants.STATUS_RELEASED
                )
                pv_obj.delete()
                pv_obj.ocp.wait_for_delete(pv_obj.name)
            else:
                pv_obj.ocp.wait_for_delete(
                    resource_name=pv_obj.name, timeout=180
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope='class')
def pod_factory_class(request, pvc_factory_class):
    return pod_factory_fixture(request, pvc_factory_class)


@pytest.fixture(scope='session')
def pod_factory_session(request, pvc_factory_session):
    return pod_factory_fixture(request, pvc_factory_session)


@pytest.fixture(scope='function')
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
        command_args=None
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
                command_args=command_args
            )
            assert pod_obj, "Failed to create pod"
        if deployment_config:
            dc_name = pod_obj.get_labels().get('name')
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
            instance.ocp.wait_for_delete(
                instance.name
            )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope='class')
def teardown_factory_class(request):
    return teardown_factory_fixture(request)


@pytest.fixture(scope='session')
def teardown_factory_session(request):
    return teardown_factory_fixture(request)


@pytest.fixture(scope='function')
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
                reclaim_policy = instance.reclaim_policy if instance.kind == constants.PVC else None
                instance.delete()
                instance.ocp.wait_for_delete(
                    instance.name
                )
                if reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                    helpers.validate_pv_delete(instance.backed_pv)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope='class')
def service_account_factory_class(request):
    return service_account_factory_fixture(request)


@pytest.fixture(scope='session')
def service_account_factory_session(request):
    return service_account_factory_fixture(request)


@pytest.fixture(scope='function')
def service_account_factory(request):
    return service_account_factory_fixture(request)


def service_account_factory_fixture(request):
    """
    Create a service account
    """
    instances = []
    active_service_account_obj = None

    def factory(
        project=None, service_account=None
    ):
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
            sa_obj = helpers.get_serviceaccount_obj(sa_name=service_account,
                                                    namespace=project.namespace)
            if not helpers.validate_scc_policy(sa_name=service_account,
                                               namespace=project.namespace):
                helpers.add_scc_policy(sa_name=service_account, namespace=project.namespace)
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
                sa_name=instance.name,
                namespace=instance.namespace
            )
            instance.delete()
            instance.ocp.wait_for_delete(resource_name=instance.name)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def dc_pod_factory(
    request,
    pvc_factory,
    service_account_factory
):
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
        wait=True
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
            sa_obj = sa_obj or service_account_factory(project=pvc.project, service_account=service_account)
            dc_pod_obj = helpers.create_pod(
                interface_type=interface, pvc_name=pvc.name, do_reload=False,
                namespace=pvc.namespace, sa_name=sa_obj.name, dc_deployment=True,
                replica_count=replica_count, node_name=node_name, node_selector=node_selector,
                raw_block_pv=raw_block_pv, pod_dict_path=constants.FEDORA_DC_YAML
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
    polarion_project_id = config.REPORTING['polarion']['project_id']
    record_testsuite_property('polarion-project-id', polarion_project_id)
    jenkins_build_url = config.RUN.get('jenkins_build_url')
    if jenkins_build_url:
        record_testsuite_property(
            'polarion-custom-description', jenkins_build_url
        )
    polarion_testrun_name = get_testrun_name()
    record_testsuite_property(
        'polarion-testrun-id', polarion_testrun_name
    )
    record_testsuite_property(
        'polarion-testrun-status-id', 'inprogress'
    )
    record_testsuite_property(
        'polarion-custom-isautomated', "True"
    )


@pytest.fixture(scope='session')
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


@pytest.fixture(scope='function', autouse=True)
def health_checker(request, tier_marks_name):
    skipped = False

    def finalizer():
        if not skipped:
            try:
                teardown = config.RUN['cli_params']['teardown']
                skip_ocs_deployment = config.ENV_DATA['skip_ocs_deployment']
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

    teardown = config.RUN['cli_params']['teardown']
    deploy = config.RUN['cli_params']['deploy']
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
        config.RUN['cli_params'].get('deploy')
        and config.DEPLOYMENT['force_download_client']
    )
    get_openshift_client(force_download=force_download)

    if deploy:
        # Deploy cluster
        deployer.deploy_cluster(log_cli_level)


@pytest.fixture(scope='class')
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
    return pytestconfig.getini('log_cli_level') or 'DEBUG'


@pytest.fixture(scope="session", autouse=True)
def cluster_load(
    request, project_factory_session, pvc_factory_session,
    service_account_factory_session, pod_factory_session
):
    """
    Run IO during the test execution
    """
    cl_load_obj = None
    io_in_bg = config.RUN.get('io_in_bg')
    log_utilization = config.RUN.get('log_utilization')
    io_load = config.RUN.get('io_load')

    # IO load should not happen during deployment
    deployment_test = True if 'deployment' in request.node.items[0].location[0] else False
    if io_in_bg and not deployment_test:
        io_load = int(io_load) * 0.01
        log.info(
            "\n===================================================\n"
            "Tests will be running while IO is in the background\n"
            "==================================================="
        )

        log.info(
            "Start running IO in the background. The amount of IO that "
            "will be written is going to be determined by the cluster "
            "capabilities according to its limit"
        )

        cl_load_obj = ClusterLoad(
            project_factory=project_factory_session,
            sa_factory=service_account_factory_session,
            pvc_factory=pvc_factory_session,
            pod_factory=pod_factory_session,
            target_percentage=io_load
        )
        cl_load_obj.reach_cluster_load_percentage()

    if (log_utilization or io_in_bg) and not deployment_test:
        if not cl_load_obj:
            cl_load_obj = ClusterLoad()

        temp_file = tempfile.NamedTemporaryFile(
            mode='w+', prefix='test_status', delete=False
        )

        def get_test_status():
            with open(temp_file.name, 'r') as t_file:
                return t_file.readline()

        def set_test_status(status):
            with open(temp_file.name, 'w') as t_file:
                t_file.writelines(status)

        set_test_status('running')

        def finalizer():
            """
            Stop the thread that executed watch_load()
            """
            set_test_status('finished')
            if thread:
                thread.join()

        request.addfinalizer(finalizer)

        def watch_load():
            """
            Watch the cluster load by monitoring the cluster latency.
            In case the latency goes beyond 1 second, start deleting FIO pods.
            Once latency drops back below 0.5 seconds, re-create the FIO pods
            to make sure that cluster load is around the target percentage

            """
            initial_num_of_pods = len(cl_load_obj.dc_objs)
            while get_test_status() == 'running':
                try:
                    cl_load_obj.print_metrics()
                    if io_in_bg:
                        latency = cl_load_obj.calc_trim_metric_mean(
                            constants.LATENCY_QUERY
                        )

                        if latency > 1 and len(cl_load_obj.dc_objs) > 0:
                            log.warning(
                                f"Latency is higher than 1 second ({latency * 1000} ms). "
                                f"Lowering IO load by deleting an FIO pod that is running "
                                f"in the test background. Once the latency drops back to "
                                f"less than 0.5 seconds, FIO pod will be re-spawned"
                            )
                            cl_load_obj.decrease_load()

                        diff = initial_num_of_pods - len(cl_load_obj.dc_objs)
                        while latency < 0.5 and diff > 0 and (
                            get_test_status() == 'running'
                        ):
                            log.info(
                                f"Latency is lower than 0.5 seconds ({latency * 1000} ms). "
                                f"Re-spinning FIO pod"
                            )
                            cl_load_obj.increase_load(rate='15M')
                            latency = cl_load_obj.calc_trim_metric_mean(
                                constants.LATENCY_QUERY
                            )
                            diff -= 1

                # Any type of exception should be caught and we should continue.
                # We don't want any test to fail
                except Exception:
                    continue
                if get_test_status() == 'running':
                    time.sleep(10)

        thread = threading.Thread(target=watch_load)
        thread.start()


@pytest.fixture(
    params=[
        pytest.param({'interface': constants.CEPHBLOCKPOOL}),
        pytest.param({'interface': constants.CEPHFILESYSTEM})
    ],
    ids=["RBD", "CephFS"]
)
def interface_iterate(request):
    """
    Iterate over interfaces - CephBlockPool and CephFileSystem

    """
    return request.param['interface']


@pytest.fixture(scope='class')
def multi_pvc_factory_class(
    project_factory_class,
    pvc_factory_class
):
    return multi_pvc_factory_fixture(
        project_factory_class,
        pvc_factory_class
    )


@pytest.fixture(scope='session')
def multi_pvc_factory_session(
    project_factory_session,
    pvc_factory_session
):
    return multi_pvc_factory_fixture(
        project_factory_session,
        pvc_factory_session
    )


@pytest.fixture(scope='function')
def multi_pvc_factory(project_factory, pvc_factory):
    return multi_pvc_factory_fixture(
        project_factory,
        pvc_factory
    )


def multi_pvc_factory_fixture(
    project_factory,
    pvc_factory
):
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
        access_modes_selection='distribute_sequential',
        access_mode_dist_ratio=None,
        status=constants.STATUS_BOUND,
        num_of_pvc=1,
        wait_each=False,
        timeout=60
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
        if access_modes_selection == 'select_random':
            for _ in range(num_of_pvc):
                mode = random.choice(access_modes)
                access_modes_list.append(mode)

        else:
            if not access_mode_dist_ratio:
                num_of_modes = len(access_modes)
                dist_val = floor(num_of_pvc / num_of_modes)
                access_mode_dist_ratio = [dist_val] * num_of_modes
                access_mode_dist_ratio[-1] = (
                    dist_val + (num_of_pvc % num_of_modes)
                )
            zipped_share = list(zip(access_modes, access_mode_dist_ratio))
            for mode, share in zipped_share:
                access_modes_list.extend([mode] * share)

        if access_modes_selection == 'distribute_random':
            random.shuffle(access_modes_list)

        for access_mode in access_modes_list:
            if '-' in access_mode:
                access_mode, volume_mode = access_mode.split('-')
            else:
                volume_mode = ''
            pvc_obj = pvc_factory(
                interface=interface,
                project=project,
                storageclass=storageclass,
                size=size,
                access_mode=access_mode,
                status=status_tmp,
                volume_mode=volume_mode
            )
            pvc_list.append(pvc_obj)
            pvc_obj.project = project
        if status and not wait_each:
            for pvc_obj in pvc_list:
                helpers.wait_for_resource_state(pvc_obj, status, timeout=timeout)
        return pvc_list

    return factory


@pytest.fixture(scope="session", autouse=True)
def rook_repo(request):
    get_rook_repo(
        config.RUN['rook_branch'], config.RUN.get('rook_to_checkout')
    )


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
        set_flag_status('terminated')
        try:
            for status in TimeoutSampler(90, 3, get_flag_status):
                if status == 'terminated':
                    break
        except TimeoutExpiredError:
            log.warning(
                "Background test execution still in progress before"
                "memory leak thread terminated"
            )
        if thread:
            thread.join()
        log_path = ocsci_log_path()
        for worker in helpers.get_worker_nodes():
            if os.path.exists(f"/tmp/{worker}-top-output.txt"):
                copyfile(
                    f"/tmp/{worker}-top-output.txt",
                    f"{log_path}/{worker}-top-output.txt"
                )
                os.remove(f"/tmp/{worker}-top-output.txt")
        log.info("Memory leak capture has stopped")

    request.addfinalizer(finalizer)

    temp_file = tempfile.NamedTemporaryFile(
        mode='w+', prefix='test_status', delete=False
    )

    def get_flag_status():
        with open(temp_file.name, 'r') as t_file:
            return t_file.readline()

    def set_flag_status(value):
        with open(temp_file.name, 'w') as t_file:
            t_file.writelines(value)

    set_flag_status('running')

    def run_memory_leak_in_bg():
        """
        Function to run memory leak in background thread
        Memory leak data is written in below format
        date time PID USER PR NI VIRT RES SHR S %CPU %MEM TIME+ COMMAND
        """
        oc = ocp.OCP(
            namespace=config.ENV_DATA['cluster_namespace']
        )
        while get_flag_status() == 'running':
            for worker in helpers.get_worker_nodes():
                filename = f"/tmp/{worker}-top-output.txt"
                top_cmd = f"debug nodes/{worker} -- chroot /host top -n 2 b"
                with open("/tmp/file.txt", "w+") as temp:
                    temp.write(str(oc.exec_oc_cmd(
                        command=top_cmd, out_yaml_format=False
                    )))
                    temp.seek(0)
                    for line in temp:
                        if line.__contains__("ceph-osd"):
                            with open(filename, "a+") as f:
                                f.write(str(datetime.now()))
                                f.write(' ')
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
    assert ec2_instances, f"Failed to get ec2 instances for node {[n.name for n in nodes]}"

    def finalizer():
        """
        Make sure all instances are running
        """
        # Getting the instances that are in status 'stopping' (if there are any), to wait for them to
        # get to status 'stopped' so it will be possible to start them
        stopping_instances = {
            key: val for key, val in ec2_instances.items() if (
                aws_obj.get_instances_status_by_id(key) == constants.INSTANCE_STOPPING
            )
        }

        # Waiting fot the instances that are in status 'stopping'
        # (if there are any) to reach 'stopped'
        if stopping_instances:
            for stopping_instance in stopping_instances:
                instance = aws_obj.get_ec2_instance(stopping_instance.key())
                instance.wait_until_stopped()
        stopped_instances = {
            key: val for key, val in ec2_instances.items() if (
                aws_obj.get_instances_status_by_id(key) == constants.INSTANCE_STOPPED
            )
        }

        # Start the instances
        if stopped_instances:
            aws_obj.start_ec2_instances(instances=stopped_instances, wait=True)

    request.addfinalizer(finalizer)

    return ec2_instances


@pytest.fixture(scope='session')
def cld_mgr(request):
    """
    Returns a cloud manager instance that'll be used throughout the session

    Returns:
        CloudManager: A CloudManager resource

    """

    # Todo: Find a more elegant method
    def finalizer():
        oc = ocp.OCP(
            namespace='openshift-storage'
        )
        oc.exec_oc_cmd(
            command='delete secret backing-store-secret-client-secret',
            out_yaml_format=False
        )

    request.addfinalizer(finalizer)

    return CloudManager()


@pytest.fixture()
def mcg_obj(request):
    return mcg_obj_fixture(request)


@pytest.fixture(scope='session')
def mcg_obj_session(request):
    return mcg_obj_fixture(request)


def mcg_obj_fixture(request):
    """
    Returns an MCG resource that's connected to the S3 endpoint

    Returns:
        MCG: An MCG resource
    """

    mcg_obj = MCG()

    def finalizer():
        if config.ENV_DATA['platform'].lower() == 'aws':
            mcg_obj.cred_req_obj.delete()

    request.addfinalizer(finalizer)

    return mcg_obj


@pytest.fixture()
def awscli_pod(request, mcg_obj):
    return awscli_pod_fixture(request, mcg_obj)


@pytest.fixture(scope='session')
def awscli_pod_session(request, mcg_obj_session):
    return awscli_pod_fixture(request, mcg_obj_session)


def awscli_pod_fixture(request, mcg_obj):
    """
    Creates a new AWSCLI pod for relaying commands

    Args:
        mcg_obj: An object representing the current
        state of the MCG in the cluster

    Returns:
        pod: A pod running the AWS CLI

    """
    # Create the service-ca configmap to be mounted upon pod creation
    service_ca_configmap = helpers.create_resource(
        **templating.load_yaml(constants.AWSCLI_SERVICE_CA_YAML)
    )
    awscli_pod_obj = helpers.create_pod(
        namespace=constants.DEFAULT_NAMESPACE,
        pod_dict_path=constants.AWSCLI_POD_YAML,
        pod_name=constants.AWSCLI_RELAY_POD_NAME
    )
    OCP(namespace=constants.DEFAULT_NAMESPACE, kind='ConfigMap').wait_for_resource(
        resource_name=service_ca_configmap.name,
        column='DATA',
        condition='1'
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
        request,
        mcg_obj,
        awscli_pod,
        verify_rgw_restart_count
    )


@pytest.fixture(scope='session')
def uploaded_objects_session(
    request,
    mcg_obj_session,
    awscli_pod_session,
    verify_rgw_restart_count_session
):
    return uploaded_objects_fixture(
        request,
        mcg_obj_session,
        awscli_pod_session,
        verify_rgw_restart_count_session
    )


def uploaded_objects_fixture(
    request,
    mcg_obj,
    awscli_pod,
    verify_rgw_restart_count
):
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
            log.info(f'Deleting object {uploaded_filename}')
            awscli_pod.exec_cmd_on_pod(
                command=helpers.craft_s3_command(
                    "rm " + uploaded_filename, mcg_obj
                ),
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_endpoint
                ]
            )

    request.addfinalizer(object_cleanup)
    return uploaded_objects_paths


@pytest.fixture()
def verify_rgw_restart_count(request):
    return verify_rgw_restart_count_fixture(request)


@pytest.fixture(scope='session')
def verify_rgw_restart_count_session(request):
    return verify_rgw_restart_count_fixture(request)


def verify_rgw_restart_count_fixture(request):
    """
    Verifies the RGW restart count at start and end of a test
    """
    if config.ENV_DATA['platform'].lower() in constants.ON_PREM_PLATFORMS:
        log.info("Getting RGW pod restart count before executing the test")
        initial_counts = get_rgw_restart_counts()

        def finalizer():
            rgw_pods = get_rgw_pods()
            for rgw_pod in rgw_pods:
                rgw_pod.reload()
            log.info("Verifying whether RGW pods changed after executing the test")
            for rgw_pod in rgw_pods:
                assert rgw_pod.restart_count in initial_counts, 'RGW pod restarted'

        request.addfinalizer(finalizer)


@pytest.fixture()
def bucket_factory(request, mcg_obj):
    return bucket_factory_fixture(request, mcg_obj)


@pytest.fixture(scope='session')
def bucket_factory_session(request, mcg_obj_session):
    return bucket_factory_fixture(request, mcg_obj_session)


def bucket_factory_fixture(request, mcg_obj):
    """
    Create a bucket factory. Calling this fixture creates a new bucket(s).
    For a custom amount, provide the 'amount' parameter.

    Args:
        mcg_obj (MCG): An MCG object containing the MCG S3 connection
        credentials

    """
    created_buckets = []

    bucketMap = {
        's3': S3Bucket,
        'oc': OCBucket,
        'cli': CLIBucket
    }

    def _create_buckets(
        amount=1, interface='S3',
        verify_health=True, *args, **kwargs
    ):
        """
        Creates and deletes all buckets that were created as part of the test

        Args:
            amount (int): The amount of buckets to create
            interface (str): The interface to use for creation of buckets.
                S3 | OC | CLI

        Returns:
            list: A list of s3.Bucket objects, containing all the created
                buckets

        """
        if interface.lower() not in bucketMap:
            raise RuntimeError(
                f'Invalid interface type received: {interface}. '
                f'available types: {", ".join(bucketMap.keys())}'
            )
        for i in range(amount):
            bucket_name = helpers.create_unique_resource_name(
                resource_description='bucket', resource_type=interface.lower()
            )
            created_bucket = bucketMap[interface.lower()](
                mcg_obj,
                bucket_name,
                *args,
                **kwargs
            )
            created_buckets.append(created_bucket)
            if verify_health:
                assert created_bucket.verify_health(), (
                    f"{bucket_name} did not reach a healthy state in time."
                )
        return created_buckets

    def bucket_cleanup():
        all_existing_buckets = mcg_obj.s3_get_all_bucket_names()
        for bucket in created_buckets:
            if bucket.name in all_existing_buckets:
                log.info(f'Cleaning up bucket {bucket.name}')
                bucket.delete()
                log.info(
                    f"Verifying whether bucket: {bucket.name} exists after"
                    f" deletion"
                )
                assert not mcg_obj.s3_verify_bucket_exists(bucket.name)
            else:
                log.info(f'Bucket {bucket.name} not found.')

    request.addfinalizer(bucket_cleanup)

    return _create_buckets


@pytest.fixture(scope='class')
def cloud_uls_factory(request, cld_mgr):
    """
     Create a Underlying Storage factory.
     Calling this fixture creates a new underlying storage(s).

     Args:
        cld_mgr (CloudManager): Cloud Manager object containing all connections to clouds

    """
    all_created_uls = {
        'aws': set(),
        'google': set(),
        'azure': set(),
        's3comp': set()
    }

    ulsMap = {
        'aws': cld_mgr.aws_client,
        'google': cld_mgr.google_client,
        'azure': cld_mgr.azure_client,
        's3comp': cld_mgr.s3comp_client
    }

    def _create_uls(uls_dict):
        """
        Creates and deletes all underlying storage that were created as part of the test

        Args:
            uls_dict (dict): Dictionary containing storage provider as key and a list of tuples
            as value.
            each tuple contain amount as first parameter and region as second parameter.
            Cloud backing stores form - 'CloudName': [(amount, region), (amount, region)]
            i.e. - 'aws': [(3, us-west-1),(2, eu-west-2)]


        Returns:
            dict: A dictionary of cloud names as keys and uls names sets as value.

        """
        current_call_created_uls = {
            'aws': set(),
            'google': set(),
            'azure': set(),
            's3comp': set()
        }

        for cloud, params in uls_dict.items():
            if cloud.lower() not in ulsMap:
                raise RuntimeError(
                    f'Invalid interface type received: {cloud}. '
                    f'available types: {", ".join(ulsMap.keys())}'
                )
            log.info(f'Creating uls for cloud {cloud.lower()}')
            for tup in params:
                amount, region = tup
                for i in range(amount):
                    uls_name = create_unique_resource_name(
                        resource_description='uls', resource_type=cloud.lower()
                    )
                    ulsMap[cloud.lower()].create_uls(uls_name, region)
                    all_created_uls[cloud].add(uls_name)
                    current_call_created_uls[cloud.lower()].add(uls_name)

            return current_call_created_uls

    def uls_cleanup():
        for cloud, uls_set in all_created_uls.items():
            client = ulsMap[cloud]
            if client is not None:
                all_existing_uls = client.get_all_uls_names()
                for uls in uls_set:
                    if uls in all_existing_uls:
                        log.info(f'Cleaning up uls {uls}')
                        client.delete_uls(uls)
                        log.info(
                            f"Verifying whether uls: {uls} exists after deletion"
                        )
                        assert not client.verify_uls_exists(uls), (
                            f'Unable to delete Underlying Storage {uls}'
                        )
                    else:
                        log.warning(f'Underlying Storage {uls} not found.')

    request.addfinalizer(uls_cleanup)

    return _create_uls


@pytest.fixture(scope='class')
def backingstore_factory(request, cld_mgr, cloud_uls_factory):
    """
        Create a Backing Store factory.
        Calling this fixture creates a new Backing Store(s).

        Args:
            cloud_uls_factory: Factory for underlying storage creation
            cld_mgr (CloudManager): Cloud Manager object containing all connections to clouds

    """
    created_backingstores = []

    cmdMap = {
        'oc': {
            'aws': oc_create_aws_backingstore,
            'google': oc_create_google_backingstore,
            'azure': oc_create_azure_backingstore,
            's3comp': oc_create_s3comp_backingstore,
            'pv': oc_create_pv_backingstore
        },
        'cli': {
            'aws': cli_create_aws_backingstore,
            'google': cli_create_google_backingstore,
            'azure': cli_create_azure_backingstore,
            's3comp': cli_create_s3comp_backingstore,
            'pv': cli_create_pv_backingstore
        }
    }

    def _create_backingstore(method, uls_dict):
        """
        Tracks creation and cleanup of all the backing stores that were created in the scope

        Args:
            method (str): String for selecting method of backing store creation (CLI/OC)
            uls_dict (dict): Dictionary containing storage provider as key and a list of tuples
            as value.
            Cloud backing stores form - 'CloudName': [(amount, region), (amount, region)]
            i.e. - 'aws': [(3, us-west-1),(2, eu-west-2)]
            PV form - 'pv': [(amount, size_in_gb, storageclass), ...]
            i.e. - 'pv': [(3, 32, ocs-storagecluster-ceph-rbd),(2, 100, ocs-storagecluster-ceph-rbd)]

        Returns:
            list: A list of backingstore names.

        """
        if method.lower() not in cmdMap:
            raise RuntimeError(
                f'Invalid method type received: {method}. '
                f'available types: {", ".join(cmdMap.keys())}'
            )
        for cloud, uls_lst in uls_dict.items():
            for uls_tup in uls_lst:
                # Todo: Replace multiple .append calls, create names in advance, according to amountoc
                if cloud.lower() not in cmdMap[method.lower()]:
                    raise RuntimeError(
                        f'Invalid cloud type received: {cloud}. '
                        f'available types: {", ".join(cmdMap[method.lower()].keys())}'
                    )
                if cloud == 'pv':
                    vol_num, size, storage_class = uls_tup
                    backingstore_name = create_unique_resource_name(
                        resource_description='backingstore', resource_type=cloud.lower()
                    )
                    # removing characters from name (pod name length bellow 64 characters issue)
                    backingstore_name = backingstore_name[:-16]
                    created_backingstores.append(backingstore_name)
                    cmdMap[method.lower()][cloud.lower()](
                        backingstore_name, vol_num, size, storage_class
                    )
                else:
                    region = uls_tup[1]
                    # Todo: Verify that the given cloud has an initialized client
                    uls_dict = cloud_uls_factory({cloud: [uls_tup]})
                    for uls_name in uls_dict[cloud.lower()]:
                        backingstore_name = create_unique_resource_name(
                            resource_description='backingstore', resource_type=cloud.lower()
                        )
                        # removing characters from name (pod name length bellow 64 characters issue)
                        backingstore_name = backingstore_name[:-16]
                        created_backingstores.append(backingstore_name)
                        cmdMap[method.lower()][cloud.lower()](
                            cld_mgr, backingstore_name, uls_name, region
                        )
                        # Todo: Raise an exception in case the BS wasn't created

        return created_backingstores

    def backingstore_cleanup():
        for backingstore_name in created_backingstores:
            log.info(f'Cleaning up backingstore {backingstore_name}')
            oc = ocp.OCP(
                namespace=config.ENV_DATA['cluster_namespace']
            )
            oc.exec_oc_cmd(
                command=f'delete backingstore {backingstore_name}',
                out_yaml_format=False
            )
            log.info(
                f"Verifying whether backingstore {backingstore_name} exists after deletion"
            )
            # Todo: implement deletion assertion

    request.addfinalizer(backingstore_cleanup)

    return _create_backingstore


@pytest.fixture()
def multiregion_resources(request, mcg_obj):
    return multiregion_resources_fixture(request, mcg_obj)


@pytest.fixture(scope='session')
def multiregion_resources_session(request, mcg_obj_session):
    return multiregion_resources_fixture(request, mcg_obj_session)


def multiregion_resources_fixture(request, mcg_obj):
    bs_objs, bs_secrets, bucketclasses, aws_buckets = (
        [] for _ in range(4)
    )

    # Cleans up all resources that were created for the test
    def resource_cleanup():
        for resource in chain(bs_secrets, bucketclasses):
            resource.delete()

        for backingstore in bs_objs:
            backingstore.delete()
            mcg_obj.send_rpc_query(
                'pool_api',
                'delete_pool',
                {'name': backingstore.name}
            )

        for aws_bucket_name in aws_buckets:
            mcg_obj.toggle_aws_bucket_readwrite(aws_bucket_name, block=False)
            for _ in range(10):
                try:
                    mcg_obj.aws_s3_resource.Bucket(
                        aws_bucket_name
                    ).objects.all().delete()
                    mcg_obj.aws_s3_resource.Bucket(aws_bucket_name).delete()
                    break
                except ClientError:
                    log.info(
                        f'Deletion of bucket {aws_bucket_name} failed. Retrying...'
                    )
                    sleep(3)

    request.addfinalizer(resource_cleanup)

    return aws_buckets, bs_secrets, bs_objs, bucketclasses


@pytest.fixture()
def multiregion_mirror_setup(mcg_obj, multiregion_resources, bucket_factory):
    return multiregion_mirror_setup_fixture(
        mcg_obj,
        multiregion_resources,
        bucket_factory
    )


@pytest.fixture(scope='session')
def multiregion_mirror_setup_session(
    mcg_obj_session,
    multiregion_resources_session,
    bucket_factory_session
):
    return multiregion_mirror_setup_fixture(
        mcg_obj_session,
        multiregion_resources_session,
        bucket_factory_session
    )


def multiregion_mirror_setup_fixture(
    mcg_obj,
    multiregion_resources,
    bucket_factory
):
    # Setup
    # Todo:
    #  add region and amount parametrization - note that `us-east-1`
    #  will cause an error as it is the default region. If usage of `us-east-1`
    #  needs to be tested, keep the 'region' field out.
    (
        aws_buckets,
        backingstore_secrets,
        backingstore_objects,
        bucketclasses
    ) = multiregion_resources

    # Define backing stores
    backingstore1 = {
        'name': helpers.create_unique_resource_name(
            resource_description='testbs',
            resource_type='s3bucket'
        ),
        'region': f'us-west-{randrange(1, 3)}'
    }
    backingstore2 = {
        'name': helpers.create_unique_resource_name(
            resource_description='testbs',
            resource_type='s3bucket'
        ),
        'region': 'us-east-2'
    }
    # Create target buckets for them
    mcg_obj.create_new_backingstore_aws_bucket(backingstore1)
    mcg_obj.create_new_backingstore_aws_bucket(backingstore2)
    aws_buckets.extend((backingstore1['name'], backingstore2['name']))
    # Create a backing store secret
    backingstore_secret = mcg_obj.create_aws_backingstore_secret(
        backingstore1['name'] + 'secret'
    )
    backingstore_secrets.append(backingstore_secret)
    # Create AWS-backed backing stores on NooBaa
    backingstore_obj_1 = mcg_obj.oc_create_aws_backingstore(
        backingstore1['name'],
        backingstore1['name'],
        backingstore_secret.name,
        backingstore1['region']
    )
    backingstore_obj_2 = mcg_obj.oc_create_aws_backingstore(
        backingstore2['name'],
        backingstore2['name'],
        backingstore_secret.name,
        backingstore2['region']
    )
    backingstore_objects.extend((backingstore_obj_1, backingstore_obj_2))
    # Create a new mirror bucketclass that'll use all the backing stores we
    # created
    bucketclass = mcg_obj.oc_create_bucketclass(
        helpers.create_unique_resource_name(
            resource_description='testbc',
            resource_type='bucketclass'
        ),
        [backingstore.name for backingstore in backingstore_objects], 'Mirror'
    )
    bucketclasses.append(bucketclass)
    # Create a NooBucket that'll use the bucket class in order to test
    # the mirroring policy
    bucket = bucket_factory(1, 'OC', bucketclass=bucketclass.name)[0]

    return bucket, backingstore1, backingstore2


@pytest.fixture(scope='session')
def default_storageclasses(request, teardown_factory_session):
    """
    Returns dictionary with storageclasses. Keys represent reclaim policy of
    storageclass. There are two storageclasses for each key. First is RBD based
    and the second one is CephFS based. Storageclasses with Retain Reclaim
    Policy are created from default storageclasses.
    """
    scs = {
        constants.RECLAIM_POLICY_DELETE: [],
        constants.RECLAIM_POLICY_RETAIN: []
    }

    # TODO(fbalak): Use proper constants after
    # https://github.com/red-hat-storage/ocs-ci/issues/1056
    # is resolved
    for sc_name in (
        'ocs-storagecluster-ceph-rbd',
        'ocs-storagecluster-cephfs'
    ):
        sc = OCS(
            kind=constants.STORAGECLASS,
            metadata={'name': sc_name}
        )
        sc.reload()
        scs[constants.RECLAIM_POLICY_DELETE].append(sc)
        sc.data['reclaimPolicy'] = constants.RECLAIM_POLICY_RETAIN
        sc.data['metadata']['name'] += '-retain'
        sc._name = sc.data['metadata']['name']
        sc.create()
        teardown_factory_session(sc)
        scs[constants.RECLAIM_POLICY_RETAIN].append(sc)
    return scs


@pytest.fixture(scope='class')
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
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    logging_csv = csv.get().get('items')
    if logging_csv:
        log.info("Logging is already configured, Skipping Installation")
        return

    log.info("Configuring Openshift-logging")

    # Checks OCP version
    ocp_version = get_ocp_version()

    # Creates namespace opensift-operators-redhat
    ocp_logging_obj.create_namespace(yaml_file=constants.EO_NAMESPACE_YAML)

    # Creates an operator-group for elasticsearch
    assert ocp_logging_obj.create_elasticsearch_operator_group(
        yaml_file=constants.EO_OG_YAML,
        resource_name='openshift-operators-redhat'
    )

    # Set RBAC policy on the project
    assert ocp_logging_obj.set_rbac(
        yaml_file=constants.EO_RBAC_YAML, resource_name='prometheus-k8s'
    )

    # Creates subscription for elastic-search operator
    subscription_yaml = templating.load_yaml(constants.EO_SUB_YAML)
    subscription_yaml['spec']['channel'] = ocp_version
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
    cl_subscription['spec']['channel'] = ocp_version
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
    return fio_pvc_dict_fixture()


@pytest.fixture(scope='session')
def fio_pvc_dict_session():
    return fio_pvc_dict_fixture()


def fio_pvc_dict_fixture():
    """
    PVC template for fio workloads.
    Note that all 'None' values needs to be defined before usage.
    """
    # TODO(fbalak): load dictionary fixtures from one place
    template = textwrap.dedent("""
        kind: PersistentVolumeClaim
        apiVersion: v1
        metadata:
          name: fio-target
        spec:
          storageClassName: None
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: None
        """)
    pvc_dict = yaml.safe_load(template)
    return pvc_dict


@pytest.fixture
def fio_configmap_dict():
    return fio_configmap_dict_fixture()


@pytest.fixture(scope='session')
def fio_configmap_dict_session():
    return fio_configmap_dict_fixture()


def fio_configmap_dict_fixture():
    """
    ConfigMap template for fio workloads.
    Note that you need to add actual configuration to workload.fio file.
    """
    # TODO(fbalak): load dictionary fixtures from one place
    template = textwrap.dedent("""
        kind: ConfigMap
        apiVersion: v1
        metadata:
          name: fio-config
        data:
          workload.fio: |
            # here comes workload configuration
        """)
    cm_dict = yaml.safe_load(template)
    return cm_dict


@pytest.fixture
def fio_job_dict():
    return fio_job_dict_fixture()


@pytest.fixture(scope='session')
def fio_job_dict_session():
    return fio_job_dict_fixture()


def fio_job_dict_fixture():
    """
    Job template for fio workloads.
    """
    node_obj = ocp.OCP(kind=constants.NODE)

    log.info('Checking architecture of system')
    node = node_obj.get(
        selector=constants.WORKER_LABEL
    ).get('items')[0]['metadata']['name']
    arch = node_obj.exec_oc_debug_cmd(node, ['uname -m'])
    if arch.startswith('x86'):
        image = 'quay.io/fbalak/fio-fedora:latest'
    else:
        image = 'quay.io/multiarch-origin-e2e/fio-fedora:latest'
    log.info(f'Discovered architecture: {arch.strip()}')
    log.info(f'Using image: {image}')

    # TODO(fbalak): load dictionary fixtures from one place
    template = textwrap.dedent(f"""
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: fio
        spec:
          backoffLimit: 0
          template:
            metadata:
              name: fio
            spec:
              containers:
                - name: fio
                  image: {image}
                  command:
                    - "/usr/bin/fio"
                    - "--output-format=json"
                    - "/etc/fio/workload.fio"
                  volumeMounts:
                    - name: fio-target
                      mountPath: /mnt/target
                    - name: fio-config-volume
                      mountPath: /etc/fio
              restartPolicy: Never
              volumes:
                - name: fio-target
                  persistentVolumeClaim:
                    claimName: fio-target
                - name: fio-config-volume
                  configMap:
                    name: fio-config
        """)
    job_dict = yaml.safe_load(template)

    # overwrite used image (required for disconnected installation)
    update_container_with_mirrored_image(job_dict)

    return job_dict


@pytest.fixture(scope='function')
def pgsql_factory_fixture(request):
    """
    Pgsql factory fixture
    """
    pgsql = Postgresql()

    def factory(
        replicas, clients=None, threads=None,
        transactions=None, scaling_factor=None,
        timeout=None
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
        pgsql.setup_postgresql(replicas=replicas)

        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(
            replicas=replicas, clients=clients, threads=threads,
            transactions=transactions, scaling_factor=scaling_factor,
            timeout=timeout
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


@pytest.fixture(scope='function')
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
        jenkins.wait_for_jenkins_deploy_status(
            status=constants.STATUS_COMPLETED
        )
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


@pytest.fixture(scope='function')
def couchbase_factory_fixture(request):
    """
    Couchbase factory fixture
    """
    couchbase = CouchBase()

    def factory(replicas=3):
        """
        Factory to start couchbase workload

        Args:
            replicas (int): Number of couchbase workers to be deployed
        """
        # Setup couchbase
        couchbase.setup_cb()
        # Create couchbase workers
        couchbase.create_couchbase_worker(replicas=replicas)
        # Run couchbase workload
        couchbase.run_workload(replicas=replicas)
        # Run sanity check on data logs
        couchbase.analyze_run()
        return couchbase

    def finalizer():
        """
        Clean up
        """
        couchbase.teardown()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope='function')
def amq_factory_fixture(request):
    """
    AMQ factory fixture
    """
    amq = AMQ()

    def factory(
        sc_name, tiller_namespace, kafka_namespace=constants.AMQ_NAMESPACE,
        size=100, replicas=3, benchmark_pod_name="benchmark",
        num_of_clients=8, worker=None, timeout=3600,
        amq_workload_yaml=None, run_in_bg=False
    ):
        """
        Factory to start amq workload

        Args:
            sc_name (str): Name of storage clase
            tiller_namespace (str): Namespace where benchmark pods to be created
            kafka_namespace (str): Namespace where kafka cluster to be created
            size (int): Size of the storage
            replicas (int): Number of kafka and zookeeper pods to be created
            benchmark_pod_name (str): Name of the benchmark pod
            num_of_clients (int): Number of clients to be created
            worker (str) : Loads to create on workloads separated with commas
                e.g http://benchmark-worker-0.benchmark-worker:8080,
                http://benchmark-worker-1.benchmark-worker:8080
            timeout (int): Time to complete the run
            amq_workload_yaml (dict): Contains amq workloads information keys and values
            run_in_bg (bool): On true the workload will run in background

        """
        # Setup kafka cluster
        amq.setup_amq_cluster(
            sc_name=sc_name, namespace=kafka_namespace, size=size, replicas=replicas
        )

        # Run amq benchmark
        result = amq.run_amq_benchmark(
            benchmark_pod_name=benchmark_pod_name, kafka_namespace=kafka_namespace,
            tiller_namespace=tiller_namespace, num_of_clients=num_of_clients, worker=worker,
            timeout=timeout, amq_workload_yaml=amq_workload_yaml, run_in_bg=run_in_bg
        )

        return amq, result

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
    if config.ENV_DATA.get('measurement_dir'):
        measurement_dir = config.ENV_DATA.get('measurement_dir')
        log.info(
            f"Using measurement dir from configuration: {measurement_dir}"
        )
    else:
        measurement_dir = os.path.join(
            os.path.dirname(tmp_path),
            'measurement_results'
        )
    if not os.path.exists(measurement_dir):
        log.info(
            f"Measurement dir {measurement_dir} doesn't exist. Creating it."
        )
        os.mkdir(measurement_dir)
    return measurement_dir


@pytest.fixture()
def multi_dc_pod(multi_pvc_factory, dc_pod_factory, service_account_factory):
    """
    Prepare multiple dc pods for the test
    Returns:
        list: Pod instances
    """

    def factory(num_of_pvcs=1, pvc_size=100, project=None, access_mode="RWO", pool_type="rbd", timeout=60):

        dict_modes = {"RWO": "ReadWriteOnce", "RWX": "ReadWriteMany", "RWX-BLK": "ReadWriteMany-Block"}
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
            timeout=timeout
        )
        dc_pods = []
        dc_pods_res = []
        sa_obj = service_account_factory(project=project)
        with ThreadPoolExecutor() as p:
            for pvc in pvc_objs:
                if create_rbd_block_rwx_pod:
                    dc_pods_res.append(
                        p.submit(
                            dc_pod_factory, interface=constants.CEPHBLOCKPOOL,
                            pvc=pvc, raw_block_pv=True, sa_obj=sa_obj
                        ))
                else:
                    dc_pods_res.append(
                        p.submit(
                            dc_pod_factory, interface=dict_types[pool_type],
                            pvc=pvc, sa_obj=sa_obj
                        ))

        for dc in dc_pods_res:
            pod_obj = dc.result()
            if create_rbd_block_rwx_pod:
                logging.info(f"#### setting attribute pod_type since"
                             f" create_rbd_block_rwx_pod = {create_rbd_block_rwx_pod}"
                             )
                setattr(pod_obj, 'pod_type', 'rbd_block_rwx')
            else:
                setattr(pod_obj, 'pod_type', '')
            dc_pods.append(pod_obj)

        with ThreadPoolExecutor() as p:
            for dc in dc_pods:
                p.submit(
                    helpers.wait_for_resource_state,
                    resource=dc,
                    state=constants.STATUS_RUNNING,
                    timeout=120)

        return dc_pods

    return factory


@pytest.fixture(scope="session", autouse=True)
def ceph_toolbox(request):
    """
    This fixture initiates ceph toolbox pod for manually created deployment
    and if it does not already exist.
    """
    deploy = config.RUN['cli_params']['deploy']
    teardown = config.RUN['cli_params'].get('teardown')
    skip_ocs = config.ENV_DATA['skip_ocs_deployment']
    if not (deploy or teardown or skip_ocs):
        # Creating toolbox pod
        setup_ceph_toolbox()
