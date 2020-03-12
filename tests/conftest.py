import logging
import os
import tempfile

import pytest
import threading
from datetime import datetime
import random
from math import floor

from ocs_ci.utility.utils import TimeoutSampler, get_rook_repo
from ocs_ci.ocs.exceptions import TimeoutExpiredError, CephHealthException
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI
from ocs_ci.utility import aws
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    deployment, ignore_leftovers
)
from ocs_ci.ocs.version import get_ocs_version, report_ocs_version
from ocs_ci.utility.environment_check import (
    get_status_before_execution, get_status_after_execution
)
from ocs_ci.utility.utils import (
    get_openshift_client, ocsci_log_path, get_testrun_name,
    ceph_health_check_base, skipif_ocs_version
)
from ocs_ci.deployment import factory as dep_factory
from tests import helpers
from ocs_ci.ocs import constants, ocp, defaults, node, platform_nodes
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pvc import PVC


log = logging.getLogger(__name__)


class OCSLogFormatter(logging.Formatter):

    def __init__(self):
        fmt = (
            "%(asctime)s - %(levelname)s - %(name)s.%(funcName)s.%(lineno)d "
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
            log.info(skip_condition)
            # skip_confition will be a tuple
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
    min_memory = 64 * 10**9

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
            real_memory = int(real_memory[0:-2]) * 2**10
        elif real_memory.endswith('Mi'):
            real_memory = int(real_memory[0:-2]) * 2**20
        elif real_memory.endswith('Gi'):
            real_memory = int(real_memory[0:-2]) * 2**30
        elif real_memory.endswith('Ti'):
            real_memory = int(real_memory[0:-2]) * 2**40
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
            ceph_pool = ceph_pool_factory(interface)
            if interface == constants.CEPHBLOCKPOOL:
                interface_name = ceph_pool.name
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
            sc_obj.ceph_pool = ceph_pool
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

    def factory():
        """

        Returns:
            object: ocs_ci.ocs.resources.ocs instance of 'Project' kind.
        """
        proj_obj = helpers.create_project()
        instances.append(proj_obj)
        return proj_obj

    def finalizer():
        """
        Delete the project
        """
        for instance in instances:
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
        volume_mode=None
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
                volume_mode=volume_mode
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
        pod_dict_path=None,
        raw_block_pv=False
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
            pod_dict_path (str): YAML path for the pod.
            raw_block_pv (bool): True for creating raw block pv based pod,
                False otherwise.

        Returns:
            object: helpers.create_pvc instance.
        """
        if custom_data:
            pod_obj = helpers.create_resource(**custom_data)
        else:
            pvc = pvc or pvc_factory(interface=interface)

            pod_obj = helpers.create_pod(
                pvc_name=pvc.name,
                namespace=pvc.namespace,
                interface_type=interface,
                pod_dict_path=pod_dict_path,
                raw_block_pv=raw_block_pv
            )
            assert pod_obj, "Failed to create PVC"
        instances.append(pod_obj)
        if status:
            helpers.wait_for_resource_state(pod_obj, status)
            pod_obj.reload()
        pod_obj.pvc = pvc

        return pod_obj

    def finalizer():
        """
        Delete the Pod
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


@pytest.fixture()
def service_account_factory(request):
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
            sa_obj = helpers.get_serviceaccount_obj(sa_name=service_account, namespace=project.namespace)
            if not helpers.validate_scc_policy(sa_name=service_account, namespace=project.namespace):
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
        """
        if custom_data:
            dc_pod_obj = helpers.create_resource(**custom_data)
        else:

            pvc = pvc or pvc_factory(interface=interface, size=size)
            sa_obj = service_account_factory(project=pvc.project, service_account=service_account)
            dc_pod_obj = helpers.create_pod(
                interface_type=interface, pvc_name=pvc.name, do_reload=False,
                namespace=pvc.namespace, sa_name=sa_obj.name, dc_deployment=True,
                replica_count=replica_count, node_name=node_name,
                node_selector=node_selector
            )
        instances.append(dc_pod_obj)
        log.info(dc_pod_obj.name)
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
            helpers.delete_deploymentconfig_pods(instance)

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


@pytest.fixture(scope='function', autouse=True)
def health_checker(request):
    node = request.node
    # Limit the health check for tier4a, tier4b, tier4c
    tier4_marks = ['tier4', 'tier4a', 'tier4b', 'tier4c']
    for mark in node.iter_markers():
        if mark.name in tier4_marks:
            log.info("Checking for Ceph Health OK ")
            try:
                status = ceph_health_check_base()
                if status:
                    log.info("Health check passed")
                    return
            except CephHealthException:
                # skip because ceph is not in good health
                pytest.skip("Ceph Health check failed")


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
    for mark in node.iter_markers():
        if mark in marks_to_ignore:
            return

    request.addfinalizer(get_status_after_execution)
    get_status_before_execution()


@pytest.fixture(scope="session")
def log_cli_level(pytestconfig):
    """
    Retrieves the log_cli_level set in pytest.ini

    Returns:
        str: log_cli_level set in pytest.ini or DEBUG if not set

    """
    return pytestconfig.getini('log_cli_level') or 'DEBUG'


@pytest.fixture(scope="session")
def run_io_in_background(request):
    """
    Run IO during the test execution
    """
    if config.RUN['cli_params'].get('io_in_bg'):
        log.info(f"Tests will be running while IO is in the background")

        g_sheet = None
        if config.RUN['google_api_secret']:
            g_sheet = GoogleSpreadSheetAPI("IO BG results", 0)
        else:
            log.warning(
                "Google API secret was not found. IO won't be reported to "
                "a Google spreadsheet"
            )
        results = list()
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
            Delete the resources created during setup, used for
            running IO in the test background
            """
            set_test_status('finished')
            try:
                for status in TimeoutSampler(90, 3, get_test_status):
                    if status == 'terminated':
                        break
            except TimeoutExpiredError:
                log.warning(
                    "Background IO was still in progress before IO "
                    "thread termination"
                )
            if thread:
                thread.join()

            log.info(f"Background IO has stopped")
            for result in results:
                log.info(f"IOPs after FIO for pod {pod_obj.name}:")
                log.info(f"Read: {result[0]}")
                log.info(f"Write: {result[1]}")

            if pod_obj:
                pod_obj.delete()
                pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
            if pvc_obj:
                pvc_obj.delete()
                pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            if sc_obj:
                sc_obj.delete()
            if cbp_obj:
                cbp_obj.delete()
            if secret_obj:
                secret_obj.delete()

        request.addfinalizer(finalizer)

        secret_obj = helpers.create_secret(
            interface_type=constants.CEPHBLOCKPOOL
        )
        cbp_obj = helpers.create_ceph_block_pool()
        sc_obj = helpers.create_storage_class(
            interface_type=constants.CEPHBLOCKPOOL,
            interface_name=cbp_obj.name,
            secret_name=secret_obj.name
        )
        pvc_obj = helpers.create_pvc(sc_name=sc_obj.name, size='2Gi')
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        pvc_obj.reload()
        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL, pvc_name=pvc_obj.name
        )
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        pod_obj.reload()

        def run_io_in_bg():
            """
            Run IO by executing FIO and deleting the file created for FIO on
            the pod, in a while true loop. Will be running as long as
            the test is running.
            """
            while get_test_status() == 'running':
                pod_obj.run_io('fs', '1G')
                result = pod_obj.get_fio_results()
                reads = result.get('jobs')[0].get('read').get('iops')
                writes = result.get('jobs')[0].get('write').get('iops')
                if g_sheet:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    g_sheet.insert_row([now, reads, writes])

                results.append((reads, writes))

                file_path = os.path.join(
                    pod_obj.get_storage_path(storage_type='fs'),
                    pod_obj.io_params['filename']
                )
                pod_obj.exec_cmd_on_pod(f'rm -rf {file_path}')
            set_test_status('terminated')

        log.info(f"Start running IO in the test background")

        thread = threading.Thread(target=run_io_in_bg)
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
        wait_each=False
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
                helpers.wait_for_resource_state(pvc_obj, status)
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
        for worker in helpers.get_worker_nodes():
            if os.path.exists(f"/tmp/{worker}-top-output.txt"):
                os.remove(f"/tmp/{worker}-top-output.txt")
        log.info(f"Memory leak capture has stopped")

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
    log.info(f"Start memory leak data capture in the test background")
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


@pytest.fixture()
def mcg_obj(request):
    """
    Returns an MCG resource that's connected to the S3 endpoint

    Returns:
        MCG: An MCG resource
    """
    mcg_obj = MCG()

    if config.ENV_DATA['platform'].lower() == 'aws':
        def finalizer():
            mcg_obj.cred_req_obj.delete()
        request.addfinalizer(finalizer)

    return mcg_obj


@pytest.fixture()
def created_pods(request):
    """
    Deletes all pods that were created as part of the test

    Returns:
        list: An empty list of pods
    """
    created_pods_objects = []

    def pod_cleanup():
        for pod in created_pods_objects:
            log.info(f'Deleting pod {pod.name}')
            pod.delete()
    request.addfinalizer(pod_cleanup)
    return created_pods_objects


@pytest.fixture()
def awscli_pod(mcg_obj, created_pods):
    """
    Creates a new AWSCLI pod for relaying commands

    Args:
        created_pods (Fixture/list): A fixture used to keep track of created
             pods and clean them up in the teardown

    Returns:
        pod: A pod running the AWS CLI
    """
    awscli_pod_obj = helpers.create_pod(
        namespace=mcg_obj.namespace,
        pod_dict_path=constants.AWSCLI_POD_YAML
    )
    helpers.wait_for_resource_state(awscli_pod_obj, constants.STATUS_RUNNING)
    created_pods.append(awscli_pod_obj)
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
