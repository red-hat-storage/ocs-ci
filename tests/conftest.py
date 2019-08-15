import logging
import os
import tempfile
import pytest
import threading
from datetime import datetime

from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    deployment, destroy, ignore_leftovers
)
from ocs_ci.utility.environment_check import (
    get_status_before_execution, get_status_after_execution
)
from ocs_ci.utility.utils import (
    get_openshift_client, ocsci_log_path, get_testrun_name
)
from ocs_ci.deployment import factory as dep_factory
from tests import helpers
from ocs_ci.ocs import constants, ocp, defaults
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


@pytest.fixture()
def secret_factory(request):
    """
    Secret factory. Calling this fixture creates a new secret.
    RBD based is default.
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
            if not instance.is_deleted:
                instance.delete()
                instance.ocp.wait_for_delete(
                    instance.name
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def ceph_pool_factory(request):
    """
    Create a Ceph pool factory.
    Calling this fixture creates new Ceph pool instance.
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
        instances.append(ceph_pool_obj)
        return ceph_pool_obj

    def finalizer():
        """
        Delete the Ceph block pool
        """
        for instance in instances:
            if not instance.is_deleted:
                instance.delete()
                instance.ocp.wait_for_delete(
                    instance.name
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def storageclass_factory(
    request,
    ceph_pool_factory,
    secret_factory
):
    """
    Create a storage class factory. Default is RBD based.
    Calling this fixture creates new storage class instance.
    """
    instances = []

    def factory(
        interface=constants.CEPHBLOCKPOOL,
        secret=None,
        custom_data=None
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

        Returns:
            object: helpers.create_storage_class instance with links to
                block_pool and secret.
        """
        if custom_data:
            sc_obj = helpers.create_resource(**custom_data, wait=False)
        else:
            secret = secret or secret_factory(interface=interface)
            ceph_pool = ceph_pool_factory(interface)
            interface_name = ceph_pool.name

            sc_obj = helpers.create_storage_class(
                interface_type=interface,
                interface_name=interface_name,
                secret_name=secret.name
            )
            assert sc_obj, f"Failed to create {interface} storage class"
            sc_obj.ceph_pool = ceph_pool
            sc_obj.secret = secret

        instances.append(sc_obj)
        return sc_obj

    def finalizer():
        """
        Delete the Ceph block pool
        """
        for instance in instances:
            if not instance.is_deleted:
                instance.delete()
                instance.ocp.wait_for_delete(
                    instance.name
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def project_factory(request):
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
        Delete the Ceph block pool
        """
        for instance in instances:
            ocp.switch_to_default_rook_cluster_project()
            instance.delete(resource_name=instance.namespace)
            instance.wait_for_delete(instance.namespace)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def pvc_factory(
    request,
    storageclass_factory,
    project_factory
):
    """
    Create a persistent Volume Claim factory. Calling this fixture creates new
    PVC. For custom PVC provide 'storageclass' parameter.
    """
    instances = []

    def factory(
        interface=constants.CEPHBLOCKPOOL,
        project=None,
        storageclass=None,
        size=None,
        custom_data=None,
        status=constants.STATUS_BOUND,
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
            custom_data (dict): If provided then PVC object is created
                by using these data. Parameters `project` and `storageclass`
                are not used but reference is set if provided.
            status (str): If provided then factory waits for object to reach
                desired state.

        Returns:
            object: helpers.create_pvc instance.
        """
        if custom_data:
            pvc_obj = PVC(**custom_data)
            pvc_obj.create(do_reload=False)
            instances.append(pvc_obj)
        else:
            project = project or project_factory()
            storageclass = storageclass or storageclass_factory(interface)
            pvc_size = f"{size}Gi" if size else None

            pvc_obj = helpers.create_pvc(
                sc_name=storageclass.name,
                namespace=project.namespace,
                size=pvc_size,
                wait=False
            )
            assert pvc_obj, "Failed to create PVC"
            instances.append(pvc_obj)
            pvc_obj.storageclass = storageclass
            pvc_obj.project = project
        if status:
            helpers.wait_for_resource_state(pvc_obj, status)

        return pvc_obj

    def finalizer():
        """
        Delete the PVC
        """
        for instance in instances:
            if not instance.is_deleted:
                instance.delete()
                instance.ocp.wait_for_delete(
                    instance.name
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def pod_factory(request, pvc_factory):
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

        Returns:
            object: helpers.create_pvc instance.
        """
        if custom_data:
            pod_obj = helpers.create_resource(**custom_data, wait=False)
        else:
            pvc = pvc or pvc_factory(interface=interface)

            pod_obj = helpers.create_pod(
                pvc_name=pvc.name,
                namespace=pvc.namespace,
                interface_type=interface,
            )
            assert pod_obj, "Failed to create PVC"
        if status:
            helpers.wait_for_resource_state(pod_obj, status)
        pod_obj.pvc = pvc

        instances.append(pod_obj)
        return pod_obj

    def finalizer():
        """
        Delete the Pod
        """
        for instance in instances:
            if not instance.is_deleted:
                instance.delete()
                instance.ocp.wait_for_delete(
                    instance.name
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def teardown_factory(request):
    """
    Tearing down a resource that was created during the test
    To use this factory, you'll need to pass 'teardown_factory' to you test
    function and call it in you test when a new resource was created and you
    want it to be removed in teardown phase:
    def test_example(self, teardown_factory):
        pvc_obj = create_pvc()
        teardown_factory(pvc_obj)

    """
    instances = []

    def factory(resource_obj):
        """
        Args:
            resource_obj (OCS object): Object to teardown after the test

        """
        instances.append(resource_obj)

    def finalizer():
        """
        Delete the resources created in the test
        """
        for instance in instances:
            if not instance.is_deleted:
                instance.delete()
                instance.ocp.wait_for_delete(
                    instance.name
                )

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
    marks_to_ignore = [m.mark for m in [deployment, destroy, ignore_leftovers]]
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
        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL, pvc_name=pvc_obj.name
        )

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
                    pod_obj.get_mount_path(),
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
