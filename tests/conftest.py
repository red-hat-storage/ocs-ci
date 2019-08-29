import logging
import os
import tempfile
import pytest
import threading
from datetime import datetime

from ocs_ci.utility.utils import TimeoutSampler, get_rook_repo
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
        custom_data=None,
        sc_name=None
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
                sc_name=sc_name
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
                storageclass = (
                    storageclass or active_rbd_storageclass
                    or storageclass_factory(interface)
                )
                active_rbd_storageclass = storageclass
            elif interface == constants.CEPHFILESYSTEM:
                storageclass = (
                    storageclass or active_cephfs_storageclass
                    or storageclass_factory(interface)
                )
                active_cephfs_storageclass = storageclass
            pvc_size = f"{size}Gi" if size else None

            pvc_obj = helpers.create_pvc(
                sc_name=storageclass.name,
                namespace=project.namespace,
                size=pvc_size,
                do_reload=False,
                access_mode=access_mode
            )
            assert pvc_obj, "Failed to create PVC"

        if status:
            helpers.wait_for_resource_state(pvc_obj, status)
        pvc_obj.storageclass = storageclass
        pvc_obj.project = project
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
        for pv_obj in pv_objs:
            pv_obj.ocp.wait_for_delete(
                resource_name=pv_obj.name, timeout=180
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
            pod_obj = helpers.create_resource(**custom_data)
        else:
            pvc = pvc or pvc_factory(interface=interface)

            pod_obj = helpers.create_pod(
                pvc_name=pvc.name,
                namespace=pvc.namespace,
                interface_type=interface,
            )
            assert pod_obj, "Failed to create PVC"
        instances.append(pod_obj)
        if status:
            helpers.wait_for_resource_state(pod_obj, status)
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


@pytest.fixture()
def teardown_factory(request):
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
            resource_obj (OCS object): Object to teardown after the test

        """
        instances.append(resource_obj)

    def finalizer():
        """
        Delete the resources created in the test
        """
        for instance in instances[::-1]:
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
    record_testsuite_property(
        'polarion-custom-isautomated', "True"
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


@pytest.fixture()
def multi_pvc_factory(
    storageclass_factory,
    project_factory,
    pvc_factory
):
    """
    Create a Persistent Volume Claims factory. Calling this fixture creates a
    set of new PVCs.
    """
    def factory(
        interface=constants.CEPHBLOCKPOOL,
        project=None,
        storageclass=None,
        size=None,
        access_mode=constants.ACCESS_MODE_RWO,
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
            access_mode (str): ReadWriteOnce, ReadOnlyMany or ReadWriteMany.
                This decides the access mode to be used for the PVC.
                ReadWriteOnce is default.
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
        storageclass = storageclass or storageclass_factory(interface)

        for _ in range(num_of_pvc):
            pvc_obj = pvc_factory(
                interface=interface,
                project=project,
                storageclass=storageclass,
                size=size,
                access_mode=access_mode,
                status=status_tmp
            )
            pvc_list.append(pvc_obj)

        if not wait_each:
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
