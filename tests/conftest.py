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
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.resources.ocs import OCS


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
def rbd_secret_factory(request):
    """
    RBD secret factory. Calling this fixture creates new secret.
    """
    instances = []

    def factory():
        rbd_secret_obj = helpers.create_secret(
            interface_type=constants.CEPHBLOCKPOOL
        )
        assert rbd_secret_obj, "Failed to create secret"
        instances.append(rbd_secret_obj)
        return rbd_secret_obj

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
def cephfs_secret_factory(request):
    """
    Create a CephFS secret. Calling this fixture creates new secret.
    """
    instances = []

    def factory():
        cephfs_secret_obj = helpers.create_secret(
            interface_type=constants.CEPHFILESYSTEM
        )
        assert cephfs_secret_obj, "Failed to create secret"
        instances.append(cephfs_secret_obj)
        return cephfs_secret_obj

    def finalizer():
        """
        Delete the FS secrets
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
def ceph_block_pool_factory(request):
    """
    Create a Ceph block pool factory.
    Calling this fixture creates new block pool instance.
    """
    instances = []

    def factory():
        cbp_obj = helpers.create_ceph_block_pool()
        assert cbp_obj, "Failed to create block pool"
        instances.append(cbp_obj)
        return cbp_obj

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
def rbd_storageclass_factory(
    request,
    ceph_block_pool_factory,
    rbd_secret_factory
):
    """
    Create an RBD storage class factory.
    Calling this fixture creates new storage class instance using RBD.
    """
    instances = []

    def factory(block_pool=None, secret=None, custom_data=None):
        """
        Args:
            block_pool (object): An OCS instance for the block pool.
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
            block_pool = block_pool or ceph_block_pool_factory()
            secret = secret or rbd_secret_factory()

            if custom_data:
                custom_data

            sc_obj = helpers.create_storage_class(
                interface_type=constants.CEPHBLOCKPOOL,
                interface_name=block_pool.name,
                secret_name=secret.name
            )
            assert sc_obj, "Failed to create storage class"
        sc_obj.block_pool = block_pool
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
def cephfs_storageclass_factory(request, cephfs_secret_factory):
    """
    Create a CephFS storage class factory.
    Calling this fixture creates new storage class instance using CephFS.
    """
    instances = []

    def factory(secret=None, custom_data=None):
        """
        Args:
            secret (object): An OCS instance for the secret.
            custom_data (dict): If provided then storageclass object is created
                by using these data. Parameter `secret` is not used but
                reference is set if provided.

        Returns:
            object: helpers.create_storage_class instance with link to secret.
        """
        if custom_data:
            sc_obj = helpers.create_resource(**custom_data, wait=False)
        else:
            secret = secret or cephfs_secret_factory()

            sc_obj = helpers.create_storage_class(
                interface_type=constants.CEPHFILESYSTEM,
                interface_name=helpers.get_cephfs_data_pool_name(),
                secret_name=secret.name
            )
            assert sc_obj, "Failed to create storage class"
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

    def factory(**kwargs):
        """

        Returns:
            object: ocs_ci.ocs.resources.ocs instance of 'Project' kind.
        """
        if 'metadata' not in kwargs or 'namespace' not in kwargs.get(
            'metadata'
        ):
            namespace = helpers.create_unique_resource_name(
                'test',
                'namespace'
            )
            kwargs['metadata'] = kwargs.get('metadata') or {}
            kwargs['metadata']['namespace'] = namespace

        proj_obj = OCS(
            kind='Project',
            **kwargs
        )
        assert proj_obj.ocp.new_project(namespace), (
            f'Failed to create new project {namespace}'
        )

        instances.append(proj_obj)
        return proj_obj

    def finalizer():
        """
        Delete the Ceph block pool
        """
        for instance in instances:
            if not instance.is_deleted:
                ocp.switch_to_default_rook_cluster_project()
                instance.ocp.delete(
                    resource_name=instance.namespace
                )
                instance.ocp.wait_for_delete(
                    instance.namespace
                )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture()
def pvc_factory(request, rbd_storageclass_factory, project_factory):
    """
    Create a persistent Volume Claim factory. Calling this fixture creates new
    PVC. For custom PVC provide 'storageclass' parameter.
    """
    instances = []
    active_project = None

    def factory(project=None, storageclass=None, custom_data=None):
        """
        Args:
            project (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'Project' kind.
            storageclass (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'StorageClass' kind.
            custom_data (dict): If provided then PVC object is created
                by using these data. Parameters `project` and `storageclass`
                are not used but reference is set if provided.

        Returns:
            object: helpers.create_pvc instance.
        """
        if custom_data:
            pvc_obj = helpers.create_resource(**custom_data, wait=False)
        else:
            nonlocal active_project
            project = project or active_project or project_factory()
            active_project = project
            storageclass = storageclass or rbd_storageclass_factory()

            pvc_obj = helpers.create_pvc(
                sc_name=storageclass.name,
                namespace=project.namespace,
                wait=False
            )
            assert pvc_obj, "Failed to create PVC"
        pvc_obj.storageclass = storageclass
        pvc_obj.project = project

        instances.append(pvc_obj)
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
def rbd_pvc_factory(request, pvc_factory, rbd_storageclass_factory):
    """
    Create a persistent Volume Claim factory. Calling this fixture creates new
    RBD based PVC. For custom PVC provide 'storageclass' parameter.
    """

    def factory(project=None, storageclass=None, custom_data=None):
        """
        Args:
            project (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'Project' kind.
            storageclass (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'StorageClass' kind.
            custom_data (dict): If provided then PVC object is created
                by using these data. Parameters `project` and `storageclass`
                are not used but reference is set if provided.

        Returns:
            object: helpers.create_pvc instance.
        """
        storageclass = storageclass or rbd_storageclass_factory()
        return pvc_factory(
            storageclass=storageclass,
            project=project,
            custom_data=custom_data
        )
    return factory


@pytest.fixture()
def cephfs_pvc_factory(request, pvc_factory, cephfs_storageclass_factory):
    """
    Create a persistent Volume Claim factory. Calling this fixture creates new
    CephFS based PVC. For custom PVC provide 'storageclass' parameter.
    """

    def factory(project=None, storageclass=None, custom_data=None):
        """
        Args:
            project (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'Project' kind.
            storageclass (object): ocs_ci.ocs.resources.ocs.OCS instance
                of 'StorageClass' kind.
            custom_data (dict): If provided then PVC object is created
                by using these data. Parameters `project` and `storageclass`
                are not used but reference is set if provided.

        Returns:
            object: helpers.create_pvc instance.
        """
        storageclass = storageclass or cephfs_storageclass_factory()
        return pvc_factory(
            storageclass=storageclass,
            project=project,
            custom_data=custom_data
        )
    return factory


@pytest.fixture()
def pod_factory(request):
    """
    Create a Pod factory. Calling this fixture creates new Pod.
    For custom Pods provide 'pvc' parameter.
    """
    instances = []

    def factory(pvc=None, custom_data=None):
        """
        Args:
            pvc (object): ocs_ci.ocs.resources.ocs.OCS instance of 'PVC' kind.
            custom_data (dict): If provided then Pod object is created
                by using these data. Parameter `pvc` is not used but reference
                is set if provided.

        Returns:
            object: helpers.create_pvc instance.
        """
        if custom_data:
            pod_obj = helpers.create_resource(**custom_data, wait=False)
        else:
            if pvc.storageclass.data[
                'provisioner'
            ] == defaults.RBD_PROVISIONER:
                interface_type = constants.CEPHBLOCKPOOL
            elif pvc.storageclass.data[
                'provisioner'
            ] == defaults.CEPHFS_PROVISIONER:
                interface_type = constants.CEPHFILESYSTEM

            pod_obj = helpers.create_pod(
                pvc_name=pvc.name,
                namespace=pvc.namespace,
                interface_type=interface_type,
                wait=False
            )
            assert pod_obj, "Failed to create PVC"
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
def rbd_pod_factory(pod_factory, rbd_pvc_factory):
    """
    Create a RBD based Pod factory. Calling this fixture creates new RBD Pod.
    For custom Pods provide 'pvc' parameter.
    """
    def factory(pvc=None, custom_data=None):
        """
        Args:
            pvc (object): ocs_ci.ocs.resources.ocs.OCS instance of 'PVC' kind.
            custom_data (dict): If provided then Pod object is created
                by using these data. Parameter `pvc` is not used but reference
                is set if provided.

        Returns:
            object: helpers.create_pvc instance.
        """
        pvc = pvc or rbd_pvc_factory()
        helpers.wait_for_resource_state(pvc, constants.STATUS_BOUND)
        return pod_factory(
            pvc=pvc,
            custom_data=custom_data
        )
    return factory


@pytest.fixture()
def cephfs_pod_factory(pod_factory, cephfs_pvc_factory):
    """
    Create a CephFS based Pod factory. Calling this fixture creates new Pod.
    For custom Pods provide 'pvc' parameter.
    """
    def factory(pvc=None, custom_data=None):
        """
        Args:
            pvc (object): ocs_ci.ocs.resources.ocs.OCS instance of 'PVC' kind.
            custom_data (dict): If provided then Pod object is created
                by using these data. Parameter `pvc` is not used but reference
                is set if provided.

        Returns:
            object: helpers.create_pvc instance.
        """
        pvc = pvc or cephfs_pvc_factory()
        helpers.wait_for_resource_state(pvc, constants.STATUS_BOUND)
        return pod_factory(
            pvc=pvc,
            custom_data=custom_data
        )
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
    factory = dep_factory.DeploymentFactory()
    deployer = factory.get_deployment()

    # Add a finalizer to teardown the cluster after test execution is finished
    if teardown:
        def cluster_teardown_finalizer():
            deployer.destroy_cluster(log_cli_level)
        request.addfinalizer(cluster_teardown_finalizer)
        log.info("Will teardown cluster because --teardown was provided")

    # Download client
    get_openshift_client()

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
