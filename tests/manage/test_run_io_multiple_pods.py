import pytest
import logging
import random
from tests import helpers
from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import ManageTest, tier2


logger = logging.getLogger(__name__)


@pytest.fixture()
def create_pvcs(request):
    """
    Create multiple PVCs
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete multiple PVCs
        """
        if hasattr(class_instance, 'pvc_objs'):
            for pvc_obj in class_instance.pvc_objs:
                pvc_obj.delete()
            for pvc_obj in class_instance.pvc_objs:
                pvc_obj.ocp.wait_for_delete(pvc_obj.name)

    request.addfinalizer(finalizer)

    class_instance.pvc_objs = helpers.create_multiple_pvcs(
        sc_name=class_instance.sc_obj.name, number_of_pvc=class_instance.num_of_pvcs,
        size=class_instance.pvc_size, namespace=class_instance.namespace
    )


@pytest.fixture()
def create_pods(request):
    """
    Create multiple pods
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete multiple pods
        """
        if hasattr(class_instance, 'pod_objs'):
            for pod in class_instance.pod_objs:
                pod.delete()

    request.addfinalizer(finalizer)

    class_instance.pod_objs = [
        helpers.create_pod(
            interface_type=class_instance.interface, pvc_name=pvc_obj.name,
            wait=False, namespace=class_instance.namespace
        ) for pvc_obj in class_instance.pvc_objs
    ]
    for pod in class_instance.pod_objs:
        assert helpers.wait_for_resource_state(
            pod, constants.STATUS_RUNNING
        ), f"Pod {pod} failed to reach {constants.STATUS_RUNNING}"


@pytest.fixture()
def init_pvc_size(request):
    """
    Initialize the PVC size for PVC creation
    """
    class_instance = request.node.cls
    class_instance.pvc_size_int = getattr(
        class_instance, 'pvc_size_int', random.randint(1, 10)
    )
    class_instance.pvc_size = f'{class_instance.pvc_size_int}Gi'


@tier2
@pytest.mark.usefixtures(
    init_pvc_size.__name__,
)
class BaseRunIOMultiplePods(ManageTest):
    """
    Run IO on multiple pods in parallel
    """
    num_of_pvcs = 10
    pvc_size_int = 5
    interface = None

    def run_io_multiple_pods(self, project):
        """
        Run IO on multiple pods in parallel
        """
        self.namespace = project.namespace

        for pod in self.pod_objs:
            pod.run_io('fs', f'{self.pvc_size_int - 1}G')

        for pod in self.pod_objs:
            fio_result = pod.get_fio_results()
            logger.info(f"IOPs after FIO for pod {pod.name}:")
            logger.info(
                f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
            )
            logger.info(
                f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
            )


@tier2
@pytest.mark.usefixtures(
    create_pvcs.__name__,
    create_pods.__name__
)
class TestRunIOMultiplePodsRBD(BaseRunIOMultiplePods):
    """
    Run IO on multiple pods in parallel - RBD
    """
    interface = constants.CEPHBLOCKPOOL
    storage_type = 'block'

    def test_run_io_multiple_pods_rbd(self, rbd_storageclass):
        """
        """
        self.sc_obj = rbd_storageclass
        self.run_io_multiple_pods()


@tier2
@pytest.mark.usefixtures(
    create_pvcs.__name__,
    create_pods.__name__
)
class TestRunIOMultiplePodsFS(BaseRunIOMultiplePods):
    """
    Run IO on multiple pods in parallel - CephFS
    """
    interface = constants.CEPHFILESYSTEM
    storage_type = 'fs'

    def test_run_io_multiple_pods_fs(self, cephs_storageclass):
        """
        """
        self.sc_obj = cephs_storageclass
        self.run_io_multiple_pods()
