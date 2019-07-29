import pytest
import logging
import random
from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import ManageTest, tier2
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_rbd_secret, create_pods, create_pvcs, create_project,
    create_cephfs_secret, create_cephfs_storageclass
)


logger = logging.getLogger(__name__)


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
    create_project.__name__,
    init_pvc_size.__name__,
)
class BaseRunIOMultiplePods(ManageTest):
    """
    Run IO on multiple pods in parallel
    """
    num_of_pvcs = 10
    pvc_size_int = 5
    interface = None

    def run_io_multiple_pods(self):
        """
        Run IO on multiple pods in parallel
        """

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
@pytest.mark.polarion_id("OCS-692")
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_pvcs.__name__,
    create_pods.__name__
)
class TestRunIOMultiplePodsRBD(BaseRunIOMultiplePods):
    """
    Run IO on multiple pods in parallel - RBD
    """
    interface = constants.CEPHBLOCKPOOL
    storage_type = 'block'

    def test_run_io_multiple_pods_rbd(self):
        """
        Run IO on multiple pods in parallel - RBD
        """
        self.run_io_multiple_pods()


@tier2
@pytest.mark.polarion_id("OCS-693")
@pytest.mark.usefixtures(
    create_cephfs_secret.__name__,
    create_cephfs_storageclass.__name__,
    create_pvcs.__name__,
    create_pods.__name__
)
class TestRunIOMultiplePodsFS(BaseRunIOMultiplePods):
    """
    Run IO on multiple pods in parallel - CephFS
    """
    interface = constants.CEPHFILESYSTEM
    storage_type = 'fs'

    def test_run_io_multiple_pods_fs(self):
        """
        Run IO on multiple pods in parallel - CephFS
        """
        self.run_io_multiple_pods()
