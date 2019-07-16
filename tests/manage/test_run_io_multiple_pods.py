import pytest
import logging
import random
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.testlib import ManageTest, tier2
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_rbd_secret, create_pods, create_pvcs, create_project
)


logger = logging.getLogger(__name__)


@pytest.fixture()
def init_pvc_size(request):
    """

    """
    class_instance = request.node.cls
    class_instance.pvc_size_int = getattr(class_instance, 'pvc_size_int', random.randint(-1, 100))
    class_instance.pvc_size = f'{class_instance.pvc_size_int}Gi'


@tier2
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_project.__name__,
    init_pvc_size.__name__,
    create_pvcs.__name__,
    create_pods.__name__
)
class TestRunIOMultiplePods(ManageTest):
    """
    Run IO on multiple pods in parallel
    """
    pvc_size_int = 5
    num_of_pvcs = 50

    def test_run_io_multiple_pods(self):
        """
        Run IO on multiple pods in parallel
        """
        results = list()
        with ThreadPoolExecutor(max_workers=self.num_of_pvcs) as executor:
            for pod in self.pod_objs:
                results.append(executor.submit(pod.run_io('fs', f'{self.pvc_size_int - 1}G')))

        for pod in self.pod_objs:
            fio_result = pod.get_fio_results()
            logger.info(f"IOPs after FIO for pod {pod.name}:")
            logger.info(
                f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
            )
            logger.info(
                f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
            )
