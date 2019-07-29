"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.ocs import exceptions
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.pvc import delete_pvcs
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool, create_rbd_secret,
    create_project, create_pvcs
)
from tests.helpers import create_multiple_pvcs

log = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request):
    """
    Setup and teardown
    """
    cls_ref = request.node.cls
    cls_ref.pvc_objs_new = []

    def finalizer():
        # Delete newly created PVCs
        assert delete_pvcs(cls_ref.pvc_objs_new), 'Failed to delete PVCs'
        log.info(f'Newly created {cls_ref.num_of_pvcs} PVCs are now deleted.')

    request.addfinalizer(finalizer)


@tier1
@pytest.mark.usefixtures(
    create_project.__name__,
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_pvcs.__name__,
    test_fixture.__name__
)
class TestMultiplePvcConcurrentDeletionCreation(ManageTest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """
    num_of_pvcs = 100
    pvc_size = '3Gi'

    def test_multiple_pvc_concurrent_creation_deletion(self):
        """
        To exercise resource creation and deletion
        """
        executor = ThreadPoolExecutor(max_workers=1)

        # Start deleting 100 PVCs
        log.info('Start deleting PVCs.')
        pvc_delete = executor.submit(
            delete_pvcs, self.pvc_objs
        )

        # Create 100 PVCs
        log.info('Start creating new PVCs')
        new_pvc_objs = create_multiple_pvcs(
            sc_name=self.sc_obj.name, namespace=self.namespace,
            number_of_pvc=self.num_of_pvcs
        )
        log.info(f'Newly created {self.num_of_pvcs} PVCs are in Bound state.')
        self.pvc_objs_new.extend(new_pvc_objs)

        # Verify PVCs are deleted
        res = pvc_delete.result()
        assert res, 'Deletion of PVCs failed'
        log.info('PVC deletion was successful.')

        # Clear pvc_objs list to avoid error in 'create_pvcs' fixture
        self.pvc_objs.clear()

        # Verify PVCs are deleted
        for pvc in self.pvc_objs:
            try:
                pvc.get()
                return False
            except exceptions.CommandFailed as exp:
                assert "not found" in str(exp), (
                    f'Failed to fetch details of PVC {pvc.name}'
                )
                log.info(f'Expected: PVC {pvc.name} does not exists')
        log.info(f'Successfully deleted initial {self.num_of_pvcs} PVCs')

        # TODO: Verify PVs using ceph toolbox. Blocked by Bz 1723656
