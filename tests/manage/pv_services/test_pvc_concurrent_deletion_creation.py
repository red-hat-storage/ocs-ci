"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.ocs import constants, exceptions
from ocs_ci.framework.testlib import tier1, ManageTest, bugzilla
from ocs_ci.ocs.resources.pvc import delete_pvcs
from tests.helpers import create_multiple_pvcs, wait_for_resource_state

log = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request, project_factory, rbd_pvc_factory):
    """
    Setup and teardown
    """
    cls_ref = request.node.cls
    cls_ref.pvc_objs_new = []
    cls_ref.num_of_pvcs = 3

    cls_ref.project = project_factory()
    cls_ref.pvc_objs = [
        rbd_pvc_factory(project=cls_ref.project) for x in range(1, cls_ref.num_of_pvcs)
    ]


@bugzilla('1734259')
@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__
)
class TestMultiplePvcConcurrentDeletionCreation(ManageTest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """

    def test_multiple_pvc_concurrent_creation_deletion(
            self,
            rbd_pvc_factory
        ):
        """
        To exercise resource creation and deletion
        """
        executor = ThreadPoolExecutor(max_workers=1)

        # Start deleting 100 PVCs
        log.info('Start deleting PVCs.')
        pvc_delete = executor.submit(
            delete_pvcs,
            self.pvc_objs
        )

        # Create 100 PVCs
        log.info('Start creating new PVCs')
        new_pvc_objs = [
            rbd_pvc_factory(project=self.project) for x in range(1, self.num_of_pvcs)
        ]

        for pvc_obj in new_pvc_objs:
            assert wait_for_resource_state(pvc_obj, constants.STATUS_BOUND), (
                f"PVC {pvc_obj.name} failed to reach {constants.STATUS_BOUND} status"
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
