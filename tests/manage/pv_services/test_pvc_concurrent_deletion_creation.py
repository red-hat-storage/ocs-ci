"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import tier2, ManageTest, bugzilla
from ocs_ci.ocs.resources.pvc import delete_pvcs
from tests.helpers import wait_for_resource_state

log = logging.getLogger(__name__)


@bugzilla('1734259')
@tier2
class TestMultiplePvcConcurrentDeletionCreation(ManageTest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """
    num_of_pvcs = 100
    pvc_size = 3

    @pytest.fixture(autouse=True)
    def setup(self, multi_pvc_factory):
        """
        Create PVCs
        """
        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=None,
            storageclass=None,
            size=self.pvc_size,
            access_modes=[constants.ACCESS_MODE_RWO],
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvcs,
            wait_each=False
        )

    def test_multiple_pvc_concurrent_creation_deletion(self, multi_pvc_factory):
        """
        To exercise resource creation and deletion
        """
        proj_obj = self.pvc_objs[0].project
        storageclass = self.pvc_objs[0].storageclass

        executor = ThreadPoolExecutor(max_workers=1)

        # Get PVs
        pv_objs = []
        for pvc in self.pvc_objs:
            pv_objs.append(pvc.backed_pv_obj)

        # Start deleting 100 PVCs
        log.info('Start deleting PVCs.')
        pvc_delete = executor.submit(
            delete_pvcs, self.pvc_objs
        )

        # Create 100 PVCs
        log.info('Start creating new PVCs')
        self.new_pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=proj_obj,
            storageclass=storageclass,
            size=self.pvc_size,
            access_modes=[constants.ACCESS_MODE_RWO],
            status='',
            num_of_pvc=self.num_of_pvcs,
            wait_each=False
        )

        for pvc_obj in self.new_pvc_objs:
            wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
        log.info(f'Newly created {self.num_of_pvcs} PVCs are in Bound state.')

        # Verify PVCs are deleted
        res = pvc_delete.result()
        assert res, 'Deletion of PVCs failed'
        log.info('PVC deletion was successful.')
        for pvc in self.pvc_objs:
            pvc.ocp.wait_for_delete(resource_name=pvc.name)
        log.info(f'Successfully deleted initial {self.num_of_pvcs} PVCs')

        # Verify PVs are deleted
        for pv_obj in pv_objs:
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)
        log.info(f'Successfully deleted initial {self.num_of_pvcs} PVs')

        # TODO: Verify PVs using ceph toolbox. Blocked by Bz 1723656
