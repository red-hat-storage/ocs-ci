"""
Test to verify concurrent creation and deletion of multiple PVCs
"""

import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients_push_missing_configs,
)
from ocs_ci.framework.testlib import tier2, ManageTest
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    verify_volume_deleted_in_backend,
    default_ceph_block_pool,
)

log = logging.getLogger(__name__)


@provider_mode
@green_squad
@tier2
@pytest.mark.parametrize(
    argnames="interface",
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-323")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-2018")
        ),
    ],
)
class TestMultiplePvcConcurrentDeletionCreation(ManageTest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """

    num_of_pvcs = 100
    pvc_size = 3
    access_modes = [constants.ACCESS_MODE_RWO]

    @pytest.fixture(autouse=True)
    def setup(self, multi_pvc_factory, interface):
        """
        Create PVCs
        """
        if interface == constants.CEPHFILESYSTEM:
            self.access_modes.append(constants.ACCESS_MODE_RWX)
        self.pvc_objs = multi_pvc_factory(
            interface=interface,
            project=None,
            storageclass=None,
            size=self.pvc_size,
            access_modes=self.access_modes,
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvcs,
            wait_each=False,
        )

    @run_on_all_clients_push_missing_configs
    def test_multiple_pvc_concurrent_creation_deletion(
        self, interface, multi_pvc_factory, cluster_index
    ):
        """
        To exercise resource creation and deletion
        """
        proj_obj = self.pvc_objs[0].project

        executor = ThreadPoolExecutor(max_workers=1)

        # Get PVs
        pv_objs = []
        for pvc in self.pvc_objs:
            pv_objs.append(pvc.backed_pv_obj)

        # Fetch image uuid associated with PVCs
        pvc_uuid_map = {}
        for pvc_obj in self.pvc_objs:
            pvc_uuid_map[pvc_obj.name] = pvc_obj.image_uuid
        log.info("Fetched image uuid associated with each PVC")

        # Start deleting 100 PVCs
        log.info("Start deleting PVCs.")
        pvc_delete = executor.submit(delete_pvcs, self.pvc_objs)

        # Create 100 PVCs
        log.info("Start creating new PVCs")
        self.new_pvc_objs = multi_pvc_factory(
            interface=interface,
            project=proj_obj,
            size=self.pvc_size,
            access_modes=self.access_modes,
            status="",
            num_of_pvc=self.num_of_pvcs,
            wait_each=False,
        )

        for pvc_obj in self.new_pvc_objs:
            wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
        log.info(f"Newly created {self.num_of_pvcs} PVCs are in Bound state.")

        # Verify PVCs are deleted
        res = pvc_delete.result()
        assert res, "Deletion of PVCs failed"
        log.info("PVC deletion was successful.")
        for pvc in self.pvc_objs:
            pvc.ocp.wait_for_delete(resource_name=pvc.name)
        log.info(f"Successfully deleted initial {self.num_of_pvcs} PVCs")

        # Verify PVs are deleted
        for pv_obj in pv_objs:
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)
        log.info(f"Successfully deleted initial {self.num_of_pvcs} PVs")

        # Verify PV using ceph toolbox. Image/Subvolume should be deleted.
        for pvc_name, uuid in pvc_uuid_map.items():
            pool_name = None
            if interface == constants.CEPHBLOCKPOOL:
                pool_name = default_ceph_block_pool()
            ret = verify_volume_deleted_in_backend(
                interface=interface, image_uuid=uuid, pool_name=pool_name
            )
            assert ret, (
                f"Volume associated with PVC {pvc_name} still exists " f"in backend"
            )

        # Verify status of nodes
        for node in get_node_objs():
            node_status = node.ocp.get_resource_status(node.name)
            assert (
                node_status == constants.NODE_READY
            ), f"Node {node.name} is in {node_status} state."
