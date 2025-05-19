import logging
import pytest


from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2


logger = logging.getLogger(__name__)


@green_squad
class TestNewPvcWhenPvAvailable(ManageTest):
    """
    Tests to verify PVC creation and provisioning of new volume when a PV is already present

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=10,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=1,
        )

    @tier2
    def test_existing_pv_is_not_used_for_new_pvc(self, create_pvcs_and_pods):
        """
        Test to verify that an existing PV is not used for a new PVC when PV name is not given in PVC yaml

        """
        for pod_obj in self.pods:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        old_pv_names = []
        for pvc_obj in self.pvcs:
            pv_obj = pvc_obj.backed_pv_obj()
            old_pv_names.append(pv_obj.name)
            # Remove claimRef, so that the PV will become available
            logger.info(f"Dropping claimRef from PV {pv_obj.name}")
            patch_result = pv_obj.ocp.patch(
                resource_name=pv_obj.name,
                params='[{ "op": "remove", "path": "/spec/claimRef" }]',
                format_type="json",
            )
            assert patch_result, f"Failed to remove claimRef from PV {pv_obj.name}"

        self.new_pvcs, self.new_pods = create_pvcs_and_pods(
            pvc_size=5,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=1,
        )

        new_pvc_old_pv = {}
        for pvc_obj_new in self.new_pvcs:
            if pvc_obj_new.backed_pv in old_pv_names:
                logger.error(
                    f"Old available PV {pvc_obj_new.backed_pv} is used for the new PVC {pvc_obj_new.name} "
                    f"which is not expected."
                )
                pvc_storage_request = (
                    pvc_obj_new.data.get("spec")
                    .get("resources")
                    .get("requests")
                    .get("storage")
                )
                pvc_storage_capacity = (
                    pvc_obj_new.data.get("status").get("capacity").get("storage")
                )
                if pvc_storage_request != pvc_storage_capacity:
                    logger.error(
                        f"The requested storage {pvc_storage_request} and capacity {pvc_storage_capacity} of the "
                        f"PVC {pvc_obj_new.name} are not matching"
                    )
                new_pvc_old_pv[pvc_obj_new.name] = pvc_obj_new.backed_pv

        assert not new_pvc_old_pv, (
            f"This is the mapping of new PVC name and old PV name. The new PVC re-used an available PV instead of "
            f"provisioning a new PV:\n {new_pvc_old_pv}"
        )
