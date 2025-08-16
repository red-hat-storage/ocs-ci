import logging
import pytest
from yaml import dump

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2


logger = logging.getLogger(__name__)


@green_squad
class TestNewPvcWhenRandomPvAvailable(ManageTest):
    """
    Tests to verify PVC creation and provisioning of new volume when a ransom PV is available in Released state

    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory):
        """
        Create PVCs

        """
        self.pvcs = []
        project = None
        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pvc_obj = pvc_factory(
                interface=interface,
                project=project,
                storageclass=None,
                size=10,
                access_mode=constants.ACCESS_MODE_RWO,
                status=constants.STATUS_BOUND,
                volume_mode=constants.VOLUME_MODE_FILESYSTEM,
                size_unit="Gi",
                wait_for_resource_status_timeout=90,
            )
            project = pvc_obj.project
            self.pvcs.append(pvc_obj)

    @tier2
    def test_existing_pv_is_not_used_for_new_pvc(
        self, teardown_factory, create_pvcs_and_pods
    ):
        """
        Test to verify that an existing PV is not used for a new PVC when PV name is not given in PVC yaml

        """
        initial_pv_names = []
        for pvc_obj in self.pvcs:
            pv_obj = pvc_obj.backed_pv_obj
            initial_pv_names.append(pv_obj.name)

            # Change the persistentVolumeReclaimPolicy of the PV to Retain
            reclaim_policy_change = constants.RECLAIM_POLICY_RETAIN
            patch_param = (
                f'{{"spec":{{"persistentVolumeReclaimPolicy":'
                f'"{reclaim_policy_change}"}}}}'
            )
            assert pv_obj.ocp.patch(
                resource_name=pv_obj.name, params=patch_param, format_type="strategic"
            ), f"Failed to change persistentVolumeReclaimPolicy of pv {pv_obj.name} to {reclaim_policy_change}"
            logger.info(
                f"Changed persistentVolumeReclaimPolicy of pv {pv_obj.name} to {reclaim_policy_change}"
            )

            # Delete the PVC
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            pvc_obj.set_deleted()
            teardown_factory(pv_obj)

            # Remove claimRef, so that the PV will become available
            logger.info(f"Removing claimRef from {constants.PV} {pv_obj.name}")
            patch_result = pv_obj.ocp.patch(
                resource_name=pv_obj.name,
                params='[{ "op": "remove", "path": "/spec/claimRef" }]',
                format_type="json",
            )
            assert patch_result, f"Failed to remove claimRef from PV {pv_obj.name}"
            logger.info(f"Logging the details of the {constants.PV} {pv_obj.name}")
            logger.info(dump(pv_obj.get()))

        # Create new PVCs and pods with a different size
        new_pvcs, new_pods = create_pvcs_and_pods(
            pvc_size=5,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=1,
        )

        # Ensure that the old PV is not re-used
        new_pvc_old_pv = {}
        for pvc_obj_new in new_pvcs:
            pvc_obj_new.reload()
            if pvc_obj_new.backed_pv in initial_pv_names:
                logger.error(
                    f"Old available {constants.PV} {pvc_obj_new.backed_pv} is used for the new {constants.PVC} "
                    f"{pvc_obj_new.name} which is not expected."
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
                        f"{constants.PVC} {pvc_obj_new.name} are not matching."
                    )
                    logger.info(
                        f"Logging the details of the {constants.PVC} {pvc_obj_new.name}"
                    )
                    logger.info(dump(pvc_obj_new.get()))
                new_pvc_old_pv[pvc_obj_new.name] = pvc_obj_new.backed_pv

        assert not new_pvc_old_pv, (
            f"This is the mapping of new {constants.PVC} name and old {constants.PV} name. The new {constants.PVC} "
            f"re-used an available {constants.PV} instead of provisioning a new {constants.PV}:\n {new_pvc_old_pv}"
        )
