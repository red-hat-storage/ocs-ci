import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_hci_provider_and_client,
)
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.helpers.helpers import (
    storagecluster_independent_check,
    wait_for_resource_state,
)
from ocs_ci.utility import version

log = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.polarion_id("OCS-2424")
class TestRestoreSnapshotUsingDifferentSc(ManageTest):
    """
    Tests to verify snapshot restore using an SC different than that of parent

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        project_factory,
        secret_factory,
        storageclass_factory,
        snapshot_restore_factory,
        create_pvcs_and_pods,
    ):
        """
        Create PVCs and pods

        """
        self.pvc_size = 3
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=[constants.ACCESS_MODE_RWO],
        )

    def test_snapshot_restore_using_different_sc(
        self,
        storageclass_factory,
        snapshot_factory,
        snapshot_restore_factory,
        pod_factory,
    ):
        """
        Test to verify snapshot restore using an SC different than that of parent

        """
        snap_objs = []
        file_name = "file_snapshot"
        # Run IO
        log.info("Start IO on all pods")
        for pod_obj in self.pods:
            pod_obj.run_io(
                storage_type="fs",
                size=f"{self.pvc_size - 1}G",
                runtime=30,
                fio_filename=file_name,
            )
        log.info("IO started on all pods")

        # Wait for IO completion
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            # Get md5sum of the file
            pod_obj.pvc.md5sum = cal_md5sum(pod_obj=pod_obj, file_name=file_name)
        log.info("IO completed on all pods")

        # Create snapshots
        log.info("Create snapshots of all PVCs")
        for pvc_obj in self.pvcs:
            log.info(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snap_obj.interface = pvc_obj.interface
            snap_objs.append(snap_obj)
        log.info("Snapshots created")

        # Verify snapshots are Ready
        log.info("Verify snapshots are ready")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )

        # Create storage classes.
        sc_objs = {
            constants.CEPHBLOCKPOOL: [
                storageclass_factory(
                    interface=constants.CEPHBLOCKPOOL,
                ).name
            ],
            constants.CEPHFILESYSTEM: [
                storageclass_factory(interface=constants.CEPHFILESYSTEM).name
            ],
        }

        # If ODF >=4.9 create one more storage class that will use new pool
        # to verify the bug 1901954
        if (
            not storagecluster_independent_check()
            and version.get_semantic_ocs_version_from_config() >= version.VERSION_4_9
        ):
            sc_objs[constants.CEPHBLOCKPOOL].append(
                storageclass_factory(
                    interface=constants.CEPHBLOCKPOOL, new_rbd_pool=True
                ).name
            )

        # Create PVCs out of the snapshots
        restore_pvc_objs = []
        log.info("Creating new PVCs from snapshots")
        for snap_obj in snap_objs:
            for storageclass in sc_objs[snap_obj.interface]:
                log.info(f"Creating a PVC from snapshot {snap_obj.name}")
                restore_pvc_obj = snapshot_restore_factory(
                    snapshot_obj=snap_obj,
                    storageclass=storageclass,
                    size=f"{self.pvc_size}Gi",
                    volume_mode=snap_obj.parent_volume_mode,
                    access_mode=snap_obj.parent_access_mode,
                    status="",
                )

                log.info(
                    f"Created PVC {restore_pvc_obj.name} from snapshot {snap_obj.name}."
                    f"Used the storage class {storageclass}"
                )
                restore_pvc_obj.md5sum = snap_obj.md5sum
                restore_pvc_objs.append(restore_pvc_obj)
        log.info("Created new PVCs from all the snapshots")

        # Confirm that the restored PVCs are Bound
        log.info("Verify the restored PVCs are Bound")
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound.")

        # Attach the restored PVCs to pods
        log.info("Attach the restored PVCs to pods")
        restore_pod_objs = []
        for restore_pvc_obj in restore_pvc_objs:
            restore_pod_obj = pod_factory(
                interface=restore_pvc_obj.snapshot.interface,
                pvc=restore_pvc_obj,
                status="",
            )
            log.info(
                f"Attached the PVC {restore_pvc_obj.name} to pod {restore_pod_obj.name}"
            )
            restore_pod_objs.append(restore_pod_obj)

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in restore_pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        log.info("Verified: New pods are running")

        # Verify md5sum
        log.info("Verifying md5sum on new pods")
        for pod_obj in restore_pod_objs:
            log.info(f"Verifying md5sum on pod {pod_obj.name}")
            verify_data_integrity(
                pod_obj=pod_obj,
                file_name=file_name,
                original_md5sum=pod_obj.pvc.snapshot.md5sum,
            )
            log.info(f"Verified md5sum on pod {pod_obj.name}")
        log.info("Verified md5sum on all pods")

        # Run IO on new pods
        log.info("Starting IO on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.run_io(storage_type="fs", size="500M", runtime=15)

        # Wait for IO completion on new pods
        log.info("Waiting for IO completion on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on new pods.")
