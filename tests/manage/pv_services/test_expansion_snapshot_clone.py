import logging
import pytest

from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import (
    default_storage_class,
    default_thick_storage_class,
    check_rbd_image_used_size,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ocs_version,
    skipif_ocp_version,
    polarion_id,
)

log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocp_version("<4.6")
@pytest.mark.parametrize(
    argnames=["pvc_create_sc_type", "restore_sc_type"],
    argvalues=[
        pytest.param(
            *["thin", "thin"],
            marks=[polarion_id("OCS-2408"), skipif_ocs_version("<4.6")],
        ),
    ],
)
class TestExpansionSnapshotClone(ManageTest):
    """
    Tests to verify snapshot, clone and expansion

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        storageclass_factory,
        project_factory,
        snapshot_restore_factory,
        pvc_clone_factory,
        create_pvcs_and_pods,
        pvc_create_sc_type,
        restore_sc_type,
    ):
        """
        Create Storage Class, PVCs and pods

        """
        self.pvc_size = 2

        if "thick" in (pvc_create_sc_type, restore_sc_type):
            # Thick provisioning is applicable only for RBD
            thick_sc = default_thick_storage_class()
            access_modes_cephfs = None
            num_of_cephfs_pvc = 0
            thin_sc = default_storage_class(constants.CEPHBLOCKPOOL)
        else:
            thick_sc = None
            access_modes_cephfs = [constants.ACCESS_MODE_RWO]
            num_of_cephfs_pvc = 1
            thin_sc = default_storage_class(constants.CEPHFILESYSTEM)

        sc_dict = {"thin": thin_sc, "thick": thick_sc}
        self.pvc_create_sc = sc_dict[pvc_create_sc_type]
        self.restore_sc = sc_dict[restore_sc_type]

        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=access_modes_cephfs,
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=num_of_cephfs_pvc,
            sc_rbd=self.pvc_create_sc,
        )

        self.ct_pod = pod.get_ceph_tools_pod()
        if pvc_create_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=self.pvcs,
                usage_to_compare=f"{self.pvc_size}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), "One or more PVCs are not thick provisioned."

    def test_expansion_snapshot_clone(
        self,
        pvc_create_sc_type,
        restore_sc_type,
        snapshot_factory,
        snapshot_restore_factory,
        pvc_clone_factory,
        pod_factory,
    ):
        """
        This test performs the following operations :

        Run IO --> Expand parent PVC --> Take snapshot --> Expand parent PVC -->
        Take clone --> Restore snapshot --> Expand cloned and restored PVC -->
        Clone restored PVC --> Snapshot and restore of cloned PVCs -->
        Expand new PVCs

        Data integrity and thick provisioning will be verified in each stage as required.
        This test verifies that the clone, snapshot and parent PVCs are
        independent and any operation in one will not impact the other.

        Use of thin or think provision storage class for PVC creation and snapshot restore
        will be based on value of the parameters 'pvc_create_sc_type' and 'restore_sc_type'
        respectively.

        """
        filename = "fio_file"
        filename_restore_clone = "fio_file_restore_clone"
        pvc_size_expand_1 = 4
        pvc_size_expand_2 = 6
        pvc_size_expand_3 = 8
        snapshots = []

        # Run IO
        log.info("Start IO on pods")
        for pod_obj in self.pods:
            log.info(f"Running IO on pod {pod_obj.name}")
            pod_obj.run_io(
                storage_type="fs", size="1G", runtime=20, fio_filename=filename
            )
        log.info("IO started on all pods")

        log.info("Wait for IO completion on pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            log.info(f"IO finished on pod {pod_obj.name}")
            # Calculate md5sum
            md5sum = pod.cal_md5sum(pod_obj, filename)
            pod_obj.pvc.md5sum = md5sum
        log.info("IO completed on all pods")

        # Expand PVCs
        log.info(f"Expanding PVCs to {pvc_size_expand_1}Gi")
        for pvc_obj in self.pvcs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expand_1}Gi")
            pvc_obj.resize_pvc(pvc_size_expand_1, True)
        log.info(f"Verified: Size of all PVCs are expanded to {pvc_size_expand_1}Gi")

        # Verify thick provision by checking the image used size
        if pvc_create_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=self.pvcs,
                usage_to_compare=f"{pvc_size_expand_1}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), "One or more PVCs are not thick provisioned after expansion"
            log.info(
                f"Verified thick provision after expanding the PVCs to {pvc_size_expand_1}GiB"
            )

        # Take snapshot of all PVCs
        log.info("Creating snapshot of all PVCs")
        for pvc_obj in self.pvcs:
            log.info(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snapshots.append(snap_obj)
            log.info(f"Created snapshot of PVC {pvc_obj.name}")
        log.info("Created snapshot of all PVCs")

        # Verify snapshots are ready
        log.info("Verify snapshots are ready")
        for snap_obj in snapshots:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snap_obj.reload()
        log.info("Verified: Snapshots are Ready")

        # Expand PVCs
        log.info(f"Expanding PVCs to {pvc_size_expand_2}Gi")
        for pvc_obj in self.pvcs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expand_2}Gi")
            pvc_obj.resize_pvc(pvc_size_expand_2, True)
        log.info(f"Verified: Size of all PVCs are expanded to {pvc_size_expand_2}Gi")

        # Verify thick provision by checking the image used size
        if pvc_create_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=self.pvcs,
                usage_to_compare=f"{pvc_size_expand_2}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), "One or more PVCs are not thick provisioned after expansion"
            log.info(
                f"Verified thick provision after expanding the PVCs to {pvc_size_expand_2}GiB"
            )

        # Clone PVCs
        log.info("Creating clone of all PVCs")
        clone_objs = []
        for pvc_obj in self.pvcs:
            log.info(f"Creating clone of PVC {pvc_obj.name}")
            clone_obj = pvc_clone_factory(
                pvc_obj=pvc_obj, status="", volume_mode=constants.VOLUME_MODE_FILESYSTEM
            )
            clone_obj.md5sum = pvc_obj.md5sum
            clone_objs.append(clone_obj)
            log.info(f"Created clone of PVC {pvc_obj.name}")
        log.info("Created clone of all PVCs")

        log.info("Wait for cloned PVCs to reach Bound state and verify size")
        for pvc_obj in clone_objs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            assert pvc_obj.size == pvc_size_expand_2, (
                f"Size is not {pvc_size_expand_2} but {pvc_obj.size} in "
                f"cloned PVC {pvc_obj.name}"
            )
        log.info(
            f"Cloned PVCs reached Bound state. Verified the size of all PVCs "
            f"as {pvc_size_expand_2}Gi"
        )

        # Ensure restore size is not impacted by parent PVC expansion
        log.info("Verify restore size of snapshots")
        for snapshot_obj in snapshots:
            snapshot_info = snapshot_obj.get()
            assert snapshot_info["status"]["restoreSize"] == (
                f"{pvc_size_expand_1}Gi"
            ), (
                f"Restore size mismatch in snapshot {snapshot_obj.name}\n"
                f"{snapshot_info}"
            )
        log.info(f"Verified: Restore size of all snapshots are {pvc_size_expand_1}Gi")

        # Restore snapshots
        log.info("Restore snapshots")
        restore_objs = []
        for snap_obj in snapshots:
            restore_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj, status="", storageclass=self.restore_sc.name
            )
            restore_obj.md5sum = snap_obj.md5sum
            restore_objs.append(restore_obj)

        log.info("Verify restored PVCs are Bound")
        for pvc_obj in restore_objs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound.")

        # Verify restored PVCs are thick provision or not by checking the image used size
        if restore_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=restore_objs,
                usage_to_compare=f"{pvc_size_expand_1}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), "One or more restored PVCs are not thick provisioned"
            log.info("Verified thick provision on restored PVCs")
        elif restore_sc_type == "thin" and (
            "thick" in (pvc_create_sc_type, restore_sc_type)
        ):
            assert check_rbd_image_used_size(
                pvc_objs=restore_objs,
                usage_to_compare=f"{pvc_size_expand_1}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=False,
            ), "One or more restored PVCs are not thin provisioned"
            log.info("Verified: Restored PVCs are not thick provisioned.")

        # Verify clones are thick provision or not by checking the image used size
        if pvc_create_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=clone_objs,
                usage_to_compare=f"{pvc_size_expand_2}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), "One or more cloned PVCs are not thick provisioned"
            log.info("Verified thick provision on cloned PVCs.")
        elif pvc_create_sc_type == "thin" and (
            "thick" in (pvc_create_sc_type, restore_sc_type)
        ):
            assert check_rbd_image_used_size(
                pvc_objs=clone_objs,
                usage_to_compare=f"{pvc_size_expand_2}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=False,
            ), "One or more cloned PVCs are not thin provisioned"
            log.info("Verified: Cloned PVCs are not thick provisioned.")

        # Attach the restored and cloned PVCs to pods
        log.info("Attach the restored and cloned PVCs to pods")
        restore_clone_pod_objs = []
        for pvc_obj in restore_objs + clone_objs:
            interface = (
                constants.CEPHFILESYSTEM
                if (constants.CEPHFS_INTERFACE in pvc_obj.backed_sc)
                else constants.CEPHBLOCKPOOL
            )
            pod_obj = pod_factory(interface=interface, pvc=pvc_obj, status="")
            log.info(f"Attached the PVC {pvc_obj.name} to pod {pod_obj.name}")
            restore_clone_pod_objs.append(pod_obj)

        log.info("Verify pods are Running")
        for pod_obj in restore_clone_pod_objs:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
            )
            pod_obj.reload()
        log.info("Verified: Pods reached Running state")

        # Expand cloned and restored PVCs
        log.info(f"Expanding cloned and restored PVCs to {pvc_size_expand_3}Gi")
        for pvc_obj in clone_objs + restore_objs:
            log.info(
                f"Expanding size of PVC {pvc_obj.name} to "
                f"{pvc_size_expand_3}Gi from {pvc_obj.size}"
            )
            pvc_obj.resize_pvc(pvc_size_expand_3, True)
        log.info(
            f"Verified: Size of all cloned and restored PVCs are expanded to "
            f"{pvc_size_expand_3}G"
        )

        # Verify thick provision or not by checking the image used size
        if pvc_create_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=clone_objs,
                usage_to_compare=f"{pvc_size_expand_3}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), (
                "One or more cloned PVCs are not thick provisioned after expanding "
                f"to {pvc_size_expand_3}GiB"
            )
            log.info(
                f"Verified thick provision after expanding cloned PVCs to {pvc_size_expand_3}GiB"
            )
        elif pvc_create_sc_type == "thin" and (
            "thick" in (pvc_create_sc_type, restore_sc_type)
        ):
            assert check_rbd_image_used_size(
                pvc_objs=clone_objs,
                usage_to_compare=f"{pvc_size_expand_3}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=False,
            ), (
                "One or more cloned PVCs are not thin provisioned after expanding "
                f"to {pvc_size_expand_3}GiB"
            )
            log.info(
                "Verified: PVCs are not thick provisioned after expanding cloned "
                f"PVCs to {pvc_size_expand_3}GiB"
            )
        if restore_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=restore_objs,
                usage_to_compare=f"{pvc_size_expand_3}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), (
                "One or more restored PVCs are not thick provisioned after"
                f" expanding to {pvc_size_expand_3}GiB"
            )
            log.info(
                "Verified thick provision after expanding restored PVCs "
                f"to {pvc_size_expand_3}GiB"
            )
        elif restore_sc_type == "thin" and (
            "thick" in (pvc_create_sc_type, restore_sc_type)
        ):
            assert check_rbd_image_used_size(
                pvc_objs=restore_objs,
                usage_to_compare=f"{pvc_size_expand_3}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=False,
            ), (
                "One or more restored PVCs are not thin provisioned after "
                f"expanding to {pvc_size_expand_3}GiB"
            )
            log.info(
                "Verified: PVCs are not thick provisioned after expanding "
                f"restored PVCs to {pvc_size_expand_3}GiB"
            )

        # Run IO on pods attached with cloned and restored PVCs
        log.info("Starting IO on pods attached with cloned and restored PVCs")
        for pod_obj in restore_clone_pod_objs:
            log.info(f"Running IO on pod {pod_obj.name}")
            pod_obj.run_io(
                storage_type="fs",
                size="1G",
                runtime=20,
                fio_filename=filename_restore_clone,
            )
        log.info("IO started on all pods")

        log.info(
            "Waiting for IO completion on pods attached with cloned and "
            "restored PVCs"
        )
        for pod_obj in restore_clone_pod_objs:
            pod_obj.get_fio_results()
            log.info(f"IO finished on pod {pod_obj.name}")
            # Calculate md5sum of second file 'filename_restore_clone'
            md5sum = pod.cal_md5sum(pod_obj, filename_restore_clone)
            pod_obj.pvc.md5sum_new = md5sum
        log.info(
            f"IO completed on all pods. Obtained md5sum of file "
            f"{filename_restore_clone}"
        )

        # Verify md5sum of first file 'filename'
        log.info(f"Verify md5sum of file {filename} on pods")
        for pod_obj in restore_clone_pod_objs:
            pod.verify_data_integrity(pod_obj, filename, pod_obj.pvc.md5sum)
            log.info(
                f"Verified: md5sum of {filename} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )
        log.info(
            "Data integrity check passed on all pods where restored and "
            "cloned PVCs are attached"
        )

        # Clone the restored PVCs
        log.info("Creating clone of restored PVCs")
        restored_clone_objs = []
        for pvc_obj in restore_objs:
            log.info(f"Creating clone of restored PVC {pvc_obj.name}")
            clone_obj = pvc_clone_factory(
                pvc_obj=pvc_obj, status="", volume_mode=constants.VOLUME_MODE_FILESYSTEM
            )
            clone_obj.md5sum = pvc_obj.md5sum
            clone_obj.md5sum_new = pvc_obj.md5sum_new
            restored_clone_objs.append(clone_obj)
            log.info(f"Created clone of restored PVC {pvc_obj.name}")
        log.info("Created clone of restored all PVCs")

        log.info("Wait for cloned PVCs to reach Bound state and verify size")
        for pvc_obj in restored_clone_objs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            assert pvc_obj.size == pvc_size_expand_3, (
                f"Size is not {pvc_size_expand_3} but {pvc_obj.size} in "
                f"cloned PVC {pvc_obj.name}"
            )
        log.info(
            f"Cloned PVCs reached Bound state. Verified the size of all PVCs "
            f"as {pvc_size_expand_3}Gi"
        )

        # Verify PVCs cloned from restored PVCs are thick provisioned or not
        # by checking the image used size
        if restore_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=restored_clone_objs,
                usage_to_compare=f"{pvc_size_expand_3}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), "One or more PVCs cloned from restored PVCs are not thick provisioned"
            log.info(
                "Verified that the PVCs cloned from restored PVCs are thick provisioned"
            )
        if restore_sc_type == "thin" and (
            "thick" in (pvc_create_sc_type, restore_sc_type)
        ):
            assert check_rbd_image_used_size(
                pvc_objs=restored_clone_objs,
                usage_to_compare=f"{pvc_size_expand_3}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=False,
            ), "One or more PVCs cloned from restored PVCs are not thin provisioned"
            log.info(
                "Verified that the PVCs cloned from restored PVCs are not thick provisioned"
            )

        # Take snapshot of all cloned PVCs
        snapshots_new = []
        log.info("Creating snapshot of all cloned PVCs")
        for pvc_obj in clone_objs + restored_clone_objs:
            log.info(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snap_obj.md5sum_new = pvc_obj.md5sum_new
            snapshots_new.append(snap_obj)
            log.info(f"Created snapshot of PVC {pvc_obj.name}")
        log.info("Created snapshot of all cloned PVCs")

        # Verify snapshots are ready
        log.info("Verify snapshots of cloned PVCs are Ready")
        for snap_obj in snapshots_new:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snap_obj.reload()
        log.info("Verified: Snapshots of cloned PVCs are Ready")

        # Restore snapshots
        log.info("Restoring snapshots of cloned PVCs")
        restore_objs_new = []
        for snap_obj in snapshots_new:
            restore_obj = snapshot_restore_factory(
                snap_obj, status="", storageclass=self.restore_sc.name
            )
            restore_obj.md5sum = snap_obj.md5sum
            restore_obj.md5sum_new = snap_obj.md5sum_new
            restore_objs_new.append(restore_obj)

        log.info("Verify restored PVCs are Bound")
        for pvc_obj in restore_objs_new:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound.")

        # Verify thick provision or not by checking the image used size
        if restore_sc_type == "thick":
            assert check_rbd_image_used_size(
                pvc_objs=restore_objs_new,
                usage_to_compare=f"{pvc_size_expand_3}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=True,
            ), "One or more restored PVCs are not thick provisioned"
            log.info("Verified thick provision on PVCs restored from the snapshots.")
        if restore_sc_type == "thin" and (
            "thick" in (pvc_create_sc_type, restore_sc_type)
        ):
            assert check_rbd_image_used_size(
                pvc_objs=restore_objs_new,
                usage_to_compare=f"{pvc_size_expand_3}GiB",
                rbd_pool=constants.DEFAULT_BLOCKPOOL,
                expect_match=False,
            ), "One or more restored PVCs are not thin provisioned"
            log.info(
                "Verified that the PVCs restored from the snapshots are not thick provisioned."
            )

        # Delete pods to attach the cloned PVCs to new pods
        log.info("Delete pods")
        for pod_obj in restore_clone_pod_objs:
            pod_obj.delete()

        for pod_obj in restore_clone_pod_objs:
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        log.info("Pods are deleted")

        # Attach the restored and cloned PVCs to new pods
        log.info("Attach the restored and cloned PVCs to new pods")
        restore_clone_pod_objs.clear()
        for pvc_obj in restore_objs_new + clone_objs:
            interface = (
                constants.CEPHFILESYSTEM
                if (constants.CEPHFS_INTERFACE in pvc_obj.backed_sc)
                else constants.CEPHBLOCKPOOL
            )
            pod_obj = pod_factory(interface=interface, pvc=pvc_obj, status="")
            log.info(f"Attached the PVC {pvc_obj.name} to pod {pod_obj.name}")
            restore_clone_pod_objs.append(pod_obj)

        log.info("Verify pods are Running")
        for pod_obj in restore_clone_pod_objs:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
            )
            pod_obj.reload()
        log.info("Verified: Pods reached Running state")

        # Expand PVCs
        pvc_size_expand_4 = pvc_size_expand_3 + 2
        log.info(f"Expanding restored and cloned PVCs to {pvc_size_expand_4}Gi")
        for pvc_obj in restore_objs_new + clone_objs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expand_4}Gi")
            pvc_obj.resize_pvc(pvc_size_expand_4, True)
        log.info(f"Verified: Size of all PVCs are expanded to {pvc_size_expand_4}Gi")

        # Verify md5sum of both files
        log.info(f"Verify md5sum of files {filename} and {filename_restore_clone}")
        for pod_obj in restore_clone_pod_objs:
            pod.verify_data_integrity(pod_obj, filename, pod_obj.pvc.md5sum)
            log.info(
                f"Verified: md5sum of {filename} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )
            pod.verify_data_integrity(
                pod_obj, filename_restore_clone, pod_obj.pvc.md5sum_new
            )
            log.info(
                f"Verified: md5sum of {filename_restore_clone} on pod "
                f"{pod_obj.name} matches with the original md5sum"
            )
        log.info(
            "Data integrity check passed on all pods where restored and "
            "cloned PVCs are attached"
        )
