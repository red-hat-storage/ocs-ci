import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.pytest_customization.marks import skipif_hci_provider_and_client
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    acceptance,
    skipif_ocp_version,
    config,
)
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_ocs_version("<4.9")
@skipif_ocp_version("<4.9")
class TestClone(ManageTest):
    """
    Tests to verify PVC to PVC clone feature
    """

    @pytest.fixture()
    def setup(self, interface_type, pvc_factory, pod_factory, pod_dict_path, access):
        """
        create resources for the test

        Args:
            interface_type(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod

        """
        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=1,
            status=constants.STATUS_BOUND,
            access_mode=access,
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=pod_dict_path,
        )

    @acceptance
    @pytest.mark.parametrize(
        argnames=["interface_type", "pod_dict_path", "access"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL,
                None,
                constants.ACCESS_MODE_RWO,
                marks=pytest.mark.polarion_id("OCS-2284"),
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                None,
                constants.ACCESS_MODE_RWO,
                marks=pytest.mark.polarion_id("OCS-256"),
            ),
        ],
    )
    def test_pvc_to_pvc_clone(self, interface_type, setup, teardown_factory):
        """
        Create a clone from an existing pvc,
        verify data is preserved in the cloning.
        """
        logger.info(f"Running IO on pod {self.pod_obj.name}")
        file_name = self.pod_obj.name
        logger.info(f"File created during IO {file_name}")
        self.pod_obj.run_io(storage_type="fs", size="500M", fio_filename=file_name)

        # Wait for fio to finish
        self.pod_obj.get_fio_results()
        logger.info(f"Io completed on pod {self.pod_obj.name}.")

        # Verify presence of the file
        file_path = pod.get_file_path(self.pod_obj, file_name)
        logger.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            self.pod_obj, file_path
        ), f"File {file_name} does not exist"
        logger.info(f"File {file_name} exists in {self.pod_obj.name}")

        # Calculate md5sum of the file.
        orig_md5_sum = pod.cal_md5sum(self.pod_obj, file_name)

        # Create a clone of the existing pvc.
        sc_name = self.pvc_obj.backed_sc
        parent_pvc = self.pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        namespace = self.pvc_obj.namespace
        if interface_type == constants.CEPHFILESYSTEM:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
        cloned_pvc_obj = pvc.create_pvc_clone(
            sc_name, parent_pvc, clone_yaml, namespace
        )
        teardown_factory(cloned_pvc_obj)
        helpers.wait_for_resource_state(
            cloned_pvc_obj, constants.STATUS_BOUND, timeout=300
        )
        cloned_pvc_obj.reload()

        # Create and attach pod to the pvc
        clone_pod_obj = helpers.create_pod(
            interface_type=interface_type,
            pvc_name=cloned_pvc_obj.name,
            namespace=cloned_pvc_obj.namespace,
            pod_dict_path=constants.NGINX_POD_YAML,
        )
        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=clone_pod_obj, state=constants.STATUS_RUNNING
        )
        clone_pod_obj.reload()
        teardown_factory(clone_pod_obj)

        # Verify file's presence on the new pod
        logger.info(
            f"Checking the existence of {file_name} on cloned pod "
            f"{clone_pod_obj.name}"
        )
        assert pod.check_file_existence(
            clone_pod_obj, file_path
        ), f"File {file_path} does not exist"
        logger.info(f"File {file_name} exists in {clone_pod_obj.name}")

        # Verify Contents of a file in the cloned pvc
        # by validating if md5sum matches.
        logger.info(
            f"Verifying that md5sum of {file_name} "
            f"on pod {self.pod_obj.name} matches with md5sum "
            f"of the same file on restore pod {clone_pod_obj.name}"
        )
        assert pod.verify_data_integrity(
            clone_pod_obj, file_name, orig_md5_sum
        ), "Data integrity check failed"
        logger.info("Data integrity check passed, md5sum are same")

        logger.info("Run IO on new pod")
        clone_pod_obj.run_io(storage_type="fs", size="100M", runtime=10)

        # Wait for IO to finish on the new pod
        clone_pod_obj.get_fio_results()
        logger.info(f"IO completed on pod {clone_pod_obj.name}")

    @acceptance
    @pytest.mark.polarion_id("OCS-5162")
    @pytest.mark.parametrize(
        argnames=["interface_type", "access"],
        argvalues=[
            pytest.param(
                constants.CEPHFILESYSTEM,
                constants.ACCESS_MODE_RWX,
            ),
        ],
    )
    def test_pvc_to_pvc_rox_clone(
        self,
        interface_type,
        access,
        pvc_factory,
        pod_factory,
        snapshot_factory,
        snapshot_restore_factory,
        teardown_factory,
    ):
        """
        Create a rox clone from an existing pvc,
        verify data is preserved in the cloning.
        """
        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=1,
            status=constants.STATUS_BOUND,
            access_mode=access,
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=constants.CSI_CEPHFS_ROX_POD_YAML,
        )
        logger.info(f"Running IO on pod {self.pod_obj.name}")
        file_name = f"{self.pod_obj.name}.txt"
        self.pod_obj.exec_cmd_on_pod(
            command=f"dd if=/dev/zero of=/mnt/{file_name} bs=1M count=1"
        )

        logger.info(f"File Created. /mnt/{file_name}")

        # Verify presence of the file
        file_path = pod.get_file_path(self.pod_obj, file_name)
        logger.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            self.pod_obj, file_path
        ), f"File {file_name} does not exist"
        logger.info(f"File {file_name} exists in {self.pod_obj.name}")

        # Taking snapshot of pvc
        logger.info("Taking Snapshot of the PVC")
        snapshot_obj = snapshot_factory(self.pvc_obj, wait=False)
        logger.info("Verify snapshots moved from false state to true state")
        teardown_factory(snapshot_obj)

        # Restoring pvc snapshot to pvc
        logger.info(f"Creating a PVC from snapshot [restore] {snapshot_obj.name}")
        restore_snapshot_obj = snapshot_restore_factory(
            snapshot_obj=snapshot_obj,
            size="1Gi",
            volume_mode=snapshot_obj.parent_volume_mode,
            access_mode=constants.ACCESS_MODE_ROX,
            status=constants.STATUS_BOUND,
            timeout=300,
        )
        teardown_factory(restore_snapshot_obj)

        # Create and attach pod to the pvc
        clone_pod_obj = helpers.create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=restore_snapshot_obj.name,
            namespace=restore_snapshot_obj.namespace,
            pod_dict_path=constants.CSI_CEPHFS_ROX_POD_YAML,
            pvc_read_only_mode=True,
        )
        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=clone_pod_obj, state=constants.STATUS_RUNNING
        )
        clone_pod_obj.reload()
        teardown_factory(clone_pod_obj)

        # Verify file's presence on the new pod
        logger.info(
            f"Checking the existence of {file_name} on cloned pod "
            f"{clone_pod_obj.name}"
        )
        assert pod.check_file_existence(
            clone_pod_obj, file_path
        ), f"File {file_path} does not exist"
        logger.info(f"File {file_name} exists in {clone_pod_obj.name}")

    @skipif_hci_provider_and_client
    @skipif_ocs_version("<4.15")
    @pytest.mark.polarion_id("OCS-5444")
    @pytest.mark.polarion_id("OCS-5446")
    @pytest.mark.parametrize(
        argnames=["interface_type", "access"],
        argvalues=[
            pytest.param(
                constants.CEPHFILESYSTEM,
                constants.ACCESS_MODE_RWX,
            ),
        ],
    )
    def test_pvc_to_pvc_rox_shallow_vol_clone(
        self,
        interface_type,
        access,
        pvc_factory,
        pod_factory,
        snapshot_factory,
        snapshot_restore_factory,
        teardown_factory,
    ):
        """
        1. Create a PVC with rwx mode
        2. Adds the PVC to pod
        3. Writes data on the mount point
        4. Check subvolumes in backend
            ceph fs subvolume ls ocs-storagecluster-cephfilesystem --group_name csi
        5. Takes snapshot of PVC
        6. Restores the snapshot in rox mode
        7. Check subvolumes in backend: count should be same
        8. Attach the restore to pod
        9. Checks if data matches
        10. Check if no new subvolumes are created.
        """
        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=1,
            status=constants.STATUS_BOUND,
            access_mode=access,
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=constants.CSI_CEPHFS_ROX_POD_YAML,
        )
        logger.info(f"Running IO on pod {self.pod_obj.name}")
        file_name = f"{self.pod_obj.name}.txt"
        self.pod_obj.exec_cmd_on_pod(
            command=f"dd if=/dev/zero of=/mnt/{file_name} bs=1M count=1"
        )

        logger.info(f"File Created. /mnt/{file_name}")

        # Verify presence of the file
        file_path = pod.get_file_path(self.pod_obj, file_name)
        logger.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            self.pod_obj, file_path
        ), f"File {file_name} does not exist"
        logger.info(f"File {file_name} exists in {self.pod_obj.name}")

        cephfs_name = config.ENV_DATA.get("cephfs_name") or helpers.get_cephfs_name()

        # Checking out subvolumes before taking snapshot
        logger.info("Checking subvolumes before snapshots.")
        toolbox = pod.get_ceph_tools_pod()
        subvolumes_before_snapshot = toolbox.exec_ceph_cmd(
            f"ceph fs subvolume ls {cephfs_name} --group_name csi"
        )
        logger.info(f"Subvolumes before snapshots are:\n{subvolumes_before_snapshot}")

        # Taking snapshot of pvc
        logger.info("Taking Snapshot of the PVC")
        snapshot_obj = snapshot_factory(self.pvc_obj, wait=False)
        logger.info("Verify snapshots moved from false state to true state")
        teardown_factory(snapshot_obj)

        # Restoring pvc snapshot to pvc
        logger.info(f"Creating a PVC from snapshot [restore] {snapshot_obj.name}")
        restore_snapshot_obj = snapshot_restore_factory(
            snapshot_obj=snapshot_obj,
            size="1Gi",
            volume_mode=snapshot_obj.parent_volume_mode,
            access_mode=constants.ACCESS_MODE_ROX,
            status=constants.STATUS_BOUND,
            timeout=300,
        )
        teardown_factory(restore_snapshot_obj)

        # Checking out subvolumes after restore of snapshot
        logger.info("Checking subvolumes before snapshots.")
        toolbox = pod.get_ceph_tools_pod()
        subvolumes_after_snapshot = toolbox.exec_ceph_cmd(
            f"ceph fs subvolume ls {cephfs_name} --group_name csi"
        )
        logger.info(f"Subvolumes before snapshots are:\n{subvolumes_after_snapshot}")
        assert (
            subvolumes_before_snapshot == subvolumes_after_snapshot
        ), "The subvolumes before and after snapshot doesnt match, thus there must be new subvolumes"

        # Create and attach pod to the pvc
        snapshot_restore_pod_obj = helpers.create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=restore_snapshot_obj.name,
            namespace=restore_snapshot_obj.namespace,
            pod_dict_path=constants.CSI_CEPHFS_ROX_POD_YAML,
            pvc_read_only_mode=True,
        )
        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=snapshot_restore_pod_obj, state=constants.STATUS_RUNNING
        )
        snapshot_restore_pod_obj.reload()
        teardown_factory(snapshot_restore_pod_obj)

        # Verify file's presence on the new pod
        logger.info(
            f"Checking the existence of {file_name} on cloned pod "
            f"{snapshot_restore_pod_obj.name}"
        )
        assert pod.check_file_existence(
            snapshot_restore_pod_obj, file_path
        ), f"File {file_path} does not exist"
        logger.info(f"File {file_name} exists in {snapshot_restore_pod_obj.name}")

    @skipif_hci_provider_and_client
    @skipif_ocs_version("<4.15")
    @pytest.mark.polarion_id("OCS-5445")
    @pytest.mark.polarion_id("OCS-5447")
    @pytest.mark.parametrize(
        argnames=["interface_type", "access"],
        argvalues=[
            pytest.param(
                constants.CEPHFILESYSTEM,
                constants.ACCESS_MODE_RWX,
            ),
        ],
    )
    def test_pvc_to_pvc_rox_shallow_vol_post_clone(
        self,
        interface_type,
        access,
        pvc_factory,
        pod_factory,
        snapshot_factory,
        snapshot_restore_factory,
        pvc_clone_factory,
        teardown_factory,
    ):
        """
        1. Create a PVC with rwx mode
        2. Adds the PVC to pod
        3. Writes data on the mount point
        4. Check subvolumes in backend
        5. Takes snapshot of PVC
        6. Restores the snapshot in rox mode
        7. Check subvolumes in backend: it should not increase, should be same
        8. Attach the restore to pod
        9. Checks if data matches
        10. create a RWX PVC-PVC clone of the ROX PVC
        11. Check subvolumes in backend: count should be one more
        12. Attach the new clone to pod
        13. Check if data matches
        14. Creating Snapshot of ROX PVC should be blocked
        15. Creating ROX PVC-PVC clone of the ROX PVC should be blocked as well
        16. Delete the parent snapshot and pvc(rox pvc)
        17. Check if data matches in rwx (already cloned pvc) pvc
        """
        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=1,
            status=constants.STATUS_BOUND,
            access_mode=access,
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=constants.CSI_CEPHFS_ROX_POD_YAML,
        )
        logger.info(f"Running IO on pod {self.pod_obj.name}")
        file_name = f"{self.pod_obj.name}.txt"
        self.pod_obj.exec_cmd_on_pod(
            command=f"dd if=/dev/zero of=/mnt/{file_name} bs=1M count=1"
        )

        logger.info(f"File Created. /mnt/{file_name}")

        # Verify presence of the file
        file_path = pod.get_file_path(self.pod_obj, file_name)
        logger.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            self.pod_obj, file_path
        ), f"File {file_name} does not exist"
        logger.info(f"File {file_name} exists in {self.pod_obj.name}")

        cephfs_name = config.ENV_DATA.get("cephfs_name") or helpers.get_cephfs_name()

        # Checking out subvolumes before taking snapshot
        logger.info("Checking subvolumes before snapshots.")
        toolbox = pod.get_ceph_tools_pod()
        subvolumes_before_snapshot = toolbox.exec_ceph_cmd(
            f"ceph fs subvolume ls {cephfs_name} --group_name csi"
        )
        logger.info(f"Subvolumes before snapshots are:\n{subvolumes_before_snapshot}")

        # Taking snapshot of pvc
        logger.info("Taking Snapshot of the PVC")
        parent_pvc_snapshot_obj = snapshot_factory(
            self.pvc_obj, wait=True, snapshot_name="snapshot-of-first-rwx-pvc-00"
        )
        logger.info("Verified snapshots moved from false state to true state")
        teardown_factory(parent_pvc_snapshot_obj)

        # Restoring pvc snapshot to rox pvc
        logger.info(
            f"Creating a PVC from snapshot [restore] {parent_pvc_snapshot_obj.name}"
        )
        restore_snapshot_obj = snapshot_restore_factory(
            snapshot_obj=parent_pvc_snapshot_obj,
            size="1Gi",
            volume_mode=parent_pvc_snapshot_obj.parent_volume_mode,
            access_mode=constants.ACCESS_MODE_ROX,
            status=constants.STATUS_BOUND,
            restore_pvc_name="first-rwx-snapshot-restore-to-rox-mode-00",
            timeout=300,
        )
        teardown_factory(restore_snapshot_obj)

        # Checking out subvolumes after restore of snapshot
        logger.info("Checking subvolumes after snapshots.")
        toolbox = pod.get_ceph_tools_pod()
        subvolumes_after_snapshot = toolbox.exec_ceph_cmd(
            f"ceph fs subvolume ls {cephfs_name} --group_name csi"
        )
        logger.info(f"Subvolumes after snapshots are:\n{subvolumes_after_snapshot}")
        assert (
            subvolumes_before_snapshot == subvolumes_after_snapshot
        ), "The subvolumes before and after snapshot doesnt match, thus there must be new subvolumes"

        # Create and attach pod to the pvc
        snapshot_restore_pod_obj = helpers.create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=restore_snapshot_obj.name,
            namespace=restore_snapshot_obj.namespace,
            pod_dict_path=constants.CSI_CEPHFS_ROX_POD_YAML,
            pvc_read_only_mode=True,
            pod_name="rox-pvc-pod-00",
        )
        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=snapshot_restore_pod_obj, state=constants.STATUS_RUNNING
        )
        snapshot_restore_pod_obj.reload()
        teardown_factory(snapshot_restore_pod_obj)

        # Verify file's presence on the new pod
        logger.info(
            f"Checking the existence of {file_name} on cloned pod "
            f"{snapshot_restore_pod_obj.name}"
        )
        assert pod.check_file_existence(
            snapshot_restore_pod_obj, file_path
        ), f"File {file_path} does not exist"
        logger.info(f"File {file_name} exists in {snapshot_restore_pod_obj.name}")

        # Creating RWX PVC-PVC clone of ROX PVC (restored snapshot of parent PVC)
        logger.info("Start creating clone of PVC")
        logger.info(f"Creating RWX PVC-PVC clone of PVC {restore_snapshot_obj.name}")
        rwx_pvc_clone_of_restored_snapshot_obj = pvc_clone_factory(
            pvc_obj=restore_snapshot_obj,
            status=constants.STATUS_BOUND,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        logger.info(
            f"RWX PVC-PVC clone created: {rwx_pvc_clone_of_restored_snapshot_obj.name}"
        )
        teardown_factory(rwx_pvc_clone_of_restored_snapshot_obj)

        # Checking out subvolumes after rwx pvc-pvc clone of restored rox snapshot
        logger.info("Checking subvolumes after PVC clone.")
        toolbox = pod.get_ceph_tools_pod()
        subvolumes_after_pvc_clone = toolbox.exec_ceph_cmd(
            f"ceph fs subvolume ls {cephfs_name} --group_name csi"
        )
        logger.info(f"Subvolumes after PVC clone are:\n{subvolumes_after_pvc_clone}")
        assert (
            len(subvolumes_after_pvc_clone) == len(subvolumes_after_snapshot) + 1
        ), "There should be one more subvolume after the pvc pvc clone"

        # Create and attach pod to the rwx pvc
        clone_pod_obj = helpers.create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=rwx_pvc_clone_of_restored_snapshot_obj.name,
            namespace=rwx_pvc_clone_of_restored_snapshot_obj.namespace,
            pod_dict_path=constants.CSI_CEPHFS_ROX_POD_YAML,
            pvc_read_only_mode=False,
        )
        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=clone_pod_obj, state=constants.STATUS_RUNNING
        )
        clone_pod_obj.reload()
        teardown_factory(clone_pod_obj)

        # Verify file's presence on the new pod
        logger.info(
            f"Checking the existence of {file_name} on cloned pod "
            f"{clone_pod_obj.name}"
        )
        assert pod.check_file_existence(
            clone_pod_obj, file_path
        ), f"File {file_path} does not exist"
        logger.info(f"File {file_name} exists in {clone_pod_obj.name}")

        # Taking snapshot of rox pvc
        logger.info("Taking Snapshot of the cloned RWX PVC")
        # we shouldnt be able to create the snapshot
        test_pvc_snapshot_obj = snapshot_factory(
            restore_snapshot_obj, wait=False, snapshot_name="snapshot-bound-to-fail-00"
        )
        test_pvc_snapshot_obj_status = test_pvc_snapshot_obj.ocp.get_resource_status(
            test_pvc_snapshot_obj.name, "READYTOUSE"
        )
        logger.info("Snapshot creation failed.")
        assert (
            test_pvc_snapshot_obj_status == "false"
        ), f"Snapshot {test_pvc_snapshot_obj.name} is created"
        teardown_factory(test_pvc_snapshot_obj)

        # Taking rox pvc-pvc clone of rox pvc
        logger.info("Taking rox pvc clone of the rox pvc")
        test_rox_pvc_clone_obj = pvc_clone_factory(
            pvc_obj=restore_snapshot_obj,
            status=constants.STATUS_PENDING,
            access_mode=constants.ACCESS_MODE_ROX,
        )
        logger.info(f"{test_rox_pvc_clone_obj}")
        teardown_factory(test_rox_pvc_clone_obj)

        # deleting parent rox pvc snapshot, failed snapshots and failed pvc
        parent_pvc_snapshot_obj.delete()
        parent_pvc_snapshot_obj.ocp.wait_for_delete(
            resource_name=parent_pvc_snapshot_obj.name, timeout=300
        ), f"PVC {parent_pvc_snapshot_obj.name} is not deleted"
        test_pvc_snapshot_obj.delete()
        test_pvc_snapshot_obj.ocp.wait_for_delete(
            resource_name=test_pvc_snapshot_obj.name, timeout=300
        ), f"PVC {test_pvc_snapshot_obj.name} is not deleted"
        test_rox_pvc_clone_obj.delete()
        test_rox_pvc_clone_obj.ocp.wait_for_delete(
            resource_name=test_rox_pvc_clone_obj.name, timeout=300
        ), f"PVC {test_rox_pvc_clone_obj.name} is not deleted"

        # deleting the parent rox pvc pod
        snapshot_restore_pod_obj.delete(wait=True)
        snapshot_restore_pod_obj.ocp.wait_for_delete(
            resource_name=snapshot_restore_pod_obj.name, timeout=900
        ), f"Pod {snapshot_restore_pod_obj.name} is not deleted"

        # deleting the parent rox pvc
        restore_snapshot_obj.delete(wait=True)
        restore_snapshot_obj.ocp.wait_for_delete(
            resource_name=restore_snapshot_obj.name, timeout=900
        ), f"Snapshot {restore_snapshot_obj.name} is not deleted"

        # Verify file's presence on the new pod post deletion of parent pvc and snapshot
        logger.info(
            f"Checking the existence of {file_name} on cloned pod "
            f"{clone_pod_obj.name}"
        )
        assert pod.check_file_existence(
            clone_pod_obj, file_path
        ), f"File {file_path} does not exist"
        logger.info(f"File {file_name} exists in {clone_pod_obj.name}")
