"""
Tests for CBT (Changed Block Tracking) metadata API on RBD PVCs.

Validates the GetMetadataAllocated and GetMetadataDelta gRPC
operations exposed by the snapshot-metadata sidecar running in
the RBD CSI controller pods.

RHSTOR-6440
"""

import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_ocs_version,
    tier1,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.cbt_metadata import (
    VerifierTool,
    ensure_sidecar_image,
    ensure_sms_cr,
)
from ocs_ci.helpers.snapshot_helpers import (
    restore_snapshot_to_block_pvc,
    write_data_to_pvc,
)

log = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_ocs_version("<4.23")
class TestRbdCBTMetadata(ManageTest):
    """
    Test CBT snapshot metadata operations on RBD PVCs.

    Each test creates its own PVCs and snapshots inside a dedicated
    project namespace. The CBT metadata runner sets up the RBAC,
    CA certificate, and tools pod required to call the gRPC API.
    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        project_factory,
        pvc_factory,
        pod_factory,
        snapshot_factory,
        teardown_factory,
    ):
        """
        Create a project, CBT runner, and store factories.

        Ensures the snapshot-metadata sidecar image and
        SnapshotMetadataService CR are in place
        (DFBUGS-9181). PVCs and pods created through
        factories are cleaned up by their respective
        factory finalizers.
        """
        ensure_sidecar_image()
        ensure_sms_cr()

        self.project = project_factory()
        self.namespace = self.project.namespace
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.snapshot_factory = snapshot_factory
        self.teardown_factory = teardown_factory

        self.cbt_runner = VerifierTool(self.namespace)
        self.cbt_runner.setup()

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """Clean up CBT runner."""

        def finalizer():
            self.cbt_runner.cleanup()

        request.addfinalizer(finalizer)

    # -- helpers used by multiple tests ----------------------------

    def _create_app_pvc(self, volume_mode, size=1):
        """
        Create the application PVC with the given mode.

        Args:
            volume_mode (str): Volume mode for the PVC
                (Block or Filesystem).
            size (int): PVC size in GiB.

        Returns:
            PVC: The created PVC object.
        """
        return self.pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project,
            size=size,
            volume_mode=volume_mode,
            status=constants.STATUS_BOUND,
        )

    def _create_block_pvc(self, size=1):
        """
        Create an empty Block-mode PVC (copy target).

        Args:
            size (int): PVC size in GiB.

        Returns:
            PVC: The created PVC object.
        """
        return self.pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project,
            size=size,
            volume_mode=constants.VOLUME_MODE_BLOCK,
            status=constants.STATUS_BOUND,
        )

    def _create_writer_pod(self, pvc_obj, volume_mode):
        """
        Create a pod that mounts the given PVC.

        Args:
            pvc_obj (PVC): PVC to mount.
            volume_mode (str): Volume mode of the PVC.

        Returns:
            Pod: The created pod object.
        """
        return self.pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            raw_block_pv=(volume_mode == constants.VOLUME_MODE_BLOCK),
            status=constants.STATUS_RUNNING,
        )

    def _take_snapshot(self, pvc_obj, name_prefix="cbt-snap"):
        """
        Take a VolumeSnapshot and wait for readyToUse.

        Args:
            pvc_obj (PVC): PVC to snapshot.
            name_prefix (str): Prefix for the snapshot name.

        Returns:
            OCS: The VolumeSnapshot object.
        """
        snap_name = helpers.create_unique_resource_name(name_prefix, "snapshot")
        snap_obj = self.snapshot_factory(
            pvc_obj,
            wait=True,
            snapshot_name=snap_name,
        )
        log.info(
            "Snapshot %s is readyToUse",
            snap_obj.name,
        )
        return snap_obj

    def _restore_and_verify(
        self,
        snap_obj,
        copy_pvc,
        volume_mode,
        previous_snapshot=None,
    ):
        """
        Restore a snapshot to a Block PVC, run the verifier, and
        assert exit code 0.

        Args:
            snap_obj (OCS): VolumeSnapshot to verify
            copy_pvc (PVC): Destination (copy) PVC
            volume_mode (str): Volume mode of the original PVC
            previous_snapshot (str): Base snapshot name for delta
                mode. Omit for allocated mode.

        Returns:
            PVC: The restored PVC object
        """
        restored_pvc = restore_snapshot_to_block_pvc(
            snap_obj=snap_obj,
            namespace=self.namespace,
            size=f"{copy_pvc.size}Gi",
            sc_name=copy_pvc.backed_sc,
            original_volume_mode=volume_mode,
        )
        self.teardown_factory(restored_pvc)

        exit_code, logs = self.cbt_runner.run_verifier(
            snapshot_name=snap_obj.name,
            source_pvc_name=restored_pvc.name,
            dest_pvc_name=copy_pvc.name,
            previous_snapshot=previous_snapshot,
        )
        log.info("Verifier logs:\n%s", logs)
        assert exit_code == 0, (
            f"Verifier exited with code {exit_code}. " f"Logs:\n{logs}"
        )
        return restored_pvc

    # -- Test 1 ----------------------------------------------------

    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
        ],
    )
    def test_cbt_allocated_empty_pvc(self, volume_mode):
        """
        Verify that an allocated copy of an empty PVC matches
        byte-for-byte.

        Creates an empty PVC (never mounted or written to), takes
        a snapshot, and verifies that the allocated block ranges
        can be correctly copied to a destination PVC.

        Steps:
        1. Create an empty application PVC (never written to)
           and a Block-mode copy PVC.
        2. Take a VolumeSnapshot of the empty PVC.
        3. Restore the snapshot, run the CBT verifier,
           and assert it exits with code 0.
        """
        log.test_step(
            "Create an empty %s application PVC and a " "Block-mode copy PVC",
            volume_mode,
        )
        app_pvc = self._create_app_pvc(volume_mode)
        copy_pvc = self._create_block_pvc()

        log.test_step("Take a VolumeSnapshot of the empty PVC")
        snap_obj = self._take_snapshot(app_pvc, "cbt-empty")

        log.test_step(
            "Restore the snapshot, run the CBT verifier, " "and assert exit code 0"
        )
        self._restore_and_verify(snap_obj, copy_pvc, volume_mode)

    # -- Test 2 ----------------------------------------------------

    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
        ],
    )
    def test_cbt_allocated_with_data(self, volume_mode):
        """
        Verify that an allocated copy of a PVC with data matches
        byte-for-byte.

        Writes 10 MiB to the PVC, takes a snapshot, runs the
        lister to confirm allocated blocks are returned, then
        runs the verifier to prove correctness.

        Steps:
        1. Create an application PVC and a Block-mode copy PVC.
        2. Create a writer pod and write 10 MiB of data.
        3. Take a VolumeSnapshot of the PVC.
        4. Run the CBT lister and verify allocated blocks are
           returned.
        5. Restore the snapshot, run the CBT verifier,
           and assert it exits with code 0.
        """
        log.test_step(
            "Create a %s application PVC and a Block-mode " "copy PVC",
            volume_mode,
        )
        app_pvc = self._create_app_pvc(volume_mode)
        copy_pvc = self._create_block_pvc()

        log.test_step("Create a writer pod and write 10 MiB")
        writer_pod = self._create_writer_pod(app_pvc, volume_mode)
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=10,
            filename="file1.bin",
        )

        log.test_step("Take a VolumeSnapshot of the PVC")
        snap_obj = self._take_snapshot(app_pvc, "cbt-data")

        log.test_step("Run the CBT lister and verify allocated blocks " "are returned")
        entries = self.cbt_runner.run_lister_allocated(
            snap_obj.name,
        )
        assert len(entries) > 0, (
            "Lister returned no allocated blocks for a PVC " "with 10 MiB of data"
        )
        log.info(
            "Lister returned %d allocated block(s)",
            len(entries),
        )

        log.test_step(
            "Restore the snapshot, run the CBT verifier, " "and assert exit code 0"
        )
        self._restore_and_verify(snap_obj, copy_pvc, volume_mode)

    # -- Test 3 ----------------------------------------------------

    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
        ],
    )
    def test_cbt_delta_incremental(self, volume_mode):
        """
        Verify that a delta copy applies only changed blocks
        correctly.

        Writes 10 MiB, takes snap-1, performs an allocated copy,
        then writes 5 MiB at a different location, takes snap-2,
        and verifies that the delta between the two snapshots
        is applied correctly to the copy PVC.

        Steps:
        1. Create an application PVC, a Block-mode copy PVC,
           and a writer pod.
        2. Write 10 MiB of data to the PVC.
        3. Take snap-1 and run an allocated-mode verification.
        4. Delete the restored PVC from the allocated copy.
        5. Write 5 MiB of additional data at offset 100 MiB.
        6. Take snap-2.
        7. Run the CBT lister in delta mode (snap-2 vs snap-1).
        8. Run the CBT verifier in delta mode and assert it
           exits with code 0.
        """
        log.test_step(
            "Create a %s application PVC, a Block-mode copy " "PVC, and a writer pod",
            volume_mode,
        )
        app_pvc = self._create_app_pvc(volume_mode)
        copy_pvc = self._create_block_pvc()
        writer_pod = self._create_writer_pod(app_pvc, volume_mode)

        # -- Phase 1: allocated copy of snap-1 ---------------------

        log.test_step("Write 10 MiB of data to the PVC")
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=10,
            filename="file1.bin",
        )

        log.test_step("Take snap-1 and run an allocated-mode " "verification")
        snap_1 = self._take_snapshot(app_pvc, "cbt-incr-1")
        restored_pvc_1 = self._restore_and_verify(
            snap_1,
            copy_pvc,
            volume_mode,
        )

        log.test_step("Delete the restored PVC from the allocated copy")
        restored_pvc_1.delete()
        restored_pvc_1.ocp.wait_for_delete(restored_pvc_1.name, timeout=120)

        # -- Phase 2: delta copy of snap-2 -------------------------

        log.test_step("Write 5 MiB of additional data at offset 100 MiB")
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=5,
            filename="file2.bin",
            offset_mb=100,
        )

        log.test_step("Take snap-2")
        snap_2 = self._take_snapshot(app_pvc, "cbt-incr-2")

        log.test_step("Run the CBT lister in delta mode " "(snap-2 vs snap-1)")
        entries = self.cbt_runner.run_lister_delta(
            target_snap=snap_2.name,
            base_snap=snap_1.name,
        )
        log.info(
            "Lister delta returned %d changed block(s)",
            len(entries),
        )

        log.test_step("Run the CBT verifier in delta mode and " "assert exit code 0")
        self._restore_and_verify(
            snap_2,
            copy_pvc,
            volume_mode,
            previous_snapshot=snap_1.name,
        )
