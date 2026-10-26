"""
Tests for CBT (Changed Block Tracking) metadata API on RBD PVCs.

Validates the GetMetadataAllocated and GetMetadataDelta gRPC
operations exposed by the snapshot-metadata sidecar running in
the RBD CSI controller pods.

These tests require the SnapshotMetadataService CR and its
connection ConfigMap to be present in openshift-storage. That is
automatic on ODF 5.0 and later.

RHSTOR-6440
"""

import logging
import re

import pytest
from cryptography import x509
from cryptography.hazmat.backends import default_backend

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.pytest_customization.marks import consumer_env_check, green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    polarion_id,
    skipif_ocs_version,
    tier1,
    tier2,
    tier3,
    tier4b,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.cbt_metadata import (
    VerifierTool,
    generate_untrusted_ca_cert,
    get_merged_lister_entries_byte_ranges,
    get_total_covered_bytes,
    get_uncovered_byte_ranges,
    validate_snapshot_metadata_sidecar,
    verify_lister_authentication_error_markers,
    verify_lister_tls_error_markers,
    wait_for_cbt_service_endpoints,
    wait_for_ready_cbt_sidecar_pods,
)
from ocs_ci.ocs.resources.pod import (
    get_pod_node,
    get_pod_obj,
    get_rbdfsplugin_provisioner_pods,
)
from ocs_ci.ocs.resources.snapshots import (
    get_snapshot_handle,
    restore_snapshot_to_block_pvc,
    write_data_to_pvc,
)

log = logging.getLogger(__name__)


class CBTMetadataTestBase(ManageTest):
    """
    Shared setup and helpers for the CBT metadata test classes.

    Each test creates its own PVCs and snapshots inside a dedicated
    project namespace. The CBT metadata runner sets up the RBAC,
    CA certificate, and tools pod required to call the gRPC API.

    The name does not start with Test, so pytest does not collect
    this class.
    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        request,
        project_factory,
        pvc_factory,
        pod_factory,
        snapshot_factory,
        teardown_factory,
    ):
        """
        Create a project, CBT runner, and store factories.

        PVCs and pods created through factories are cleaned
        up by their respective factory finalizers.

        Note: in multicluster mode the consumer context is intentionally NOT
        restored to provider on teardown so that the pvc_factory finalizer
        runs on the correct (consumer) cluster. In standalone mode there is
        only one cluster so no context switching is needed.
        """
        if config.multicluster:
            config.switch_to_consumer()

        def finalizer():
            if hasattr(self, "cbt_runner"):
                self.cbt_runner.cleanup()

        request.addfinalizer(finalizer)

        self.project = project_factory()
        self.namespace = self.project.namespace
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.snapshot_factory = snapshot_factory
        self.teardown_factory = teardown_factory

        self.cbt_runner = VerifierTool(self.namespace)
        self.cbt_runner.setup()

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
        previous_snapshot_id=None,
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
            previous_snapshot_id (str): Base CSI snapshot handle
                for delta mode, used instead of previous_snapshot

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
            previous_snapshot_id=previous_snapshot_id,
        )
        log.info("Verifier logs:\n%s", logs)
        assert exit_code == 0, f"Verifier exited with code {exit_code}. Logs:\n{logs}"

        # Verify that the verifier actually ran (not just built successfully)
        assert "snapshot-metadata-verifier" in logs and (
            "=== Verifier build OK ===" in logs
        ), "Verifier binary did not build or execute. Check logs for build/runtime errors."

        return restored_pvc

    def _delete_verifier_pod_and_restored_pvc(self, restored_pvc):
        """
        Delete the verifier pod and the restored PVC of a finished
        copy step.

        The copy PVC accumulates state across steps and is kept,
        but it is a RWO Block PVC, so the verifier pod of the
        finished step has to be gone and its volumes detached
        before the next step attaches the copy PVC again. The
        restored PVC is a throwaway full-size clone of the
        snapshot, so it is dropped here instead of being held
        until the teardown_factory finalizer runs.

        Args:
            restored_pvc (PVC): Restored PVC of the finished step
        """
        if self.cbt_runner.verifier_pod_name:
            self.cbt_runner.delete_pod(self.cbt_runner.verifier_pod_name)
        restored_pvc.delete()
        restored_pvc.ocp.wait_for_delete(restored_pvc.name, timeout=120)


@green_squad
@consumer_env_check
@skipif_ocs_version("<4.23")
class TestRbdCBTMetadata(CBTMetadataTestBase):
    """
    Test CBT snapshot metadata operations on RBD PVCs.

    Covers the happy paths of the GetMetadataAllocated and
    GetMetadataDelta gRPC operations, and the resiliency of the
    metadata service to the loss of a controller node.
    """

    # -- Test 1 ----------------------------------------------------

    @tier1
    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-8231"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-8232"),
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
            "Create an empty %s application PVC and a Block-mode copy PVC",
            volume_mode,
        )
        app_pvc = self._create_app_pvc(volume_mode)
        copy_pvc = self._create_block_pvc()

        log.test_step("Take a VolumeSnapshot of the empty PVC")
        snap_obj = self._take_snapshot(app_pvc, "cbt-empty")

        log.test_step(
            "Restore the snapshot, run the CBT verifier, and assert exit code 0"
        )
        self._restore_and_verify(snap_obj, copy_pvc, volume_mode)

    # -- Test 2 ----------------------------------------------------

    @tier1
    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-8233"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-8234"),
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
            "Create a %s application PVC and a Block-mode copy PVC",
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

        log.test_step("Run the CBT lister and verify allocated blocks are returned")
        entries = self.cbt_runner.run_lister_allocated(
            snap_obj.name,
        )
        assert (
            len(entries) > 0
        ), "Lister returned no allocated blocks for a PVC with 10 MiB of data"
        log.info(
            "Lister returned %d allocated block(s)",
            len(entries),
        )

        log.test_step(
            "Restore the snapshot, run the CBT verifier, and assert exit code 0"
        )
        self._restore_and_verify(snap_obj, copy_pvc, volume_mode)

    # -- Test 3 ----------------------------------------------------

    @tier1
    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-8235"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-8236"),
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
        4. Delete the verifier pod and the restored PVC of the
           allocated copy.
        5. Write 5 MiB of additional data (at offset 100 MiB in
           Block mode).
        6. Take snap-2.
        7. Run the CBT lister in delta mode (snap-2 vs snap-1).
        8. Run the CBT verifier in delta mode and assert it
           exits with code 0.
        """
        log.test_step(
            "Create a %s application PVC, a Block-mode copy PVC, and a writer pod",
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

        log.test_step("Take snap-1 and run an allocated-mode verification")
        snap_1 = self._take_snapshot(app_pvc, "cbt-incr-1")
        restored_pvc_1 = self._restore_and_verify(
            snap_1,
            copy_pvc,
            volume_mode,
        )

        log.test_step(
            "Delete the verifier pod and the restored PVC of the allocated copy"
        )
        self._delete_verifier_pod_and_restored_pvc(restored_pvc_1)

        # -- Phase 2: delta copy of snap-2 -------------------------

        log.test_step(
            "Write 5 MiB of additional data (at offset 100 MiB in Block mode)"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=5,
            filename="file2.bin",
            offset_mb=100,
        )

        log.test_step("Take snap-2")
        snap_2 = self._take_snapshot(app_pvc, "cbt-incr-2")

        log.test_step("Run the CBT lister in delta mode (snap-2 vs snap-1)")
        entries = self.cbt_runner.run_lister_delta(
            target_snap=snap_2.name,
            base_snap_name=snap_1.name,
        )
        log.info(
            "Lister delta returned %d changed block(s)",
            len(entries),
        )
        assert len(entries) > 0, "Delta lister returned no changed blocks"

        log.test_step("Run the CBT verifier in delta mode and assert exit code 0")
        self._restore_and_verify(
            snap_2,
            copy_pvc,
            volume_mode,
            previous_snapshot=snap_1.name,
        )

    # -- Test 4 ----------------------------------------------------

    @tier2
    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-8327"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-8328"),
            ),
        ],
    )
    def test_cbt_delta_skipping_intermediate_snapshot(self, volume_mode):
        """
        Verify that a delta works when an intermediate snapshot is
        skipped.

        Takes three snapshots with a write before each one, then
        computes the delta from snap-1 straight to snap-3, leaving
        snap-2 out. The delta must cover the changes of both the
        second and the third write, so it has to be a superset of
        the snap-1 to snap-2 delta.

        Steps:
        1. Create an application PVC, a Block-mode copy PVC,
           and a writer pod.
        2. Write data A (10 MiB), take snap-1 and run an
           allocated-mode verification.
        3. Write data B (5 MiB, at offset 100 MiB in Block mode)
           and take snap-2.
        4. Write data C (5 MiB, at offset 200 MiB in Block mode)
           and take snap-3.
        5. Run the CBT lister in delta mode for snap-2 vs snap-1
           and for snap-3 vs snap-1, and verify the skipping delta
           covers the intermediate one.
        6. Run the CBT verifier in delta mode for snap-3 vs snap-1
           and assert it exits with code 0.
        """
        log.test_step(
            "Create a %s application PVC, a Block-mode copy PVC, and a writer pod",
            volume_mode,
        )
        app_pvc = self._create_app_pvc(volume_mode)
        copy_pvc = self._create_block_pvc()
        writer_pod = self._create_writer_pod(app_pvc, volume_mode)

        log.test_step(
            "Write data A (10 MiB), take snap-1 and run an allocated-mode verification"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=10,
            filename="file1.bin",
        )
        snap_1 = self._take_snapshot(app_pvc, "cbt-skip-1")
        restored_pvc_1 = self._restore_and_verify(snap_1, copy_pvc, volume_mode)
        self._delete_verifier_pod_and_restored_pvc(restored_pvc_1)

        log.test_step(
            "Write data B (5 MiB, at offset 100 MiB in Block mode) and take snap-2"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=5,
            filename="file2.bin",
            offset_mb=100,
        )
        snap_2 = self._take_snapshot(app_pvc, "cbt-skip-2")

        log.test_step(
            "Write data C (5 MiB, at offset 200 MiB in Block mode) and take snap-3"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=5,
            filename="file3.bin",
            offset_mb=200,
        )
        snap_3 = self._take_snapshot(app_pvc, "cbt-skip-3")

        log.test_step(
            "Run the CBT lister in delta mode for snap-2 vs snap-1 and for "
            "snap-3 vs snap-1, and verify the skipping delta covers the "
            "intermediate one"
        )
        entries_1_to_2 = self.cbt_runner.run_lister_delta(
            target_snap=snap_2.name,
            base_snap_name=snap_1.name,
        )
        entries_1_to_3 = self.cbt_runner.run_lister_delta(
            target_snap=snap_3.name,
            base_snap_name=snap_1.name,
        )
        log.info(
            "Delta snap-1 to snap-2 returned %d block(s), "
            "delta snap-1 to snap-3 returned %d block(s)",
            len(entries_1_to_2),
            len(entries_1_to_3),
        )
        assert len(entries_1_to_2) > 0, "Delta lister returned no blocks for write B"
        assert len(entries_1_to_3) > 0, "Delta lister returned no blocks for writes B+C"

        coverage_1_to_2 = get_merged_lister_entries_byte_ranges(entries_1_to_2)
        coverage_1_to_3 = get_merged_lister_entries_byte_ranges(entries_1_to_3)
        uncovered = get_uncovered_byte_ranges(coverage_1_to_2, coverage_1_to_3)
        assert not uncovered, (
            f"Delta snap-1 to snap-3 does not cover the snap-1 to snap-2 "
            f"delta. Uncovered byte ranges: {uncovered}"
        )

        bytes_1_to_2 = get_total_covered_bytes(coverage_1_to_2)
        bytes_1_to_3 = get_total_covered_bytes(coverage_1_to_3)
        log.info(
            "Delta snap-1 to snap-2 covers %d byte(s), "
            "delta snap-1 to snap-3 covers %d byte(s)",
            bytes_1_to_2,
            bytes_1_to_3,
        )
        assert bytes_1_to_3 > bytes_1_to_2, (
            f"Delta snap-1 to snap-3 covers the same {bytes_1_to_3} byte(s) "
            f"as the snap-1 to snap-2 delta, so the changes of write C were "
            f"not reported"
        )

        log.test_step(
            "Run the CBT verifier in delta mode for snap-3 vs snap-1 and "
            "assert exit code 0"
        )
        self._restore_and_verify(
            snap_3,
            copy_pvc,
            volume_mode,
            previous_snapshot=snap_1.name,
        )

    # -- Test 5 ----------------------------------------------------

    @tier2
    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-8329"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-8330"),
            ),
        ],
    )
    def test_cbt_chained_deltas(self, volume_mode):
        """
        Verify that chained deltas produce the same result as a
        full copy.

        Applies three incremental steps to a single copy PVC: an
        allocated copy of snap-1, a delta from snap-1 to snap-2,
        and a delta from snap-2 to snap-3. The verifier compares
        the copy PVC against the restored snapshot after every
        step, so the final exit code 0 proves the chain produced a
        volume matching the full snap-3 state.

        Steps:
        1. Create an application PVC, a Block-mode copy PVC,
           and a writer pod.
        2. Write data A (10 MiB), take snap-1 and run an
           allocated-mode verification.
        3. Write data B (5 MiB, at offset 100 MiB in Block mode),
           take snap-2 and run a delta verification for snap-2
           vs snap-1.
        4. Write data C (5 MiB, at offset 200 MiB in Block mode),
           take snap-3 and run a delta verification for snap-3
           vs snap-2.
        """
        log.test_step(
            "Create a %s application PVC, a Block-mode copy PVC, and a writer pod",
            volume_mode,
        )
        app_pvc = self._create_app_pvc(volume_mode)
        copy_pvc = self._create_block_pvc()
        writer_pod = self._create_writer_pod(app_pvc, volume_mode)

        # -- Step 1: allocated copy of snap-1 ----------------------

        log.test_step(
            "Write data A (10 MiB), take snap-1 and run an allocated-mode verification"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=10,
            filename="file1.bin",
        )
        snap_1 = self._take_snapshot(app_pvc, "cbt-chain-1")
        restored_pvc_1 = self._restore_and_verify(snap_1, copy_pvc, volume_mode)
        self._delete_verifier_pod_and_restored_pvc(restored_pvc_1)

        # -- Step 2: delta snap-1 to snap-2 ------------------------

        log.test_step(
            "Write data B (5 MiB, at offset 100 MiB in Block mode), take "
            "snap-2 and run a delta verification for snap-2 vs snap-1"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=5,
            filename="file2.bin",
            offset_mb=100,
        )
        snap_2 = self._take_snapshot(app_pvc, "cbt-chain-2")
        restored_pvc_2 = self._restore_and_verify(
            snap_2,
            copy_pvc,
            volume_mode,
            previous_snapshot=snap_1.name,
        )
        self._delete_verifier_pod_and_restored_pvc(restored_pvc_2)

        # -- Step 3: delta snap-2 to snap-3 ------------------------

        log.test_step(
            "Write data C (5 MiB, at offset 200 MiB in Block mode), take "
            "snap-3 and run a delta verification for snap-3 vs snap-2"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=5,
            filename="file3.bin",
            offset_mb=200,
        )
        snap_3 = self._take_snapshot(app_pvc, "cbt-chain-3")
        self._restore_and_verify(
            snap_3,
            copy_pvc,
            volume_mode,
            previous_snapshot=snap_2.name,
        )

    # -- Test 6 ----------------------------------------------------

    @tier2
    @pytest.mark.parametrize(
        argnames=["volume_mode"],
        argvalues=[
            pytest.param(
                constants.VOLUME_MODE_BLOCK,
                marks=pytest.mark.polarion_id("OCS-8333"),
            ),
            pytest.param(
                constants.VOLUME_MODE_FILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-8334"),
            ),
        ],
    )
    def test_cbt_delta_with_csi_snapshot_handle(self, volume_mode):
        """
        Verify that a delta works when the base snapshot is given
        by CSI snapshot handle instead of by name.

        Runs the same delta twice, once with the base snapshot
        referenced by name (-p) and once by the CSI handle read
        from the VolumeSnapshotContent (-P), and asserts both
        produce the same block ranges.

        Steps:
        1. Create an application PVC, a Block-mode copy PVC,
           and a writer pod.
        2. Write 10 MiB of data, take snap-1 and run an
           allocated-mode verification.
        3. Write 5 MiB of additional data (at offset 100 MiB in
           Block mode) and take snap-2.
        4. Get the CSI snapshot handle of snap-1 from its
           VolumeSnapshotContent.
        5. Run the CBT lister in delta mode with the base snapshot
           given by name and by CSI handle, and verify both cover
           the same bytes.
        6. Run the CBT verifier in delta mode with the base
           snapshot given by CSI handle and assert it exits with
           code 0.
        """
        log.test_step(
            "Create a %s application PVC, a Block-mode copy PVC, and a writer pod",
            volume_mode,
        )
        app_pvc = self._create_app_pvc(volume_mode)
        copy_pvc = self._create_block_pvc()
        writer_pod = self._create_writer_pod(app_pvc, volume_mode)

        log.test_step(
            "Write 10 MiB of data, take snap-1 and run an allocated-mode verification"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=10,
            filename="file1.bin",
        )
        snap_1 = self._take_snapshot(app_pvc, "cbt-handle-1")
        restored_pvc_1 = self._restore_and_verify(snap_1, copy_pvc, volume_mode)
        self._delete_verifier_pod_and_restored_pvc(restored_pvc_1)

        log.test_step(
            "Write 5 MiB of additional data (at offset 100 MiB in Block mode) "
            "and take snap-2"
        )
        write_data_to_pvc(
            writer_pod,
            volume_mode,
            size_mb=5,
            filename="file2.bin",
            offset_mb=100,
        )
        snap_2 = self._take_snapshot(app_pvc, "cbt-handle-2")

        log.test_step("Get the CSI snapshot handle of snap-1")
        snap_1_handle = get_snapshot_handle(snap_1)
        assert snap_1_handle, f"Snapshot {snap_1.name} has an empty CSI snapshot handle"

        log.test_step(
            "Run the CBT lister in delta mode with the base snapshot given by "
            "name and by CSI handle, and verify both cover the same bytes"
        )
        entries_by_name = self.cbt_runner.run_lister_delta(
            target_snap=snap_2.name,
            base_snap_name=snap_1.name,
        )
        entries_by_handle = self.cbt_runner.run_lister_delta(
            target_snap=snap_2.name,
            base_snap_id=snap_1_handle,
        )
        log.info(
            "Delta by name returned %d block(s), delta by CSI handle "
            "returned %d block(s)",
            len(entries_by_name),
            len(entries_by_handle),
        )
        assert len(entries_by_name) > 0, "Delta lister by name returned no blocks"

        coverage_by_name = get_merged_lister_entries_byte_ranges(entries_by_name)
        coverage_by_handle = get_merged_lister_entries_byte_ranges(entries_by_handle)
        only_by_name = get_uncovered_byte_ranges(coverage_by_name, coverage_by_handle)
        only_by_handle = get_uncovered_byte_ranges(coverage_by_handle, coverage_by_name)
        assert not only_by_name and not only_by_handle, (
            f"Delta by CSI handle covers different bytes than delta by name. "
            f"Covered only by name: {only_by_name}, only by handle: {only_by_handle}"
        )

        log.test_step(
            "Run the CBT verifier in delta mode with the base snapshot given "
            "by CSI handle and assert exit code 0"
        )
        self._restore_and_verify(
            snap_2,
            copy_pvc,
            volume_mode,
            previous_snapshot_id=snap_1_handle,
        )

    # -- Test 7 ----------------------------------------------------

    @tier4b
    @ignore_leftovers
    @polarion_id("OCS-8357")
    def test_cbt_service_survives_controller_node_shutdown(
        self, nodes, node_restart_teardown
    ):
        """
        Verify that the CBT service keeps serving while one of the
        RBD CSI controller nodes is down, and recovers once the node
        is powered on again.

        Shuts down the node of one controller pod, checks that the
        Service drops its endpoint while the surviving replica still
        answers allocated and delta requests, then powers the node
        back on and checks that every controller pod serves the CBT
        sidecar again, that the Service endpoints match them, and
        that the metadata is unchanged.

        The replacement of the controller pod is not required to
        land back on the recovered node, since the scheduler is free
        to place it anywhere.

        Steps:
        1. Create a Block application PVC, a Block-mode copy PVC,
           and a writer pod, and write 10 MiB of data.
        2. Take snap-1, write 5 MiB more and take snap-2.
        3. Run an allocated-mode verification of snap-1.
        4. Get the RBD CSI controller pods and verify at least two
           of them run on different nodes with a ready CBT sidecar.
        5. Shut down the node of one controller pod that does not
           host the CBT lister pod.
        6. Verify the Service drops the endpoint of the stopped pod
           and keeps at least one endpoint.
        7. Run the CBT lister in allocated mode on snap-1 while the
           node is down.
        8. Run a delta-mode verification of snap-2 against snap-1
           while the node is down.
        9. Power the node on and wait for it to become Ready.
        10. Wait for all the controller pods to have a ready CBT
            sidecar again and for the Service endpoints to match
            them.
        11. Run the CBT lister in delta mode and in allocated mode,
            and verify the allocated metadata is unchanged.
        12. Run an allocated-mode verification of snap-2.
        """
        volume_mode = constants.VOLUME_MODE_BLOCK

        log.test_step(
            "Create a Block application PVC, a Block-mode copy PVC, and a "
            "writer pod, and write 10 MiB of data"
        )
        app_pvc = self._create_app_pvc(volume_mode)
        copy_pvc = self._create_block_pvc()
        writer_pod = self._create_writer_pod(app_pvc, volume_mode)
        write_data_to_pvc(writer_pod, volume_mode, size_mb=10)

        log.test_step("Take snap-1, write 5 MiB more and take snap-2")
        snap_1 = self._take_snapshot(app_pvc, "cbt-node-1")
        write_data_to_pvc(writer_pod, volume_mode, size_mb=5, offset_mb=100)
        snap_2 = self._take_snapshot(app_pvc, "cbt-node-2")

        log.test_step("Run an allocated-mode verification of snap-1")
        restored_pvc_1 = self._restore_and_verify(snap_1, copy_pvc, volume_mode)
        self._delete_verifier_pod_and_restored_pvc(restored_pvc_1)

        log.test_step(
            "Get the RBD CSI controller pods and verify at least two of them "
            "run on different nodes with a ready CBT sidecar"
        )
        controller_pods = get_rbdfsplugin_provisioner_pods()
        expected_ready_pods = len(controller_pods)
        pod_ip_by_node = {}
        for controller_pod in controller_pods:
            _, pod_ip = validate_snapshot_metadata_sidecar(controller_pod)
            pod_ip_by_node[get_pod_node(controller_pod).name] = pod_ip
        log.info(
            "RBD CSI controller pod IPs by node: %s",
            pod_ip_by_node,
        )
        if len(pod_ip_by_node) < 2:
            pytest.skip(
                f"The CBT service needs at least 2 RBD CSI controller pods on "
                f"different nodes to survive a node shutdown, found "
                f"{len(pod_ip_by_node)}: {pod_ip_by_node}"
            )

        log.test_step(
            "Shut down the node of one controller pod that does not host the "
            "CBT lister pod"
        )
        lister_pod_node = get_pod_node(
            get_pod_obj(self.cbt_runner.lister_pod_name, self.namespace)
        ).name
        selected_node_name = next(
            node_name for node_name in pod_ip_by_node if node_name != lister_pod_node
        )
        selected_pod_ip = pod_ip_by_node[selected_node_name]
        log.info(
            "Shutting down node %s hosting controller pod IP %s. The CBT "
            "lister pod runs on node %s",
            selected_node_name,
            selected_pod_ip,
            lister_pod_node,
        )
        selected_node = get_node_objs([selected_node_name])[0]
        nodes.stop_nodes([selected_node])
        wait_for_nodes_status(
            node_names=[selected_node_name],
            status=constants.NODE_NOT_READY,
        )

        log.test_step(
            "Verify the Service drops the endpoint of the stopped pod and "
            "keeps at least one endpoint"
        )
        endpoint_ips = wait_for_cbt_service_endpoints(absent_ips=[selected_pod_ip])
        assert endpoint_ips, (
            f"The CBT service has no ready endpoint left after node "
            f"{selected_node_name} was shut down"
        )
        log.info("CBT service endpoints after the shutdown: %s", endpoint_ips)

        log.test_step(
            "Run the CBT lister in allocated mode on snap-1 while the node is down"
        )
        entries_down = self.cbt_runner.run_lister_allocated(snap_1.name)
        assert entries_down, (
            f"The CBT lister returned no allocated blocks for {snap_1.name} "
            f"while node {selected_node_name} was down"
        )
        coverage_down = get_merged_lister_entries_byte_ranges(entries_down)
        log.info(
            "Allocated metadata of snap-1 while the node is down: %d block(s), "
            "%d byte(s)",
            len(entries_down),
            get_total_covered_bytes(coverage_down),
        )

        log.test_step(
            "Run a delta-mode verification of snap-2 against snap-1 while the "
            "node is down"
        )
        restored_pvc_2 = self._restore_and_verify(
            snap_2,
            copy_pvc,
            volume_mode,
            previous_snapshot=snap_1.name,
        )
        self._delete_verifier_pod_and_restored_pvc(restored_pvc_2)

        log.test_step("Power the node on and wait for it to become Ready")
        nodes.start_nodes(nodes=[selected_node])
        wait_for_nodes_status(node_names=[selected_node_name], timeout=600)

        log.test_step(
            "Wait for all the controller pods to have a ready CBT sidecar "
            "again and for the Service endpoints to match them"
        )
        ready_pod_ips = wait_for_ready_cbt_sidecar_pods(expected_ready_pods)
        log.info(
            "All %d RBD CSI controller pods have a ready CBT sidecar again "
            "(IPs: %s)",
            expected_ready_pods,
            ready_pod_ips,
        )
        endpoint_ips = wait_for_cbt_service_endpoints(exact_ips=ready_pod_ips)
        log.info("CBT service endpoints after the recovery: %s", endpoint_ips)

        log.test_step(
            "Run the CBT lister in delta mode and in allocated mode, and "
            "verify the allocated metadata is unchanged"
        )
        entries_delta = self.cbt_runner.run_lister_delta(
            target_snap=snap_2.name,
            base_snap_name=snap_1.name,
        )
        assert entries_delta, (
            f"The CBT lister returned no changed blocks between "
            f"{snap_1.name} and {snap_2.name} after the node recovery"
        )
        log.info(
            "Delta snap-1 to snap-2 after the recovery: %d block(s)",
            len(entries_delta),
        )

        coverage_up = get_merged_lister_entries_byte_ranges(
            self.cbt_runner.run_lister_allocated(snap_1.name)
        )
        missing = get_uncovered_byte_ranges(coverage_down, coverage_up)
        extra = get_uncovered_byte_ranges(coverage_up, coverage_down)
        assert not missing and not extra, (
            f"The allocated metadata of {snap_1.name} changed across the node "
            f"shutdown. Reported only before: {missing}, only after: {extra}"
        )
        log.info(
            "The allocated metadata of snap-1 is unchanged across the node "
            "shutdown (%d byte(s))",
            get_total_covered_bytes(coverage_up),
        )

        log.test_step("Run an allocated-mode verification of snap-2")
        self._restore_and_verify(snap_2, copy_pvc, volume_mode)


@green_squad
@tier3
@consumer_env_check
@skipif_ocs_version("<4.23")
class TestRbdCBTMetadataNegative(CBTMetadataTestBase):
    """
    Test that the CBT metadata service rejects invalid callers.

    Each test calls the gRPC API with one credential broken on
    purpose, a token minted for the wrong audience or an unrelated
    CA certificate, and expects the call to be refused before any
    block metadata is returned.

    These tests use the lister rather than the verifier, because the
    rejection happens during the gRPC handshake, before any block
    range is read, so mounting source and destination PVCs would add
    nothing to the check.
    """

    # -- Test 1 ----------------------------------------------------

    @polarion_id("OCS-8356")
    def test_cbt_wrong_audience_rejected(self):
        """
        Verify that a token issued for the wrong audience is
        rejected by the CBT metadata service.

        The sidecar validates the caller token with a TokenReview
        bound to its own audience, so a token minted for a different
        audience must not be accepted. The CBT lister is run with a
        bogus --audience value and is expected to fail before any
        block metadata is returned.

        Steps:
        1. Create a Block-mode application PVC and a writer pod,
           and write 10 MiB of data.
        2. Take a VolumeSnapshot of the PVC.
        3. Run the CBT lister against the snapshot with a bogus
           --audience value, and assert it fails with an
           authentication error.
        """
        log.test_step(
            "Create a Block application PVC and a writer pod, and write 10 MiB"
        )
        volume_mode = constants.VOLUME_MODE_BLOCK
        app_pvc = self._create_app_pvc(volume_mode)
        writer_pod = self._create_writer_pod(app_pvc, volume_mode)
        write_data_to_pvc(writer_pod, volume_mode, size_mb=10)

        log.test_step("Take a VolumeSnapshot of the PVC")
        snap_obj = self._take_snapshot(app_pvc, "cbt-wrong-aud")

        log.test_step(
            "Run the CBT lister with a wrong audience and assert it fails "
            "with an authentication error"
        )
        exit_code, output = self.cbt_runner.run_lister_expecting_failure(
            snap_obj.name,
            audience="wrong-audience",
        )
        assert exit_code != 0, (
            f"The CBT lister succeeded with a wrong audience. It should have "
            f"been rejected by the metadata service. Output:\n{output}"
        )
        verify_lister_authentication_error_markers(output)

    # -- Test 2 ----------------------------------------------------

    @polarion_id("OCS-8355")
    def test_cbt_wrong_ca_cert_rejected(self):
        """
        Verify that the TLS handshake fails when the client trusts
        the wrong CA certificate.

        The CBT metadata service is served over TLS with a
        certificate issued by the OpenShift service CA. A client
        that trusts an unrelated self-signed CA must not be able to
        complete the handshake, so no block metadata can be read.

        Steps:
        1. Create a Block-mode application PVC and a writer pod,
           and write 10 MiB of data.
        2. Take a VolumeSnapshot of the PVC.
        3. Generate an untrusted self-signed CA certificate and
           write it into the CBT lister pod.
        4. Run the CBT lister against the snapshot with the
           untrusted CA certificate passed as --ca-cert-file, and
           assert it fails with a TLS error.
        """
        log.test_step(
            "Create a Block application PVC and a writer pod, and write 10 MiB"
        )
        volume_mode = constants.VOLUME_MODE_BLOCK
        app_pvc = self._create_app_pvc(volume_mode)
        writer_pod = self._create_writer_pod(app_pvc, volume_mode)
        write_data_to_pvc(writer_pod, volume_mode, size_mb=10)

        log.test_step("Take a VolumeSnapshot of the PVC")
        snap_obj = self._take_snapshot(app_pvc, "cbt-wrong-ca")

        log.test_step(
            "Generate an untrusted CA certificate and write it into the "
            "CBT lister pod"
        )
        untrusted_ca_path = self.cbt_runner.write_file_in_lister_pod(
            "/tmp/untrusted-ca.crt",
            generate_untrusted_ca_cert(),
        )

        log.test_step(
            "Run the CBT lister with the wrong CA certificate and assert it "
            "fails with a TLS error"
        )
        exit_code, output = self.cbt_runner.run_lister_expecting_failure(
            snap_obj.name,
            ca_cert_file=untrusted_ca_path,
        )
        assert exit_code != 0, (
            f"The CBT lister succeeded with an unrelated self-signed CA "
            f"certificate. The TLS handshake should have failed. "
            f"Output:\n{output}"
        )
        verify_lister_tls_error_markers(output)


@green_squad
@tier1
@skipif_ocs_version("<4.23")
class TestRbdCBTInfrastructure(ManageTest):
    """
    Test CBT infrastructure deployment and service health.

    Validates that the CBT snapshot metadata service is correctly
    deployed, the sidecar containers are running in the RBD CSI
    controller pods, and the ConfigMap contains valid connection
    details.

    These tests check the infrastructure components rather than
    the metadata API operations themselves.
    """

    @polarion_id("OCS-8239")
    def test_cbt_sidecar_running(self):
        """
        Verify that the CBT sidecar container is running and healthy
        in all RBD CSI controller pods.

        Checks that:
        - RBD CSI controller pods exist and are running
        - Each pod has a csi-snapshot-metadata container that is ready
        - The Service for the metadata endpoint exists
        - Service Endpoints match the controller pod IPs

        Steps:
        1. Get the RBD CSI controller pods by label.
        2. Check each pod for a container named
           csi-snapshot-metadata.
        3. Get the Service openshift-storage-rbd-snapshot-metadata
           in the openshift-storage namespace.
        4. Get the Endpoints for the Service.
        5. Compare the endpoint IPs with the RBD controller pod IPs.
        """
        log.test_step("Get the RBD CSI controller pods by label")
        controller_pods = get_rbdfsplugin_provisioner_pods()

        assert len(controller_pods) > 0, (
            f"No RBD CSI controller pods found with label "
            f"{constants.RBD_CTRLPLUGIN_LABEL}"
        )
        log.info(
            "Found %d RBD CSI controller pod(s)",
            len(controller_pods),
        )

        log.test_step("Check each pod for a container named csi-snapshot-metadata")
        pod_ips = []
        for controller_pod in controller_pods:
            _, pod_ip = validate_snapshot_metadata_sidecar(controller_pod)
            pod_ips.append(pod_ip)

        log.test_step("Get the Service openshift-storage-rbd-snapshot-metadata")
        svc_ocp = OCP(
            kind="Service",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        svc_name = constants.CBT_SERVICE_NAME
        svc = svc_ocp.get(resource_name=svc_name)

        assert svc is not None, f"Service {svc_name} not found"
        log.info("Service %s exists", svc_name)

        ports = svc["spec"].get("ports", [])
        port_6443 = next((p for p in ports if p["port"] == 6443), None)
        assert port_6443 is not None, (
            f"Service {svc_name} does not expose port 6443. " f"Found ports: {ports}"
        )

        # Verify the port uses TCP protocol
        protocol = port_6443.get("protocol", "TCP")
        assert protocol == "TCP", (
            f"Service {svc_name} port 6443 uses protocol {protocol}, " f"expected TCP"
        )
        log.info(
            "Service %s exposes port 6443/TCP",
            svc_name,
        )

        log.test_step("Get the Endpoints for the Service")
        ep_ocp = OCP(
            kind=constants.ENDPOINTS,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        endpoints = ep_ocp.get(resource_name=svc_name)
        assert endpoints is not None, f"Endpoints {svc_name} not found"

        subsets = endpoints.get("subsets", [])
        assert (
            len(subsets) > 0
        ), f"Endpoints {svc_name} has no subsets (no ready endpoints)"
        log.info("Endpoints %s has %d subset(s)", svc_name, len(subsets))

        log.test_step("Compare the endpoint IPs with the RBD controller pod IPs")
        endpoint_ips = []
        for subset in subsets:
            addresses = subset.get("addresses", [])
            for addr in addresses:
                endpoint_ips.append(addr["ip"])

        assert len(endpoint_ips) > 0, f"Endpoints {svc_name} has no IPs"
        log.info(
            "Endpoints IPs: %s, Pod IPs: %s",
            sorted(endpoint_ips),
            sorted(pod_ips),
        )

        assert set(endpoint_ips) == set(pod_ips), (
            f"Endpoints IPs {sorted(endpoint_ips)} do not match "
            f"controller pod IPs {sorted(pod_ips)}"
        )
        log.info(
            "All %d endpoint IP(s) match the RBD controller pod IP(s)",
            len(endpoint_ips),
        )

    @polarion_id("OCS-8252")
    def test_cbt_configmap_connection_details(self):
        """
        Verify that the CBT service ConfigMap contains correct
        connection details.

        Checks that:
        - The ConfigMap exists in the openshift-storage namespace
        - Required keys are present and non-empty
        - The driverName value is correct
        - The address format is valid (host:port)
        - The caCert is a valid PEM-encoded certificate

        Steps:
        1. Get ConfigMap openshift-storage.rbd.csi.ceph.com from the
           openshift-storage namespace.
        2. Check that the ConfigMap exists.
        3. Check that the keys address, audience, caCert, and
           driverName are present and non-empty.
        4. Check the value of the driverName key.
        5. Check the format of the address value.
        6. Check the format of the caCert value.
        """
        log.test_step(
            "Get ConfigMap %s from the openshift-storage namespace",
            constants.CBT_CONFIGMAP_NAME,
        )
        cm_ocp = OCP(
            kind="ConfigMap",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        cm = cm_ocp.get(resource_name=constants.CBT_CONFIGMAP_NAME)

        log.test_step("Check that the ConfigMap exists")
        assert cm is not None, (
            f"ConfigMap {constants.CBT_CONFIGMAP_NAME} not found in "
            f"{constants.OPENSHIFT_STORAGE_NAMESPACE}"
        )
        log.info("ConfigMap %s exists", constants.CBT_CONFIGMAP_NAME)

        log.test_step(
            "Check that keys address, audience, caCert, and driverName "
            "are present and non-empty"
        )
        data = cm.get("data", {})
        required_keys = ["address", "audience", "caCert", "driverName"]
        for key in required_keys:
            assert key in data, (
                f"ConfigMap {constants.CBT_CONFIGMAP_NAME} missing " f"key '{key}'"
            )
            assert data[key], (
                f"ConfigMap {constants.CBT_CONFIGMAP_NAME} key '{key}' " f"is empty"
            )
        log.info("All required keys are present and non-empty")

        log.test_step("Check the value of the driverName key")
        driver_name = data["driverName"]
        expected_driver = "openshift-storage.rbd.csi.ceph.com"
        assert (
            driver_name == expected_driver
        ), f"driverName is '{driver_name}', expected '{expected_driver}'"
        log.info("driverName is correct: %s", driver_name)

        log.test_step("Check the format of the address value")
        address = data["address"]
        host_port_pattern = r"^[a-zA-Z0-9._-]+:\d+$"
        assert re.match(
            host_port_pattern, address
        ), f"address '{address}' is not a valid host:port format"
        log.info("address format is valid: %s", address)

        log.test_step("Check the format of the caCert value")
        ca_cert = data["caCert"]
        assert ca_cert.startswith(
            "-----BEGIN CERTIFICATE-----"
        ), "caCert does not start with PEM header"
        assert ca_cert.strip().endswith(
            "-----END CERTIFICATE-----"
        ), "caCert does not end with PEM footer"

        # Parse the certificate to verify it's valid
        try:
            cert = x509.load_pem_x509_certificate(
                ca_cert.encode("utf-8"), default_backend()
            )
        except ValueError as e:
            raise AssertionError(
                f"caCert has valid PEM delimiters but failed to parse as "
                f"X.509 certificate: {e}"
            ) from e
        log.info(
            "caCert is a valid PEM-encoded certificate (Subject: %s)",
            cert.subject.rfc4514_string(),
        )
