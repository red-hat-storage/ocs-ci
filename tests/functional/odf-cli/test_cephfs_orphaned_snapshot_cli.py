"""
RHSTOR-7731 — Cleanup orphaned CephFS snapshots: CLI test cases TC2-TC7.

TC1 (Detect and Clean Up an Orphaned CephFS Snapshot, default SVG) already
exists in:
  tests/functional/pv/pvc_snapshot/test_cephfs_orphaned_snapshot_alert.py
  ::TestCephFSOrphanedSnapshotAlert::test_cephfs_orphaned_snapshot_alert

This file covers the remaining cases:
  TC2: Orphaned snapshot in a non-default subvolume group (--svg flag)
  TC3: List snapshots explicitly using the default SVG (--svg csi)
  TC4: Partial manual cleanup of orphaned snapshots
  TC5: Attempt to delete a Bound (non-orphaned) snapshot — error expected
  TC6: Attempt to delete a non-existent snapshot — error expected
  TC7: Cross-subvolume-group deletion validation — error expected

Helper additions vs TC1
-----------------------
Steps that were inline in TC1 are now provided by helpers in
ocs_ci/helpers/odf_cephfs_snap.py:
  - assert_no_cephfs_snapshots(snap_runner)
  - create_orphaned_cephfs_snapshot(pvc_obj, snapclass_name, snap_runner)
  - delete_all_orphaned_snaps(snap_runner)
These helpers are also used here to keep tests concise.
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2, skipif_ocs_version
from ocs_ci.helpers import helpers
from ocs_ci.helpers.odf_cephfs_snap import (
    assert_no_cephfs_snapshots,
    create_orphaned_cephfs_snapshot,
    delete_all_orphaned_snaps,
    parse_snap_ls,
)
from ocs_ci.helpers.odf_cli import odf_cli_cephfs_snap_setup_helper
from ocs_ci.ocs import constants, templating
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources import ocs
from ocs_ci.ocs.resources.storage_cluster import get_storage_client

log = logging.getLogger(__name__)

RETAIN_SNAPCLASS_NAME = "test-cephfs-retain-snapclass-cli"
# Non-default subvolume group name used in TC2 and TC7
NON_DEFAULT_SVG = "test-svg"


@green_squad
@tier2
@skipif_ocs_version("<4.22")
class TestCephFSOrphanedSnapshotCLI(ManageTest):
    """
    Verify the odf cephfs-snap CLI handles various snapshot states and
    subvolume-group combinations correctly (TC2-TC7 of RHSTOR-7731).

    TC1 is already covered by TestCephFSOrphanedSnapshotAlert in
    tests/functional/pv/pvc_snapshot/test_cephfs_orphaned_snapshot_alert.py.
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, pvc_factory, teardown_factory):
        """
        Create a CephFS PVC and a Retain VolumeSnapshotClass shared by all
        tests in this class.
        """
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=1,
            status=constants.STATUS_BOUND,
        )

        snapclass_data = templating.load_yaml(constants.CSI_CEPHFS_SNAPSHOTCLASS_YAML)
        snapclass_data["metadata"]["name"] = RETAIN_SNAPCLASS_NAME
        snapclass_data["deletionPolicy"] = constants.RECLAIM_POLICY_RETAIN
        self.snapclass_obj = ocs.OCS(**snapclass_data)
        assert self.snapclass_obj.create(
            do_reload=True
        ), f"Failed to create VolumeSnapshotClass {RETAIN_SNAPCLASS_NAME}"
        teardown_factory(self.snapclass_obj)

        sc_obj = get_storage_client()
        self.snap_runner = odf_cli_cephfs_snap_setup_helper(
            storage_client=sc_obj.resource_name
        )

    def test_orphaned_snapshot_non_default_svg(self, teardown_factory):
        """
        TC2 — Detect and clean up an orphaned snapshot in a non-default
        subvolume group (--svg NON_DEFAULT_SVG).

        Steps:
        1. Verify no CephFS snapshots exist.
        2. Create a non-default CephFS subvolume group via the Ceph toolbox.
        3. Create a VolumeSnapshotClass that targets the non-default SVG
           and set deletionPolicy=Retain.
        4. Create a VolumeSnapshot using the non-default SVG snapclass.
        5. Orphan the snapshot (delete k8s objects).
        6. List snapshots with --svg NON_DEFAULT_SVG; verify state orphaned.
        7. Delete the orphaned snapshot with --svg NON_DEFAULT_SVG.
        8. Verify no snapshots remain.

        TODO: Step 2 requires creating a StorageClass with
        subVolumeGroup=test-svg and using it for the PVC. Currently the
        default CephFS StorageClass always uses the "csi" SVG, so this test
        needs a dedicated StorageClass factory that passes the subVolumeGroup
        parameter.  Tracked in OCSQE-4558.
        """
        pytest.skip(
            "TC2 requires a StorageClass factory that configures a custom "
            "subVolumeGroup. Tracked in OCSQE-4558."
        )

        # --- Implementation outline (uncomment once prerequisite is ready) ---
        # runner_non_default = odf_cli_cephfs_snap_setup_helper(
        #     storage_client=sc_obj.resource_name,
        #     svg=NON_DEFAULT_SVG,
        # )
        # assert_no_cephfs_snapshots(runner_non_default)
        # subvolume, ceph_snap = create_orphaned_cephfs_snapshot(
        #     self.pvc_obj, RETAIN_SNAPCLASS_NAME, runner_non_default
        # )
        # runner_non_default.delete(subvolume, ceph_snap)
        # assert_no_cephfs_snapshots(runner_non_default)

    def test_orphaned_snapshot_explicit_default_svg(self, teardown_factory):
        """
        TC3 — List and delete an orphaned snapshot while explicitly specifying
        the default subvolume group (--svg csi).

        The result must be identical to the default behaviour without --svg.

        Steps:
        1. Verify no CephFS snapshots exist (with and without --svg csi).
        2. Create and orphan a snapshot via the default flow.
        3. List with --svg csi; verify state is orphaned.
        4. Delete with --svg csi.
        5. Verify no snapshots remain.
        """
        runner_default_svg = odf_cli_cephfs_snap_setup_helper(
            storage_client=self.snap_runner.storage_client,
            svg="csi",
        )

        # Step 1
        log.info("TC3 Step 1: Verify no snapshots exist with explicit --svg csi")
        assert_no_cephfs_snapshots(runner_default_svg)

        # Steps 2-3: orphan a snapshot
        log.info("TC3 Steps 2-3: Create and orphan a snapshot")
        subvolume, ceph_snap = create_orphaned_cephfs_snapshot(
            self.pvc_obj, RETAIN_SNAPCLASS_NAME, runner_default_svg
        )

        # Step 4: delete via CLI with --svg csi
        log.info("TC3 Step 4: Delete orphan with --svg csi")
        runner_default_svg.delete(subvolume, ceph_snap)

        # Step 5: verify clean
        log.info("TC3 Step 5: Verify no snapshots remain")
        assert_no_cephfs_snapshots(runner_default_svg)

    def test_partial_manual_cleanup(self, teardown_factory):
        """
        TC4 — Partial manual cleanup: create multiple orphaned snapshots,
        delete some directly at the Ceph level, then verify the CLI only
        lists and deletes the remaining ones.

        Steps:
        1. Create 3 snapshots and orphan all of them.
        2. Delete one orphan directly via the Ceph toolbox (simulating
           manual cleanup).
        3. List with --orphaned; verify only 2 remain.
        4. Delete the remaining orphans via the CLI helper.
        5. Verify no snapshots remain.
        """
        from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

        # Step 1: create 3 orphaned snapshots
        log.info("TC4 Step 1: Create 3 orphaned snapshots")
        assert_no_cephfs_snapshots(self.snap_runner)
        orphans = []
        for _ in range(3):
            subvol, snap = create_orphaned_cephfs_snapshot(
                self.pvc_obj, RETAIN_SNAPCLASS_NAME, self.snap_runner
            )
            orphans.append((subvol, snap))

        result = self.snap_runner.ls(orphaned=True)
        entries = parse_snap_ls(result.stdout.decode())
        assert len(entries) == 3, f"Expected 3 orphaned snapshots, found {len(entries)}"

        # Step 2: delete first orphan directly via toolbox
        log.info(
            f"TC4 Step 2: Manually delete first orphan via toolbox: "
            f"subvolume={orphans[0][0]}, snapshot={orphans[0][1]}"
        )
        toolbox = get_ceph_tools_pod()
        fs_name = constants.DEFAULT_CEPHFILESYSTEM_NAME
        toolbox.exec_ceph_cmd(
            f"ceph fs subvolume snapshot rm {fs_name} "
            f"{orphans[0][0]} {orphans[0][1]} --group_name csi",
        )

        # Step 3: verify CLI shows only 2 remaining
        log.info("TC4 Step 3: Verify CLI shows 2 remaining orphans")
        result = self.snap_runner.ls(orphaned=True)
        entries = parse_snap_ls(result.stdout.decode())
        assert len(entries) == 2, (
            f"Expected 2 orphaned snapshots after manual deletion, "
            f"found {len(entries)}"
        )

        # Step 4-5: delete remaining via helper and verify clean
        log.info("TC4 Steps 4-5: Delete remaining orphans and verify clean")
        deleted = delete_all_orphaned_snaps(self.snap_runner)
        assert deleted == 2, f"Expected to delete 2 orphans, deleted {deleted}"
        assert_no_cephfs_snapshots(self.snap_runner)

    def test_delete_bound_snapshot_error(self, teardown_factory):
        """
        TC5 — Attempt to delete a Bound (non-orphaned) snapshot via the CLI.
        The CLI must refuse and return a non-zero exit code.

        Steps:
        1. Create a VolumeSnapshot (Retain policy, so it stays Bound while
           the k8s objects exist).
        2. List snapshots; verify state is NOT orphaned.
        3. Attempt to delete via odf cephfs-snap delete.
        4. Verify CommandFailed is raised.
        5. Verify the snapshot still exists and is still Bound.
        """
        from ocs_ci.ocs.resources import pvc as pvc_resource

        log.info("TC5 Step 1: Create VolumeSnapshot with Retain policy")
        assert_no_cephfs_snapshots(self.snap_runner)
        snap_name = helpers.create_unique_resource_name("test", "cephfs-snap")
        snap_obj = pvc_resource.create_pvc_snapshot(
            pvc_name=self.pvc_obj.name,
            snap_yaml=constants.CSI_CEPHFS_SNAPSHOT_YAML,
            snap_name=snap_name,
            namespace=self.pvc_obj.namespace,
            sc_name=RETAIN_SNAPCLASS_NAME,
            wait=True,
            timeout=120,
        )
        teardown_factory(snap_obj)

        # Step 2: verify snapshot is Bound (not orphaned)
        log.info("TC5 Step 2: Verify snapshot is Bound")
        result = self.snap_runner.ls()
        entries = parse_snap_ls(result.stdout.decode())
        assert entries, "No snapshot entries found after creation"
        state = entries[0]["state"]
        assert (
            state != constants.CEPHFS_SNAPSHOT_STATE_ORPHANED
        ), "Snapshot unexpectedly orphaned before k8s objects are deleted"
        subvolume = entries[0]["subvolume"]
        ceph_snap = entries[0]["snapshot"]

        # Step 3-4: attempt deletion — must fail
        log.info(
            "TC5 Steps 3-4: Attempt to delete Bound snapshot (expect CommandFailed)"
        )
        with pytest.raises(CommandFailed):
            self.snap_runner.delete(subvolume, ceph_snap)

        # Step 5: snapshot still present and Bound
        log.info("TC5 Step 5: Verify snapshot is still present")
        result = self.snap_runner.ls()
        entries = parse_snap_ls(result.stdout.decode())
        assert entries, "Snapshot disappeared after failed delete — unexpected"
        assert (
            entries[0]["state"] != constants.CEPHFS_SNAPSHOT_STATE_ORPHANED
        ), "Snapshot state changed unexpectedly after failed delete"

    def test_delete_nonexistent_snapshot_error(self, teardown_factory):
        """
        TC6 — Attempt to delete a snapshot name that does not exist.
        The CLI must return a non-zero exit code.

        Steps:
        1. Verify no snapshots exist.
        2. Call odf cephfs-snap delete with a fabricated subvolume and
           snapshot name.
        3. Verify CommandFailed is raised.
        4. Verify no existing snapshots were affected.
        """
        # Step 1
        assert_no_cephfs_snapshots(self.snap_runner)

        # Steps 2-3
        log.info(
            "TC6 Steps 2-3: Attempt to delete a non-existent snapshot "
            "(expect CommandFailed)"
        )
        with pytest.raises(CommandFailed):
            self.snap_runner.delete(
                "nonexistent-subvolume-xyz", "nonexistent-snapshot-xyz"
            )

        # Step 4: no snapshots created as side-effect
        assert_no_cephfs_snapshots(self.snap_runner)

    def test_cross_svg_deletion_validation(self, teardown_factory):
        """
        TC7 — Cross-subvolume-group deletion: a snapshot can only be deleted
        when the correct --svg is specified.

        Steps:
        1. Create an orphaned snapshot in the default SVG ("csi").
        2. Attempt to delete it while specifying --svg NON_DEFAULT_SVG.
           Expect CommandFailed.
        3. Delete it correctly (without --svg override or with --svg csi).
        4. Verify no snapshots remain.

        TODO: The reverse (deleting a non-default-SVG snapshot with default
        --svg) requires TC2's prerequisite — a custom StorageClass.
        Tracked in OCSQE-4558.
        """
        # Step 1: create orphaned snapshot in default SVG
        log.info("TC7 Step 1: Create orphaned snapshot in default SVG")
        assert_no_cephfs_snapshots(self.snap_runner)
        subvolume, ceph_snap = create_orphaned_cephfs_snapshot(
            self.pvc_obj, RETAIN_SNAPCLASS_NAME, self.snap_runner
        )

        # Step 2: attempt deletion with wrong --svg (expect failure)
        log.info(
            f"TC7 Step 2: Attempt deletion with --svg {NON_DEFAULT_SVG} "
            f"(expect CommandFailed)"
        )
        runner_wrong_svg = odf_cli_cephfs_snap_setup_helper(
            storage_client=self.snap_runner.storage_client,
            svg=NON_DEFAULT_SVG,
        )
        with pytest.raises(CommandFailed):
            runner_wrong_svg.delete(subvolume, ceph_snap)

        # Step 3: verify snapshot still present, then delete correctly
        log.info("TC7 Step 3: Verify orphan still present; delete with correct SVG")
        result = self.snap_runner.ls()
        entries = parse_snap_ls(result.stdout.decode())
        assert entries, "Orphaned snapshot disappeared after failed wrong-SVG delete"
        self.snap_runner.delete(subvolume, ceph_snap)

        # Step 4: verify clean
        log.info("TC7 Step 4: Verify no snapshots remain")
        assert_no_cephfs_snapshots(self.snap_runner)
