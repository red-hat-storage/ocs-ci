import logging

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pvc as pvc_resource

log = logging.getLogger(__name__)


def parse_snap_ls(output):
    """
    Parse the tabular output of `odf cephfs-snap ls` into a list of dicts.

    Args:
        output (str): stdout from `odf cephfs-snap ls`.

    Returns:
        list[dict]: Each dict has keys: filesystem, subvolume,
            subvolumegroup, snapshot, state.
    """
    lines = [line for line in output.strip().splitlines() if line.strip()]
    if len(lines) <= 1:
        return []
    entries = []
    for line in lines[1:]:  # skip header row
        parts = line.split()
        if len(parts) >= 5:
            entries.append(
                {
                    "filesystem": parts[0],
                    "subvolume": parts[1],
                    "subvolumegroup": parts[2],
                    "snapshot": parts[3],
                    "state": parts[4],
                }
            )
    return entries


def assert_no_cephfs_snapshots(snap_runner):
    """
    Assert that no CephFS snapshots exist via the odf CLI.

    Args:
        snap_runner (ODFCLICephfsSnapRunner): Initialized snap runner.

    Raises:
        AssertionError: If any snapshots are found.
    """
    result = snap_runner.ls()
    entries = parse_snap_ls(result.stdout.decode())
    assert not entries, f"Expected no CephFS snapshots but found: {entries}"


def create_orphaned_cephfs_snapshot(pvc_obj, snapclass_name, snap_runner):
    """
    Create a CephFS VolumeSnapshot with a Retain policy, then delete the k8s
    objects so the Ceph-side snapshot becomes orphaned.

    Steps performed:
      1. Create VolumeSnapshot using the given Retain VolumeSnapshotClass.
      2. Wait until readyToUse.
      3. List snapshots via odf CLI to capture subvolume and snapshot names.
      4. Delete the k8s VolumeSnapshot and VolumeSnapshotContent.
      5. Verify the Ceph snapshot state is 'orphaned'.

    Args:
        pvc_obj: PVC object to snapshot.
        snapclass_name (str): Name of a VolumeSnapshotClass with
            deletionPolicy=Retain.
        snap_runner (ODFCLICephfsSnapRunner): Initialized snap runner.

    Returns:
        tuple[str, str]: (subvolume, ceph_snap_name) captured from the CLI.
    """
    snap_name = helpers.create_unique_resource_name("test", "cephfs-snap")
    snap_obj = pvc_resource.create_pvc_snapshot(
        pvc_name=pvc_obj.name,
        snap_yaml=constants.CSI_CEPHFS_SNAPSHOT_YAML,
        snap_name=snap_name,
        namespace=pvc_obj.namespace,
        sc_name=snapclass_name,
        wait=True,
        timeout=120,
    )

    result = snap_runner.ls()
    snap_entries = parse_snap_ls(result.stdout.decode())
    assert snap_entries, "Expected snapshot to appear in odf cephfs-snap ls output"
    subvolume = snap_entries[0]["subvolume"]
    ceph_snap_name = snap_entries[0]["snapshot"]

    snapcontent_obj = helpers.get_snapshot_content_obj(snap_obj)
    snap_obj.delete()
    snap_obj.ocp.wait_for_delete(snap_obj.name)
    snapcontent_obj.delete()
    snapcontent_obj.ocp.wait_for_delete(snapcontent_obj.name)

    result = snap_runner.ls()
    snap_entries = parse_snap_ls(result.stdout.decode())
    assert snap_entries, "Expected orphaned snapshot in odf cephfs-snap ls output"
    assert snap_entries[0]["state"] == constants.CEPHFS_SNAPSHOT_STATE_ORPHANED, (
        f"Expected state '{constants.CEPHFS_SNAPSHOT_STATE_ORPHANED}', "
        f"got '{snap_entries[0]['state']}'"
    )

    log.info(
        f"Orphaned snapshot created — subvolume: {subvolume}, "
        f"snapshot: {ceph_snap_name}"
    )
    return subvolume, ceph_snap_name


def delete_all_orphaned_snaps(snap_runner):
    """
    Delete all orphaned CephFS snapshots found by the odf CLI.

    Args:
        snap_runner (ODFCLICephfsSnapRunner): Initialized snap runner.

    Returns:
        int: Number of snapshots deleted.
    """
    result = snap_runner.ls(orphaned=True)
    orphans = parse_snap_ls(result.stdout.decode())
    for entry in orphans:
        log.info(
            f"Deleting orphaned snapshot: subvolume={entry['subvolume']}, "
            f"snapshot={entry['snapshot']}"
        )
        snap_runner.delete(entry["subvolume"], entry["snapshot"])
    log.info(f"Deleted {len(orphans)} orphaned snapshot(s)")
    return len(orphans)
