"""
Snapshot helper functions.

Provides utilities for restoring snapshots to Block-mode PVCs
with proper annotations and writing data to PVCs via dd.
"""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


def annotate_snapshot_for_block_restore(snap_obj):
    """
    Add the allow-volume-mode-change annotation to the
    VolumeSnapshotContent bound to the given VolumeSnapshot.

    This annotation is required when restoring a Filesystem-mode
    snapshot as a Block-mode PVC.

    Args:
        snap_obj (OCS): VolumeSnapshot object

    Returns:
        str: Name of the annotated VolumeSnapshotContent
    """
    ocp_snap = OCP(
        kind=constants.VOLUMESNAPSHOT,
        namespace=snap_obj.namespace,
    )
    snap_data = ocp_snap.get(resource_name=snap_obj.name)
    content_name = snap_data["status"]["boundVolumeSnapshotContentName"]

    ocp = OCP()
    ocp.exec_oc_cmd(
        f"annotate volumesnapshotcontent {content_name} "
        "snapshot.storage.kubernetes.io/"
        "allow-volume-mode-change=true --overwrite",
        out_yaml_format=False,
    )
    logger.info(
        "Annotated VolumeSnapshotContent %s for mode change",
        content_name,
    )
    return content_name


def restore_snapshot_to_block_pvc(
    snap_obj,
    namespace,
    size,
    sc_name,
    pvc_name=None,
    original_volume_mode=None,
):
    """
    Restore a VolumeSnapshot to a new Block-mode PVC.

    If the original PVC used Filesystem mode, the required
    allow-volume-mode-change annotation is added automatically.

    Args:
        snap_obj (OCS): VolumeSnapshot object
        namespace (str): Namespace for the restored PVC
        size (str): PVC size (e.g. "1Gi")
        sc_name (str): StorageClass name
        pvc_name (str): Name for the restored PVC (auto-generated
            if not provided)
        original_volume_mode (str): Volume mode of the original PVC.
            When set to Filesystem, the VolumeSnapshotContent is
            annotated for volume mode change.

    Returns:
        PVC: The created PVC object, in Bound state
    """
    if pvc_name is None:
        pvc_name = helpers.create_unique_resource_name("cbt-restored", "pvc")

    if original_volume_mode and original_volume_mode != constants.VOLUME_MODE_BLOCK:
        annotate_snapshot_for_block_restore(snap_obj)

    from ocs_ci.ocs.resources.pvc import create_restore_pvc

    restored_pvc = create_restore_pvc(
        sc_name=sc_name,
        snap_name=snap_obj.name,
        namespace=namespace,
        size=size,
        pvc_name=pvc_name,
        volume_mode=constants.VOLUME_MODE_BLOCK,
    )
    helpers.wait_for_resource_state(restored_pvc, constants.STATUS_BOUND, timeout=300)
    logger.info(
        "Restored snapshot %s to Block PVC %s",
        snap_obj.name,
        pvc_name,
    )
    return restored_pvc


def write_data_to_pvc(pod_obj, volume_mode, size_mb, filename=None, offset_mb=0):
    """
    Write random data to a PVC using dd.

    For Filesystem mode, data is written as a file under the
    standard mount path. For Block mode, data is written directly
    to the raw block device at the specified offset.

    Args:
        pod_obj (Pod): Pod with the PVC mounted
        volume_mode (str): VOLUME_MODE_BLOCK or VOLUME_MODE_FILESYSTEM
        size_mb (int): Amount of data to write in MiB
        filename (str): Filename for Filesystem mode (default
            "testdata.bin"). Ignored for Block mode.
        offset_mb (int): Write offset in MiB. For Block mode this
            maps to dd seek=. For Filesystem mode this is ignored.
    """
    if volume_mode == constants.VOLUME_MODE_FILESYSTEM:
        if filename is None:
            filename = "testdata.bin"
        cmd = (
            f"dd if=/dev/urandom "
            f"of={constants.MOUNT_POINT}/{filename} "
            f"bs=1M count={size_mb} conv=fsync"
        )
    else:
        cmd = (
            f"dd if=/dev/urandom "
            f"of={constants.RAW_BLOCK_DEVICE} "
            f"bs=1M count={size_mb} "
            f"seek={offset_mb} conv=fsync"
        )
    pod_obj.exec_cmd_on_pod(cmd, out_yaml_format=False, timeout=120)
    logger.info(
        "Wrote %d MiB to pod %s (mode=%s, offset=%d MiB)",
        size_mb,
        pod_obj.name,
        volume_mode,
        offset_mb,
    )
