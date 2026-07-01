import json
import logging
import re

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import get_snapshot_content_obj
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating
from ocs_ci.ocs.resources import ocs
from ocs_ci.ocs.resources.storageconsumer import find_consumer_for_storage_client
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


def _create_standalone_retain_snapclass(snapclass_name):
    """
    Create a Retain-policy CephFS VolumeSnapshotClass directly on the current
    cluster (standalone / single-cluster mode, no StorageConsumer patching).

    Args:
        snapclass_name (str): Name to give the new VolumeSnapshotClass.

    Returns:
        callable: Teardown function that deletes the snapclass.
    """
    snapclass_data = templating.load_yaml(constants.CSI_CEPHFS_SNAPSHOTCLASS_YAML)
    snapclass_data["metadata"]["name"] = snapclass_name
    snapclass_data["deletionPolicy"] = constants.RECLAIM_POLICY_RETAIN
    snapclass_data["parameters"][
        constants.CSI_SNAPSHOTTER_LIST_SECRET_NAME_PARAM
    ] = constants.CEPHFS_PROVISIONER_SECRET
    snapclass_data["parameters"][
        constants.CSI_SNAPSHOTTER_LIST_SECRET_NAMESPACE_PARAM
    ] = config.ENV_DATA["cluster_namespace"]
    snapclass = ocs.OCS(**snapclass_data)
    assert snapclass.create(
        do_reload=True
    ), f"Failed to create VolumeSnapshotClass {snapclass_name}"
    log.info("Created VolumeSnapshotClass %s", snapclass_name)

    def _teardown():
        snapclass.delete()
        log.info("Deleted VolumeSnapshotClass %s", snapclass_name)

    return _teardown


def create_provider_retain_cephfs_snapclass(snapclass_name, storage_client_name):
    """
    Create a CephFS VolumeSnapshotClass with deletionPolicy Retain.

    In multicluster (provider-consumer) mode: creates the snapclass on the
    provider, registers it on the matching StorageConsumer so it propagates
    to the client, and waits for it to appear on the client cluster.

    In standalone (single-cluster) mode: creates the snapclass directly on
    the current cluster without StorageConsumer patching.

    Must be called while the active config context is the *client* cluster
    so that ``storage_client_name`` is resolved correctly before switching.

    Args:
        snapclass_name (str): Name to give the new VolumeSnapshotClass.
        storage_client_name (str): Name of the StorageClient resource on the
            client cluster (used to identify the matching StorageConsumer on
            the provider). Unused in standalone mode.

    Returns:
        callable: A no-argument teardown function that removes the snapclass.
            Register it with ``request.addfinalizer``.
    """
    if not config.multicluster:
        return _create_standalone_retain_snapclass(snapclass_name)

    # Capture the client cluster name before switching to provider context
    # so find_consumer_for_storage_client can disambiguate when multiple
    # consumers share the same storage-client name.
    client_cluster_name = config.ENV_DATA.get("cluster_name")

    with config.RunWithProviderConfigContextIfAvailable():
        snapclass_data = templating.load_yaml(constants.CSI_CEPHFS_SNAPSHOTCLASS_YAML)
        snapclass_data["metadata"]["name"] = snapclass_name
        snapclass_data["deletionPolicy"] = constants.RECLAIM_POLICY_RETAIN
        # Provider-side snapclass requires snapshotter-list-secret params in
        # addition to the snapshotter-secret params in the base template
        # (see DFBUGS-6539 QA verification notes).
        snapclass_data["parameters"][
            constants.CSI_SNAPSHOTTER_LIST_SECRET_NAME_PARAM
        ] = constants.CEPHFS_PROVISIONER_SECRET
        snapclass_data["parameters"][
            constants.CSI_SNAPSHOTTER_LIST_SECRET_NAMESPACE_PARAM
        ] = config.ENV_DATA["cluster_namespace"]
        provider_snapclass = ocs.OCS(**snapclass_data)
        assert provider_snapclass.create(
            do_reload=True
        ), f"Failed to create VolumeSnapshotClass {snapclass_name} on provider"
        log.info("Created VolumeSnapshotClass %s on provider", snapclass_name)

        consumer_name, consumer_data = find_consumer_for_storage_client(
            storage_client_name,
            client_cluster_name=client_cluster_name,
        )
        consumer_ocp = ocp.OCP(
            kind=constants.STORAGECONSUMER,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        current_snapclasses = consumer_data.get("spec", {}).get(
            "volumeSnapshotClasses", []
        )

        updated_snapclasses = [*current_snapclasses, {"name": snapclass_name}]
        consumer_ocp.patch(
            resource_name=consumer_name,
            params=json.dumps({"spec": {"volumeSnapshotClasses": updated_snapclasses}}),
            format_type="merge",
        )
        log.info("Added %s to StorageConsumer %s", snapclass_name, consumer_name)

    log.info(
        "Waiting for VolumeSnapshotClass %s to propagate to client",
        snapclass_name,
    )
    vsc_ocp = ocp.OCP(kind=constants.VOLUMESNAPSHOTCLASS)
    for propagated in TimeoutSampler(
        timeout=60,
        sleep=5,
        func=vsc_ocp.is_exist,
        resource_name=snapclass_name,
    ):
        if propagated:
            log.info("VolumeSnapshotClass %s is available on client", snapclass_name)
            break

    def _teardown():
        with config.RunWithProviderConfigContextIfAvailable():
            consumer_data = consumer_ocp.get(resource_name=consumer_name)
            remaining = [
                sc
                for sc in consumer_data.get("spec", {}).get("volumeSnapshotClasses", [])
                if sc.get("name") != snapclass_name
            ]
            consumer_ocp.patch(
                resource_name=consumer_name,
                params=json.dumps({"spec": {"volumeSnapshotClasses": remaining}}),
                format_type="merge",
            )
            log.info(
                "Removed %s from StorageConsumer %s",
                snapclass_name,
                consumer_name,
            )
            provider_snapclass.delete()
            log.info("Deleted VolumeSnapshotClass %s from provider", snapclass_name)

    return _teardown


def get_cephfs_snap_entries(snap_runner):
    """
    Run ``odf cephfs-snap ls`` and return the parsed snapshot entries.

    Args:
        snap_runner: Runner object with an ``ls()`` method that returns a
            subprocess result (stdout as bytes).

    Returns:
        list[dict]: Parsed entries; empty list if no snapshots exist.
            Each dict has keys: filesystem, subvolume, subvolumegroup,
            snapshot, state.
    """
    result = snap_runner.ls()
    return parse_snap_ls(result.stdout.decode())


def get_cephfs_snap_by_name(snap_entries, cephfs_snap_name):
    """
    Return the snap entry whose Ceph-side snapshot name matches.

    Args:
        snap_entries (list[dict]): Output of :func:`get_cephfs_snap_entries`.
        cephfs_snap_name (str): Ceph snapshot name to search for
            (the ``snapshot`` column, e.g. ``csi-snap-<uuid>``).

    Returns:
        dict: Matching entry.

    Raises:
        AssertionError: If no entry with the given name is found.
    """
    entry = next((e for e in snap_entries if e["snapshot"] == cephfs_snap_name), None)
    assert (
        entry is not None
    ), f"Ceph snapshot '{cephfs_snap_name}' not found in entries: {snap_entries}"
    return entry


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


def delete_volumesnaps_volumesnapcontents(snap_list_names):
    """
    Delete every VolumeSnapshot and its VolumeSnapshotContent for each
    entry in ``snap_list_names``.

    The Ceph-side snapshot is retained due to the Retain deletion policy,
    becoming orphaned.

    Args:
        snap_list_names (list[dict]): Snapshot data as produced by
            ``TestCephFSOrphanedSnapshotAlert.create_retain_cephfs_snapshots``.
            Each dict must contain a ``"snap_obj"`` key holding the k8s
            VolumeSnapshot object.
    """
    for snap_data in snap_list_names:
        snap_obj = snap_data["snap_obj"]
        snapcontent_obj = get_snapshot_content_obj(snap_obj)
        log.info(
            "Deleting VolumeSnapshot '%s' and VolumeSnapshotContent '%s'",
            snap_obj.name,
            snapcontent_obj.name,
        )
        snap_obj.delete()
        snap_obj.ocp.wait_for_delete(snap_obj.name)
        snapcontent_obj.delete()
        snapcontent_obj.ocp.wait_for_delete(snapcontent_obj.name)


def verify_bound_snapshot_delete_rejected(snap_runner, snap_data):
    """
    Attempt to delete a Bound CephFS snapshot via the odf CLI and
    verify the operation does not remove it.

    Two CLI behaviours are handled:
    - Non-zero exit code → ``CommandFailed`` is raised (expected path).
    - Exit code 0 without actually deleting the snapshot (tolerated).

    In either case the rejection error message must appear in the CLI
    output.

    Args:
        snap_runner: Initialised ``ODFCLICephfsSnapRunner`` instance.
        snap_data (dict): Snapshot data dict with keys ``"subvolume"``
            and ``"ceph_snap_name"``.

    Raises:
        AssertionError: If the expected rejection message is absent.
    """
    rejection_pattern = re.compile(r"(?=.*\bbound\b)(?=.*\bdelet)", re.IGNORECASE)
    ceph_snap_name = snap_data["ceph_snap_name"]
    subvolume = snap_data["subvolume"]

    delete_raised = False
    delete_result = None
    try:
        delete_result = snap_runner.delete(subvolume, ceph_snap_name)
    except CommandFailed as ex:
        delete_raised = True
        assert rejection_pattern.search(str(ex)), (
            f"Expected rejection pattern (bound + delet*) not found "
            f"in CommandFailed output: {ex}"
        )
        log.info(
            "Delete of bound snapshot '%s' rejected as expected. Error: %s",
            ceph_snap_name,
            ex,
        )

    if not delete_raised:
        stderr_out = delete_result.stderr.decode() if delete_result is not None else ""
        assert rejection_pattern.search(stderr_out), (
            f"Expected rejection pattern (bound + delet*) not found "
            f"in delete stderr: {stderr_out}"
        )
        log.info(
            "Delete of bound snapshot '%s' rejected as expected (exit 0). " "Error: %s",
            ceph_snap_name,
            stderr_out.strip(),
        )


def wait_and_verify_snapshot_bound(snap_runner, snap_data, timeout=30, sleep=5):
    """
    Poll ``odf cephfs-snap ls`` to confirm that ``snap_data`` remains
    in Bound state, then do a final explicit assertion.

    Args:
        snap_runner: Initialised ``ODFCLICephfsSnapRunner`` instance.
        snap_data (dict): Snapshot data dict with a ``"ceph_snap_name"``
            key.
        timeout (int): Maximum seconds to poll (default 30).
        sleep (int): Seconds between polls (default 5).

    Raises:
        AssertionError: If the snapshot disappears or is not in Bound state
            when first observed after the delete attempt.
        TimeoutExpiredError: If ``get_cephfs_snap_entries`` keeps failing
            for the entire ``timeout`` window.
    """
    ceph_snap_name = snap_data["ceph_snap_name"]
    log.info(
        "Polling to confirm snapshot '%s' is in Bound state",
        ceph_snap_name,
    )
    for snap_entries in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=get_cephfs_snap_entries,
        snap_runner=snap_runner,
    ):
        entry = next(
            (e for e in snap_entries if e["snapshot"] == ceph_snap_name),
            None,
        )
        assert entry is not None, (
            f"Snapshot '{ceph_snap_name}' disappeared after " f"failed delete attempt"
        )
        state = entry["state"]
        assert state == constants.CEPHFS_SNAPSHOT_STATE_BOUND, (
            f"Snapshot '{ceph_snap_name}' is in '{state}' state, " f"expected Bound"
        )
        break
    log.info(
        "Snapshot '%s' confirmed in '%s' state after delete rejection",
        ceph_snap_name,
        constants.CEPHFS_SNAPSHOT_STATE_BOUND,
    )
