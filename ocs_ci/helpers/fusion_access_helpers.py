"""
Helpers for verifying IBM Storage Scale / Fusion Access (SAN) storage health.

These are the Fusion Access equivalent of the Ceph-side checks used by the ODF
node-replacement tests. On a Fusion Access cluster there is no CephCluster and
no OSDs, so "storage is healthy" has to be asserted against the Scale objects
instead:

    Filesystem  -- conditions Success / Healthy / Mounted, mountedNodeCount
    LocalDisk   -- conditions Healthy / Available / Used, and which node the
                   disk is currently attached through
    Daemon      -- the GPFS (mmfsd) daemon pods, one per Scale worker
    CSI         -- the Scale CSI node plugin DaemonSet pod per worker

The LocalDisk checks matter most during node replacement: a LocalDisk CR records
``spec.node`` and a kernel device path (``/dev/sdX``). When the node holding that
binding is replaced, the binding must move to a surviving node and the device
path may renumber on the replacement. That recovery is the thing most likely to
break, so it is asserted explicitly rather than inferred from "all pods running".
"""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

SCALE_NS = constants.IBM_STORAGE_SCALE_NAMESPACE


def _condition_map(resource_data):
    """
    Flatten a resource's ``status.conditions`` into ``{type: status}``.

    Args:
        resource_data (dict): Resource definition

    Returns:
        dict: Mapping of condition type to its status string
    """
    conditions = (resource_data.get("status", {}) or {}).get("conditions", []) or []
    return {c.get("type"): c.get("status") for c in conditions}


def get_scale_filesystems(namespace=SCALE_NS):
    """
    Get all Spectrum Scale Filesystem resources.

    Args:
        namespace (str): Scale namespace

    Returns:
        list: Filesystem resource dicts
    """
    fs_ocp = OCP(kind=constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND, namespace=namespace)
    return fs_ocp.get().get("items", [])


def get_scale_filesystem_names(namespace=SCALE_NS):
    """
    Get the names of all Spectrum Scale Filesystems.

    Args:
        namespace (str): Scale namespace

    Returns:
        list: Filesystem names
    """
    return [fs["metadata"]["name"] for fs in get_scale_filesystems(namespace)]


def check_filesystem_healthy(fs_name, expected_mounted_nodes=None, namespace=SCALE_NS):
    """
    Check a single Filesystem is created, healthy and mounted.

    Args:
        fs_name (str): Filesystem name
        expected_mounted_nodes (int): Expected ``status.mounting.mountedNodeCount``.
            Skipped when None.
        namespace (str): Scale namespace

    Returns:
        bool: True if the filesystem is healthy
    """
    fs_ocp = OCP(kind=constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND, namespace=namespace)
    try:
        fs_data = fs_ocp.get(resource_name=fs_name)
    except Exception as ex:
        logger.warning(f"Could not read Filesystem '{fs_name}': {ex}")
        return False

    conditions = _condition_map(fs_data)
    for cond in ("Success", "Healthy", "Mounted"):
        if conditions.get(cond) != "True":
            logger.warning(
                f"Filesystem '{fs_name}' condition '{cond}' is "
                f"'{conditions.get(cond)}', expected 'True'. All: {conditions}"
            )
            return False

    if expected_mounted_nodes is not None:
        mounted = (fs_data.get("status", {}).get("mounting", {}) or {}).get(
            "mountedNodeCount"
        )
        if mounted != expected_mounted_nodes:
            logger.warning(
                f"Filesystem '{fs_name}' mountedNodeCount={mounted}, "
                f"expected {expected_mounted_nodes}"
            )
            return False

    logger.info(
        f"Filesystem '{fs_name}' is healthy "
        f"(conditions={conditions}, mountedNodeCount={expected_mounted_nodes})"
    )
    return True


def check_localdisks_healthy(
    expected_count=None, absent_nodes=None, namespace=SCALE_NS
):
    """
    Check all LocalDisk resources are healthy and not bound to a removed node.

    Args:
        expected_count (int): Expected number of LocalDisks. Skipped when None.
        absent_nodes (list): Node names that must NOT appear in any
            ``LocalDisk.spec.node``. Use the replaced node name here.
        namespace (str): Scale namespace

    Returns:
        bool: True if every LocalDisk is healthy and correctly bound
    """
    ld_ocp = OCP(kind=constants.IBM_STORAGE_SCALE_LOCALDISK_KIND, namespace=namespace)
    disks = ld_ocp.get().get("items", [])

    if expected_count is not None and len(disks) != expected_count:
        logger.warning(f"Found {len(disks)} LocalDisks, expected {expected_count}")
        return False

    absent_nodes = absent_nodes or []
    healthy = True
    for disk in disks:
        name = disk["metadata"]["name"]
        spec = disk.get("spec", {}) or {}
        bound_node = spec.get("node")
        conditions = _condition_map(disk)

        for cond in ("Healthy", "Available"):
            if conditions.get(cond) != "True":
                logger.warning(
                    f"LocalDisk '{name}' condition '{cond}' is "
                    f"'{conditions.get(cond)}', expected 'True'. All: {conditions}"
                )
                healthy = False

        if bound_node in absent_nodes:
            logger.warning(
                f"LocalDisk '{name}' is still bound to removed node "
                f"'{bound_node}'. The disk binding did not move to a surviving node."
            )
            healthy = False

        logger.info(
            f"LocalDisk '{name}': node='{bound_node}' device='{spec.get('device')}' "
            f"conditions={conditions}"
        )

    return healthy


def check_scale_daemon_ready(expected_pods=None, namespace=SCALE_NS):
    """
    Check the Scale Daemon (GPFS/mmfsd) CR reports all its pods running.

    Args:
        expected_pods (int): Expected pod count, normally the Scale worker count.
            Skipped when None.
        namespace (str): Scale namespace

    Returns:
        bool: True if the daemon is available with all pods running
    """
    daemon_ocp = OCP(kind=constants.IBM_STORAGE_SCALE_DAEMON_KIND, namespace=namespace)
    try:
        daemon = daemon_ocp.get(resource_name=constants.IBM_STORAGE_SCALE_DAEMON_NAME)
    except Exception as ex:
        logger.warning(f"Could not read Scale Daemon: {ex}")
        return False

    status = daemon.get("status", {}) or {}
    conditions = _condition_map(daemon)

    # The operator reports these counters as strings, e.g. pods={'desired':'3'}
    def _as_int(value, default=-1):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    pods = status.get("pods", {}) or {}
    pods_status = status.get("podsStatus", {}) or {}
    quorum = status.get("quorumPods", {}) or {}

    desired = _as_int(pods.get("desired"))
    total = _as_int(pods.get("total"))
    running = _as_int(pods_status.get("running"))
    quorum_running = _as_int(quorum.get("running"))
    quorum_total = _as_int(quorum.get("total"))

    if conditions.get("Available") != "True":
        logger.warning(
            f"Scale Daemon condition Available='{conditions.get('Available')}', "
            f"expected 'True'. All: {conditions}"
        )
        return False

    if conditions.get("Healthy") != "True":
        logger.warning(
            f"Scale Daemon condition Healthy='{conditions.get('Healthy')}', "
            f"expected 'True'. All: {conditions}"
        )
        return False

    if desired != total or running != desired:
        logger.warning(
            f"Scale Daemon pods desired={desired} total={total} running={running}; "
            f"podsStatus={pods_status}"
        )
        return False

    # GPFS needs its quorum nodes back after a replacement, otherwise the
    # filesystem can look mounted while the cluster is one failure from down.
    if quorum_total > 0 and quorum_running != quorum_total:
        logger.warning(
            f"Scale Daemon quorum pods running={quorum_running} of {quorum_total}"
        )
        return False

    if expected_pods is not None and desired != expected_pods:
        logger.warning(f"Scale Daemon desired pods={desired}, expected {expected_pods}")
        return False

    logger.info(
        f"Scale Daemon healthy: pods={desired} running={running} "
        f"quorum={quorum_running}/{quorum_total}"
    )
    return True


def check_scale_csi_pod_on_node(node_name, namespace=SCALE_NS):
    """
    Check the Scale CSI node plugin pod is running on a given node.

    After a node replacement the CSI DaemonSet must schedule onto the new node,
    otherwise volumes cannot be mounted there even though the node is Ready.

    Args:
        node_name (str): Node name
        namespace (str): Scale namespace

    Returns:
        bool: True if a running Scale CSI pod exists on that node
    """
    pod_ocp = OCP(kind=constants.POD, namespace=namespace)
    pods = pod_ocp.get().get("items", [])
    for pod_data in pods:
        name = pod_data["metadata"]["name"]
        spec_node = pod_data.get("spec", {}).get("nodeName")
        phase = pod_data.get("status", {}).get("phase")
        if (
            "ibm-spectrum-scale-csi" in name
            and spec_node == node_name
            and phase == constants.STATUS_RUNNING
        ):
            logger.info(f"Scale CSI pod '{name}' is Running on node '{node_name}'")
            return True

    logger.warning(f"No running Scale CSI pod found on node '{node_name}'")
    return False


def check_gpfs_daemon_pod_on_node(node_name, namespace=SCALE_NS):
    """
    Check the GPFS daemon pod for a node exists and is running.

    The Scale operator names the mmfsd pod after the node it runs on, so a
    replacement node joining the GPFS cluster shows up as a pod with that name.

    Args:
        node_name (str): Node name
        namespace (str): Scale namespace

    Returns:
        bool: True if the node has a running GPFS daemon pod
    """
    pod_ocp = OCP(kind=constants.POD, namespace=namespace)
    try:
        pod_data = pod_ocp.get(resource_name=node_name)
    except Exception as ex:
        logger.warning(f"No GPFS daemon pod named '{node_name}' found: {ex}")
        return False

    phase = pod_data.get("status", {}).get("phase")
    if phase != constants.STATUS_RUNNING:
        logger.warning(f"GPFS daemon pod '{node_name}' phase='{phase}'")
        return False

    logger.info(f"GPFS daemon pod for node '{node_name}' is Running")
    return True


def fusion_access_storage_health_check(
    expected_worker_count,
    absent_nodes=None,
    new_node_name=None,
    expected_localdisk_count=None,
    filesystem_names=None,
    timeout=1200,
    sleep=30,
):
    """
    Full Fusion Access / Spectrum Scale storage health check.

    This is the Fusion Access counterpart to the Ceph health + rebalance checks
    in the ODF node-replacement tests. Every sub-check is retried until timeout,
    because the Scale operator takes time to re-form the cluster after a node is
    replaced.

    Args:
        expected_worker_count (int): Expected number of Scale worker nodes, used
            for daemon pod count and filesystem mountedNodeCount
        absent_nodes (list): Node names that must no longer appear in any
            LocalDisk binding (the replaced node)
        new_node_name (str): If given, assert the new node has both a GPFS
            daemon pod and a Scale CSI pod running
        expected_localdisk_count (int): Expected LocalDisk count
        filesystem_names (list): Filesystems to verify. Discovered when None.
        timeout (int): Overall timeout per sub-check in seconds
        sleep (int): Poll interval in seconds

    Raises:
        AssertionError: If any sub-check does not pass within the timeout
    """
    absent_nodes = absent_nodes or []
    filesystem_names = filesystem_names or get_scale_filesystem_names()
    assert filesystem_names, "No Spectrum Scale Filesystem found on the cluster"
    logger.info(f"Verifying Fusion Access storage health for: {filesystem_names}")

    def _wait(func, description, **kwargs):
        logger.info(f"Waiting for: {description}")
        sampler = TimeoutSampler(timeout=timeout, sleep=sleep, func=func, **kwargs)
        assert sampler.wait_for_func_status(
            result=True
        ), f"Fusion Access health check failed: {description}"
        logger.info(f"OK: {description}")

    _wait(
        check_scale_daemon_ready,
        f"Scale GPFS daemon with {expected_worker_count} running pods",
        expected_pods=expected_worker_count,
    )

    _wait(
        check_localdisks_healthy,
        f"all LocalDisks healthy and not bound to {absent_nodes}",
        expected_count=expected_localdisk_count,
        absent_nodes=absent_nodes,
    )

    for fs_name in filesystem_names:
        _wait(
            check_filesystem_healthy,
            f"Filesystem '{fs_name}' healthy and mounted on "
            f"{expected_worker_count} nodes",
            fs_name=fs_name,
            expected_mounted_nodes=expected_worker_count,
        )

    if new_node_name:
        _wait(
            check_gpfs_daemon_pod_on_node,
            f"new node '{new_node_name}' joined the GPFS cluster",
            node_name=new_node_name,
        )
        _wait(
            check_scale_csi_pod_on_node,
            f"Scale CSI node plugin running on '{new_node_name}'",
            node_name=new_node_name,
        )

    logger.info("Fusion Access / Spectrum Scale storage health verified")
