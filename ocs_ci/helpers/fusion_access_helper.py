"""
Helper functions specific to Fusion Access (IBM Storage Scale / CNSA) stretch
clusters.

Everything that is common with the ODF arbiter stretch cluster, such as the
zone aware logwriter workload, finding the nodes of a zone or looking for a
write pause, is reused from
:py:mod:`ocs_ci.ocs.resources.stretchcluster` and
:py:mod:`ocs_ci.helpers.stretchcluster_helper`. Only the bits that have no
counterpart there live in this module.

"""

import logging
import re

from datetime import datetime, timedelta, timezone

import ocpnetsplit

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.node import get_all_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import Pod, get_pods_having_label
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def get_fusion_access_filesystem():
    """
    Get the name of the IBM Storage Scale filesystem backing the storage class

    Returns:
        str: Name of the filesystem

    """
    return config.ENV_DATA.get(
        "fusion_access_filesystem", constants.FUSION_ACCESS_FILESYSTEM
    )


def get_scale_core_pod(exclude_nodes=None):
    """
    Get a running IBM Storage Scale core pod to run the 'mm*' commands from.

    There is one core pod per node of the Scale cluster and any of them can be
    asked about the state of the cluster, so the first running one is good
    enough. Nodes can be excluded, asking the core pod of a node that is cut
    off from the rest of the cluster either fails outright or blocks.

    The pod is matched to a node through its 'spec.nodeName' rather than
    through its name. The core pods happen to be named after their node
    today, but relying on that would silently stop excluding anything if the
    naming ever changed.

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            picked

    Returns:
        Pod: Pod object of a running Scale core pod

    Raises:
        UnexpectedBehaviour: If there is no usable core pod

    """
    exclude_nodes = exclude_nodes or []
    for pod_data in get_pods_having_label(
        label=constants.IBM_STORAGE_SCALE_CORE_LABEL,
        namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
    ):
        if (
            pod_data.get("status", {}).get("phase") == constants.STATUS_RUNNING
            and pod_data["metadata"].get("deletionTimestamp") is None
            and pod_data.get("spec", {}).get("nodeName") not in exclude_nodes
        ):
            return Pod(**pod_data)

    raise UnexpectedBehaviour(
        f"No running IBM Storage Scale core pod found outside of the nodes "
        f"{exclude_nodes}"
    )


def get_filesystem_mounted_nodes(exclude_nodes=None):
    """
    Get the nodes the IBM Storage Scale filesystem is currently mounted on.

    'mmlsmount <fs> -L' lists one line per node holding the mount, with the
    daemon node name in the second column. The node names of the Scale cluster
    match the OCP node names, so the short name is what gets returned.

    An empty set is returned when the command cannot be run at all, which is
    what happens on a node that has been cut off and no longer has quorum.
    That is a state a caller polling through a network partition has to cope
    with rather than an error.

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command

    Returns:
        set: Names of the nodes the filesystem is mounted on

    """
    filesystem = get_fusion_access_filesystem()
    try:
        core_pod = get_scale_core_pod(exclude_nodes=exclude_nodes)
        out = core_pod.exec_cmd_on_pod(
            command=f"mmlsmount {filesystem} -L",
            container_name="gpfs",
            out_yaml_format=False,
        )
    except (CommandFailed, UnexpectedBehaviour) as ex:
        logger.info(f"mmlsmount could not be run: {ex}")
        return set()

    # Lines look like '  10.1.160.212    compute-3  ', the header and the
    # summary line above them have no leading IP address
    mounted_nodes = set()
    for line in str(out).splitlines():
        fields = line.split()
        if len(fields) >= 2 and re.fullmatch(r"[\d]{1,3}(\.[\d]{1,3}){3}", fields[0]):
            mounted_nodes.add(fields[1].split(".")[0])

    logger.info(f"Filesystem {filesystem} is mounted on {sorted(mounted_nodes)}")
    return mounted_nodes


def wait_for_filesystem_mounted(node_names, timeout=1800, sleep_time=30):
    """
    Wait for the IBM Storage Scale filesystem to be mounted on the given nodes.

    A node reports Ready well before the GPFS daemon has brought its NSDs back
    up and remounted the filesystem, so this is what tells a zone is actually
    usable again rather than merely reachable.

    Args:
        node_names (list): Names of the nodes expected to hold the mount
        timeout (int): Time in seconds to wait for the mount
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If the filesystem is not mounted on all the nodes
            within the timeout

    """
    logger.info(f"Waiting for the filesystem to be mounted on {node_names}")
    try:
        for mounted_nodes in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=get_filesystem_mounted_nodes
        ):
            not_mounted = set(node_names) - mounted_nodes
            if not not_mounted:
                logger.info(f"The filesystem is mounted on {node_names}")
                return
            logger.info(
                f"The filesystem is not mounted yet on {sorted(not_mounted)}, waiting"
            )
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"The filesystem is not mounted on some of the nodes {node_names} "
            f"after {timeout} seconds, the cluster did not recover from the "
            f"network partition"
        )


def get_gpfs_inactive_nodes(exclude_nodes=None):
    """
    Get the nodes of the Storage Scale cluster whose GPFS daemon is not active.

    'mmgetstate -a' lists one line per node, the node name in the second column
    and its state in the third. While the cluster has no quorum the command
    fails outright, which is reported as every node being inactive rather than
    raising, so that a caller polling for recovery keeps waiting.

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command

    Returns:
        set: Names of the nodes whose GPFS daemon is not active

    """
    try:
        core_pod = get_scale_core_pod(exclude_nodes=exclude_nodes)
        out = core_pod.exec_cmd_on_pod(
            command="mmgetstate -a", container_name="gpfs", out_yaml_format=False
        )
    except (CommandFailed, UnexpectedBehaviour) as ex:
        logger.info(f"mmgetstate is not answering yet: {ex}")
        return {"<cluster has no quorum>"}

    inactive = set()
    for line in str(out).splitlines():
        fields = line.split()
        # Skip the header and the separator, a node line starts with its number
        if len(fields) >= 3 and fields[0].isdigit():
            if fields[2] != "active":
                inactive.add(fields[1])

    return inactive


def wait_for_gpfs_cluster_active(timeout=1800, sleep_time=30, exclude_nodes=None):
    """
    Wait for every node of the Storage Scale cluster to report GPFS active.

    A node being reachable again says nothing about GPFS on it, the daemon
    still has to rejoin the cluster after it was expelled. After a network
    partition heals this is the check that tells the storage cluster really
    recovered.

    Args:
        timeout (int): Time in seconds to wait for the cluster to be active
        sleep_time (int): Time in seconds between two checks
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command

    Raises:
        UnexpectedBehaviour: If any node is still not active within the timeout

    """
    logger.info("Waiting for every node of the Storage Scale cluster to be active")
    try:
        for inactive in TimeoutSampler(
            timeout=timeout,
            sleep=sleep_time,
            func=get_gpfs_inactive_nodes,
            exclude_nodes=exclude_nodes,
        ):
            if not inactive:
                logger.info("Every node of the Storage Scale cluster is active")
                return
            logger.info(f"GPFS is not active yet on {sorted(inactive)}, waiting")
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"Some nodes of the Storage Scale cluster are still not active "
            f"after {timeout} seconds"
        )


def schedule_zone_netsplit(split_name, duration, lead_minutes=5):
    """
    Schedule a network partition between the zones of the stretch cluster.

    The split is applied by the ocp-network-split systemd units that the
    deployment lays down on every node, so it needs the cluster to have been
    deployed with DEPLOYMENT['network_split_setup'] enabled. The units drop
    the traffic between the node addresses of the zones named in
    'split_name', which is how a site to site link failure is reproduced
    without touching the SAN.

    The split is scheduled a few minutes ahead rather than started right away,
    all the nodes have to pick the same start time and they only poll their
    timer periodically.

    Args:
        split_name (str): Network split configuration, one of the
            'NETSPLIT_*' constants. The zone letters are assigned at
            deployment time, 'a' is always the arbiter zone and 'b' and 'c'
            are the two data zones
        duration (int): Number of minutes the partition stays in place. The
            units lift it on their own afterwards, there is nothing to undo
        lead_minutes (int): Number of minutes to schedule the start ahead of
            now

    Returns:
        tuple: Start and end datetime of the partition, both in UTC

    """
    target_time = datetime.now() + timedelta(minutes=lead_minutes)
    start_time = target_time.astimezone(timezone.utc)
    end_time = start_time + timedelta(minutes=duration)

    ocpnetsplit.main.schedule_split(
        nodes=get_all_nodes(),
        split_name=split_name,
        target_dt=target_time,
        target_length=duration,
        kubeconfig=config.RUN.get("kubeconfig"),
    )
    logger.info(f"Network split {split_name} scheduled from {start_time} to {end_time}")
    return start_time, end_time


def get_zone_nodes(zone):
    """
    Get the names of the nodes of a zone

    Args:
        zone (str): Value of the 'topology.kubernetes.io/zone' label

    Returns:
        set: Names of the nodes labeled with that zone

    """
    node_items = (
        OCP(kind=constants.NODE)
        .get(selector=f"{constants.ZONE_LABEL}={zone}")
        .get("items", [])
    )
    return {node["metadata"]["name"] for node in node_items}


def wait_for_filesystem_available_in_a_data_zone(
    data_zones, timeout=900, sleep_time=30
):
    """
    Wait until the filesystem is mounted across a whole data zone.

    During a partition between the two data zones the minority side is
    expelled from the Storage Scale cluster and loses its mount, while the
    side that keeps quorum carries on serving. This asserts that at least one
    data zone is still whole, which is what 'workloads continue on the
    accessible zone' means in practice. Which of the two survives is decided
    by the arbiter and is not fixed in advance, so it is discovered rather
    than assumed.

    Args:
        data_zones (list): The two data zone label values
        timeout (int): Time in seconds to wait
        sleep_time (int): Time in seconds between two checks

    Returns:
        str: The data zone that kept the filesystem

    Raises:
        UnexpectedBehaviour: If neither data zone has the filesystem mounted
            on all its nodes within the timeout

    """
    zone_nodes = {zone: get_zone_nodes(zone) for zone in data_zones}
    logger.info(f"Looking for a data zone that kept the filesystem: {zone_nodes}")

    def _whole_zone_mounted():
        # The core pod of an expelled node cannot answer, so the surviving
        # side has to be asked. Trying each zone in turn covers both outcomes
        for zone, nodes in zone_nodes.items():
            other = set().union(*[n for z, n in zone_nodes.items() if z != zone])
            mounted = get_filesystem_mounted_nodes(exclude_nodes=list(other))
            if nodes and nodes.issubset(mounted):
                return zone
        return None

    try:
        for surviving_zone in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=_whole_zone_mounted
        ):
            if surviving_zone:
                logger.info(
                    f"Data zone {surviving_zone} kept the filesystem mounted "
                    f"through the partition"
                )
                return surviving_zone
            logger.info("Neither data zone has the filesystem yet, waiting")
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"Neither of the data zones {data_zones} kept the filesystem "
            f"mounted during the network partition, the whole cluster lost "
            f"access to its storage"
        )


def verify_io_on_pod(pod_obj, mount_path):
    """
    Verify a workload can still write to and read back from its volume.

    Used during a network partition to show the workloads of the accessible
    zone carry on rather than merely staying in Running state, a pod whose
    filesystem has gone away keeps running until something touches the mount.

    Args:
        pod_obj (Pod): Pod object of the workload
        mount_path (str): Path the workload volume is mounted at

    Raises:
        AssertionError: If the data read back does not match what was written

    """
    file_path = f"{mount_path}/netsplit-io-check.txt"
    written = f"netsplit-io-{datetime.now(timezone.utc).strftime('%H%M%S%f')}"
    # 'sh' and not the default 'bash', the busybox based workload images do
    # not ship bash
    pod_obj.exec_sh_cmd_on_pod(command=f"echo {written} > {file_path} && sync", sh="sh")
    read_back = pod_obj.exec_sh_cmd_on_pod(command=f"cat {file_path}", sh="sh").strip()
    assert read_back == written, (
        f"IO check failed on {pod_obj.name}, wrote '{written}' to {file_path} "
        f"but read back '{read_back}'"
    )
    logger.info(f"IO on {pod_obj.name} works, wrote and read back '{written}'")
