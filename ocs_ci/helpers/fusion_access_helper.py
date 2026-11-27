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

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
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
    enough. Nodes can be excluded, asking the core pod of a node that has just
    lost its storage would either fail outright or block on the broken IO path.

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
        pod_obj = Pod(**pod_data)
        if (
            pod_obj.status() == constants.STATUS_RUNNING
            and pod_obj.get()["metadata"].get("deletionTimestamp") is None
            and pod_obj.get()["spec"].get("nodeName") not in exclude_nodes
        ):
            return pod_obj

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

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command

    Returns:
        set: Names of the nodes the filesystem is mounted on

    """
    filesystem = get_fusion_access_filesystem()
    core_pod = get_scale_core_pod(exclude_nodes=exclude_nodes)
    out = core_pod.exec_cmd_on_pod(
        command=f"mmlsmount {filesystem} -L",
        container_name="gpfs",
        out_yaml_format=False,
    )

    # Lines look like '  10.1.160.212    compute-3  ', the header and the
    # summary line above them have no leading IP address
    mounted_nodes = set()
    for line in str(out).splitlines():
        fields = line.split()
        if len(fields) >= 2 and re.fullmatch(r"[\d]{1,3}(\.[\d]{1,3}){3}", fields[0]):
            mounted_nodes.add(fields[1].split(".")[0])

    logger.info(f"Filesystem {filesystem} is mounted on {sorted(mounted_nodes)}")
    return mounted_nodes


def wait_for_filesystem_mounted(node_names, timeout=900, sleep_time=30):
    """
    Wait for the IBM Storage Scale filesystem to be mounted on the given nodes.

    A node reports Ready, and its LUNs come back, well before the GPFS daemon
    has brought its NSDs back up and remounted the filesystem, so this is what
    tells a zone is actually usable again rather than merely powered on.

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
            f"after {timeout} seconds, the recovered zone did not rejoin the "
            f"Storage Scale cluster"
        )


def is_api_available(timeout=20):
    """
    Check whether the OpenShift API server answers.

    Used by the tests that take down a majority of the control plane nodes,
    where losing the API is the expected condition rather than a failure.

    Args:
        timeout (int): Time in seconds to allow the request to take

    Returns:
        bool: True if the API answered, False otherwise

    """
    try:
        # '--request-timeout' bounds the call, without it the request hangs
        # for the whole duration of the outage instead of failing fast
        OCP().exec_oc_cmd(
            f"get nodes --request-timeout={timeout}s", out_yaml_format=False
        )
        return True
    except Exception as ex:
        logger.debug(f"The OpenShift API did not answer: {ex}")
        return False


def wait_for_api_unavailable(timeout=600, sleep_time=20):
    """
    Wait for the OpenShift API server to stop answering.

    Taking down a data zone together with the arbiter takes two of the three
    control plane nodes with it, so etcd loses quorum and the API goes away.
    That is the condition under test, waiting for it is how the test confirms
    the quorum loss really happened rather than assuming it.

    Args:
        timeout (int): Time in seconds to wait for the API to go away
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If the API is still answering within the timeout

    """
    logger.info("Waiting for the OpenShift API to become unavailable")
    try:
        for available in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=is_api_available
        ):
            if not available:
                logger.info("The OpenShift API is unavailable, as expected")
                return
            logger.info("The OpenShift API is still answering, waiting")
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"The OpenShift API is still answering after {timeout} seconds. "
            f"etcd did not lose quorum, so the nodes that were supposed to go "
            f"down either did not, or they do not hold a majority of the "
            f"control plane"
        )


def wait_for_api_available(timeout=1800, sleep_time=30):
    """
    Wait for the OpenShift API server to answer again.

    etcd has to re-form quorum and catch up before the API serves requests, so
    this takes considerably longer than the nodes booting. Nothing else in the
    test can run until it returns.

    Args:
        timeout (int): Time in seconds to wait for the API to come back
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If the API does not answer within the timeout

    """
    logger.info("Waiting for the OpenShift API to become available again")
    try:
        for available in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=is_api_available
        ):
            if available:
                logger.info("The OpenShift API is answering again")
                return
            logger.info("The OpenShift API is still down, waiting")
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"The OpenShift API did not come back within {timeout} seconds, "
            f"etcd failed to re-form quorum after the nodes were started"
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

    A node reporting Ready says nothing about GPFS on it: the core pod still
    has to start, load the kmod and rejoin the cluster. After a quorum loss the
    whole cluster has to re-form before any node is active, so this is the
    check that tells the storage cluster really recovered.

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
