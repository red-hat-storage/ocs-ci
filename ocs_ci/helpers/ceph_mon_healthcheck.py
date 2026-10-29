import json
import logging
import random
import re
import time

from ocs_ci.ocs.node import get_mon_running_nodes, get_node_mon_ids
from ocs_ci.ocs.resources.storage_cluster import get_default_storagecluster
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework import config
from ocs_ci.ocs.resources.deployment import get_mon_deployments
from ocs_ci.ocs.resources.pod import (
    wait_for_matching_pattern_in_pod_logs,
    get_operator_pods,
    get_mon_pod_by_id,
    get_pod_node,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


def patch_storagecluster_mon_healthcheck(
    mon_timeout, mon_interval="20s", sc_obj=None
) -> bool:
    """
    Patch the ceph mon healthcheck configuration in the storagecluster resource

    Args:
        mon_timeout (str): The timeout value for the mon healthcheck(e.g., "3m", "20m", 600s")
        mon_interval (str): The interval value for the mon healthcheck (e.g., "20s", "1m")
        sc_obj (ocs_ci.ocs.ocp.OCP): StorageCluster. If None, it will be fetched.

    Returns:
        bool: True if the patch was successful, False otherwise

    """
    sc_obj = sc_obj or get_default_storagecluster()
    patch_ops = [
        {
            "op": "add",
            "path": "/spec/managedResources/cephCluster/healthCheck",
            "value": {
                "daemonHealth": {
                    "mon": {
                        "interval": mon_interval,
                        "timeout": mon_timeout,
                    }
                }
            },
        }
    ]
    return sc_obj.patch(params=json.dumps(patch_ops), format_type="json")


def delete_storagecluster_mon_healthcheck(sc_obj=None) -> bool:
    """
    Delete the ceph mon healthcheck configuration from the storagecluster resource

    Args:
        sc_obj (ocs_ci.ocs.ocp.OCP): StorageCluster object. If None, it will be fetched.

    Returns:
        bool: True if the deletion was successful, False otherwise

    """
    sc_obj = sc_obj or get_default_storagecluster()
    patch_ops = [
        {
            "op": "remove",
            "path": "/spec/managedResources/cephCluster/healthCheck",
        }
    ]
    return sc_obj.patch(params=json.dumps(patch_ops), format_type="json")


def get_storagecluster_mon_healthcheck(sc_obj=None) -> dict:
    """
    Get the ceph mon healthcheck status from the storagecluster resource
    Handles both object structures:
    - sc_obj["items"][0]["spec"]["managedResources"]["cephCluster"]["healthCheck"]["daemonHealth"]["mon"]
    - sc_obj["spec"]["managedResources"]["cephCluster"]["healthCheck"]["daemonHealth"]["mon"]

    Args:
        sc_obj (ocs_ci.ocs.ocp.OCP): StorageCluster object. If None, it will be fetched.

    Returns:
        dict: The mon healthCheck configuration

    """
    sc_obj = sc_obj or get_default_storagecluster()

    sc_dict = sc_obj.get()
    base_sc_dict = sc_dict["items"][0] if sc_dict.get("items") else sc_dict
    ceph_cluster = base_sc_dict["spec"]["managedResources"]["cephCluster"]
    mon_healthcheck = (
        ceph_cluster.get("healthCheck", {}).get("daemonHealth", {}).get("mon", {})
    )
    logger.info(f"Current mon healthcheck configuration: {mon_healthcheck}")
    return mon_healthcheck


def get_cephcluster_mon_healthcheck(cc_obj=None) -> dict:
    """
    Get the ceph mon healthcheck status from the cephcluster resource
    Handles both object structures:
    - cc_obj["items"][0]["spec"]["healthCheck"]["daemonHealth"]["mon"]
    - cc_obj["spec"]["healthCheck"]["daemonHealth"]["mon"]

    Args:
        cc_obj (object): CephCluster object. If None, it will be fetched.

    Returns:
        dict: The mon healthCheck configuration

    """
    if not cc_obj:
        cc_obj = ocp.OCP(
            kind=constants.CEPH_CLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.CEPH_CLUSTER_NAME,
        )

    cc_dict = cc_obj.get()
    base_cc_dict = cc_dict["items"][0] if cc_dict.get("items") else cc_dict
    mon_healthcheck = (
        base_cc_dict.get("spec", {})
        .get("healthCheck", {})
        .get("daemonHealth", {})
        .get("mon", {})
    )
    logger.info(f"Current cephcluster mon healthcheck configuration: {mon_healthcheck}")
    return mon_healthcheck


def get_cephcluster_phase(cc_obj=None) -> str:
    """
    Get the current phase of the cephcluster resource

    Args:
        cc_obj (object): CephCluster object. If None, it will be fetched.

    Returns:
        str: The cephcluster phase (e.g. 'Ready', 'Progressing'), or an empty
            string if the phase is not set yet.

    """
    if not cc_obj:
        cc_obj = ocp.OCP(
            kind=constants.CEPH_CLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.CEPH_CLUSTER_NAME,
        )

    cc_dict = cc_obj.get()
    base_cc_dict = cc_dict["items"][0] if cc_dict.get("items") else cc_dict
    return base_cc_dict.get("status", {}).get("phase", "")


def wait_for_cephcluster_to_settle(timeout=900, sleep=20, stable_samples=3) -> None:
    """
    Wait until the cephcluster phase stays Ready for a few consecutive samples.

    The rook-ceph-operator stops the mon health check while a cephcluster
    orchestration is running, and resumes it only when the orchestration ends.
    Taking a mon down while an orchestration is in progress can therefore
    suppress the mon healthcheck timeout entirely, so the caller should wait
    for the cluster to settle first.

    Args:
        timeout (int): Maximum time to wait in seconds. Defaults to 900 seconds.
        sleep (int): Time to wait between checks in seconds. Defaults to 20 seconds.
        stable_samples (int): Number of consecutive Ready samples required.

    Raises:
        TimeoutExpiredError: If the cephcluster does not stay Ready within the timeout.

    """
    logger.info(
        f"Waiting up to {timeout} seconds for the cephcluster phase to stay "
        f"{constants.STATUS_READY} for {stable_samples} consecutive samples."
    )
    cc_obj = ocp.OCP(
        kind=constants.CEPH_CLUSTER,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.CEPH_CLUSTER_NAME,
    )
    ready_samples = 0
    try:
        for phase in TimeoutSampler(
            timeout=timeout, sleep=sleep, func=get_cephcluster_phase, cc_obj=cc_obj
        ):
            if phase == constants.STATUS_READY:
                ready_samples += 1
                logger.info(
                    f"Cephcluster phase is {phase} "
                    f"({ready_samples}/{stable_samples} consecutive samples)"
                )
                if ready_samples >= stable_samples:
                    logger.info("The cephcluster has settled")
                    return
            else:
                ready_samples = 0
                logger.info(
                    f"Cephcluster phase is '{phase}'. Waiting for it to become "
                    f"{constants.STATUS_READY}"
                )
    except TimeoutExpiredError as ex:
        raise TimeoutExpiredError(
            f"The cephcluster did not stay in the phase {constants.STATUS_READY} "
            f"for {stable_samples} consecutive samples within {timeout} seconds."
        ) from ex


def verify_mon_healthcheck_consistency(sc_obj=None, cc_obj=None) -> bool:
    """
    Verify that the mon healthcheck configurations in StorageCluster and CephCluster are consistent.

    Args:
        sc_obj (ocs_ci.ocs.ocp.OCP): StorageCluster object. If None, it will be fetched.
        cc_obj (object): CephCluster object. If None, it will be fetched.

    Returns:
        bool: True if configurations match, False otherwise

    """
    sc_mon_healthcheck = get_storagecluster_mon_healthcheck(sc_obj)
    cc_mon_healthcheck = get_cephcluster_mon_healthcheck(cc_obj)

    if not sc_mon_healthcheck == cc_mon_healthcheck:
        logger.warning(
            f"Mon healthcheck mismatch: StorageCluster {sc_mon_healthcheck} != "
            f"CephCluster {cc_mon_healthcheck}"
        )
        return False

    logger.info(
        "Mon healthcheck configurations are consistent between StorageCluster and CephCluster."
    )
    return True


def wait_for_mon_healthcheck_consistency(
    sc_obj=None, cc_obj=None, timeout=120, sleep=10
) -> None:
    """
    Wait until the mon healthcheck configurations in StorageCluster and CephCluster are consistent.

    Args:
        sc_obj (ocs_ci.ocs.ocp.OCP): StorageCluster object. If None, it will be fetched.
        cc_obj (object): CephCluster object. If None, it will be fetched.
        timeout (int): Maximum time to wait in seconds. Defaults to 300 seconds (5 minutes).
        sleep (int): Time to wait between checks in seconds. Defaults to 20 seconds.

    Raises:
        TimeoutExpiredError: If the configurations do not match within the timeout.

    """
    logger.info(
        f"Waiting for mon healthcheck consistency between StorageCluster and CephCluster "
        f"for up to {timeout} seconds."
    )
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=verify_mon_healthcheck_consistency,
        sc_obj=sc_obj,
        cc_obj=cc_obj,
    )
    if not sample.wait_for_func_status(result=True):
        raise TimeoutExpiredError(
            "Mon healthcheck configurations did not become consistent within "
            f"{timeout} seconds."
        )


def wait_for_mon_healthcheck_timeout_in_logs(
    mon_id,
    since="1m",
    timeout=300,
    sleep=20,
) -> list:
    """
    Wait for expected mon healthcheck timeout patterns to appear in the
    rook-ceph-operator pod logs and return the matched log lines.

    Args:
        mon_id (str): The monitor ID (e.g., 'a', 'b', 'c') to check in the logs.
        since (str): Time duration to look back in the logs (e.g., '1m', '5m').
        timeout (int): Maximum time in seconds to wait for the patterns to appear.
        sleep (int): Interval in seconds to wait between log polling attempts.

    Returns:
        list: A list of matched log lines containing the timeout message.

    Raises:
        ValueError: If the rook-ceph-operator pod is not found.
        TimeoutExpiredError: If no matching pattern is found within the timeout period.

    Example:
        >>> wait_for_mon_healthcheck_timeout_in_logs("c", since="2m", timeout=600, sleep=30)
        [
            "2025-11-25 13:08:47... mon 'c' not found in quorum, waiting for timeout (599 seconds left)...",
            "2025-11-25 13:09:32... mon 'c' not found in quorum, waiting for timeout (554 seconds left)...",
        ]

    """
    rook_operator_pods = get_operator_pods()
    if not rook_operator_pods:
        raise ValueError("Rook Ceph Operator pod not found.")

    rook_operator_pod = rook_operator_pods[0]
    # Note: Using double quotes " as seen in rook-ceph logs
    pattern = rf'(?i)mon\s+"{re.escape(mon_id)}".*(not found in|lost|loose)\s+quorum.*waiting for timeout'

    return wait_for_matching_pattern_in_pod_logs(
        rook_operator_pod.name,
        pattern,
        since=since,
        timeout=timeout,
        sleep=sleep,
    )


def extract_timeout_seconds(line: str) -> int | None:
    """
    Extract the timeout seconds value from a log line.
    Args:
        line (str): A log line containing the timeout message.

    Returns:
        int | None: The extracted timeout seconds value, or None if not found.

    """
    # Matches: "599 seconds left", "(599 seconds left)", "599 seconds remaining",
    # or "599 seconds before failover"
    m = re.search(
        r"\(?\s*(\d+)\s+seconds\s+(left|remaining|before)\b", line, flags=re.IGNORECASE
    )
    return int(m.group(1)) if m else None


def verify_mon_healthcheck_timeout_value_in_logs(
    mon_id,
    timeout_value,
    since="1m",
    timeout=300,
    sleep=20,
) -> bool:
    """
    Verify that the expected mon healthcheck timeout value appears in the
    rook-ceph-operator pod logs. The function waits for the log pattern to appear and then checks
    if the timeout value is present in the matched log lines.

    Args:
        mon_id (str): The monitor ID to check in the logs.
        timeout_value (int): The timeout value to check in the logs.
        since (str): Time duration to look back in the logs.
        timeout (int): Maximum time to wait for the patterns.
        sleep (int): Time to wait between log checks.

    Returns:
        bool: True if the timeout value is found, False otherwise.

    """
    try:
        mon_timeout_lines = wait_for_mon_healthcheck_timeout_in_logs(
            mon_id, since, timeout, sleep
        )
    except TimeoutExpiredError as ex:
        logger.warning(
            f"Timeout expired while waiting for mon '{mon_id}' healthcheck timeout logs: {ex}"
        )
        return False

    if not mon_timeout_lines:
        logger.warning(f"No log lines found for mon '{mon_id}' healthcheck timeout.")
        return False

    logger.info(
        f"Found {len(mon_timeout_lines)} log lines for mon '{mon_id}' healthcheck timeout. "
        f"mon timeout_lines: {mon_timeout_lines}"
    )
    # The mon healthcheck timeout value in logs may vary slightly due to timing, so we allow
    # a range check.
    timeout_value_range = (
        timeout_value - 90,
        timeout_value,
    )  # e.g., for 600s, range is 510-600s
    line = mon_timeout_lines[0]
    extracted_timeout = extract_timeout_seconds(line)
    if extracted_timeout is None:
        logger.warning(f"Could not extract timeout seconds from log line: \n{line}")
        return False

    if not (timeout_value_range[0] <= extracted_timeout <= timeout_value_range[1]):
        logger.warning(
            f"Mon '{mon_id}' healthcheck timeout value {extracted_timeout} not in expected range "
            f"{timeout_value_range} as per log line: \n{line}"
        )
        return False

    logger.info(
        f"Mon '{mon_id}' healthcheck timeout value {extracted_timeout} is within expected range "
        f"{timeout_value_range} as per log line: \n{line}"
    )
    return True


def get_mon_ids() -> set:
    """
    Get the mon ids of the current mon deployments.

    The mon deployments are used instead of the mon pods, because a mon pod can
    be missing or Pending when its node is unschedulable, while the deployment
    exists as soon as the rook-ceph-operator decides to create the mon.

    Returns:
        set: The mon ids (e.g. {'a', 'b', 'c'})

    """
    mon_ids = set()
    for mon_deployment in get_mon_deployments():
        labels = mon_deployment.data.get("metadata", {}).get("labels", {})
        mon_ids.add(labels.get("mon") or mon_deployment.name.rsplit("-", 1)[-1])
    return mon_ids


def delete_mon_pod(mon_id, wait=True) -> None:
    """
    Delete the mon pod of a specified monitor ID to take the mon down.

    When the mon node is unschedulable, the mon pod created in its place stays
    Pending, so the mon stays out of quorum until the mon healthcheck times out.

    Args:
        mon_id (str): The monitor ID (e.g., 'a', 'b', 'c') to take down.
        wait (bool): Wait for the mon pod to be deleted. True by default.

    Raises:
        ValueError: If the mon pod is not found.

    """
    mon_pod = get_mon_pod_by_id(mon_id)
    logger.info(f"Deleting the pod {mon_pod.name} to take the mon '{mon_id}' down")
    mon_pod.delete(wait=wait)


def wait_for_mon_failover(mon_ids_before, timeout=600, sleep=20, min_wait=0) -> str:
    """
    Wait until the rook-ceph-operator fails the mon over by creating a new mon.

    The new mon deployment is the signal of the failover. The new mon pod
    itself can stay Pending until the mon node is schedulable again, as the
    mons have an anti affinity that keeps two mons from sharing a node.

    Args:
        mon_ids_before (set): The mon ids from before the mon went down.
        timeout (int): Maximum time to wait in seconds. Defaults to 600 seconds.
        sleep (int): Time to wait between checks in seconds. Defaults to 20 seconds.
        min_wait (float): Minimum time for which no new mon may appear.

    Returns:
        str: The mon id of the new mon

    Raises:
        TimeoutExpiredError: If the mon does not fail over within the timeout.
        AssertionError: If the minimum wait cannot be established.

    """
    logger.info(
        f"Waiting up to {timeout} seconds for the mon failover. The mon ids "
        f"before the failover: {sorted(mon_ids_before)}"
    )
    start = time.monotonic()
    last_absent = 0

    def sample_mon_ids():
        sample_start = time.monotonic()
        return get_mon_ids(), sample_start

    try:
        for mon_ids, sample_start in TimeoutSampler(
            timeout=timeout, sleep=sleep, func=sample_mon_ids
        ):
            elapsed = time.monotonic() - start
            new_mon_ids = mon_ids - set(mon_ids_before)
            if new_mon_ids:
                new_mon_id = sorted(new_mon_ids)[0]
                if elapsed > timeout:
                    raise TimeoutExpiredError(
                        f"The new mon '{new_mon_id}' was observed after {elapsed:.0f} "
                        f"seconds, exceeding the {timeout}-second failover timeout."
                    )
                if last_absent < min_wait:
                    raise AssertionError(
                        f"The new mon '{new_mon_id}' appeared before the minimum "
                        f"wait of {min_wait:.0f} seconds could be confirmed; "
                        f"it was last absent at {last_absent:.0f} seconds."
                    )
                logger.info(f"The operator created the new mon '{new_mon_id}'")
                return new_mon_id
            last_absent = sample_start - start
            logger.info(
                f"The mon did not fail over yet. Current mon ids: {sorted(mon_ids)}"
            )
    except TimeoutExpiredError as ex:
        raise TimeoutExpiredError(
            f"The mon did not fail over within {timeout} seconds. The mon ids "
            f"are still {sorted(mon_ids_before)}. Check the rook-ceph-operator "
            f"log for the mon health check messages."
        ) from ex


def select_mon_id_and_node() -> tuple[str, str]:
    """
    Select a monitor ID and its corresponding node name to take the mon down.

    The node running rook-ceph-operator is excluded. The mon healthcheck
    timeout is tracked in memory by the operator and reported in its log, so
    anything that restarts the operator wipes the state the caller asserts on.
    Keeping the operator away from the node we disturb avoids that.

    Returns:
        tuple: A tuple containing the monitor ID (str) and node name (str).

    Raises:
        ValueError: If no monitor nodes are found, rook-ceph-operator is
            missing, or every monitor node also runs the operator.

    """
    mon_nodes = get_mon_running_nodes()
    if not mon_nodes:
        raise ValueError("No monitor nodes found in the cluster")

    operator_pods = get_operator_pods()
    if not operator_pods:
        raise ValueError("Rook Ceph Operator pod not found.")
    operator_nodes = {get_pod_node(operator_pod).name for operator_pod in operator_pods}
    logger.info(f"rook-ceph-operator is running on node(s): {sorted(operator_nodes)}")
    eligible_nodes = [
        node_name for node_name in mon_nodes if node_name not in operator_nodes
    ]
    if not eligible_nodes:
        raise ValueError(
            "Cannot select a mon node: every monitor node also "
            f"runs rook-ceph-operator ({sorted(operator_nodes)})."
        )

    node_name = random.choice(eligible_nodes)
    logger.info(f"Selected mon node: {node_name}")
    mon_id = get_node_mon_ids(node_name)[0]
    logger.info(f"Selected mon id: '{mon_id}'")
    return mon_id, node_name
