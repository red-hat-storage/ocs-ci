import json
import logging
import random
import re

from ocs_ci.ocs.node import get_mon_running_nodes, get_node_mon_ids
from ocs_ci.ocs.resources.storage_cluster import get_default_storagecluster
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import (
    wait_for_matching_pattern_in_pod_logs,
    get_operator_pods,
    wait_for_pods_to_be_in_statuses,
    get_mon_pod_by_id,
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
    pattern = rf'.*mon "{re.escape(mon_id)}" not found in quorum, waiting for timeout.*'

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


def wait_for_mon_pod_restart(mon_id, timeout=300, sleep=20) -> None:
    """
    Wait until the monitor pod for a specified monitor ID restarts.

    Args:
        mon_id (str): The monitor ID (e.g., 'a', 'b', 'c') to wait for.
        timeout (int): Maximum time to wait in seconds. Defaults to 300 seconds (5 minutes).
        sleep (int): Time to wait between checks in seconds. Defaults to 20 seconds.

    Raises:
        TimeoutExpiredError: If the monitor pod does not restart within the timeout.

    """
    mon_pod = get_mon_pod_by_id(mon_id)
    logger.info(f"Waiting {timeout} seconds for the monitor pod {mon_id} to restart.")
    res = wait_for_pods_to_be_in_statuses(
        expected_statuses=[constants.STATUS_TERMINATING],
        pod_names=[mon_pod.name],
        timeout=timeout,
        sleep=sleep,
    )
    if not res:
        raise TimeoutExpiredError(
            f"Monitor pod {mon_id} did not restart within {timeout} seconds."
        )


def select_mon_id_and_node() -> tuple[str, str]:
    """
    Select a monitor ID and its corresponding node name.

    Returns:
        tuple: A tuple containing the monitor ID (str) and node name (str).

    """
    mon_nodes = get_mon_running_nodes()
    if not mon_nodes:
        raise ValueError("No monitor nodes found in the cluster")

    node_name = random.choice(mon_nodes)
    logger.info(f"Selected mon node for drain: {node_name}")
    mon_id = get_node_mon_ids(node_name)[0]
    logger.info(f"Selected mon id for drain: '{mon_id}'")
    return mon_id, node_name
