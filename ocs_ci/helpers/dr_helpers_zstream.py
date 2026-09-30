"""
New helper functions to be appended to the end of ocs_ci/helpers/dr_helpers.py.

These functions are placed here temporarily for the z-stream PR.
To integrate permanently: append the three functions below into dr_helpers.py
after the existing function `get_drpc_resource_condition`, then delete this file.

The test imports these from dr_helpers.py, which re-exports them from here.
"""

import logging

from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def wait_for_drpc_phase(drpc_obj, phase, timeout=300, sleep=15):
    """
    Wait for a DRPC resource to reach a specific phase.

    Args:
        drpc_obj: The DRPC resource object (OCP or DRPC class instance).
        phase (str): The expected phase (e.g., "Deployed", "Relocated", "FailedOver").
        timeout (int): Maximum time to wait in seconds. Default 300.
        sleep (int): Polling interval in seconds. Default 15.

    Raises:
        TimeoutExpiredError: If the DRPC does not reach the expected phase within timeout.
    """
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=lambda: drpc_obj.get().get("status", {}).get("phase", ""),
    ):
        if sample == phase:
            logger.info(f"DRPC {drpc_obj.resource_name} reached phase '{phase}'")
            return


def wait_for_first_kube_object_protection(drpc_obj, timeout=600, sleep=20):
    """
    Wait for a DRPC resource to complete its first kube object protection cycle.

    Args:
        drpc_obj: The DRPC resource object (OCP or DRPC class instance).
        timeout (int): Maximum time to wait in seconds. Default 600.
        sleep (int): Polling interval in seconds. Default 20.

    Returns:
        str: The lastKubeObjectProtectionTime value.

    Raises:
        TimeoutExpiredError: If kube object protection is not completed within timeout.
    """
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=lambda: drpc_obj.get().get("status", {}).get(
            "lastKubeObjectProtectionTime"
        ),
    ):
        if sample:
            logger.info(
                f"DRPC {drpc_obj.resource_name} completed first kube object "
                f"protection at {sample}"
            )
            return sample


def monitor_drpc_protected_condition_stability(
    drpc_obj, observation_window=600, poll_interval=30
):
    """
    Monitor the DRPC ClusterDataProtected condition over an observation window
    and collect stability metrics.

    Args:
        drpc_obj: The DRPC resource object (OCP or DRPC class instance).
        observation_window (int): Total observation time in seconds. Default 600.
        poll_interval (int): Polling interval in seconds. Default 30.

    Returns:
        dict: A dictionary with the following keys:
            - transitions (int): Number of status transitions between states.
            - bsl_errors (list): List of BSL error messages observed.
            - capture_not_started_count (int): Number of times
              'KubeObjectsCaptureNotStarted' was observed.
            - uploading_seen (bool): Whether 'Uploading' reason was observed.
            - final_protected (dict or None): The final Protected condition dict.
    """
    transitions = 0
    bsl_errors = []
    capture_not_started_count = 0
    uploading_seen = False
    previous_status = None
    final_protected = None
    polls_done = 0
    max_polls = observation_window // poll_interval

    for sample in TimeoutSampler(
        timeout=observation_window + poll_interval,
        sleep=poll_interval,
        func=drpc_obj.get,
    ):
        status = sample.get("status", {})
        conditions = status.get("conditions", [])

        protected_condition = None
        for cond in conditions:
            if cond.get("type") == "Protected":
                protected_condition = cond
                break

        if protected_condition is None:
            for cond in conditions:
                if "protect" in cond.get("type", "").lower():
                    protected_condition = cond
                    break

        final_protected = protected_condition

        if protected_condition:
            current_status = protected_condition.get("status", "")
            reason = protected_condition.get("reason", "")
            message = protected_condition.get("message", "")

            if previous_status is not None and current_status != previous_status:
                transitions += 1
                logger.debug(
                    f"DRPC {drpc_obj.resource_name} Protected condition transitioned: "
                    f"'{previous_status}' -> '{current_status}' (reason={reason})"
                )

            previous_status = current_status

            if "BSL" in reason or "BSL" in message:
                bsl_errors.append(f"reason={reason}, message={message}")

            if (
                "KubeObjectsCaptureNotStarted" in reason
                or "CaptureNotStarted" in reason
            ):
                capture_not_started_count += 1

            if "Uploading" in reason:
                uploading_seen = True

        polls_done += 1
        if polls_done >= max_polls:
            break

    return {
        "transitions": transitions,
        "bsl_errors": bsl_errors,
        "capture_not_started_count": capture_not_started_count,
        "uploading_seen": uploading_seen,
        "final_protected": final_protected,
    }