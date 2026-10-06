"""
Event-related utility functions for OCS-CI
"""

import logging

from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)


@retry(UnexpectedBehaviour, tries=10, delay=5, backoff=1)
def verify_expected_failure_event(ocs_obj, failure_strs):
    """
    Checks for the expected failure event message in oc describe command

    Args:
        ocs_obj (OCS): The resource to check the events of
        failure_strs (list): Failure messages, any one of which is expected to
            be present in the events of the resource

    Returns:
        bool: True if one of the failure messages is present

    Raises:
        UnexpectedBehaviour: If none of the failure messages is present

    """
    log.info("Check expected failure event message in oc describe command")
    describe_output = ocs_obj.describe()
    for failure_str in failure_strs:
        if failure_str in describe_output:
            log.info(f"Failure string {failure_str} is present in oc describe command")
            return True
    raise UnexpectedBehaviour(
        f"None of the failure strings {failure_strs} were found in oc describe command"
    )


def count_pvc_volume_health_events(pvc_obj, reason, event_type, message_substr):
    """
    Return the total occurrence count of K8s core v1 events matching the
    given criteria for the PVC, with ``source.component == 'CSI-Addons'``.
    No assertion is made — zero is a valid return value.

    Args:
        pvc_obj: PVC object
        reason (str): Event reason
            (e.g. 'VolumeConditionHealthy', 'VolumeConditionAbnormal')
        event_type (str): Event type ('Normal' or 'Warning')
        message_substr (str): Substring expected in the event message

    Returns:
        int: Sum of ``count`` fields across all matching events (may be 0)
    """
    event_ocp = OCP(kind="Event", namespace=pvc_obj.namespace)
    events = event_ocp.get(
        field_selector=f"involvedObject.name={pvc_obj.name}",
    )["items"]
    return sum(
        e.get("count", 1)
        for e in events
        if (
            e.get("reason") == reason
            and message_substr in e.get("message", "")
            and e.get("type") == event_type
            and e.get("source", {}).get("component") == "CSI-Addons"
        )
    )
