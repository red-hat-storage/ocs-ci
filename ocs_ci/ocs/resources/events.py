"""
Event-related utility functions for OCS-CI
"""

import logging
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


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
