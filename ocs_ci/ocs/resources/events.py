"""
Event-related utility functions for OCS-CI
"""

import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


def count_pvc_volume_health_events(
    pvc_obj, reason=None, event_type=None, message_substring=None
):
    """
    Count Kubernetes events for a PVC matching specified criteria.

    Args:
        pvc_obj (OCS): PVC object to query events for
        reason (str): Optional event reason filter
        event_type (str): Optional event type filter ("Normal" or "Warning")
        message_substring (str): Optional substring to match in event message

    Returns:
        int: Total count of matching events (may be zero)

    """
    event_obj = OCP(
        kind=constants.EVENT,
        namespace=pvc_obj.namespace,
        field_selector=f"involvedObject.name={pvc_obj.name}",
    )
    events = event_obj.get().get("items", [])
    events = [e for e in events if e.get("source", {}).get("component") == "csi-addons"]
    if reason:
        events = [e for e in events if e.get("reason") == reason]
    if event_type:
        events = [e for e in events if e.get("type") == event_type]
    if message_substring:
        events = [e for e in events if message_substring in e.get("message", "")]
    total_count = sum(e.get("count", 1) for e in events)
    log.info(f"Found {total_count} events for PVC {pvc_obj.name}")
    return total_count
