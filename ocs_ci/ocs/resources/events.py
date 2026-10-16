"""
Event-related utility functions for OCS-CI
"""

import logging
import re

from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


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
    logger.debug(
        f"Querying volume health events: pvc={pvc_obj.name}, "
        f"namespace={pvc_obj.namespace}, reason={reason}, type={event_type}"
    )
    event_ocp = OCP(kind="Event", namespace=pvc_obj.namespace)
    events = event_ocp.get(
        field_selector=f"involvedObject.name={pvc_obj.name}",
    )["items"]

    matching_events = [
        e
        for e in events
        if (
            e.get("reason") == reason
            and message_substr in e.get("message", "")
            and e.get("type") == event_type
            and e.get("source", {}).get("component") == "CSI-Addons"
        )
    ]

    count = sum(e.get("count", 1) for e in matching_events)
    logger.debug(
        f"Found {len(matching_events)} matching events with total count: {count}"
    )
    return count


def parse_event_age_seconds(age_str):
    """
    Parse Kubernetes event age string (e.g., "12s", "5m", "2h") into seconds.

    Args:
        age_str (str): Age string from event (e.g., "12s", "5m30s", "2h", "3d",
                      or "2s (x3 over 1h)" for repeated events)

    Returns:
        int: Age in seconds, or None if unable to parse

    """
    if not age_str:
        return None

    age_str = age_str.split("(")[0].strip()

    total_seconds = 0
    found_match = False
    patterns = [
        (r"(\d+)d", 86400),  # days
        (r"(\d+)h", 3600),  # hours
        (r"(\d+)m", 60),  # minutes
        (r"(\d+)s", 1),  # seconds
    ]

    for pattern, multiplier in patterns:
        match = re.search(pattern, age_str)
        if match:
            total_seconds += int(match.group(1)) * multiplier
            found_match = True

    return total_seconds if found_match else None


def filter_events_by_age(events_text, max_age_seconds=180):
    """
    Filter Kubernetes events to only include recent ones within max_age_seconds.

    Parses the Events section from 'oc describe pod' output and filters out
    events older than the specified threshold. This is useful to avoid detecting
    stale errors that have already been resolved.

    Args:
        events_text (str): Events section from oc describe pod output
        max_age_seconds (int): Maximum age in seconds (default: 180s = 3 minutes)

    Returns:
        str: Filtered events text containing only recent events

    """
    if not events_text or "Events:" not in events_text:
        logger.debug("No events section found in input text")
        return events_text

    logger.debug(f"Filtering events by age: max_age={max_age_seconds}s")
    lines = events_text.split("\n")
    filtered_lines = []
    filtered_count = 0

    for line in lines:
        if line.startswith("Events:") or line.startswith("Type") or not line.strip():
            filtered_lines.append(line)
            continue

        parts = line.split(None, 4)
        if len(parts) >= 4:
            age_str = parts[2]
            age_seconds = parse_event_age_seconds(age_str)

            if age_seconds is not None and age_seconds <= max_age_seconds:
                filtered_lines.append(line)
            else:
                filtered_count += 1
        else:
            filtered_lines.append(line)

    logger.debug(f"Filtered out {filtered_count} events older than {max_age_seconds}s")
    return "\n".join(filtered_lines)
