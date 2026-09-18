"""Analysis functions for events"""

from ..utils import Colors, UNKNOWN, items_or_empty, print_header
from ..utils import read_yaml_file
from collections import defaultdict


def analyze_events(mg_dir, deployment_type="internal"):
    """Analyze recent events for errors/warnings"""
    print_header("RECENT WARNINGS & ERRORS")

    events_file = mg_dir / "namespaces/openshift-storage/core/events.yaml"
    if events_file.exists():
        events_data = read_yaml_file(events_file)
        events = items_or_empty(events_data)
        if events:
            # Filter warning and error events
            problem_events = []
            for event in events:
                event_type = event.get("type", "")
                if event_type in ["Warning"]:
                    reason = event.get("reason", UNKNOWN)
                    message = event.get("message", "")
                    involved_obj = event.get("involvedObject", {})
                    obj_name = involved_obj.get("name", UNKNOWN)
                    last_timestamp = event.get("lastTimestamp", "")

                    problem_events.append(
                        {
                            "type": event_type,
                            "reason": reason,
                            "message": message,
                            "object": obj_name,
                            "timestamp": last_timestamp,
                        }
                    )

            # Sort by timestamp (most recent first), handle None timestamps
            problem_events.sort(
                key=lambda x: x["timestamp"] if x["timestamp"] else "", reverse=True
            )

            # Group by reason
            reason_counts = defaultdict(int)
            for event in problem_events:
                reason_counts[event["reason"]] += 1

            print(f"{Colors.CYAN}Warning Summary:{Colors.END}")
            for reason, count in sorted(
                reason_counts.items(), key=lambda x: x[1], reverse=True
            )[:10]:
                print(f"  {Colors.YELLOW}⚠{Colors.END} {reason}: {count} occurrences")

            # Show recent unique warnings
            print(f"\n{Colors.CYAN}Recent Unique Warnings (last 10):{Colors.END}")
            seen_messages = set()
            shown = 0
            for event in problem_events:
                msg_key = f"{event['reason']}:{event['message'][:50]}"
                if msg_key not in seen_messages and shown < 10:
                    seen_messages.add(msg_key)
                    shown += 1
                    print(
                        f"\n  {Colors.YELLOW}⚠{Colors.END} {event['reason']} - {event['object']}"
                    )
                    msg = event["message"]
                    if len(msg) > 150:
                        msg = msg[:150] + "..."
                    print(f"    {msg}")
