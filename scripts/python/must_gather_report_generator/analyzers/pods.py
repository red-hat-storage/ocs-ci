"""Analysis functions for pods"""

from ..utils import Colors, UNKNOWN, items_or_empty, print_header
from ..utils import read_yaml_file
from collections import defaultdict


def analyze_pods(mg_dir, deployment_type="internal"):
    """Analyze pod status"""
    print_header("POD STATUS SUMMARY")

    pods_file = mg_dir / "namespaces/openshift-storage/core/pods.yaml"
    if pods_file.exists():
        pods_data = read_yaml_file(pods_file)
        pods = items_or_empty(pods_data)
        if pods:
            # Count by phase
            phase_counts = defaultdict(int)
            for pod in pods:
                phase = pod.get("status", {}).get("phase", UNKNOWN)
                phase_counts[phase] += 1

            print(f"{Colors.CYAN}Total Pods:{Colors.END} {len(pods)}\n")

            print(f"{Colors.CYAN}Pods by Phase:{Colors.END}")
            for phase in sorted(phase_counts.keys()):
                count = phase_counts[phase]
                # Use appropriate icons based on phase meaning
                if phase in ["Running", "Succeeded"]:
                    print(f"  {Colors.GREEN}✓{Colors.END} {phase}: {count}")
                elif phase in ["Pending", "ContainerCreating"]:
                    print(f"  {Colors.YELLOW}⚠{Colors.END} {phase}: {count}")
                elif phase in ["Failed", "CrashLoopBackOff", "Error"]:
                    print(f"  {Colors.RED}✗{Colors.END} {phase}: {count}")
                else:
                    print(f"  {phase}: {count}")

            # Show problematic pods
            print(f"\n{Colors.CYAN}Problematic Pods:{Colors.END}")
            problem_pods = []

            for pod in pods:
                pod_name = pod.get("metadata", {}).get("name", UNKNOWN)
                phase = pod.get("status", {}).get("phase", UNKNOWN)

                if phase in ["Pending", "Failed"]:
                    conditions = pod.get("status", {}).get("conditions", [])
                    reason = UNKNOWN
                    message = ""

                    for cond in conditions:
                        if (
                            cond.get("type") == "PodScheduled"
                            and cond.get("status") == "False"
                        ):
                            reason = cond.get("reason", "SchedulingIssue")
                            message = cond.get("message", "")
                        elif (
                            cond.get("type") == "Ready"
                            and cond.get("status") == "False"
                        ):
                            reason = cond.get("reason", "NotReady")
                            message = cond.get("message", "")

                    problem_pods.append(
                        {
                            "name": pod_name,
                            "phase": phase,
                            "reason": reason,
                            "message": message,
                        }
                    )
                elif phase == "Running":
                    # Check for containers not ready
                    container_statuses = pod.get("status", {}).get(
                        "containerStatuses", []
                    )
                    for container in container_statuses:
                        if not container.get("ready", True):
                            state = container.get("state", {})
                            waiting = state.get("waiting")
                            if isinstance(waiting, dict):
                                reason = waiting.get("reason", UNKNOWN)
                                problem_pods.append(
                                    {
                                        "name": pod_name,
                                        "phase": f"Running (Container: {container.get('name', UNKNOWN)})",
                                        "reason": reason,
                                        "message": waiting.get("message", ""),
                                    }
                                )
                            else:
                                terminated = state.get("terminated")
                                if isinstance(terminated, dict):
                                    reason = terminated.get("reason", "Terminated")
                                    problem_pods.append(
                                        {
                                            "name": pod_name,
                                            "phase": f"Running (Container: {container.get('name', UNKNOWN)})",
                                            "reason": reason,
                                            "message": terminated.get("message", ""),
                                        }
                                    )

            if problem_pods:
                for pod in problem_pods[:20]:  # Show first 20
                    print(f"\n  {Colors.RED}✗{Colors.END} {pod['name']}")
                    print(f"    Phase: {pod['phase']}")
                    print(f"    Reason: {pod['reason']}")
                    if pod["message"]:
                        # Truncate long messages
                        msg = pod["message"]
                        if len(msg) > 100:
                            msg = msg[:100] + "..."
                        print(f"    Message: {msg}")

                if len(problem_pods) > 20:
                    print(f"\n  ... and {len(problem_pods) - 20} more problematic pods")
            else:
                print(f"  {Colors.GREEN}✓ No problematic pods found{Colors.END}")
