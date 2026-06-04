"""Find and print container ``current.log`` snippets from must-gather pod directories."""

from __future__ import annotations

from pathlib import Path

from .colors import Colors
from .file_readers import read_file_tail


def show_pod_logs_tail(
    mg_dir: Path,
    *,
    pod_name_substring: str,
    banner: str,
    not_found_message: str,
    max_lines: int = 50,
) -> None:
    """
    Print the last ``max_lines`` of the first matching pod's ``current.log`` under
    ``namespaces/openshift-storage/pods`` (container path repeated: ``<ct>/<ct>/logs/``).
    """
    print(f"\n{Colors.YELLOW}{banner}{Colors.END}\n")

    pods_dir = Path(mg_dir) / "namespaces/openshift-storage/pods"
    if not pods_dir.exists():
        print(f"{Colors.YELLOW}{not_found_message}{Colors.END}\n")
        return

    for pod_dir in pods_dir.iterdir():
        if not pod_dir.is_dir() or pod_name_substring not in pod_dir.name:
            continue

        for container_dir in pod_dir.iterdir():
            if not container_dir.is_dir() or container_dir.name == pod_dir.name:
                continue

            inner_container_dir = container_dir / container_dir.name
            if not (inner_container_dir.exists() and inner_container_dir.is_dir()):
                continue

            log_file = inner_container_dir / "logs/current.log"
            if not log_file.exists():
                continue

            print(f"{Colors.CYAN}Pod: {pod_dir.name}{Colors.END}")
            print(f"{Colors.CYAN}Container: {container_dir.name}{Colors.END}\n")

            logs = read_file_tail(log_file, max_lines=max_lines)
            if logs:
                lines = logs.splitlines()
                for line in lines:
                    print(f"  {line}")
                print(f"\n{Colors.CYAN}[Showing last {len(lines)} lines]{Colors.END}\n")
            else:
                print(f"{Colors.YELLOW}  No logs found{Colors.END}\n")
            return

    print(f"{Colors.YELLOW}{not_found_message}{Colors.END}\n")
