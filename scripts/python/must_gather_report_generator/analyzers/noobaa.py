"""Analysis functions for noobaa"""

from collections import defaultdict

from ..utils import Colors, UNKNOWN, print_header, print_status
from ..utils import read_yaml_file, read_file


def analyze_noobaa(mg_dir):
    """Analyze NooBaa status"""
    print_header("NOOBAA STATUS")

    noobaa_file = (
        mg_dir / "noobaa/namespaces/openshift-storage/noobaa.io/noobaas/noobaa.yaml"
    )
    is_unhealthy = False

    if noobaa_file.exists():
        noobaa = read_yaml_file(noobaa_file)
        if noobaa:
            status = noobaa.get("status", {})
            phase = status.get("phase", UNKNOWN)
            print_status("Phase", phase)

            # Check if NooBaa is unhealthy
            if phase not in ["Ready"]:
                is_unhealthy = True

            # DB Status
            db_status = status.get("dbStatus", {})
            if db_status:
                print(f"\n{Colors.CYAN}Database Status:{Colors.END}")
                print(f"  Cluster Status: {db_status.get('dbClusterStatus', UNKNOWN)}")
                print(
                    f"  PostgreSQL Version: {db_status.get('currentPgMajorVersion', UNKNOWN)}"
                )
                print(f"  Image: {db_status.get('dbCurrentImage', UNKNOWN)}")

            # Conditions
            conditions = status.get("conditions", [])
            if conditions:
                print(f"\n{Colors.CYAN}Conditions:{Colors.END}")
                for cond in conditions:
                    cond_type = cond.get("type", UNKNOWN)
                    cond_status = cond.get("status", UNKNOWN)
                    message = cond.get("message", "")
                    print_status(f"  {cond_type}", cond_status, message)
                    # Also check conditions for unhealthy state
                    if cond_status not in ["True"] and cond_type in [
                        "Available",
                        "Progressing",
                    ]:
                        is_unhealthy = True

    # Show NooBaa core logs if unhealthy
    if is_unhealthy:
        show_noobaa_logs(mg_dir)


def analyze_backingstores(mg_dir):
    """Analyze NooBaa BackingStores"""
    print_header("NOOBAA BACKINGSTORES")

    bs_dir = mg_dir / "noobaa/namespaces/openshift-storage/noobaa.io/backingstores"
    if bs_dir.exists():
        bs_files = list(bs_dir.glob("*.yaml"))

        if bs_files:
            print(f"{Colors.CYAN}Total BackingStores:{Colors.END} {len(bs_files)}\n")

            phase_counts = defaultdict(int)

            for bs_file in bs_files:
                bs_data = read_yaml_file(bs_file)
                if bs_data:
                    name = bs_data.get("metadata", {}).get("name", UNKNOWN)
                    spec = bs_data.get("spec", {})
                    status = bs_data.get("status", {})

                    bs_type = spec.get("type", UNKNOWN)
                    phase = status.get("phase", UNKNOWN)
                    mode = status.get("mode", {}).get("modeCode", UNKNOWN)

                    phase_counts[phase] += 1

                    print(f"{Colors.CYAN}{name}{Colors.END}")
                    print(f"  Type: {bs_type}")
                    print_status("  Phase", phase)

                    if mode and mode != "OPTIMAL":
                        print(f"  {Colors.YELLOW}⚠ Mode:{Colors.END} {mode}")

                    # Show conditions for non-ready stores
                    if phase != "Ready":
                        conditions = status.get("conditions", [])
                        for cond in conditions:
                            cond_type = cond.get("type", "")
                            cond_status = cond.get("status", "")
                            message = cond.get("message", "")
                            if cond_status != "True":
                                print(
                                    f"  {Colors.YELLOW}⚠ {cond_type}:{Colors.END} {message[:80]}"
                                )

                    print()

            # Summary
            print(f"{Colors.CYAN}Summary by Phase:{Colors.END}")
            for phase in sorted(phase_counts.keys()):
                count = phase_counts[phase]
                if phase == "Ready":
                    print(f"  {Colors.GREEN}✓{Colors.END} {phase}: {count}")
                else:
                    print(f"  {Colors.YELLOW}⚠{Colors.END} {phase}: {count}")
        else:
            print(f"{Colors.YELLOW}No BackingStore files found{Colors.END}")
    else:
        print(f"{Colors.YELLOW}BackingStores directory not found{Colors.END}")


def show_noobaa_logs(mg_dir):
    """Show last 50 lines of noobaa-core pod logs"""
    print(f"\n{Colors.YELLOW}NooBaa is unhealthy - showing recent logs:{Colors.END}\n")

    # Find noobaa-core pod logs
    pods_dir = mg_dir / "namespaces/openshift-storage/pods"
    if pods_dir.exists():
        for pod_dir in pods_dir.iterdir():
            if pod_dir.is_dir() and "noobaa-core" in pod_dir.name:
                # Look for container logs
                # Note: The structure is pods/<pod>/<container>/<container>/logs/current.log
                # The container name is repeated twice
                for container_dir in pod_dir.iterdir():
                    if container_dir.is_dir() and container_dir.name != pod_dir.name:
                        # Look for the repeated container directory
                        inner_container_dir = container_dir / container_dir.name
                        if (
                            inner_container_dir.exists()
                            and inner_container_dir.is_dir()
                        ):
                            log_file = inner_container_dir / "logs/current.log"
                            if log_file.exists():
                                print(f"{Colors.CYAN}Pod: {pod_dir.name}{Colors.END}")
                                print(
                                    f"{Colors.CYAN}Container: {container_dir.name}{Colors.END}\n"
                                )

                                logs = read_file(log_file)
                                if logs:
                                    lines = logs.strip().split("\n")
                                    last_50 = lines[-50:] if len(lines) > 50 else lines
                                    for line in last_50:
                                        print(f"  {line}")
                                    print(
                                        f"\n{Colors.CYAN}[Showing last {len(last_50)} lines]{Colors.END}\n"
                                    )
                                else:
                                    print(
                                        f"{Colors.YELLOW}  No logs found{Colors.END}\n"
                                    )
                                return

    print(f"{Colors.YELLOW}Could not find noobaa-core pod logs{Colors.END}\n")
