"""Analysis functions for noobaa"""

from collections import defaultdict

from ..utils import Colors, UNKNOWN, print_header, print_status, show_pod_logs_tail
from ..utils import read_yaml_file


def analyze_noobaa(mg_dir, deployment_type="internal"):
    """Analyze NooBaa status"""
    print_header("NOOBAA STATUS")

    if deployment_type == "external":
        print(f"{Colors.YELLOW}⚠ External Ceph deployment{Colors.END}")
        print(
            f"{Colors.CYAN}NooBaa is typically not used with external Ceph deployments.{Colors.END}"
        )

        # Check if NooBaa exists anyway (hybrid setup)
        noobaa_file = (
            mg_dir / "noobaa/namespaces/openshift-storage/noobaa.io/noobaas/noobaa.yaml"
        )
        if noobaa_file.exists():
            print(f"{Colors.CYAN}NooBaa CR found - analyzing...{Colors.END}\n")
            # Fall through to analysis
        else:
            print(
                f"{Colors.CYAN}NooBaa CR not found (expected for external mode).{Colors.END}\n"
            )
            return

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


def analyze_backingstores(mg_dir, deployment_type="internal"):
    """Analyze NooBaa BackingStores"""
    print_header("NOOBAA BACKINGSTORES")

    if deployment_type == "external":
        print(f"{Colors.YELLOW}⚠ External Ceph deployment{Colors.END}")
        print(
            f"{Colors.CYAN}BackingStores typically not used with external Ceph.{Colors.END}"
        )

        # Check if any exist (hybrid setup)
        bs_dir = mg_dir / "noobaa/namespaces/openshift-storage/noobaa.io/backingstores"
        if bs_dir.exists() and list(bs_dir.glob("*.yaml")):
            print(f"{Colors.CYAN}BackingStores found - analyzing...{Colors.END}\n")
            # Fall through to analysis
        else:
            print(
                f"{Colors.CYAN}No BackingStores found (expected for external mode).{Colors.END}\n"
            )
            return

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
    show_pod_logs_tail(
        mg_dir,
        pod_name_substring="noobaa-core",
        banner="NooBaa is unhealthy - showing recent logs:",
        not_found_message="Could not find noobaa-core pod logs",
    )
