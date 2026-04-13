"""Analysis functions for operators"""

from ..utils import Colors, print_header, print_status
from ..utils import read_yaml_file, read_file
import re


def analyze_csv(mg_dir):
    """Analyze ClusterServiceVersion (CSV) status"""
    print_header("OPERATOR STATUS (CSV)")

    csv_file = mg_dir / "namespaces/openshift-storage/oc_output/csv"
    if csv_file.exists():
        content = read_file(csv_file)
        if content:
            lines = content.strip().split("\n")
            if len(lines) > 1:
                # Parse table format (skip header line)
                succeeded_count = 0
                failed_count = 0
                other_count = 0
                problematic_operators = []

                for line in lines[1:]:  # Skip header
                    parts = line.split()
                    if len(parts) >= 4:
                        name = parts[0]
                        # Display name might be multiple words, so we need to find where VERSION starts
                        # The version typically looks like X.Y.Z-something
                        version_idx = -1
                        for i, part in enumerate(parts):
                            if re.match(r"\d+\.\d+", part):
                                version_idx = i
                                break

                        if version_idx > 1:
                            display_name = " ".join(parts[1:version_idx])
                            version = parts[version_idx]
                            # Phase is the last element
                            phase = parts[-1]
                        else:
                            display_name = parts[1] if len(parts) > 1 else name
                            version = parts[2] if len(parts) > 2 else "Unknown"
                            phase = parts[-1]

                        # Count by phase
                        if phase == "Succeeded":
                            succeeded_count += 1
                        elif phase in ["Failed", "Error"]:
                            failed_count += 1
                            problematic_operators.append(
                                {
                                    "name": name,
                                    "display_name": display_name,
                                    "version": version,
                                    "phase": phase,
                                }
                            )
                        else:
                            other_count += 1
                            problematic_operators.append(
                                {
                                    "name": name,
                                    "display_name": display_name,
                                    "version": version,
                                    "phase": phase,
                                }
                            )

                total_operators = succeeded_count + failed_count + other_count
                print(
                    f"{Colors.CYAN}Total Installed Operators:{Colors.END} {total_operators}\n"
                )

                # Summary by phase
                print(f"{Colors.CYAN}Operators by Phase:{Colors.END}")
                print(f"  {Colors.GREEN}✓{Colors.END} Succeeded: {succeeded_count}")
                if failed_count > 0:
                    print(f"  {Colors.RED}✗{Colors.END} Failed: {failed_count}")
                if other_count > 0:
                    print(f"  {Colors.YELLOW}⚠{Colors.END} Other: {other_count}")

                # Show problematic operators only
                if problematic_operators:
                    print(f"\n{Colors.YELLOW}Problematic Operators:{Colors.END}")
                    for op in problematic_operators:
                        color = (
                            Colors.RED
                            if op["phase"] in ["Failed", "Error"]
                            else Colors.YELLOW
                        )
                        print(f"\n  {color}⚠{Colors.END} {op['display_name']}")
                        print(f"    Name: {op['name']}")
                        print(f"    Version: {op['version']}")
                        print_status("    Phase", op["phase"])
                else:
                    print(f"\n  {Colors.GREEN}✓ All operators are healthy{Colors.END}")
            else:
                print(f"{Colors.YELLOW}No CSV data found{Colors.END}")
        else:
            print(f"{Colors.YELLOW}Could not read CSV file{Colors.END}")
    else:
        print(f"{Colors.YELLOW}CSV file not found{Colors.END}")


def analyze_subscriptions(mg_dir):
    """Analyze Operator Subscription status"""
    print_header("OPERATOR SUBSCRIPTIONS")

    subs_dir = (
        mg_dir / "namespaces/openshift-storage/operators.coreos.com/subscriptions"
    )
    if subs_dir.exists() and subs_dir.is_dir():
        sub_files = list(subs_dir.glob("*.yaml"))

        if sub_files:
            print(f"{Colors.CYAN}Active Subscriptions:{Colors.END} {len(sub_files)}\n")

            for sub_file in sorted(sub_files):
                sub = read_yaml_file(sub_file)
                if sub:
                    name = sub.get("metadata", {}).get("name", "Unknown")
                    spec = sub.get("spec", {})
                    status = sub.get("status", {})

                    package = spec.get("name", "Unknown")
                    channel = spec.get("channel", "Unknown")
                    source = spec.get("source", "Unknown")
                    install_plan_approval = spec.get("installPlanApproval", "Automatic")

                    current_csv = status.get("currentCSV", "Unknown")
                    installed_csv = status.get("installedCSV", "Unknown")
                    state = status.get("state", "Unknown")

                    print(f"{Colors.CYAN}{package}{Colors.END}")
                    print(f"  Name: {name}")
                    print(f"  Channel: {channel}")
                    print(f"  Source: {source}")
                    print_status("  State", state)
                    print(f"  Current CSV: {current_csv}")

                    if current_csv != installed_csv and installed_csv != "Unknown":
                        print(
                            f"  {Colors.YELLOW}⚠ Installed CSV:{Colors.END} {installed_csv}"
                        )
                        print(
                            f"  {Colors.YELLOW}⚠ Update available or in progress{Colors.END}"
                        )

                    print(f"  Approval: {install_plan_approval}")

                    # Check conditions
                    conditions = status.get("conditions", [])
                    if conditions:
                        for cond in conditions:
                            cond_type = cond.get("type", "")
                            cond_status = cond.get("status", "")
                            if (
                                cond_status != "True"
                                or cond_type != "CatalogSourcesUnhealthy"
                            ):
                                reason = cond.get("reason", "")
                                if reason and cond_status != "True":
                                    print(
                                        f"  {Colors.YELLOW}⚠ {cond_type}:{Colors.END} {reason}"
                                    )

                    print()
        else:
            print(f"{Colors.YELLOW}No subscription files found{Colors.END}")
    else:
        print(f"{Colors.YELLOW}Subscriptions directory not found{Colors.END}")
