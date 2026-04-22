"""Analysis functions for nodes"""

from ..utils import Colors, UNKNOWN, first_item, print_header
from ..utils import read_yaml_file


def analyze_nodes(mg_dir, deployment_type="internal"):
    """Analyze Node health and capacity"""
    print_header("NODE HEALTH & CAPACITY")

    nodes_dir = mg_dir / "cluster-scoped-resources/core/nodes"
    if not nodes_dir.exists():
        print(f"{Colors.YELLOW}Nodes directory not found{Colors.END}")
        return

    nodes = []

    sc_failure_domain_names = set()
    sc_file = mg_dir / "namespaces/openshift-storage/oc_output/storagecluster.yaml"
    if sc_file.exists():
        sc_data = read_yaml_file(sc_file)
        sc_item = first_item(sc_data)
        if sc_item:
            raw_vals = sc_item.get("status", {}).get("failureDomainValues", [])
            sc_failure_domain_names = {str(v) for v in raw_vals if v is not None}

    for node_file in nodes_dir.glob("*.yaml"):
        node_data = read_yaml_file(node_file)
        if node_data:
            node_name = node_data.get("metadata", {}).get("name", UNKNOWN)
            status = node_data.get("status", {})

            labels = node_data.get("metadata", {}).get("labels", {})
            is_storage_node = "cluster.ocs.openshift.io/openshift-storage" in labels
            in_sc_failure_domain = node_name in sc_failure_domain_names

            conditions = status.get("conditions", [])
            ready = False
            disk_pressure = False
            memory_pressure = False
            pid_pressure = False

            for cond in conditions:
                cond_type = cond.get("type", "")
                cond_status = cond.get("status", "")
                if cond_type == "Ready" and cond_status == "True":
                    ready = True
                elif cond_type == "DiskPressure" and cond_status == "True":
                    disk_pressure = True
                elif cond_type == "MemoryPressure" and cond_status == "True":
                    memory_pressure = True
                elif cond_type == "PIDPressure" and cond_status == "True":
                    pid_pressure = True

            taints = node_data.get("spec", {}).get("taints", [])
            unschedulable = node_data.get("spec", {}).get("unschedulable", False)

            capacity = status.get("capacity", {})
            allocatable = status.get("allocatable", {})

            nodes.append(
                {
                    "name": node_name,
                    "ready": ready,
                    "disk_pressure": disk_pressure,
                    "memory_pressure": memory_pressure,
                    "pid_pressure": pid_pressure,
                    "taints": taints,
                    "unschedulable": unschedulable,
                    "is_storage_node": is_storage_node,
                    "in_sc_failure_domain": in_sc_failure_domain,
                    "capacity": capacity,
                    "allocatable": allocatable,
                }
            )

    total_nodes = len(nodes)
    ready_nodes = sum(1 for n in nodes if n["ready"])
    storage_node_count = sum(1 for n in nodes if n["is_storage_node"])

    print(f"{Colors.CYAN}Total Nodes:{Colors.END} {total_nodes}")
    print(f"{Colors.CYAN}Ready Nodes:{Colors.END} {ready_nodes}/{total_nodes}")
    print(f"{Colors.CYAN}ODF Storage Nodes:{Colors.END} {storage_node_count}\n")

    if sc_failure_domain_names:
        matched_fd = sum(1 for n in nodes if n["in_sc_failure_domain"])
        print(
            f"{Colors.CYAN}StorageCluster failure domain entries:{Colors.END} "
            f"{len(sc_failure_domain_names)} (matched to node names: {matched_fd})"
        )
        if matched_fd == 0:
            print(
                f"{Colors.YELLOW}  Note: values are compared to Node metadata.name; "
                f"zone-based domains often do not match.{Colors.END}"
            )
        print()

    print(f"{Colors.CYAN}Storage Nodes:{Colors.END}")
    for node in sorted(nodes, key=lambda x: (not x["is_storage_node"], x["name"])):
        if node["is_storage_node"]:
            status_icon = Colors.GREEN + "✓" if node["ready"] else Colors.RED + "✗"
            print(
                f"\n  {status_icon}{Colors.END} {Colors.BOLD}{node['name']}{Colors.END}"
            )

            if not node["ready"]:
                print(f"    {Colors.RED}Status: Not Ready{Colors.END}")

            if node["disk_pressure"]:
                print(f"    {Colors.RED}⚠ Disk Pressure{Colors.END}")
            if node["memory_pressure"]:
                print(f"    {Colors.RED}⚠ Memory Pressure{Colors.END}")
            if node["pid_pressure"]:
                print(f"    {Colors.RED}⚠ PID Pressure{Colors.END}")
            if node["unschedulable"]:
                print(f"    {Colors.YELLOW}⚠ Unschedulable{Colors.END}")
            if sc_failure_domain_names:
                fd_note = (
                    "listed in SC failure domain"
                    if node["in_sc_failure_domain"]
                    else "not in SC failure domain list"
                )
                print(f"    {Colors.CYAN}Failure domain:{Colors.END} {fd_note}")

            if node["taints"]:
                print(f"    Taints: {len(node['taints'])}")
                for taint in node["taints"][:3]:
                    print(
                        f"      - {taint.get('key', UNKNOWN)}: "
                        f"{taint.get('effect', UNKNOWN)}"
                    )

            storage_capacity = node["capacity"].get("ephemeral-storage", "0")
            storage_allocatable = node["allocatable"].get("ephemeral-storage", "0")
            if storage_capacity:
                print(
                    f"    Storage: {storage_allocatable} allocatable / "
                    f"{storage_capacity} capacity"
                )

    problematic_nodes = [
        n
        for n in nodes
        if not n["is_storage_node"]
        and (
            not n["ready"]
            or n["disk_pressure"]
            or n["memory_pressure"]
            or n["pid_pressure"]
        )
    ]

    if problematic_nodes:
        print(f"\n{Colors.YELLOW}Problematic Non-Storage Nodes:{Colors.END}")
        for node in problematic_nodes:
            print(f"  {Colors.YELLOW}⚠{Colors.END} {node['name']}")
            if not node["ready"]:
                print("    Not Ready")
            if node["disk_pressure"]:
                print("    Disk Pressure")
            if node["memory_pressure"]:
                print("    Memory Pressure")
            if node["pid_pressure"]:
                print("    PID Pressure")
