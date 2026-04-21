"""Analysis functions for storage"""

from collections import defaultdict

from ..utils import (
    Colors,
    NOT_AVAILABLE,
    UNKNOWN,
    ZERO,
    first_item,
    items_or_empty,
)
from ..utils import print_header, print_status
from ..utils import read_yaml_file, read_file


def analyze_storagecluster(mg_dir):
    """Analyze StorageCluster status"""
    print_header("STORAGECLUSTER STATUS")

    sc_file = mg_dir / "namespaces/openshift-storage/oc_output/storagecluster.yaml"
    is_not_ready = False

    if sc_file.exists():
        sc_data = read_yaml_file(sc_file)
        sc = first_item(sc_data)
        if sc:
            # Basic info
            name = sc.get("metadata", {}).get("name", NOT_AVAILABLE)
            print(f"{Colors.CYAN}Name:{Colors.END} {name}")

            # Phase and conditions
            status = sc.get("status", {})
            phase = status.get("phase", UNKNOWN)
            print_status("Phase", phase)

            # Check if StorageCluster is not ready
            if phase not in ["Ready"]:
                is_not_ready = True

            # Version
            version = status.get("version", UNKNOWN)
            print(f"{Colors.CYAN}Version:{Colors.END} {version}")

            # Conditions
            conditions = status.get("conditions", [])
            if conditions:
                print(f"\n{Colors.CYAN}Conditions:{Colors.END}")
                for cond in conditions:
                    cond_type = cond.get("type", UNKNOWN)
                    cond_status = cond.get("status", UNKNOWN)
                    message = cond.get("message", "")
                    reason = cond.get("reason", "")

                    status_str = f"{cond_type}: {cond_status}"
                    if reason:
                        status_str += f" ({reason})"
                    print_status(f"  {cond_type}", cond_status, message)

            # Failure domain
            failure_domain = status.get("failureDomain", NOT_AVAILABLE)
            print(f"\n{Colors.CYAN}Failure Domain:{Colors.END} {failure_domain}")

            # Node count
            failure_domain_values = status.get("failureDomainValues", [])
            print(
                f"{Colors.CYAN}Storage Nodes:{Colors.END} {len(failure_domain_values)}"
            )
            for node in failure_domain_values:
                print(f"  - {node}")

    # Show rook-ceph-operator logs if not ready
    if is_not_ready:
        show_rook_operator_logs(mg_dir)


def analyze_storageclient(mg_dir):
    """Analyze StorageClient status"""
    print_header("STORAGE CLIENT STATUS")

    sc_file = (
        mg_dir
        / "cluster-scoped-resources/ocs.openshift.io/storageclients/ocs-storagecluster.yaml"
    )
    if sc_file.exists():
        sc_data = read_yaml_file(sc_file)
        if sc_data:
            status = sc_data.get("status", {})
            phase = status.get("phase", UNKNOWN)
            client_id = status.get("id", NOT_AVAILABLE)
            maintenance_mode = status.get("inMaintenanceMode", False)

            print_status("Phase", phase)
            print(f"{Colors.CYAN}Client ID:{Colors.END} {client_id}")

            if maintenance_mode:
                print(f"{Colors.YELLOW}⚠ Maintenance Mode:{Colors.END} Enabled")
            else:
                print(f"{Colors.CYAN}Maintenance Mode:{Colors.END} Disabled")

            # Show driver requirements
            cephfs_reqs = status.get("cephFsDriverRequirements", {})
            rbd_reqs = status.get("rbdDriverRequirements", {})

            if cephfs_reqs or rbd_reqs:
                print(f"\n{Colors.CYAN}Driver Requirements:{Colors.END}")
                if cephfs_reqs:
                    host_network = cephfs_reqs.get("ctrlPluginHostNetwork", False)
                    print(f"  CephFS Ctrl Plugin Host Network: {host_network}")
                if rbd_reqs:
                    host_network = rbd_reqs.get("ctrlPluginHostNetwork", False)
                    print(f"  RBD Ctrl Plugin Host Network: {host_network}")
    else:
        print(f"{Colors.YELLOW}StorageClient file not found{Colors.END}")


def analyze_pvcs(mg_dir):
    """Analyze PersistentVolumeClaims"""
    print_header("PERSISTENT VOLUME CLAIMS")

    pvc_file = mg_dir / "namespaces/openshift-storage/core/persistentvolumeclaims.yaml"
    if pvc_file.exists():
        pvc_data = read_yaml_file(pvc_file)
        pvcs = items_or_empty(pvc_data)
        if pvcs:
            # Count by phase
            phase_counts = defaultdict(int)
            storage_class_counts = defaultdict(int)
            total_capacity = ZERO

            pending_pvcs = []

            for pvc in pvcs:
                phase = pvc.get("status", {}).get("phase", UNKNOWN)
                phase_counts[phase] += 1

                storage_class = pvc.get("spec", {}).get("storageClassName", "default")
                storage_class_counts[storage_class] += 1

                # Calculate capacity for bound PVCs
                if phase == "Bound":
                    capacity = (
                        pvc.get("status", {}).get("capacity", {}).get("storage", "0")
                    )
                    # Parse capacity (e.g., "10Gi" -> GB)
                    if isinstance(capacity, str) and capacity.endswith("Gi"):
                        total_capacity += int(capacity[:-2])
                    elif isinstance(capacity, str) and capacity.endswith("Ti"):
                        total_capacity += int(capacity[:-2]) * 1024

                # Track pending PVCs
                if phase == "Pending":
                    pvc_name = pvc.get("metadata", {}).get("name", UNKNOWN)
                    conditions = pvc.get("status", {}).get("conditions", [])
                    reason = UNKNOWN
                    message = ""
                    for cond in conditions:
                        if (
                            cond.get("type") == "Provisioning"
                            and cond.get("status") == "False"
                        ):
                            reason = cond.get("reason", UNKNOWN)
                            message = cond.get("message", "")

                    pending_pvcs.append(
                        {
                            "name": pvc_name,
                            "reason": reason,
                            "message": message,
                            "storage_class": storage_class,
                        }
                    )

            print(f"{Colors.CYAN}Total PVCs:{Colors.END} {len(pvcs)}\n")

            print(f"{Colors.CYAN}By Phase:{Colors.END}")
            for phase in sorted(phase_counts.keys()):
                count = phase_counts[phase]
                # Don't use print_status for counts, just show the numbers with appropriate colors
                if phase == "Bound":
                    print(f"  {Colors.GREEN}✓{Colors.END} {phase}: {count}")
                elif phase == "Pending":
                    print(f"  {Colors.YELLOW}⚠{Colors.END} {phase}: {count}")
                elif phase in ["Failed", "Lost"]:
                    print(f"  {Colors.RED}✗{Colors.END} {phase}: {count}")
                else:
                    print(f"  {phase}: {count}")

            print(f"\n{Colors.CYAN}By StorageClass:{Colors.END}")
            for sc, count in sorted(
                storage_class_counts.items(), key=lambda x: x[1], reverse=True
            ):
                print(f"  {sc}: {count}")

            print(
                f"\n{Colors.CYAN}Total Capacity (Bound):{Colors.END} {total_capacity} Gi"
            )

            if pending_pvcs:
                print(f"\n{Colors.YELLOW}Pending PVCs:{Colors.END}")
                for pvc in pending_pvcs[:10]:  # Show first 10
                    print(f"\n  {Colors.YELLOW}⚠{Colors.END} {pvc['name']}")
                    print(f"    StorageClass: {pvc['storage_class']}")
                    print(f"    Reason: {pvc['reason']}")
                    if pvc["message"]:
                        msg = pvc["message"]
                        if len(msg) > 100:
                            msg = msg[:100] + "..."
                        print(f"    Message: {msg}")

                if len(pending_pvcs) > 10:
                    print(f"\n  ... and {len(pending_pvcs) - 10} more pending PVCs")
    else:
        print(f"{Colors.YELLOW}PVC file not found{Colors.END}")


def analyze_csi_drivers(mg_dir):
    """Analyze CSI driver status"""
    print_header("CSI DRIVER STATUS")

    pods_file = mg_dir / "namespaces/openshift-storage/oc_output/pods"
    if pods_file.exists():
        content = read_file(pods_file)
        if content:
            # Parse CSI pods
            rbd_plugins = []
            cephfs_plugins = []

            for line in content.split("\n"):
                if "rbd.csi.ceph.com" in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        rbd_plugins.append(
                            {"name": parts[0], "ready": parts[1], "status": parts[2]}
                        )
                elif "cephfs.csi.ceph.com" in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        cephfs_plugins.append(
                            {"name": parts[0], "ready": parts[1], "status": parts[2]}
                        )

            # RBD CSI
            print(f"{Colors.CYAN}RBD CSI Driver:{Colors.END}")
            rbd_healthy = 0
            rbd_total = len(rbd_plugins)
            for pod in rbd_plugins:
                ready_parts = pod["ready"].split("/")
                if len(ready_parts) == 2 and ready_parts[0] == ready_parts[1]:
                    rbd_healthy += 1
                    status_color = Colors.GREEN
                    icon = "✓"
                else:
                    status_color = Colors.YELLOW
                    icon = "⚠"

                print(
                    f"  {status_color}{icon}{Colors.END} {pod['name']}: {pod['ready']} {pod['status']}"
                )

            print(f"\n  Summary: {rbd_healthy}/{rbd_total} RBD CSI pods ready")

            # CephFS CSI
            print(f"\n{Colors.CYAN}CephFS CSI Driver:{Colors.END}")
            cephfs_healthy = 0
            cephfs_total = len(cephfs_plugins)
            for pod in cephfs_plugins:
                ready_parts = pod["ready"].split("/")
                if len(ready_parts) == 2 and ready_parts[0] == ready_parts[1]:
                    cephfs_healthy += 1
                    status_color = Colors.GREEN
                    icon = "✓"
                else:
                    status_color = Colors.YELLOW
                    icon = "⚠"

                print(
                    f"  {status_color}{icon}{Colors.END} {pod['name']}: {pod['ready']} {pod['status']}"
                )

            print(f"\n  Summary: {cephfs_healthy}/{cephfs_total} CephFS CSI pods ready")


def show_rook_operator_logs(mg_dir):
    """Show last 50 lines of rook-ceph-operator pod logs"""
    print(
        f"\n{Colors.YELLOW}StorageCluster is not ready - showing rook-ceph-operator logs:{Colors.END}\n"
    )

    # Find rook-ceph-operator pod logs
    pods_dir = mg_dir / "namespaces/openshift-storage/pods"
    if pods_dir.exists():
        for pod_dir in pods_dir.iterdir():
            if pod_dir.is_dir() and "rook-ceph-operator" in pod_dir.name:
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

    print(f"{Colors.YELLOW}Could not find rook-ceph-operator pod logs{Colors.END}\n")
