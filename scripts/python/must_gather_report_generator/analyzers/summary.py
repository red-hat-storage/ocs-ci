"""Analysis functions for summary"""

from ..utils import Colors, HEALTH_STATUS_UNKNOWN, UNKNOWN, first_item, print_header
from ..utils import read_yaml_file, read_file


def generate_summary(mg_dir, deployment_type="internal"):
    """Generate overall deployment summary"""
    print_header("DEPLOYMENT SUMMARY")

    # Determine overall status
    ceph_health = HEALTH_STATUS_UNKNOWN

    if deployment_type == "internal":
        health_file = mg_dir / "ceph/must_gather_commands/ceph_health_detail"
        if health_file.exists():
            raw = read_file(health_file)
            if raw is not None:
                ceph_health = raw.strip()
    elif deployment_type == "external":
        ceph_health = "EXTERNAL_CEPH"
        # Try to get status from StorageCluster
        sc_file = mg_dir / "namespaces/openshift-storage/oc_output/storagecluster.yaml"
        if sc_file.exists():
            sc_data = read_yaml_file(sc_file)
            sc_item = first_item(sc_data)
            if sc_item:
                # Check for external storage health in status
                external_status = sc_item.get("status", {}).get("externalStorage", {})
                if external_status:
                    # Use whatever health info is available
                    ext_health = external_status.get("health", "UNKNOWN")
                    ceph_health = f"EXTERNAL ({ext_health})"

    sc_file = mg_dir / "namespaces/openshift-storage/oc_output/storagecluster.yaml"
    sc_phase = HEALTH_STATUS_UNKNOWN
    if sc_file.exists():
        sc_data = read_yaml_file(sc_file)
        sc_item = first_item(sc_data)
        if sc_item:
            sc_phase = sc_item.get("status", {}).get("phase", UNKNOWN)

    noobaa_file = (
        mg_dir / "noobaa/namespaces/openshift-storage/noobaa.io/noobaas/noobaa.yaml"
    )
    noobaa_phase = HEALTH_STATUS_UNKNOWN
    if noobaa_file.exists():
        noobaa = read_yaml_file(noobaa_file)
        if noobaa:
            noobaa_phase = noobaa.get("status", {}).get("phase", UNKNOWN)

    # Overall status determination
    if deployment_type == "external":
        # Adjusted logic for external - don't require NooBaa or internal Ceph health
        if sc_phase == "Ready":
            overall_status = "HEALTHY"
            status_color = Colors.GREEN
            icon = "✓"
        elif "Progressing" in sc_phase:
            overall_status = "DEPLOYING"
            status_color = Colors.YELLOW
            icon = "⚠"
        else:
            overall_status = "DEGRADED"
            status_color = Colors.YELLOW
            icon = "⚠"
    else:
        # Original internal logic
        if (
            ceph_health == "HEALTH_OK"
            and sc_phase == "Ready"
            and noobaa_phase == "Ready"
        ):
            overall_status = "HEALTHY"
            status_color = Colors.GREEN
            icon = "✓"
        elif "Progressing" in sc_phase or "Creating" in noobaa_phase:
            overall_status = "DEPLOYING"
            status_color = Colors.YELLOW
            icon = "⚠"
        elif "HEALTH_WARN" in ceph_health:
            overall_status = "DEGRADED"
            status_color = Colors.YELLOW
            icon = "⚠"
        else:
            overall_status = "UNHEALTHY"
            status_color = Colors.RED
            icon = "✗"

    print(f"{status_color}{icon} OVERALL STATUS: {overall_status}{Colors.END}")
    print(f"  {Colors.CYAN}Deployment Type: {deployment_type.upper()}{Colors.END}")
    print(f"  Ceph Cluster: {ceph_health}")
    print(f"  StorageCluster: {sc_phase}")
    if deployment_type == "internal":
        print(f"  NooBaa: {noobaa_phase}")

    # Recommendations
    print(f"\n{Colors.CYAN}Recommendations:{Colors.END}")
    if overall_status == "HEALTHY":
        print(
            f"  {Colors.GREEN}✓ ODF deployment is healthy and ready to use{Colors.END}"
        )
    elif overall_status == "DEPLOYING":
        print(f"  {Colors.YELLOW}⚠ ODF deployment is in progress{Colors.END}")
        print("  - Wait for all components to reach Ready state")
        print("  - Monitor pod status for any failures")
        print("  - Check CSI driver registration on all nodes")
    elif overall_status == "DEGRADED":
        print(f"  {Colors.YELLOW}⚠ ODF deployment has warnings{Colors.END}")
        print("  - Review Ceph health details above")
        print("  - Check for OSD or PG issues")
        print("  - Verify all storage nodes are accessible")
    else:
        print(f"  {Colors.RED}✗ ODF deployment has critical issues{Colors.END}")
        print("  - Review all error messages above")
        print("  - Check pod logs for failed components")
        print("  - Verify network connectivity between nodes")
