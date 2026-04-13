"""Analysis functions for xml_output"""

import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from xml.dom import minidom

from ..utils import Colors
from ..utils import read_file, read_json_file, read_yaml_file


def prettify_xml(elem):
    """Return a pretty-printed XML string for the Element"""
    rough_string = ET.tostring(elem, encoding="unicode")
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def generate_xml_output(mg_dir, mg_base, output_file):
    """Generate XML format of the analysis"""
    root = ET.Element("odf-must-gather-analysis")
    root.set("generated", datetime.now().isoformat())

    # Collection info
    info = ET.SubElement(root, "collection-info")
    ET.SubElement(info, "base-directory").text = str(mg_base)
    ET.SubElement(info, "data-directory").text = str(mg_dir)

    timestamp_file = mg_base / "timestamp"
    if timestamp_file.exists():
        timestamp = read_file(timestamp_file)
        if timestamp:
            # Parse timestamp - take first line and extract date/time only
            lines = timestamp.strip().split("\n")
            if lines:
                # Extract just the date and time, remove Go runtime info (m=+...)
                first_line = lines[0]
                # Split by space and take first 3 parts (date time timezone)
                parts = first_line.split()
                if len(parts) >= 3:
                    clean_timestamp = " ".join(parts[:3])
                    ET.SubElement(info, "collection-time").text = clean_timestamp
                else:
                    ET.SubElement(info, "collection-time").text = first_line

    # Overall summary
    summary = ET.SubElement(root, "deployment-summary")

    # Get component statuses
    health_file = mg_dir / "ceph/must_gather_commands/ceph_health_detail"
    ceph_health = "UNKNOWN"
    if health_file.exists():
        raw = read_file(health_file)
        if raw is not None:
            ceph_health = raw.strip()
    ET.SubElement(summary, "ceph-health").text = ceph_health

    sc_file = mg_dir / "namespaces/openshift-storage/oc_output/storagecluster.yaml"
    sc_phase = "UNKNOWN"
    if sc_file.exists():
        sc_data = read_yaml_file(sc_file)
        if sc_data and "items" in sc_data and len(sc_data["items"]) > 0:
            sc_phase = sc_data["items"][0].get("status", {}).get("phase", "Unknown")
    ET.SubElement(summary, "storagecluster-phase").text = sc_phase

    noobaa_file = (
        mg_dir / "noobaa/namespaces/openshift-storage/noobaa.io/noobaas/noobaa.yaml"
    )
    noobaa_phase = "UNKNOWN"
    if noobaa_file.exists():
        noobaa = read_yaml_file(noobaa_file)
        if noobaa:
            noobaa_phase = noobaa.get("status", {}).get("phase", "Unknown")
    ET.SubElement(summary, "noobaa-phase").text = noobaa_phase

    # Determine overall status
    if ceph_health == "HEALTH_OK" and sc_phase == "Ready" and noobaa_phase == "Ready":
        overall_status = "HEALTHY"
    elif "Progressing" in sc_phase or "Creating" in noobaa_phase:
        overall_status = "DEPLOYING"
    elif "HEALTH_WARN" in ceph_health:
        overall_status = "DEGRADED"
    else:
        overall_status = "UNHEALTHY"
    ET.SubElement(summary, "overall-status").text = overall_status

    # Nodes
    nodes_section = ET.SubElement(root, "nodes")
    nodes_dir = mg_dir / "cluster-scoped-resources/core/nodes"
    if nodes_dir.exists():
        for node_file in nodes_dir.glob("*.yaml"):
            node_data = read_yaml_file(node_file)
            if node_data:
                node_elem = ET.SubElement(nodes_section, "node")
                node_name = node_data.get("metadata", {}).get("name", "unknown")
                ET.SubElement(node_elem, "name").text = node_name

                labels = node_data.get("metadata", {}).get("labels", {})
                is_storage = "cluster.ocs.openshift.io/openshift-storage" in labels
                ET.SubElement(node_elem, "is-storage-node").text = str(is_storage)

                status = node_data.get("status", {})
                conditions = status.get("conditions", [])
                for cond in conditions:
                    if cond.get("type") == "Ready":
                        ET.SubElement(node_elem, "ready").text = cond.get(
                            "status", "Unknown"
                        )
                        break

    # Pods
    pods_section = ET.SubElement(root, "pods")
    pods_file = mg_dir / "namespaces/openshift-storage/core/pods.yaml"
    if pods_file.exists():
        pods_data = read_yaml_file(pods_file)
        if pods_data and "items" in pods_data:
            phase_counts = defaultdict(int)
            problematic_pods = []

            for pod in pods_data["items"]:
                phase = pod.get("status", {}).get("phase", "Unknown")
                phase_counts[phase] += 1

                pod_name = pod.get("metadata", {}).get("name", "unknown")

                if phase in ["Pending", "Failed"]:
                    problematic_pods.append({"name": pod_name, "phase": phase})
                elif phase == "Running":
                    container_statuses = pod.get("status", {}).get(
                        "containerStatuses", []
                    )
                    for container in container_statuses:
                        if not container.get("ready", True):
                            state = container.get("state", {})
                            if "waiting" in state:
                                reason = state["waiting"].get("reason", "Unknown")
                                problematic_pods.append(
                                    {"name": pod_name, "phase": f"Running-{reason}"}
                                )

            ET.SubElement(pods_section, "total").text = str(len(pods_data["items"]))

            phases = ET.SubElement(pods_section, "phases")
            for phase, count in phase_counts.items():
                phase_elem = ET.SubElement(phases, "phase")
                ET.SubElement(phase_elem, "name").text = phase
                ET.SubElement(phase_elem, "count").text = str(count)

            if problematic_pods:
                problems = ET.SubElement(pods_section, "problematic-pods")
                for pod in problematic_pods[:20]:
                    pod_elem = ET.SubElement(problems, "pod")
                    ET.SubElement(pod_elem, "name").text = pod["name"]
                    ET.SubElement(pod_elem, "phase").text = pod["phase"]

    # CSV/Operators
    operators_section = ET.SubElement(root, "operators")
    csv_file = mg_dir / "namespaces/openshift-storage/oc_output/csv"
    if csv_file.exists():
        content = read_file(csv_file)
        if content:
            lines = content.strip().split("\n")
            if len(lines) > 1:
                succeeded = 0
                failed = 0
                other = 0

                for line in lines[1:]:
                    parts = line.split()
                    if len(parts) >= 4:
                        phase = parts[-1]
                        if phase == "Succeeded":
                            succeeded += 1
                        elif phase in ["Failed", "Error"]:
                            failed += 1
                        else:
                            other += 1

                ET.SubElement(operators_section, "total").text = str(
                    succeeded + failed + other
                )
                ET.SubElement(operators_section, "succeeded").text = str(succeeded)
                ET.SubElement(operators_section, "failed").text = str(failed)
                ET.SubElement(operators_section, "other").text = str(other)

    # Ceph status
    ceph_section = ET.SubElement(root, "ceph-cluster")
    ET.SubElement(ceph_section, "health").text = ceph_health

    status_file = (
        mg_dir
        / "ceph/must_gather_commands_json_output/ceph_status_--format_json-pretty"
    )
    if status_file.exists():
        status = read_json_file(status_file)
        if status:
            mon_map = status.get("monmap", {})
            monitors = ET.SubElement(ceph_section, "monitors")
            ET.SubElement(monitors, "total").text = str(mon_map.get("num_mons", 0))
            ET.SubElement(monitors, "quorum-size").text = str(
                len(status.get("quorum", []))
            )

            osd_map = status.get("osdmap", {})
            osds = ET.SubElement(ceph_section, "osds")
            ET.SubElement(osds, "total").text = str(osd_map.get("num_osds", 0))
            ET.SubElement(osds, "up").text = str(osd_map.get("num_up_osds", 0))
            ET.SubElement(osds, "in").text = str(osd_map.get("num_in_osds", 0))

            pg_map = status.get("pgmap", {})
            pgs = ET.SubElement(ceph_section, "placement-groups")
            ET.SubElement(pgs, "total").text = str(pg_map.get("num_pgs", 0))

    # Operator Subscriptions
    subscriptions_section = ET.SubElement(root, "operator-subscriptions")
    subs_dir = (
        mg_dir / "namespaces/openshift-storage/operators.coreos.com/subscriptions"
    )
    if subs_dir.exists() and subs_dir.is_dir():
        sub_files = list(subs_dir.glob("*.yaml"))
        ET.SubElement(subscriptions_section, "total").text = str(len(sub_files))

        for sub_file in sorted(sub_files):
            sub = read_yaml_file(sub_file)
            if sub:
                sub_elem = ET.SubElement(subscriptions_section, "subscription")
                ET.SubElement(sub_elem, "name").text = sub.get("metadata", {}).get(
                    "name", "Unknown"
                )
                ET.SubElement(sub_elem, "package").text = sub.get("spec", {}).get(
                    "name", "Unknown"
                )
                ET.SubElement(sub_elem, "channel").text = sub.get("spec", {}).get(
                    "channel", "Unknown"
                )
                ET.SubElement(sub_elem, "source").text = sub.get("spec", {}).get(
                    "source", "Unknown"
                )
                ET.SubElement(sub_elem, "state").text = sub.get("status", {}).get(
                    "state", "Unknown"
                )
                ET.SubElement(sub_elem, "current-csv").text = sub.get("status", {}).get(
                    "currentCSV", "Unknown"
                )

    # StorageCluster
    storagecluster_section = ET.SubElement(root, "storagecluster")
    if sc_file.exists():
        sc_data = read_yaml_file(sc_file)
        if sc_data and "items" in sc_data and len(sc_data["items"]) > 0:
            sc = sc_data["items"][0]
            ET.SubElement(storagecluster_section, "name").text = sc.get(
                "metadata", {}
            ).get("name", "N/A")
            ET.SubElement(storagecluster_section, "phase").text = sc.get(
                "status", {}
            ).get("phase", "Unknown")
            ET.SubElement(storagecluster_section, "version").text = sc.get(
                "status", {}
            ).get("version", "Unknown")
            ET.SubElement(storagecluster_section, "failure-domain").text = sc.get(
                "status", {}
            ).get("failureDomain", "N/A")

            # Conditions
            conditions = sc.get("status", {}).get("conditions", [])
            if conditions:
                conds_elem = ET.SubElement(storagecluster_section, "conditions")
                for cond in conditions:
                    cond_elem = ET.SubElement(conds_elem, "condition")
                    ET.SubElement(cond_elem, "type").text = cond.get("type", "Unknown")
                    ET.SubElement(cond_elem, "status").text = cond.get(
                        "status", "Unknown"
                    )
                    ET.SubElement(cond_elem, "reason").text = cond.get("reason", "")
                    ET.SubElement(cond_elem, "message").text = cond.get("message", "")

    # NooBaa
    noobaa_section = ET.SubElement(root, "noobaa")
    if noobaa_file.exists():
        noobaa = read_yaml_file(noobaa_file)
        if noobaa:
            status = noobaa.get("status", {})
            ET.SubElement(noobaa_section, "phase").text = status.get("phase", "Unknown")

            # DB Status
            db_status = status.get("dbStatus", {})
            if db_status:
                db_elem = ET.SubElement(noobaa_section, "database")
                ET.SubElement(db_elem, "cluster-status").text = db_status.get(
                    "dbClusterStatus", "Unknown"
                )
                ET.SubElement(db_elem, "postgresql-version").text = str(
                    db_status.get("currentPgMajorVersion", "Unknown")
                )

            # Conditions
            conditions = status.get("conditions", [])
            if conditions:
                conds_elem = ET.SubElement(noobaa_section, "conditions")
                for cond in conditions:
                    cond_elem = ET.SubElement(conds_elem, "condition")
                    ET.SubElement(cond_elem, "type").text = cond.get("type", "Unknown")
                    ET.SubElement(cond_elem, "status").text = cond.get(
                        "status", "Unknown"
                    )
                    ET.SubElement(cond_elem, "message").text = cond.get("message", "")

    # NooBaa BackingStores
    backingstores_section = ET.SubElement(root, "noobaa-backingstores")
    bs_dir = mg_dir / "noobaa/namespaces/openshift-storage/noobaa.io/backingstores"
    if bs_dir.exists():
        bs_files = list(bs_dir.glob("*.yaml"))
        ET.SubElement(backingstores_section, "total").text = str(len(bs_files))

        for bs_file in bs_files:
            bs_data = read_yaml_file(bs_file)
            if bs_data:
                bs_elem = ET.SubElement(backingstores_section, "backingstore")
                ET.SubElement(bs_elem, "name").text = bs_data.get("metadata", {}).get(
                    "name", "unknown"
                )
                ET.SubElement(bs_elem, "type").text = bs_data.get("spec", {}).get(
                    "type", "Unknown"
                )
                ET.SubElement(bs_elem, "phase").text = bs_data.get("status", {}).get(
                    "phase", "Unknown"
                )
                mode = (
                    bs_data.get("status", {}).get("mode", {}).get("modeCode", "Unknown")
                )
                ET.SubElement(bs_elem, "mode").text = mode

    # Ceph Pools
    ceph_pools_section = ET.SubElement(root, "ceph-pools")
    pool_file = (
        mg_dir
        / "ceph/must_gather_commands_json_output/ceph_osd_dump_--format_json-pretty"
    )
    if pool_file.exists():
        pool_data = read_json_file(pool_file)
        if pool_data and "pools" in pool_data:
            pools = pool_data["pools"]
            ET.SubElement(ceph_pools_section, "total").text = str(len(pools))

            for pool in pools:
                pool_elem = ET.SubElement(ceph_pools_section, "pool")
                ET.SubElement(pool_elem, "name").text = pool.get("pool_name", "unknown")
                ET.SubElement(pool_elem, "id").text = str(pool.get("pool", "N/A"))
                ET.SubElement(pool_elem, "type").text = (
                    "replicated" if pool.get("type", 1) == 1 else "erasure"
                )
                ET.SubElement(pool_elem, "size").text = str(pool.get("size", 0))
                ET.SubElement(pool_elem, "min-size").text = str(pool.get("min_size", 0))
                ET.SubElement(pool_elem, "pg-num").text = str(pool.get("pg_num", 0))

    # Storage Client
    storageclient_section = ET.SubElement(root, "storageclient")
    sc_client_file = (
        mg_dir
        / "cluster-scoped-resources/ocs.openshift.io/storageclients/ocs-storagecluster.yaml"
    )
    if sc_client_file.exists():
        sc_data = read_yaml_file(sc_client_file)
        if sc_data:
            status = sc_data.get("status", {})
            ET.SubElement(storageclient_section, "phase").text = status.get(
                "phase", "Unknown"
            )
            ET.SubElement(storageclient_section, "client-id").text = status.get(
                "id", "N/A"
            )
            ET.SubElement(storageclient_section, "maintenance-mode").text = str(
                status.get("inMaintenanceMode", False)
            )

    # PVCs
    pvcs_section = ET.SubElement(root, "persistent-volume-claims")
    pvc_file = mg_dir / "namespaces/openshift-storage/core/persistentvolumeclaims.yaml"
    if pvc_file.exists():
        pvc_data = read_yaml_file(pvc_file)
        if pvc_data and "items" in pvc_data:
            phase_counts = defaultdict(int)
            pending_pvcs = []

            for pvc in pvc_data["items"]:
                phase = pvc.get("status", {}).get("phase", "Unknown")
                phase_counts[phase] += 1

                if phase == "Pending":
                    pvc_name = pvc.get("metadata", {}).get("name", "unknown")
                    storage_class = pvc.get("spec", {}).get(
                        "storageClassName", "default"
                    )
                    pending_pvcs.append(
                        {"name": pvc_name, "storage_class": storage_class}
                    )

            ET.SubElement(pvcs_section, "total").text = str(len(pvc_data["items"]))

            phases = ET.SubElement(pvcs_section, "phases")
            for phase, count in phase_counts.items():
                phase_elem = ET.SubElement(phases, "phase")
                ET.SubElement(phase_elem, "name").text = phase
                ET.SubElement(phase_elem, "count").text = str(count)

            if pending_pvcs:
                pending_elem = ET.SubElement(pvcs_section, "pending-pvcs")
                for pvc in pending_pvcs[:10]:
                    pvc_elem = ET.SubElement(pending_elem, "pvc")
                    ET.SubElement(pvc_elem, "name").text = pvc["name"]
                    ET.SubElement(pvc_elem, "storage-class").text = pvc["storage_class"]

    # CSI Drivers
    csi_section = ET.SubElement(root, "csi-drivers")
    pods_output_file = mg_dir / "namespaces/openshift-storage/oc_output/pods"
    if pods_output_file.exists():
        content = read_file(pods_output_file)
        if content:
            rbd_count = 0
            rbd_ready = 0
            cephfs_count = 0
            cephfs_ready = 0

            for line in content.split("\n"):
                if "rbd.csi.ceph.com" in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        rbd_count += 1
                        ready_parts = parts[1].split("/")
                        if len(ready_parts) == 2 and ready_parts[0] == ready_parts[1]:
                            rbd_ready += 1
                elif "cephfs.csi.ceph.com" in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        cephfs_count += 1
                        ready_parts = parts[1].split("/")
                        if len(ready_parts) == 2 and ready_parts[0] == ready_parts[1]:
                            cephfs_ready += 1

            rbd_elem = ET.SubElement(csi_section, "rbd-csi")
            ET.SubElement(rbd_elem, "total").text = str(rbd_count)
            ET.SubElement(rbd_elem, "ready").text = str(rbd_ready)

            cephfs_elem = ET.SubElement(csi_section, "cephfs-csi")
            ET.SubElement(cephfs_elem, "total").text = str(cephfs_count)
            ET.SubElement(cephfs_elem, "ready").text = str(cephfs_ready)

    # Events (Warnings & Errors)
    events_section = ET.SubElement(root, "events")
    events_file = mg_dir / "namespaces/openshift-storage/core/events.yaml"
    if events_file.exists():
        events_data = read_yaml_file(events_file)
        if events_data and "items" in events_data:
            events = events_data["items"]

            warning_events = [e for e in events if e.get("type", "") == "Warning"]
            ET.SubElement(events_section, "total-warnings").text = str(
                len(warning_events)
            )

            # Count by reason
            reason_counts = defaultdict(int)
            for event in warning_events:
                reason = event.get("reason", "Unknown")
                reason_counts[reason] += 1

            reasons_elem = ET.SubElement(events_section, "warning-summary")
            for reason, count in sorted(
                reason_counts.items(), key=lambda x: x[1], reverse=True
            )[:10]:
                reason_elem = ET.SubElement(reasons_elem, "reason")
                ET.SubElement(reason_elem, "name").text = reason
                ET.SubElement(reason_elem, "count").text = str(count)

    # Write XML to file
    try:
        xml_string = prettify_xml(root)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(xml_string)
    except OSError as exc:
        print(
            f"{Colors.RED}Error writing XML output to {output_file}: {exc}{Colors.END}",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    except Exception as exc:
        print(
            f"{Colors.RED}Error building or writing XML output: {exc}{Colors.END}",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    print(f"\n{Colors.GREEN}✓ XML output written to: {output_file}{Colors.END}")
