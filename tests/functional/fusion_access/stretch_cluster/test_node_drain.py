import logging
import random
import time
from datetime import datetime, timezone

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    fa_functional,
    fusion_access_required,
    polarion_id,
    stretchcluster_required,
    tier4a,
    yellow_squad,
)
from ocs_ci.helpers.fusion_access_helper import (
    get_data_zones,
    get_workload_pod,
    get_zone_nodes,
    get_zone_of_node,
    verify_baseline_file,
    verify_route_accessible,
    wait_for_filesystem_mounted,
    wait_for_gpfs_cluster_active,
    wait_for_workload_pods,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import drain_nodes, schedule_nodes, wait_for_nodes_status
from ocs_ci.ocs.resources.pod import get_pod_node
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)

WORKLOAD_MOUNT_PATHS = {
    constants.BUSYBOX_FUSION_LABEL: "/mnt/test",
    constants.FILE_UPLOADER_FUSION_LABEL: "/srv",
}


@tier4a
@yellow_squad
@fa_functional
@fusion_access_required
@stretchcluster_required
class TestFusionAccessNodeDrain:
    """
    Tests covering node drain in a data zone and recovery in a Fusion Access
    stretch cluster
    """

    @polarion_id("OCS-8379")
    def test_zone_node_drain(
        self,
        setup_fusion_access_workloads,
        setup_stretch_cluster_project,
    ):
        """
        Drain nodes of a data zone one by one and verify that any workload 
        pod running on the drained node moves to another node, 
        schedule the node back, and repeat for all nodes in the zone

        Steps:
            1) Deploy the logwriter, busybox and file-uploader workloads and
               take a data integrity baseline of every volume
            2) Select a data zone and fetch the nodes in that zone
            3) For each node in the zone:
               a) Identify any workload pod currently running on this node
               b) Drain the node with disable_eviction=True
               c) Verify the node transitions to SchedulingDisabled
               d) If a workload pod was present, verify it moved to another node,
                  reached Running state, and has its data intact
               e) Schedule (uncordon) the node back and verify Ready state
               f) Wait for GPFS daemon to be active and filesystem mounted
            4) Record overall start and end time of the drain sequence and perform
               post-drain checks
            5) Verify all workloads are running with expected replica counts
            6) Verify data integrity on every volume and route accessibility
            7) Verify no data loss occurred on the logwriter workload

        Run with conf/ocsci/fusion_access_stretch_cluster.yaml, which sets the
        'arbiter_deployment' flag the 'stretchcluster_required' mark gates on.
        """
        namespace = setup_stretch_cluster_project.namespace
        sc_obj, md5sum_before = setup_fusion_access_workloads()

        # Note the logfiles created so far, used for the data loss check
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        # Identify data zones and workload placement
        data_zones = get_data_zones()
        workload_zones = {}
        for label in WORKLOAD_MOUNT_PATHS:
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            workload_zones[label] = get_zone_of_node(get_pod_node(pod_obj).name)
        log.info(f"Workload placement before node drain: {workload_zones}")

        # Choose the zone where workloads are running, or randomly select from data zones
        zone = workload_zones.get(constants.BUSYBOX_FUSION_LABEL)
        if not zone or zone not in data_zones:
            zone = random.choice(data_zones)

        zone_nodes = get_zone_nodes(zone)
        assert (
            len(zone_nodes) != 0
        ), f"There are 0 nodes labeled with {constants.ZONE_LABEL}={zone}!"
        nodes_to_drain = sorted(zone_nodes)
        log.info(f"Nodes in zone {zone} to be drained one by one: {nodes_to_drain}")

        # Drain and restore nodes one by one in the selected zone
        start_time = datetime.now(timezone.utc)
        currently_drained = None
        try:
            for node_name in nodes_to_drain:
                log.info(f"--- Draining node: {node_name} ---")

                # Step 3a: Check which workload pods are running on this node before drain
                pods_on_node = []
                for label in WORKLOAD_MOUNT_PATHS:
                    pod_obj = get_workload_pod(label=label, namespace=namespace)
                    if get_pod_node(pod_obj).name == node_name:
                        pods_on_node.append((label, pod_obj.name))

                if pods_on_node:
                    log.info(f"Workload pods currently running on {node_name}: {pods_on_node}")
                else:
                    log.info(f"No deployment workload pods currently running on {node_name}")

                # Step 3b & 3c: Drain the node with eviction disabled to bypass CNSA PDB
                drain_nodes(node_names=[node_name], disable_eviction=True)
                currently_drained = node_name
                wait_for_nodes_status(
                    node_names=[node_name],
                    status=constants.NODE_READY_SCHEDULING_DISABLED,
                    timeout=300,
                )
                log.info(f"Node {node_name} is drained and scheduling is disabled")

                # Step 3d: If workload pods were on this node, verify they moved to another node
                for label, old_pod_name in pods_on_node:
                    log.info(
                        f"Verifying workload {label} (was {old_pod_name} on {node_name}) "
                        f"moved to another node and reached Running state"
                    )

                    def _pod_moved_and_running():
                        try:
                            pod = get_workload_pod(label=label, namespace=namespace)
                            cur_node = get_pod_node(pod).name
                            if cur_node != node_name and pod.status() == constants.STATUS_RUNNING:
                                return pod
                        except Exception as ex:
                            log.debug(f"Waiting for pod {label} to reschedule: {ex}")
                        return None

                    moved_pod = None
                    for pod in TimeoutSampler(
                        timeout=600, sleep=10, func=_pod_moved_and_running
                    ):
                        if pod:
                            moved_pod = pod
                            break

                    assert moved_pod is not None, (
                        f"Workload {label} failed to move off drained node {node_name} "
                        f"within timeout!"
                    )
                    new_node = get_pod_node(moved_pod).name
                    log.info(
                        f"Workload {label} successfully moved to node {new_node} "
                        f"(pod: {moved_pod.name})"
                    )

                    # Verify data integrity on the moved workload
                    verify_baseline_file(
                        moved_pod, WORKLOAD_MOUNT_PATHS[label], md5sum_before[label]
                    )
                    log.info(f"Data integrity verified on rescheduled workload {label}")

                # Step 3e: Schedule (uncordon) the node back
                log.info(f"Scheduling (uncordoning) node {node_name} back")
                schedule_nodes(node_names=[node_name])
                currently_drained = None
                wait_for_nodes_status(
                    node_names=[node_name],
                    status=constants.NODE_READY,
                    timeout=300,
                )
                log.info(f"Node {node_name} is Ready and schedulable")

                # Step 3f: Wait for GPFS daemon and filesystem mount to recover on the node
                wait_for_gpfs_cluster_active()
                wait_for_filesystem_mounted(node_names=[node_name])
                log.info(f"GPFS active and filesystem mounted on node {node_name}")

        finally:
            if currently_drained:
                log.warning(
                    f"Cleaning up: re-scheduling drained node: {currently_drained}"
                )
                schedule_nodes(node_names=[currently_drained])
                wait_for_nodes_status(
                    node_names=[currently_drained],
                    status=constants.NODE_READY,
                    timeout=300,
                )

        end_time = datetime.now(timezone.utc)
        log.info(
            f"Zone {zone} node drain sequence started at {start_time} and ended at {end_time}"
        )

        # Post-failure checks for logwriter
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, namespace=namespace
        )
        sc_obj.post_failure_checks(
            start_time, end_time, types=["rbd"], wait_for_read_completion=False
        )
        log.info("Logwriter post-failure checks passed")

        # Verify all workloads are running with expected replica counts
        wait_for_workload_pods(
            labels=[constants.LOGWRITER_RBD_LABEL],
            namespace=namespace,
            expected_count=sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][2],
            timeout=1800,
        )
        wait_for_workload_pods(
            labels=list(WORKLOAD_MOUNT_PATHS),
            namespace=namespace,
            timeout=1800,
        )
        log.info("Every workload is running after node drain and recovery")

        # Verify data written before the drain is intact on every volume
        for pod_obj in sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][0]:
            verify_baseline_file(pod_obj, "/mnt/target", md5sum_before[pod_obj.name])

        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            verify_baseline_file(pod_obj, mount_path, md5sum_before[label])

        verify_route_accessible(
            route_name=constants.FILE_UPLOADER_FUSION_NAME, namespace=namespace
        )
        log.info("Every workload recovered with its data intact")

        # Verify no data loss detected for the logwriter workload
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "Data loss detected for the logwriter workload"
        log.info("No data loss detected for the logwriter workload")
