import logging
from datetime import datetime, timezone

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    fa_functional,
    fusion_access_required,
    stretchcluster_required,
    tier4a,
    yellow_squad,
)
from ocs_ci.helpers.fusion_access_helper import (
    discover_iscsi_luns,
    get_data_zones,
    get_workload_pod,
    get_zone_of_node,
    wait_for_workload_pods,
    verify_baseline_file,
    verify_route_accessible,
    verify_workload_moved_out_of_zone,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources.pod import get_pod_node

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
class TestFusionAccessZoneFailureAndRecovery:
    """
    Tests covering a one of the zone nodes failure and recovery of a Fusion Access stretch
    cluster

    """

    @pytest.mark.polarion_id("OCS-8325")
    def test_zone_failure_and_recovery(
        self,
        nodes,
        node_restart_teardown,
        setup_fusion_access_workloads,
        setup_stretch_cluster_project,
    ):
        """
        Zone Failure and Recovery - only one of the zone nodes

        Steps:
            1) Deploy the logwriter, busybox and file-uploader workloads and
               take a data integrity baseline of every volume
            2) Shut down all the ocpnodes of one data zone and make sure they
               transition to NotReady
            3) Make sure the Deployment based workloads are rescheduled on a
               node of the surviving zone and reach Running state
            4) Verify the data written before the failure is intact and the
               file-uploader route is still accessible from the surviving zone
            5) Restart the nodes of the failed zone and wait for them to rejoin
               the cluster and transition to Ready
            6) Rediscover the shared SAN LUNs so the recovered nodes get their
               iSCSI sessions back
            7) Make sure the zone aware logwriter workload did not see a write
               pause in the surviving zone and that there is no data loss
            8) Verify the data integrity of every volume once more

        Run with conf/ocsci/fusion_access_stretch_cluster.yaml, which sets the
        'arbiter_deployment' flag the 'stretchcluster_required' mark gates on.

        Required config keys in your private cluster config YAML:
            ENV_DATA:
              san_iscsi_ip: "19X.16XX.1.1XX"            # iSCSI target portal IP
              san_iscsi_iqn: "iqn.202X-XX.com.example:storage"  # iSCSI target IQN

        """
        namespace = setup_stretch_cluster_project.namespace
        sc_obj, md5sum_before = setup_fusion_access_workloads()

        # Note the logfiles created so far, used for the data loss check
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        # Fetch the nodes of the zone that needs to be shutdown. The zone is
        # taken from where the Deployment based workloads actually landed, not
        # fixed in advance: failing a zone they do not run in would leave them
        # untouched and the failover checks below would pass without anything
        # having moved. The file-uploader template keeps them together with a
        # pod affinity, which is a preference and not a guarantee, so the
        # co-location is asserted rather than assumed
        data_zones = get_data_zones()
        workload_zones = {}
        for label in WORKLOAD_MOUNT_PATHS:
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            workload_zones[label] = get_zone_of_node(get_pod_node(pod_obj).name)
        log.info(f"Workload placement before the failure: {workload_zones}")

        zone = workload_zones[constants.BUSYBOX_FUSION_LABEL]
        assert zone in data_zones, (
            f"The workloads started in zone {zone}, which is not one of the "
            f"data zones {data_zones}"
        )
        assert set(workload_zones.values()) == {zone}, (
            f"The Deployment based workloads did not all start in the same "
            f"zone, their placement is {workload_zones}. Shutting down zone "
            f"{zone} would leave the workloads of the other zone running and "
            f"their failover would never be exercised"
        )

        nodes_to_shutdown = sc_obj.get_nodes_in_zone(zone)
        assert (
            len(nodes_to_shutdown) != 0
        ), f"There are 0 nodes labeled as {constants.ZONE_LABEL}={zone}!!"

        # Step 1: shutdown the zone nodes
        log.info(
            f"Shutting down zone {zone} nodes "
            f"{[node.name for node in nodes_to_shutdown]}"
        )
        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=nodes_to_shutdown)
        wait_for_nodes_status(
            node_names=[node.name for node in nodes_to_shutdown],
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        log.info(f"Nodes of zone {zone} are shutdown successfully")

        sc_obj.non_quorum_nodes = [node.name for node in nodes_to_shutdown]

        # Step 2: make sure the Deployment based workloads failover to the
        # surviving zone. The volumes are RWX so the replacement pod can attach
        # while the VolumeAttachment of the unreachable node is still around,
        # which means no manual intervention is needed for the failover. It is
        # not immediate though, the original pods are only evicted once the 5
        # minute 'node.kubernetes.io/unreachable' toleration expires, so this
        # waits rather than asserting on the placement right away
        workload_pods = {}
        for label in WORKLOAD_MOUNT_PATHS:
            workload_pods[label] = verify_workload_moved_out_of_zone(
                label=label, namespace=namespace, zone=zone
            )
        log.info("All the Deployment based workloads moved to the surviving zone")

        # Step 3: verify the data written before the failure is intact and the
        # workloads are accessible from the surviving zone
        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            verify_baseline_file(workload_pods[label], mount_path, md5sum_before[label])
        verify_route_accessible(
            route_name=constants.FILE_UPLOADER_FUSION_NAME, namespace=namespace
        )
        log.info("Workloads are accessible with data integrity preserved")

        # TODO: verify the VM is rescheduled on a node of the surviving zone,
        # reaches Running state and that the test file written into it before
        # the failure still matches. Blocked on the CNSA OFD lock regression,
        # see the 'setup_fusion_access_workloads' fixture

        # Step 4: start the zone nodes back up and wait for them to rejoin
        nodes.start_nodes(nodes=nodes_to_shutdown)
        wait_for_nodes_status(timeout=900)

        # The nodes that were powered off come back without their iSCSI
        # sessions, so the shared SAN LUNs have to be rediscovered before the
        # Storage Scale cluster can bring the NSDs of those nodes back up
        discover_iscsi_luns()

        # A node reports Ready well before the Storage Scale filesystem is
        # remounted on it and before the logwriter replicas it used to run are
        # recreated, so wait for the StatefulSet to be whole again rather than
        # assuming the zone is usable as soon as the nodes are Ready
        wait_for_workload_pods(
            labels=[constants.LOGWRITER_RBD_LABEL],
            namespace=namespace,
            expected_count=sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][2],
            timeout=900,
        )
        log.info("The logwriter workload is back to its full set of replicas")

        end_time = datetime.now(timezone.utc)
        log.info(f"Zone {zone} failure started at {start_time} and ended at {end_time}")

        # Step 5: the logwriter replica of the surviving zone must not have
        # seen a write pause, and the replica of the failed zone must come back
        # on its own and restore the zone spread
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, namespace=namespace
        )
        sc_obj.post_failure_checks(
            start_time, end_time, types=["rbd"], wait_for_read_completion=False
        )
        log.info("No write pause seen for the zone aware logwriter workload")

        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "Data loss detected for the logwriter workload"
        log.info("No data loss detected for the logwriter workload")

        # Step 6: verify the data integrity of every volume post recovery
        for pod_obj in sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][0]:
            verify_baseline_file(pod_obj, "/mnt/target", md5sum_before[pod_obj.name])

        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            verify_baseline_file(pod_obj, mount_path, md5sum_before[label])

        verify_route_accessible(
            route_name=constants.FILE_UPLOADER_FUSION_NAME, namespace=namespace
        )
        log.info("All the workloads recovered with data integrity preserved")
