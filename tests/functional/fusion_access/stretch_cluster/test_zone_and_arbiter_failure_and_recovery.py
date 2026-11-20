import logging
from datetime import datetime, timezone
from time import sleep

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
    wait_for_api_available,
    wait_for_api_unavailable,
    wait_for_filesystem_mounted,
    wait_for_gpfs_cluster_active,
    wait_for_workload_pods,
    verify_baseline_file,
    verify_route_accessible,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources.pod import get_pod_node

log = logging.getLogger(__name__)

WORKLOAD_MOUNT_PATHS = {
    constants.BUSYBOX_FUSION_LABEL: "/mnt/test",
    constants.FILE_UPLOADER_FUSION_LABEL: "/srv",
}

# How long the cluster is left without quorum before the nodes are started
# again. Long enough that the surviving zone has really settled into the
# quorum loss rather than the test racing the failure detection
OUTAGE_DURATION = 600


@tier4a
@yellow_squad
@fa_functional
@fusion_access_required
@stretchcluster_required
class TestFusionAccessZoneAndArbiterFailureAndRecovery:
    """
    Tests covering the simultaneous failure and recovery of one data zone and
    the arbiter of a Fusion Access stretch cluster

    """

    @pytest.mark.polarion_id("OCS-8352")
    def test_zone_and_arbiter_failure_and_recovery(
        self,
        nodes,
        node_restart_teardown,
        setup_fusion_access_workloads,
        setup_stretch_cluster_project,
    ):
        """
        One data zone and the arbiter go down together, taking the quorum of
        the Storage Scale cluster with them

        Unlike the single zone failure, this is not a failover test: with the
        arbiter and one data zone gone only two of the five quorum nodes are
        left, so there is no majority and the filesystem becomes unavailable
        on the surviving zone too. Nothing fails over because there is nowhere
        to fail over to. What is verified is that the loss is graceful and
        that the cluster comes back whole, with its data intact, once the
        nodes return.

        The same outage also takes two of the three OCP control plane nodes,
        so etcd loses quorum and the OpenShift API goes away for the duration.
        Nothing can be observed through the API while the zone is down, which
        is why the test asserts on the state either side of the outage and on
        the API disappearing, rather than polling through it. The nodes are
        stopped and started through the platform layer, which does not go via
        the API and therefore keeps working.

        Steps:
            1) Deploy the logwriter, busybox and file-uploader workloads and
               take a data integrity baseline of every volume
            2) Shut down the nodes of one data zone together with the arbiter
               node
            3) Make sure the OpenShift API becomes unavailable, which confirms
               the quorum was really lost
            4) Leave the cluster without quorum for OUTAGE_DURATION
            5) Start every node that was shut down
            6) Wait for the API to answer again, then for all the nodes to be
               Ready
            7) Rediscover the SAN LUNs, then wait for the Storage Scale
               cluster to be active again and the filesystem to be mounted on
               every node that was down
            8) Wait for the workloads to be running again and verify the data
               written before the failure is intact on every volume

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

        # Unlike the single zone tests the zone is not chosen from where the
        # workloads landed. Quorum is lost either way, so there is no failover
        # to exercise and no reason to prefer one data zone over the other.
        # The first one is taken so the test is deterministic
        data_zones = get_data_zones()
        zone = data_zones[0]
        log.info(f"Data zones {data_zones}, failing {zone} along with the arbiter")

        zone_nodes = sc_obj.get_nodes_in_zone(zone)
        arbiter_nodes = sc_obj.get_nodes_in_zone(
            constants.FUSION_ACCESS_ARBITER_ZONE_LABEL
        )
        assert (
            len(zone_nodes) != 0
        ), f"There are 0 nodes labeled as {constants.ZONE_LABEL}={zone}!!"
        assert len(arbiter_nodes) != 0, (
            f"There are 0 nodes labeled as {constants.ZONE_LABEL}="
            f"{constants.FUSION_ACCESS_ARBITER_ZONE_LABEL}!! The cluster has "
            f"no arbiter, so this test has nothing to take down with the zone"
        )

        nodes_to_shutdown = zone_nodes + arbiter_nodes
        node_names = [node.name for node in nodes_to_shutdown]

        # Record where the workloads were, to tell afterwards whether the ones
        # of the surviving zone stayed put. They are expected to, nothing
        # evicts them, but they do lose their storage along with everyone else
        placement_before = {}
        for label in WORKLOAD_MOUNT_PATHS:
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            placement_before[label] = get_zone_of_node(get_pod_node(pod_obj).name)
        log.info(f"Workload placement before the failure: {placement_before}")

        # Step 1: shut the zone and the arbiter down together
        log.info(f"Shutting down zone {zone} and the arbiter, nodes {node_names}")
        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=nodes_to_shutdown)
        sc_obj.non_quorum_nodes = node_names

        # Step 2: the API going away is the proof that the quorum really went
        # with those nodes. Asserting it also stops the test from carrying on
        # against a cluster that never lost quorum, where everything below
        # would pass for the wrong reason
        wait_for_api_unavailable()
        log.info(
            f"Zone {zone} and the arbiter are down, the cluster has lost "
            f"quorum and the OpenShift API is unavailable"
        )

        # Step 3: hold the outage. Nothing is observable here, the API is gone
        log.info(f"Holding the outage for {OUTAGE_DURATION} seconds")
        sleep(OUTAGE_DURATION)

        # Step 4: bring everything back
        log.info(f"Starting the nodes {node_names}")
        nodes.start_nodes(nodes=nodes_to_shutdown)

        # etcd has to re-form quorum and catch up before anything can be
        # queried, which takes considerably longer than the nodes booting
        wait_for_api_available()
        wait_for_nodes_status(timeout=1800)
        log.info("Every node is Ready again")

        # The nodes come back without their iSCSI sessions, so the shared SAN
        # LUNs have to be rediscovered before the Storage Scale cluster can
        # bring the NSDs of those nodes back up. The nodes that were down are
        # passed explicitly, a data zone of a stretch cluster also holds a
        # control plane node and the worker node default would skip it
        discover_iscsi_luns(node_names=node_names)

        # Step 5: a node being Ready says nothing about GPFS on it. After a
        # quorum loss the whole cluster has to re-form, so wait for every node
        # to report active and for the filesystem to be mounted again
        wait_for_gpfs_cluster_active()
        wait_for_filesystem_mounted(node_names=node_names)
        log.info("The Storage Scale cluster is active and the filesystem is mounted")

        wait_for_workload_pods(
            labels=[constants.LOGWRITER_RBD_LABEL],
            namespace=namespace,
            expected_count=sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][2],
            timeout=1800,
        )
        wait_for_workload_pods(
            labels=list(WORKLOAD_MOUNT_PATHS), namespace=namespace, timeout=1800
        )
        log.info("Every workload is running again")

        end_time = datetime.now(timezone.utc)
        log.info(
            f"Zone {zone} and arbiter failure started at {start_time} and "
            f"ended at {end_time}"
        )

        # Step 6: the data written before the quorum was lost has to be intact
        # on every volume. This is the point of the test: an unplanned loss of
        # quorum must not cost any data
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, namespace=namespace
        )
        for pod_obj in sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][0]:
            verify_baseline_file(pod_obj, "/mnt/target", md5sum_before[pod_obj.name])

        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            verify_baseline_file(pod_obj, mount_path, md5sum_before[label])

        verify_route_accessible(
            route_name=constants.FILE_UPLOADER_FUSION_NAME, namespace=namespace
        )
        log.info("Every workload recovered with its data intact")

        # The write pause checks of the single zone tests are deliberately not
        # done here. The logwriter of the surviving zone loses its storage too
        # when quorum goes, so a pause is expected rather than a failure, and
        # asserting there was none would be asserting the opposite of what
        # this scenario is documented to do
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "Data loss detected for the logwriter workload"
        log.info("No data loss detected for the logwriter workload")
