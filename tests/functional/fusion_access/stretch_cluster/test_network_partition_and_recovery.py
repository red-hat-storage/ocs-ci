import logging
import time
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
    get_data_zones,
    get_workload_pod,
    get_zone_nodes,
    get_zone_of_node,
    schedule_zone_netsplit,
    verify_io_on_pod,
    wait_for_filesystem_available_in_a_data_zone,
    wait_for_filesystem_mounted,
    wait_for_gpfs_cluster_active,
    wait_for_workload_pods,
    verify_baseline_file,
    verify_route_accessible,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pod_node

log = logging.getLogger(__name__)

WORKLOAD_MOUNT_PATHS = {
    constants.BUSYBOX_FUSION_LABEL: "/mnt/test",
    constants.FILE_UPLOADER_FUSION_LABEL: "/srv",
}

# How long the zones stay partitioned. The ocp-network-split units lift the
# split on their own after this, so there is nothing to undo even if the test
# fails in the middle
PARTITION_DURATION = 15


@tier4a
@yellow_squad
@fa_functional
@fusion_access_required
@stretchcluster_required
class TestFusionAccessNetworkPartition:
    """
    Tests covering a network partition between the zones of a Fusion Access
    stretch cluster and the recovery once the link is restored

    """

    @pytest.mark.parametrize(
        argnames="split_name",
        argvalues=[
            pytest.param(
                constants.NETSPLIT_DATA_1_DATA_2,
                marks=pytest.mark.polarion_id("OCS-8371"),
            ),
            pytest.param(
                constants.NETSPLIT_ARBITER_DATA_1,
                marks=pytest.mark.polarion_id("OCS-8372"),
            ),
        ],
        ids=["data-zone-to-data-zone", "arbiter-to-data-zone"],
    )
    def test_network_partition_and_recovery(
        self,
        split_name,
        setup_fusion_access_workloads,
        setup_stretch_cluster_project,
    ):
        """
        A network partition between the zones of the stretch cluster, with
        the nodes themselves left running

        Two partitions are covered:

        * data zone to data zone, where the two data zones cannot reach each
          other but both still reach the arbiter. The arbiter breaks the tie,
          one data zone keeps quorum and carries on serving the filesystem
          while the other is expelled
        * arbiter to data zone, where one data zone cannot reach the arbiter
          but the two data zones still reach each other. A majority can still
          form, so the cluster is expected to stay whole

        Which of the two data zones survives the first case is decided by the
        cluster and is not fixed in advance, so the test discovers it rather
        than assuming it.

        This is distinct from the node and SAN failure tests. Nothing is
        powered off and no storage is disconnected, the nodes stay Ready
        throughout and only the links between zones go away, which is what a
        site to site link failure looks like.

        Steps:
            1) Deploy the logwriter, busybox and file-uploader workloads and
               take a data integrity baseline of every volume
            2) Schedule the network partition between the zones
            3) While the partition is in place, verify a whole data zone still
               has the filesystem mounted and that a workload there can still
               write to and read back from its volume
            4) Wait for the partition to be lifted
            5) Wait for every node to report GPFS active again and for the
               filesystem to be mounted on all of them
            6) Wait for the workloads to be running again and verify the data
               written before the partition is intact on every volume

        The cluster has to have been deployed with
        DEPLOYMENT['network_split_setup'] enabled, the partition is applied by
        the ocp-network-split systemd units that the deployment lays down on
        every node. Without them 'schedule_split' has nothing to drive.

        Run with conf/ocsci/fusion_access_stretch_cluster.yaml, which sets the
        'arbiter_deployment' flag the 'stretchcluster_required' mark gates on.

        """
        namespace = setup_stretch_cluster_project.namespace
        sc_obj, md5sum_before = setup_fusion_access_workloads()

        # Note the logfiles created so far, used for the data loss check
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        data_zones = get_data_zones()
        all_nodes = set()
        for zone in data_zones:
            all_nodes |= get_zone_nodes(zone)
        log.info(f"Data zones {data_zones}, partition {split_name}")

        placement_before = {}
        for label in WORKLOAD_MOUNT_PATHS:
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            placement_before[label] = get_zone_of_node(get_pod_node(pod_obj).name)
        log.info(f"Workload placement before the partition: {placement_before}")

        # Step 1: schedule the partition. It is scheduled a few minutes ahead
        # so that every node starts it at the same moment, and the units lift
        # it on their own once the duration is up
        start_time, end_time = schedule_zone_netsplit(
            split_name=split_name, duration=PARTITION_DURATION
        )

        # Wait for the partition to actually start before asserting anything
        # about it, the checks below are meaningless while the link is still up
        lead = (start_time - datetime.now(timezone.utc)).total_seconds()
        if lead > 0:
            log.info(f"Waiting {int(lead)} seconds for the partition to start")
            time.sleep(lead)
        log.info(f"The zones are now partitioned, {split_name}")

        # Step 2: a whole data zone has to still have the filesystem. This is
        # what 'workloads continue on the accessible zone' means: the losing
        # side is expelled and loses its mount, the winning side carries on
        surviving_zone = wait_for_filesystem_available_in_a_data_zone(
            data_zones=data_zones
        )

        # Staying in Running state proves nothing on its own, a pod whose
        # filesystem has gone away keeps running until something touches the
        # mount. Writing and reading back is what shows the zone really works
        surviving_nodes = get_zone_nodes(surviving_zone)
        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            if get_pod_node(pod_obj).name in surviving_nodes:
                verify_io_on_pod(pod_obj, mount_path)
            else:
                log.info(
                    f"Workload {label} is not in the surviving zone "
                    f"{surviving_zone}, skipping its IO check"
                )

        # Step 3: let the partition run its course. The units lift it, there
        # is nothing to undo here
        remaining = (end_time - datetime.now(timezone.utc)).total_seconds()
        if remaining > 0:
            log.info(f"Waiting {int(remaining)} seconds for the partition to lift")
            time.sleep(remaining)
        log.info(f"The network partition {split_name} has been lifted")

        # Step 4: the expelled nodes have to rejoin on their own. Being
        # reachable again is not the same as having rejoined the Storage Scale
        # cluster, so both the daemon state and the mounts are waited for
        wait_for_gpfs_cluster_active()
        wait_for_filesystem_mounted(node_names=sorted(all_nodes))
        log.info("Every node rejoined the cluster and remounted the filesystem")

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

        # Step 5: the data written before the partition has to be intact on
        # every volume, including the ones of the zone that was expelled
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

        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "Data loss detected for the logwriter workload"
        log.info("No data loss detected for the logwriter workload")

        # A write pause is deliberately not asserted against. The logwriter
        # replica of the expelled zone loses its storage for the duration of
        # the partition, so a pause there is the documented behaviour rather
        # than a failure
        log.info(
            f"Partition {split_name} ran from {start_time} to {end_time}, "
            f"data zone {surviving_zone} stayed accessible throughout"
        )
