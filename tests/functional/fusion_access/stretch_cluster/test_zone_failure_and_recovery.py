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
    disconnect_iscsi_luns,
    discover_iscsi_luns,
    get_data_zones,
    get_workload_pod,
    get_zone_of_node,
    wait_for_filesystem_mounted,
    wait_for_filesystem_unmounted,
    wait_for_workload_pods,
    verify_baseline_file,
    verify_route_accessible,
    verify_workload_moved_out_of_zone,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs, get_node_status, wait_for_nodes_status
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

        # Fetch the nodes of the zone that needs to be shutdown
        zone, nodes_to_shutdown = self._select_zone_to_fail(sc_obj, namespace)
        zone_node_names = [node.name for node in nodes_to_shutdown]

        # Step 1: shutdown the zone nodes
        log.info(f"Shutting down zone {zone} nodes {zone_node_names}")
        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=nodes_to_shutdown)
        wait_for_nodes_status(
            node_names=zone_node_names,
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        log.info(f"Nodes of zone {zone} are shutdown successfully")

        sc_obj.non_quorum_nodes = zone_node_names

        # Step 2: make sure the Deployment based workloads failover to the
        # surviving zone and their data is intact. The volumes are RWX so the
        # replacement pod can attach while the VolumeAttachment of the
        # unreachable node is still around, which means no manual intervention
        # is needed for the failover. It is not immediate though, the original
        # pods are only evicted once the 5 minute
        # 'node.kubernetes.io/unreachable' toleration expires
        self._verify_failover(namespace, zone, md5sum_before)

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
        # seen a write pause, the replica of the failed zone must come back on
        # its own and restore the zone spread, and every volume must still
        # hold its baseline file
        self._verify_recovery(sc_obj, namespace, md5sum_before, start_time, end_time)

    @staticmethod
    def _select_zone_to_fail(sc_obj, namespace):
        """
        Pick the data zone to fail and return it along with its nodes.

        The zone is taken from where the Deployment based workloads actually
        landed, not fixed in advance: failing a zone they do not run in would
        leave them untouched and the failover checks would pass without
        anything having moved. The file-uploader template keeps them together
        with a pod affinity, which is a preference and not a guarantee, so the
        co-location is asserted rather than assumed.

        Args:
            sc_obj (StretchCluster): StretchCluster object
            namespace (str): Namespace of the workloads

        Returns:
            tuple: Name of the zone to fail and the list of its node objects

        """
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
            f"zone, their placement is {workload_zones}. Failing zone {zone} "
            f"would leave the workloads of the other zone running and their "
            f"failover would never be exercised"
        )

        zone_nodes = sc_obj.get_nodes_in_zone(zone)
        assert (
            len(zone_nodes) != 0
        ), f"There are 0 nodes labeled as {constants.ZONE_LABEL}={zone}!!"

        return zone, zone_nodes

    @staticmethod
    def _verify_failover(namespace, zone, md5sum_before):
        """
        Verify the Deployment based workloads moved to the surviving zone with
        their data intact and are reachable from there.

        Args:
            namespace (str): Namespace of the workloads
            zone (str): Zone that was failed
            md5sum_before (dict): Baseline md5sum of each workload volume

        """
        workload_pods = {}
        for label in WORKLOAD_MOUNT_PATHS:
            workload_pods[label] = verify_workload_moved_out_of_zone(
                label=label, namespace=namespace, zone=zone
            )
        log.info("All the Deployment based workloads moved to the surviving zone")

        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            verify_baseline_file(workload_pods[label], mount_path, md5sum_before[label])
        verify_route_accessible(
            route_name=constants.FILE_UPLOADER_FUSION_NAME, namespace=namespace
        )
        log.info("Workloads are accessible with data integrity preserved")

    @staticmethod
    def _verify_recovery(sc_obj, namespace, md5sum_before, start_time, end_time):
        """
        Verify the cluster is whole again after the failed zone recovered: the
        zone aware workload is back to its full set of replicas, it saw no
        write pause and no data loss, and every volume still holds its
        baseline file.

        Args:
            sc_obj (StretchCluster): StretchCluster object
            namespace (str): Namespace of the workloads
            md5sum_before (dict): Baseline md5sum of each workload volume
            start_time (datetime): Time the failure was induced at
            end_time (datetime): Time the recovery completed at

        """
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

        for pod_obj in sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][0]:
            verify_baseline_file(pod_obj, "/mnt/target", md5sum_before[pod_obj.name])

        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            verify_baseline_file(pod_obj, mount_path, md5sum_before[label])

        verify_route_accessible(
            route_name=constants.FILE_UPLOADER_FUSION_NAME, namespace=namespace
        )
        log.info("All the workloads recovered with data integrity preserved")

    @pytest.mark.polarion_id("OCS-8345")
    def test_full_zone_failure_and_recovery(
        self,
        nodes,
        node_restart_teardown,
        reconnect_san_teardown,
        setup_fusion_access_workloads,
        setup_stretch_cluster_project,
    ):
        """
        Full Zone Failure and Recovery - the OCP nodes of one data zone and the
        SAN storage attached to them go down together

        This is the whole site outage, as opposed to
        'test_zone_failure_and_recovery' which only loses the OCP nodes and
        leaves the SAN serving the surviving zone, and
        'test_san_storage_failure_and_recovery' which only loses the storage.

        Steps:
            1) Deploy the logwriter, busybox and file-uploader workloads and
               take a data integrity baseline of every volume
            2) Take the SAN LUNs away from the nodes of one data zone and then
               shut those nodes down, so the zone loses both its compute and
               its storage
            3) Make sure the nodes transition to NotReady
            4) Make sure the Deployment based workloads are rescheduled on a
               node of the surviving zone and reach Running state
            5) Verify the data written before the failure is intact and the
               file-uploader route is still accessible from the surviving zone
            6) Start the nodes back up, rediscover the SAN LUNs and wait for
               the Storage Scale filesystem to be mounted on them again
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

        zone, zone_nodes = self._select_zone_to_fail(sc_obj, namespace)
        zone_node_names = [node.name for node in zone_nodes]

        # Step 1: take the storage away first and only then power the nodes
        # off. The logout runs through 'oc debug' on the node itself, so it
        # needs the node to still be up, and the other order would leave the
        # recovered nodes logged into the target on the next boot, which is not
        # the outage this is reproducing
        log.info(f"Disconnecting the SAN storage of zone {zone} from {zone_node_names}")
        start_time = datetime.now(timezone.utc)
        disconnect_iscsi_luns(node_names=zone_node_names)

        log.info(f"Shutting down zone {zone} nodes {zone_node_names}")
        nodes.stop_nodes(nodes=zone_nodes)
        wait_for_nodes_status(
            node_names=zone_node_names,
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        log.info(f"Zone {zone} lost both its nodes and its storage")

        sc_obj.non_quorum_nodes = zone_node_names

        # Step 2: the volumes are RWX so the replacement pod can attach while
        # the VolumeAttachment of the unreachable node is still around, which
        # means no manual intervention is needed for the failover. It is not
        # immediate though, the original pods are only evicted once the 5
        # minute 'node.kubernetes.io/unreachable' toleration expires
        self._verify_failover(namespace, zone, md5sum_before)

        # TODO: verify the VM is rescheduled on a node of the surviving zone,
        # reaches Running state and that the test file written into it before
        # the failure still matches. Blocked on the CNSA OFD lock regression,
        # see the 'setup_fusion_access_workloads' fixture

        # Step 3: bring the zone back, both the nodes and their storage
        nodes.start_nodes(nodes=zone_nodes)
        wait_for_nodes_status(timeout=900)

        # The nodes come back without their iSCSI sessions, so the shared SAN
        # LUNs have to be rediscovered before the Storage Scale cluster can
        # bring the NSDs of those nodes back up. The zone nodes are passed
        # explicitly rather than relying on the worker node default, a data
        # zone of a stretch cluster also holds a control plane node and that
        # one lost its storage too
        discover_iscsi_luns(node_names=zone_node_names)

        # A node reports Ready, and its LUNs come back, well before the GPFS
        # daemon has remounted the filesystem on it, so wait for the mount
        # rather than assuming the zone is usable as soon as the nodes are
        # Ready
        wait_for_filesystem_mounted(node_names=zone_node_names)

        wait_for_workload_pods(
            labels=[constants.LOGWRITER_RBD_LABEL],
            namespace=namespace,
            expected_count=sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][2],
            timeout=900,
        )
        log.info("The logwriter workload is back to its full set of replicas")

        end_time = datetime.now(timezone.utc)
        log.info(
            f"Full zone {zone} failure started at {start_time} and ended at {end_time}"
        )

        # Step 4: the logwriter replica of the surviving zone must not have
        # seen a write pause, the replica of the failed zone must come back on
        # its own, and every volume must still hold its baseline file
        self._verify_recovery(sc_obj, namespace, md5sum_before, start_time, end_time)

    @pytest.mark.polarion_id("OCS-8346")
    def test_san_storage_failure_and_recovery(
        self,
        reconnect_san_teardown,
        setup_fusion_access_workloads,
        setup_stretch_cluster_project,
    ):
        """
        SAN Storage Failure and Recovery - only the storage attached to one
        data zone goes down, the OCP nodes of that zone stay up and Ready

        This is the storage only half of the full zone outage. The distinction
        matters: because the nodes keep reporting Ready, Kubernetes applies no
        'node.kubernetes.io/unreachable' taint and therefore never evicts the
        pods of the affected zone on its own. Whether the workloads recover is
        down to the Storage Scale filesystem panicking on those nodes and the
        workloads reacting to the IO error, which is exactly what this is
        validating rather than assuming.

        Steps:
            1) Deploy the logwriter, busybox and file-uploader workloads and
               take a data integrity baseline of every volume
            2) Take the SAN LUNs away from the nodes of one data zone, leaving
               the nodes themselves running
            3) Make sure the nodes stay Ready, so that this really is a storage
               only outage, and that the Storage Scale filesystem unmounts on
               them
            4) Make sure the Deployment based workloads end up on a node of the
               surviving zone and reach Running state
            5) Verify the data written before the failure is intact and the
               file-uploader route is still accessible from the surviving zone
            6) Reconnect the SAN LUNs and wait for the filesystem to be mounted
               on the affected nodes again
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

        zone, zone_nodes = self._select_zone_to_fail(sc_obj, namespace)
        zone_node_names = [node.name for node in zone_nodes]

        # Step 1: take only the storage away, the nodes stay up
        log.info(f"Disconnecting the SAN storage of zone {zone} from {zone_node_names}")
        start_time = datetime.now(timezone.utc)
        disconnect_iscsi_luns(node_names=zone_node_names)

        # Step 2: assert this is a storage only outage. If the nodes went
        # NotReady the test would silently turn into the node failure one and
        # the failover below would be driven by the eviction taint instead of
        # by the storage going away
        for node_obj in get_node_objs(zone_node_names):
            node_status = get_node_status(node_obj)
            assert node_status == constants.NODE_READY, (
                f"Node {node_obj.name} of zone {zone} is {node_status} after "
                f"its storage was disconnected, expected it to stay "
                f"{constants.NODE_READY}. This is no longer a storage only "
                f"outage"
            )
        log.info(f"Nodes of zone {zone} stayed Ready, only their storage is gone")

        # The GPFS daemon loses its NSDs on these nodes and panics the
        # filesystem there, which is what makes the LUNs going away visible to
        # the Storage Scale cluster rather than just to the kernel
        wait_for_filesystem_unmounted(node_names=zone_node_names)
        log.info(f"The Storage Scale filesystem is unmounted on zone {zone} nodes")

        # Step 3: the workloads of the affected zone have to end up in the
        # surviving zone. Nothing taints these nodes, so this leans entirely on
        # the workloads failing their IO and being restarted elsewhere. A
        # timeout here is a genuine result worth reporting, not a test bug
        self._verify_failover(namespace, zone, md5sum_before)

        # TODO: verify the VM fails over to a node of the surviving zone,
        # reaches Running state and that the test file written into it before
        # the failure still matches. Blocked on the CNSA OFD lock regression,
        # see the 'setup_fusion_access_workloads' fixture

        # Step 4: give the storage back. The nodes never rebooted, so only the
        # iSCSI login is needed, no node restart
        discover_iscsi_luns(node_names=zone_node_names)
        wait_for_filesystem_mounted(node_names=zone_node_names)

        wait_for_workload_pods(
            labels=[constants.LOGWRITER_RBD_LABEL],
            namespace=namespace,
            expected_count=sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][2],
            timeout=900,
        )
        log.info("The logwriter workload is back to its full set of replicas")

        end_time = datetime.now(timezone.utc)
        log.info(
            f"SAN storage failure of zone {zone} started at {start_time} and "
            f"ended at {end_time}"
        )

        # Step 5: no write pause in the surviving zone, no data loss, and every
        # volume still holds its baseline file
        self._verify_recovery(sc_obj, namespace, md5sum_before, start_time, end_time)
