import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.storage_agnostic_dr_helpers import (
    create_psk_secret_for_app,
    migrate_pvc_pv,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestAgnosticDRFailoverAndRelocate:
    """
    Test Failover and Relocate actions for storage-agnostic DR
    (LSO + VolSync + mock-storage-operator, no ODF/Ceph).

    Prerequisites (configured via conf/ocsci/agnostic_dr.yaml):
      - agnostic_dr: true
      - skip_ocs_deployment: true
      - local_storage: true

    The agnostic DR infrastructure (VolSync, mock-storage-operator, MinIO,
    VolumeGroupReplicationClass, DRClusters, DRPolicy) must be deployed
    before these tests run (done during cluster setup).
    """

    params = [
        pytest.param(
            False,
            marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
            id="primary_up",
        ),
        pytest.param(
            True,
            marks=[pytest.mark.polarion_id("OCS-YYYY")],
            id="primary_down",
        ),
    ]

    @pytest.mark.parametrize(argnames=["primary_cluster_down"], argvalues=params)
    def test_agnostic_dr_failover_and_relocate(
        self,
        primary_cluster_down,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,  # ensures nodes are restarted on test failure
    ):
        """
        Verify failover and relocate for a busybox workload protected by
        agnostic DR.

        Steps:
          1.  Deploy the busybox ApplicationSet workload and DR-protect it.
          2.  Create the VolSync PSK secret in the workload namespace on all
              managed clusters.
          3.  Run the PVC/PV migration script.
          4.  Validate on primary: VGR state is Primary, busybox pods are
              Running, PVCs are Bound, ReplicationSource resources exist.
          5.  Validate on secondary: VGR state is Secondary, PVCs are Bound,
              destination pods exist, ReplicationDestination resources exist.
          6.  Wait for at least one sync cycle to complete.
          7.  Confirm lastGroupSyncTime before failover.
          8.  If primary_cluster_down: stop all primary cluster nodes.
          9.  Initiate failover to the secondary cluster.
         10.  Verify workload resources are created on the secondary cluster.
         11.  If primary_cluster_down: start primary nodes and wait for
              stabilisation.
         12.  Verify workload resources are deleted from the primary cluster.
         13.  Wait for post-failover sync and confirm lastGroupSyncTime.
         14.  Initiate relocate back to the primary cluster.
         15.  Verify workload resources are deleted from the secondary cluster.
         16.  Verify workload resources are re-created on the primary cluster.
        """
        logger.test_step("Deploy busybox ApplicationSet workload (pull model)")
        workload = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            appset_model="pull",
            skip_replication_resources=True,
        )[0]

        drpc_obj = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workload.appset_placement_name}-drpc",
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workload.workload_namespace, workload.workload_type
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload.workload_namespace, workload.workload_type
        )

        primary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(primary_cluster_name)
        ]
        secondary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(secondary_cluster_name)
        ]

        logger.test_step(
            "Create VolSync PSK secret in workload namespace on all managed clusters"
        )
        create_psk_secret_for_app(workload.workload_namespace)

        logger.test_step(
            "Wait for Ramen to label PVCs with consistency-group and create VGR"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)

        consistency_group = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.PVC, namespace=workload.workload_namespace
            )
            .get()
            .get("items", [{}])[0]
            .get("metadata", {})
            .get("labels", {})
            .get("ramendr.openshift.io/consistency-group"),
        )
        for cg_value in sample:
            if cg_value:
                consistency_group = cg_value
                break
        assert consistency_group, (
            "Label 'ramendr.openshift.io/consistency-group' not found"
            f" on PVCs in namespace '{workload.workload_namespace}'"
            f" after waiting 300s"
        )
        logger.info("Found consistency-group: %s", consistency_group)

        vgr_name = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.VOLUME_GROUP_REPLICATION,
                namespace=workload.workload_namespace,
            )
            .get()
            .get("items", []),
        )
        for vgr_items in sample:
            if vgr_items:
                vgr_name = vgr_items[0]["metadata"]["name"]
                break
        assert vgr_name, (
            f"No VolumeGroupReplication found in '{workload.workload_namespace}'"
            f" on primary cluster '{primary_cluster_name}' after waiting 300s"
        )
        logger.info("Found VGR: %s", vgr_name)

        logger.test_step(
            "Run PVC/PV migration script (consistency-group=%s, vgr_name=%s)",
            consistency_group,
            vgr_name,
        )
        migrate_pvc_pv(
            consistency_group=consistency_group,
            primary_cluster_config=primary_cluster_config,
            secondary_cluster_config=secondary_cluster_config,
            vgr_name=vgr_name,
            vgr_namespace=workload.workload_namespace,
            vgr_class=constants.MOCK_VGRC_NAME,
        )

        logger.test_step(
            "Verify workload deployment after migration (VGR, pods, PVCs, "
            "replication resources)"
        )
        workload.verify_workload_deployment(skip_replication_resources=True)

        logger.test_step("Wait for initial sync cycle to complete")
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace, workload.workload_type
        )
        wait_time = 2 * scheduling_interval
        logger.info("Waiting %s minutes for initial sync to complete", wait_time)
        sleep(wait_time * 60)

        logger.test_step("Confirm lastGroupSyncTime before failover")
        before_failover_sync_time = dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval
        )

        if primary_cluster_down:
            logger.test_step(
                "Stop all primary cluster nodes (%s)", primary_cluster_name
            )
            config.switch_to_cluster_by_name(primary_cluster_name)
            primary_cluster_index = config.cur_index
            primary_cluster_nodes = get_node_objs()
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        logger.test_step(
            "Initiate failover to secondary cluster (%s)", secondary_cluster_name
        )
        dr_helpers.failover(
            secondary_cluster_name,
            workload.workload_namespace,
            workload.workload_type,
            workload.appset_placement_name,
        )

        # Verify resources on new primary (failoverCluster)
        logger.test_step(
            "Verify new primary (%s): busybox pods Running, PVCs Bound",
            secondary_cluster_name,
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            skip_replication_resources=True,
        )

        # Verify ReplicationDestination created on new secondary (old primary)
        logger.test_step(
            "Verify new secondary (%s): ReplicationDestination created",
            primary_cluster_name,
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_replication_destinations_creation(
            workload.workload_pvc_count, workload.workload_namespace
        )

        logger.test_step("Wait for post-failover sync and confirm lastGroupSyncTime")
        logger.info("Waiting %s minutes for post-failover sync to complete", wait_time)
        sleep(wait_time * 60)
        dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval, before_failover_sync_time
        )

        if primary_cluster_down:
            logger.test_step(
                "Start primary cluster nodes (%s) before relocate",
                primary_cluster_name,
            )
            config.switch_to_cluster_by_name(primary_cluster_name)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
            logger.info("Waiting 180 seconds for pods to stabilize")
            sleep(180)
            logger.assertion("All pods Running after primary cluster restart")
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all pods reached Running state after primary cluster restart"

        logger.test_step(
            "Initiate relocate back to primary cluster (%s)", primary_cluster_name
        )
        dr_helpers.relocate(
            primary_cluster_name,
            workload.workload_namespace,
            workload.workload_type,
            workload.appset_placement_name,
        )

        # Verify resources on primary after relocate
        logger.test_step(
            "Verify primary (%s) after relocate: busybox pods Running, PVCs Bound",
            primary_cluster_name,
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            skip_replication_resources=True,
        )

        # Verify ReplicationDestination created on secondary after relocate
        logger.test_step(
            "Verify secondary (%s) after relocate: ReplicationDestination created",
            secondary_cluster_name,
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_replication_destinations_creation(
            workload.workload_pvc_count, workload.workload_namespace
        )
