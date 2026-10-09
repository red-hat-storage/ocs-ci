import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestFailoverRelocateUI:
    """
    Test Failover and Relocate actions for a GitOps/AppSet workload via the ACM UI.

    Deploys a single AppSet workload (CEPHBLOCKPOOL or CEPHFILESYSTEM), then:
      1. Deploy AppSet workload.
      2. (CephFS only) Verify ReplicationDestination creation on secondary cluster.
      3. Wait 2× scheduling interval; record lastGroupSyncTime before failover.
      4. (UI) Validate submariner from ACM console.
      5. Stop primary nodes if primary_cluster_down=True.
      6. (UI) Verify cluster marked Unknown when primary_cluster_down=True.
      7. Trigger failover via ACM UI for the workload.
      8. Verify resources created on secondary cluster.
      9. Restore primary nodes if they were stopped.
      10. Verify resources deleted from primary cluster.
      11. (RBD) Verify mirroring status OK after failover.
      12. Wait 2× scheduling interval; verify lastGroupSyncTime after failover.
      13. Record lastGroupSyncTime before relocate.
      14. (UI) Validate cluster status and submariner; trigger relocate via ACM UI.
      15. Verify resources deleted from secondary cluster.
      16. Verify resources created on primary cluster.
      17. (RBD) Verify mirroring status OK after relocate.
      18. Verify lastGroupSyncTime after relocate.
    """

    params = [
        pytest.param(
            False,
            constants.CEPHBLOCKPOOL,
            marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
            id="primary_up-rbd-ui",
        ),
        pytest.param(
            True,
            constants.CEPHBLOCKPOOL,
            marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
            id="primary_down-rbd-ui",
        ),
    ]

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface"],
        argvalues=params,
    )
    def test_failover_relocate_ui(
        self,
        primary_cluster_down,
        pvc_interface,
        setup_acm_ui,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests GitOps/AppSet workload failover and relocate via the ACM UI.

        The workload is deployed using an ApplicationSet (num_of_appset=1).
        Both failover and relocate actions are triggered exclusively through the
        ACM web console using the UI helper functions.
        """
        acm_obj = AcmAddClusters()

        # Step 1: Deploy AppSet workload
        workloads = dr_workload(
            num_of_subscription=0, num_of_appset=1, pvc_interface=pvc_interface
        )
        wl = workloads[0]

        drpc_obj = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{wl.appset_placement_name}-drpc",
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            wl.workload_namespace, workload_type=constants.APPLICATION_SET
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            wl.workload_namespace, workload_type=constants.APPLICATION_SET
        )

        # Step 2: (CephFS only) Verify ReplicationDestination creation on secondary cluster
        if pvc_interface == constants.CEPHFILESYSTEM:
            config.switch_to_cluster_by_name(secondary_cluster_name)
            if dr_helpers.is_cg_cephfs_enabled():
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=wl.workload_namespace,
                    should_exist=True,
                )
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=wl.workload_namespace,
                    expected_count=wl.workload_pvc_count,
                )
            dr_helpers.wait_for_replication_destinations_creation(
                wl.workload_pvc_count, wl.workload_namespace
            )

        # Step 3: Wait 2× scheduling interval; record lastGroupSyncTime before failover
        scheduling_interval = dr_helpers.get_scheduling_interval(
            wl.workload_namespace, workload_type=constants.APPLICATION_SET
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes to run IOs before failover")
        sleep(wait_time * 60)

        before_failover_last_group_sync_time = dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval
        )
        logger.info("Verified lastGroupSyncTime before failover.")

        # Step 4: (UI) Validate submariner from ACM console before stopping cluster
        logger.info("Validating submariner from ACM UI before failover")
        config.switch_acm_ctx()
        dr_submariner_validation_from_ui(acm_obj)

        # Step 5: Stop primary nodes if primary_cluster_down=True
        if primary_cluster_down:
            config.switch_to_cluster_by_name(primary_cluster_name)
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

            # Step 6: (UI) Verify cluster is marked Unknown in ACM console
            config.switch_acm_ctx()
            check_cluster_status_on_acm_console(
                acm_obj,
                down_cluster_name=primary_cluster_name,
                expected_text="Unknown",
            )

        # Step 7: Trigger failover via ACM UI
        logger.info("Triggering failover via ACM UI")
        failover_relocate_ui(
            acm_obj,
            scheduling_interval=scheduling_interval,
            workload_to_move=f"{wl.workload_name}-1",
            policy_name=wl.dr_policy_name,
            failover_or_preferred_cluster=secondary_cluster_name,
            workload_type=wl.workload_type,
        )

        # Step 8: Verify resources created on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            wl.workload_namespace,
            performed_dr_action=True,
        )

        # Step 9: Restore primary nodes if they were stopped
        config.switch_to_cluster_by_name(primary_cluster_name)
        if primary_cluster_down:
            logger.info(
                f"Waiting {wait_time} minutes before starting nodes of primary cluster: {primary_cluster_name}"
            )
            sleep(wait_time * 60)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
            logger.info("Waiting 180 seconds for pods to stabilize")
            sleep(180)
            logger.info("Waiting for all pods in openshift-storage to be Running")
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking Ceph Health OK")
            ceph_health_check()

        # Step 10: Verify resources deleted from primary cluster
        dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        # Step 11: Post-failover storage checks
        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=wl.workload_pvc_count
            )
        elif pvc_interface == constants.CEPHFILESYSTEM:
            config.switch_to_cluster_by_name(secondary_cluster_name)
            cg_enabled = dr_helpers.is_cg_cephfs_enabled()
            if cg_enabled:
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=wl.workload_namespace,
                    should_exist=False,
                )
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                config.switch_to_cluster_by_name(primary_cluster_name)
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=wl.workload_namespace,
                    should_exist=True,
                )
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=wl.workload_namespace,
                    expected_count=wl.workload_pvc_count,
                )

        # Step 12: Wait 2× scheduling interval; verify lastGroupSyncTime after failover
        logger.info(f"Waiting {wait_time} minutes to run IOs after failover")
        sleep(wait_time * 60)

        before_relocate_last_group_sync_time = dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval, before_failover_last_group_sync_time
        )
        logger.info("Verified lastGroupSyncTime after failover.")

        # Step 13–14: (UI) Validate status and submariner; trigger relocate via ACM UI
        logger.info("Triggering relocate via ACM UI")
        config.switch_acm_ctx()
        check_cluster_status_on_acm_console(acm_obj)
        dr_submariner_validation_from_ui(acm_obj)
        failover_relocate_ui(
            acm_obj,
            scheduling_interval=scheduling_interval,
            workload_to_move=f"{wl.workload_name}-1",
            policy_name=wl.dr_policy_name,
            failover_or_preferred_cluster=primary_cluster_name,
            action=constants.ACTION_RELOCATE,
            workload_type=wl.workload_type,
        )

        # Step 15: Verify resources deleted from secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        # Step 16: Verify resources created on primary cluster (preferredCluster)
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            wl.workload_namespace,
            performed_dr_action=True,
        )

        # Step 17: Post-relocate storage checks
        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=wl.workload_pvc_count
            )
        elif pvc_interface == constants.CEPHFILESYSTEM:
            config.switch_to_cluster_by_name(primary_cluster_name)
            dr_helpers.wait_for_replication_destinations_deletion(wl.workload_namespace)
            cg_enabled = dr_helpers.is_cg_cephfs_enabled()
            if cg_enabled:
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=wl.workload_namespace,
                    should_exist=False,
                )
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_creation(
                wl.workload_pvc_count, wl.workload_namespace
            )
            if cg_enabled:
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=wl.workload_namespace,
                    should_exist=True,
                )
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=wl.workload_namespace,
                    expected_count=wl.workload_pvc_count,
                )

        # Step 18: Verify lastGroupSyncTime after relocate
        dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval, before_relocate_last_group_sync_time
        )
        logger.info("Verified lastGroupSyncTime after relocate.")
