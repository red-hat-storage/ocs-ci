import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1, skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestFailoverAndRelocateStatefulApp:
    """
    Test Failover and Relocate with an AppSet-only BusyBox StatefulSet workload.

    The workload is deployed from the ocs-workloads repo using the
    ``dr_workload_appset_<interface>_statefulsets`` config key (see
    conf/ocsci/dr_workload.yaml).  The StatefulSet manifests live under
    ``rdr/busybox/<interface>/workloads/statefulsets/`` in that repo.
    The ApplicationSet YAML is at
    ``rdr/busybox/<interface>/appset/statefulsets/``.

    Steps:
        1. Deploy AppSet StatefulSet workload
           (num_of_subscription=0, num_of_appset=1, use_statefulsets=True).
        2. If CephFS: verify ReplicationDestination resources are created on the secondary cluster.
        3. Wait 2× scheduling interval for IOs; verify lastGroupSyncTime BEFORE failover.
        4. If primary_cluster_down=True: stop nodes on the primary cluster.
        5. Trigger failover for the AppSet workload to the secondary cluster.
        6. Verify all resources are created on the secondary cluster (failoverCluster).
        7. If primary_cluster_down=True: restart primary nodes and wait for pods/Ceph health.
        8. Verify all resources are deleted from the primary cluster.
        9. Post-failover storage checks:
           - CephFS: verify ReplicationDestination deletion (old secondary) and creation (new secondary).
           - RBD: wait_for_mirroring_status_ok.
        10. Wait 2× scheduling interval; verify lastGroupSyncTime AFTER failover.
        11. Verify lastGroupSyncTime BEFORE relocate.
        12. Trigger relocate for the AppSet workload back to the primary cluster.
        13. Verify all resources are deleted from the secondary cluster.
        14. Verify all resources are created on the primary cluster (preferredCluster).
        15. Post-relocate storage checks:
            - CephFS: verify ReplicationDestination deletion (old secondary) and creation (new secondary).
            - RBD: wait_for_mirroring_status_ok.
        16. Verify lastGroupSyncTime AFTER relocate.
    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface"],
        argvalues=[
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                marks=[acceptance, pytest.mark.polarion_id("OCS-XXX")],
                id="primary_up-rbd",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                marks=[acceptance, pytest.mark.polarion_id("OCS-YYY")],
                id="primary_down-rbd",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                marks=[
                    skipif_ocs_version("<4.18"),
                    acceptance,
                    pytest.mark.polarion_id("OCS-ZZZ"),
                ],
                id="primary_up-cephfs",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                marks=[
                    skipif_ocs_version("<4.18"),
                    pytest.mark.polarion_id("OCS-QQQ"),
                ],
                id="primary_down-cephfs",
            ),
        ],
    )
    def test_failover_and_relocate_stateful_app(
        self,
        primary_cluster_down,
        pvc_interface,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Test failover and relocate of an AppSet-only BusyBox StatefulSet workload.

        Steps:
            1. Deploy AppSet StatefulSet workload
               (num_of_subscription=0, num_of_appset=1, use_statefulsets=True).
            2. If CephFS: verify ReplicationDestination resources on secondary cluster.
            3. Wait 2× scheduling interval; verify lastGroupSyncTime BEFORE failover.
            4. If primary_cluster_down=True: stop primary cluster nodes.
            5. Trigger failover to the secondary cluster.
            6. Verify resources created on the secondary cluster.
            7. If primary_cluster_down=True: restart primary nodes; wait for pods and Ceph health.
            8. Verify resources deleted from the primary cluster.
            9. Post-failover storage checks (CephFS: RepDest lifecycle; RBD: mirroring status).
            10. Wait 2× scheduling interval; verify lastGroupSyncTime AFTER failover.
            11. Verify lastGroupSyncTime BEFORE relocate.
            12. Trigger relocate back to primary cluster.
            13. Verify resources deleted from the secondary cluster.
            14. Verify resources created on the primary cluster.
            15. Post-relocate storage checks (CephFS: RepDest lifecycle; RBD: mirroring status).
            16. Verify lastGroupSyncTime AFTER relocate.
        """
        # Step 1: Deploy StatefulSet workload via the _statefulsets config key
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=pvc_interface,
            use_statefulsets=True,
        )
        appset_wl = workloads[0]

        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{appset_wl.appset_placement_name}-drpc",
        )
        drpc_objs = [drpc_appset]

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            appset_wl.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            appset_wl.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )

        # Step 2: CephFS — verify ReplicationDestination on secondary cluster
        if pvc_interface == constants.CEPHFILESYSTEM:
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_creation(
                appset_wl.workload_pvc_count, appset_wl.workload_namespace
            )

        # Step 3: Wait and verify lastGroupSyncTime BEFORE failover
        scheduling_interval = dr_helpers.get_scheduling_interval(
            appset_wl.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        wait_time = 2 * scheduling_interval  # minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        before_failover_last_group_sync_time = []
        for drpc_obj in drpc_objs:
            before_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime before failover.")

        # Step 4: Stop primary nodes if requested
        if primary_cluster_down:
            config.switch_to_cluster_by_name(primary_cluster_name)
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        # Step 5: Trigger failover
        dr_helpers.failover(
            secondary_cluster_name,
            appset_wl.workload_namespace,
            appset_wl.workload_type,
            appset_wl.appset_placement_name,
            skip_odf_cli_validation=primary_cluster_down,
        )

        # Step 6: Verify resources created on secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            appset_wl.workload_pvc_count,
            appset_wl.workload_pod_count,
            appset_wl.workload_namespace,
            performed_dr_action=True,
        )

        # Step 7: Restart primary nodes if they were stopped
        config.switch_to_cluster_by_name(primary_cluster_name)
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} minutes before starting nodes of primary cluster: "
                f"{primary_cluster_name}"
            )
            sleep(wait_time * 60)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
            logger.info("Wait for 180 seconds for pods to stabilize")
            sleep(180)
            logger.info(
                "Wait for all the pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking for Ceph Health OK")
            ceph_health_check()

        # Step 8: Verify resources deleted from primary cluster
        dr_helpers.wait_for_all_resources_deletion(appset_wl.workload_namespace)

        # Step 9: Post-failover storage checks
        if pvc_interface == constants.CEPHFILESYSTEM:
            # Old secondary (now active) — verify ReplicationDestinations deleted
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_deletion(
                appset_wl.workload_namespace
            )
            # New secondary (old primary) — verify ReplicationDestinations created
            config.switch_to_cluster_by_name(primary_cluster_name)
            dr_helpers.wait_for_replication_destinations_creation(
                appset_wl.workload_pvc_count, appset_wl.workload_namespace
            )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=appset_wl.workload_pvc_count
            )

        # Step 10: Wait and verify lastGroupSyncTime AFTER failover
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        post_failover_last_group_sync_time = []
        for drpc_obj, initial_sync_time in zip(
            drpc_objs, before_failover_last_group_sync_time
        ):
            post_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(
                    drpc_obj, scheduling_interval, initial_sync_time
                )
            )
        logger.info("Verified lastGroupSyncTime after failover.")

        # Step 11: Verify lastGroupSyncTime BEFORE relocate
        before_relocate_last_group_sync_time = []
        for drpc_obj in drpc_objs:
            before_relocate_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime before relocate.")

        # Step 12: Trigger relocate
        dr_helpers.relocate(
            primary_cluster_name,
            appset_wl.workload_namespace,
            appset_wl.workload_type,
            appset_wl.appset_placement_name,
        )

        # Step 13: Verify resources deleted from secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_deletion(appset_wl.workload_namespace)

        # Step 14: Verify resources created on primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            appset_wl.workload_pvc_count,
            appset_wl.workload_pod_count,
            appset_wl.workload_namespace,
            performed_dr_action=True,
        )

        # Step 15: Post-relocate storage checks
        if pvc_interface == constants.CEPHFILESYSTEM:
            # Old secondary (now primary again) — verify ReplicationDestinations deleted
            config.switch_to_cluster_by_name(primary_cluster_name)
            dr_helpers.wait_for_replication_destinations_deletion(
                appset_wl.workload_namespace
            )
            # New secondary — verify ReplicationDestinations created
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_creation(
                appset_wl.workload_pvc_count, appset_wl.workload_namespace
            )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=appset_wl.workload_pvc_count
            )

        # Step 16: Verify lastGroupSyncTime AFTER relocate
        for drpc_obj, initial_sync_time in zip(
            drpc_objs, before_relocate_last_group_sync_time
        ):
            dr_helpers.verify_last_group_sync_time(
                drpc_obj, scheduling_interval, initial_sync_time
            )
        logger.info("Verified lastGroupSyncTime after relocate.")
