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
    verify_failover_relocate_status_ui,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@turquoise_squad
@acceptance
@tier1
class TestFailoverAndRelocateForOffloadedVR:
    """
    Test Failover and Relocate actions via CLI and UI for third party storage
    providers when vr is offloaded

    """

    params = [
        pytest.param(
            False,  # primary_cluster_down = False
            constants.CEPHBLOCKPOOL,
            True,  # via_ui = True,
            id="primary_up-rbd-ui",
        ),
        pytest.param(
            True,  # primary_cluster_down = True
            constants.CEPHBLOCKPOOL,
            True,  # via_ui = True,
            id="primary_down-rbd-ui",
        ),
    ]

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface", "via_ui"], argvalues=params
    )
    def test_failover_and_relocate(
        self,
        primary_cluster_down,
        pvc_interface,
        via_ui,
        setup_acm_ui,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify application failover when the primary cluster is either UP or DOWN and relocate between managed
        clusters.

        This test will run twice both via CLI and UI

        """
        if via_ui:
            acm_obj = AcmAddClusters()

        workload = dr_workload(
            num_of_subscription=0, num_of_appset=1, pvc_interface=pvc_interface
        )
        # drpc_subscription = DRPC(namespace=workloads[0].workload_namespace)
        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workload.appset_placement_name}-drpc",
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workload.workload_namespace
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload.workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        before_failover_last_group_sync_time = []
        for obj in drpc_appset:
            before_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime before failover.")

        if via_ui:
            logger.info("Start the process of Failover from ACM UI")
            config.switch_acm_ctx()
            dr_submariner_validation_from_ui(acm_obj)

        # Stop primary cluster nodes
        if primary_cluster_down:
            config.switch_to_cluster_by_name(primary_cluster_name)
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

            # Verify if cluster is marked unavailable on ACM console
            if via_ui:
                config.switch_acm_ctx()
                check_cluster_status_on_acm_console(
                    acm_obj,
                    down_cluster_name=primary_cluster_name,
                    expected_text="Unknown",
                )
        elif via_ui:
            check_cluster_status_on_acm_console(acm_obj)

            # Failover action via CLI
            dr_helpers.failover_for_offloade_vr(
                workload.workload_namespace,
            )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)

        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )

        # Verify resources deletion from primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Start nodes if cluster is down
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} minutes before starting nodes of primary cluster: {primary_cluster_name}"
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

        dr_helpers.wait_for_all_resources_deletion(workload.workload_namespace)

        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify the deletion of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_deletion(
                workload.workload_namespace
            )
            # Verify the creation of ReplicationDestination resources on primary cluster
            config.switch_to_cluster_by_name(primary_cluster_name)
            dr_helpers.wait_for_replication_destinations_creation(
                workload.workload_pvc_count, workload.workload_namespace
            )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum([workload.workload_pvc_count])
            )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        post_failover_last_group_sync_time = []
        for obj, initial_last_group_sync_time in zip(
            drpc_appset, before_failover_last_group_sync_time
        ):
            post_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(
                    obj, scheduling_interval, initial_last_group_sync_time
                )
            )
        logger.info("Verified lastGroupSyncTime after failover.")

        # Relocate action
        dr_helpers.relocate_for_offloade_vr(
            workload.workload_namespace,
        )

        # Verify resources deletion from secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)

        dr_helpers.wait_for_all_resources_deletion(workload.workload_namespace)

        # Verify resources creation on primary cluster (preferredCluster)
        config.switch_to_cluster_by_name(primary_cluster_name)

        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify the deletion of ReplicationDestination resources on primary cluster
            config.switch_to_cluster_by_name(primary_cluster_name)
            dr_helpers.wait_for_replication_destinations_deletion(
                workload.workload_namespace
            )
            # Verify the creation of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_creation(
                workload.workload_pvc_count, workload.workload_namespace
            )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum([workload.workload_pvc_count])
            )

        if via_ui:
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )

        for obj, initial_last_group_sync_time in zip(
            drpc_appset, post_failover_last_group_sync_time
        ):
            dr_helpers.verify_last_group_sync_time(
                obj, scheduling_interval, initial_last_group_sync_time
            )
        logger.info("Verified lastGroupSyncTime after relocate.")
