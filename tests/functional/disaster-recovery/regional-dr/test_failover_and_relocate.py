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
class TestFailoverAndRelocate:
    """
    Test Failover and Relocate actions via CLI and UI

    """

    params = [
        pytest.param(
            False,  # primary_cluster_down = False
            constants.CEPHBLOCKPOOL,
            False,  # via_ui = False
            marks=pytest.mark.polarion_id("OCS-4430"),
            id="primary_up-rbd-cli",
        ),
        pytest.param(
            True,  # primary_cluster_down = True
            constants.CEPHBLOCKPOOL,
            False,  # via_ui = False
            marks=pytest.mark.polarion_id("OCS-4427"),
            id="primary_down-rbd-cli",
        ),
        pytest.param(
            False,  # primary_cluster_down = False
            constants.CEPHFILESYSTEM,
            False,  # via_ui = False
            marks=pytest.mark.polarion_id("OCS-4730"),
            id="primary_up-cephfs-cli",
        ),
        pytest.param(
            True,  # primary_cluster_down = True
            constants.CEPHFILESYSTEM,
            False,  # via_ui = False
            marks=pytest.mark.polarion_id("OCS-4727"),
            id="primary_down-cephfs-cli",
        ),
        pytest.param(
            False,  # primary_cluster_down = False
            constants.CEPHBLOCKPOOL,
            True,  # via_ui = True
            marks=pytest.mark.polarion_id("OCS-6861"),
            id="primary_up-rbd-ui",
        ),
        pytest.param(
            True,  # primary_cluster_down = True
            constants.CEPHBLOCKPOOL,
            True,  # via_ui = True
            marks=pytest.mark.polarion_id("OCS-4743"),
            id="primary_down-rbd-ui",
        ),
        pytest.param(
            False,  # primary_cluster_down = False
            constants.CEPHFILESYSTEM,
            True,  # via_ui = True
            marks=pytest.mark.polarion_id("OCS-6860"),
            id="primary_up-cephfs-ui",
        ),
        pytest.param(
            True,  # primary_cluster_down = True
            constants.CEPHFILESYSTEM,
            True,  # via_ui = True
            marks=pytest.mark.polarion_id("OCS-6859"),
            id="primary_down-cephfs-ui",
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

        workloads = dr_workload(
            num_of_subscription=1, num_of_appset=1, pvc_interface=pvc_interface
        )
        drpc_subscription = DRPC(namespace=workloads[0].workload_namespace)
        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workloads[1].appset_placement_name}-drpc",
        )
        drpc_objs = [drpc_subscription, drpc_appset]

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workloads[0].workload_namespace
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workloads[0].workload_namespace
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify the creation of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            for wl in workloads:
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        before_failover_last_group_sync_time = []
        for obj in drpc_objs:
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

        for wl in workloads:
            if via_ui:
                # Failover via ACM UI
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=f"{wl.workload_name}-1",
                    policy_name=wl.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                )
            else:
                # Failover action via CLI
                dr_helpers.failover(
                    secondary_cluster_name,
                    wl.workload_namespace,
                    wl.workload_type,
                    (
                        wl.appset_placement_name
                        if wl.workload_type == constants.APPLICATION_SET
                        else None
                    ),
                )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
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

        for wl in workloads:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        if pvc_interface == constants.CEPHFILESYSTEM:
            for wl in workloads:
                # Verify the deletion of ReplicationDestination resources on secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                # Verify the creation of ReplicationDestination resources on primary cluster
                config.switch_to_cluster_by_name(primary_cluster_name)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum([wl.workload_pvc_count for wl in workloads])
            )

        if via_ui:
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(acm_obj)

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        post_failover_last_group_sync_time = []
        for obj, initial_last_group_sync_time in zip(
            drpc_objs, before_failover_last_group_sync_time
        ):
            post_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(
                    obj, scheduling_interval, initial_last_group_sync_time
                )
            )
        logger.info("Verified lastGroupSyncTime after failover.")

        # Relocate action
        for wl in workloads:
            if via_ui:
                logger.info("Start the process of Relocate from ACM UI")
                check_cluster_status_on_acm_console(acm_obj)
                dr_submariner_validation_from_ui(acm_obj)
                # Relocate via ACM UI
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=f"{wl.workload_name}-1",
                    policy_name=wl.dr_policy_name,
                    failover_or_preferred_cluster=primary_cluster_name,
                    action=constants.ACTION_RELOCATE,
                )
            else:
                # Relocate action via CLI
                dr_helpers.relocate(
                    primary_cluster_name,
                    wl.workload_namespace,
                    wl.workload_type,
                    (
                        wl.appset_placement_name
                        if wl.workload_type == constants.APPLICATION_SET
                        else None
                    ),
                )

        # Verify resources deletion from secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        # Verify resources creation on primary cluster (preferredCluster)
        config.switch_to_cluster_by_name(primary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        if pvc_interface == constants.CEPHFILESYSTEM:
            for wl in workloads:
                # Verify the deletion of ReplicationDestination resources on primary cluster
                config.switch_to_cluster_by_name(primary_cluster_name)
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                # Verify the creation of ReplicationDestination resources on secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum([wl.workload_pvc_count for wl in workloads])
            )

        if via_ui:
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )

        for obj, initial_last_group_sync_time in zip(
            drpc_objs, post_failover_last_group_sync_time
        ):
            dr_helpers.verify_last_group_sync_time(
                obj, scheduling_interval, initial_last_group_sync_time
            )
        logger.info("Verified lastGroupSyncTime after relocate.")
