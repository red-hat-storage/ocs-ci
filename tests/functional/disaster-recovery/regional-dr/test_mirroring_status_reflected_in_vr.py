import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_ocp_version, tier1
from ocs_ci.helpers import dr_helpers, helpers
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@skipif_ocs_version("<4.20")
@skipif_ocp_version("<4.20")
@tier1
@turquoise_squad
class TestMirroringStatusReflectedInVR:
    """
    Test failover and relocate actions via CLI and UI.
    """

    params = [
        pytest.param(False, constants.CEPHBLOCKPOOL, False),
    ]

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface", "via_ui"],
        argvalues=params,
    )
    def test_vr_status_and_type_for_mirroring_in_healthy_status(
        self,
        primary_cluster_down: bool,
        pvc_interface: str,
        via_ui: bool,
        setup_acm_ui,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Validate that on the primary VR/VGR, a status message is updated to reflect
        the current mirroring status.
        """
        workloads = dr_workload(
            num_of_subscription=0, num_of_appset=1, pvc_interface=pvc_interface
        )
        namespace = workloads[0].workload_namespace
        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            namespace, workloads[0].workload_type
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            namespace, workloads[0].workload_type
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace, workloads[0].workload_type
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Check VR created on the primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_resource_state(
            kind=constants.VOLUME_REPLICATION,
            state="primary",
            namespace=namespace,
        )

        # Fetch status and type from VR
        dr_helpers.fetch_status_and_type_reflecting_on_vr_or_vgr(namespace)

        # Check mirroring health on secondary
        config.switch_to_cluster_by_name(secondary_cluster_name)
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        assert mirroring_health_secondary == "OK", "Mirroring image health is not 'OK'"

        # Validate VR shows correct mirroring status
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
            namespace, mirroring_health_secondary
        )

        logger.info("Validating VR reflects mirroring down state")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=0
        )
        logger.info("Scaled down RBD mirroring deployment to 0")
        sleep(120)

        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        assert (
            mirroring_health_secondary == "UNKNOWN"
        ), "Mirroring image health is not 'UNKNOWN'"

        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
            namespace, mirroring_health_secondary
        )

        logger.info("Restoring mirroring on secondary cluster")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=1
        )
        logger.info("Scaled up RBD mirroring deployment to 1")
        sleep(120)

        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        assert mirroring_health_secondary == "OK", "Mirroring image health not 'OK'"

    params = [
        pytest.param(True, constants.CEPHBLOCKPOOL, False),
        pytest.param(True, constants.CEPHBLOCKPOOL, True),
    ]

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface", "via_ui"],
        argvalues=params,
    )
    def test_failover_and_relocate(
        self,
        primary_cluster_down: bool,
        pvc_interface: str,
        via_ui: bool,
        setup_acm_ui,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Test that mirroring status updates correctly during and after
        failover and relocate between clusters (via CLI and UI).
        """
        acm_obj = AcmAddClusters() if via_ui else None

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
            config.switch_to_cluster_by_name(secondary_cluster_name)
            for wl in workloads:
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting for {wait_time} minutes before failover")
        sleep(wait_time * 60)

        before_failover_last_group_sync_time = [
            dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
            for obj in drpc_objs
        ]
        logger.info("Verified lastGroupSyncTime before failover")

        if via_ui:
            logger.info("Starting failover from ACM UI")
            config.switch_acm_ctx()
            dr_submariner_validation_from_ui(acm_obj)

        if primary_cluster_down:
            config.switch_to_cluster_by_name(primary_cluster_name)
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

            if via_ui:
                config.switch_acm_ctx()
                check_cluster_status_on_acm_console(
                    acm_obj,
                    down_cluster_name=primary_cluster_name,
                    expected_text="Unknown",
                )
        elif via_ui:
            check_cluster_status_on_acm_console(acm_obj)

        config.switch_to_cluster_by_name(secondary_cluster_name)
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        logger.info(
            f"After failover, mirroring image status: {mirroring_health_secondary}"
        )
        assert (
            mirroring_health_secondary == "WARNING"
        ), "Mirroring image health not 'WARNING'"

        for wl in workloads:
            dr_helpers.check_resource_existence(
                kind=constants.VOLUME_REPLICATION,
                should_exist=False,
                timeout=120,
                resource_name=wl.workload_namespace,
            )
            assert False, "VR resource unexpectedly created on secondary cluster"

        for wl in workloads:
            if via_ui:
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=f"{wl.workload_name}-1",
                    policy_name=wl.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                )
            else:
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

        # Verify resources deletion from primary
        config.switch_to_cluster_by_name(primary_cluster_name)
        mirroring_health_primary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            primary_cluster_name
        )
        logger.info(
            f"After failover, primary mirroring health: {mirroring_health_primary}"
        )
        assert mirroring_health_primary == "OK", "Primary mirroring health not 'OK'"

        # Verify new resources on secondary
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count, wl.workload_pod_count, wl.workload_namespace
            )
            dr_helpers.fetch_status_and_type_reflecting_on_vr_or_vgr(
                wl.workload_namespace
            )
            dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
                wl.workload_namespace, mirroring_health_primary
            )

        if primary_cluster_down:
            logger.info(
                f"Waiting {wait_time} minutes before starting nodes of primary cluster"
            )
            sleep(wait_time * 60)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
            logger.info("Waiting 180 seconds for pods to stabilize")
            sleep(180)
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all pods reached running state"
            ceph_health_check()

        for wl in workloads:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum(wl.workload_pvc_count for wl in workloads)
            )

        if via_ui:
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(acm_obj)

        logger.info(f"Waiting for {wait_time} minutes before relocate")
        sleep(wait_time * 60)

        post_failover_last_group_sync_time = [
            dr_helpers.verify_last_group_sync_time(
                obj, scheduling_interval, before_sync
            )
            for obj, before_sync in zip(drpc_objs, before_failover_last_group_sync_time)
        ]
        logger.info("Verified lastGroupSyncTime after failover")

        # Relocate
        for wl in workloads:
            if via_ui:
                logger.info("Starting relocate from ACM UI")
                check_cluster_status_on_acm_console(acm_obj)
                dr_submariner_validation_from_ui(acm_obj)
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=f"{wl.workload_name}-1",
                    policy_name=wl.dr_policy_name,
                    failover_or_preferred_cluster=primary_cluster_name,
                    action=constants.ACTION_RELOCATE,
                )
            else:
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

        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        logger.info(f"After relocate, mirroring health: {mirroring_health_secondary}")
        assert mirroring_health_secondary == "OK", "Mirroring health not 'OK'"

        config.switch_to_cluster_by_name(primary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count, wl.workload_pod_count, wl.workload_namespace
            )
            dr_helpers.fetch_status_and_type_reflecting_on_vr_or_vgr(
                wl.workload_namespace
            )
            dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
                wl.workload_namespace, mirroring_health_secondary
            )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum(wl.workload_pvc_count for wl in workloads)
            )

        if via_ui:
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )

        for obj, before_sync in zip(drpc_objs, post_failover_last_group_sync_time):
            dr_helpers.verify_last_group_sync_time(
                obj, scheduling_interval, before_sync
            )

        logger.info("Verified lastGroupSyncTime after relocate")
