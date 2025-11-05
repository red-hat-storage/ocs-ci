import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers, helpers
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
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    tier1,
)

logger = logging.getLogger(__name__)


@rdr
@skipif_ocs_version("<4.20")
@skipif_ocp_version("<4.20")
@tier1
@turquoise_squad
class TestMirroringStatusReflectedInVR:
    """
    Test Failover and Relocate actions via CLI and UI

    """

    params = [
        pytest.param(
            False,  # primary_cluster_down = False
            constants.CEPHBLOCKPOOL,
            False,  # via_ui = False
        ),
    ]

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface", "via_ui"], argvalues=params
    )
    def test_vr_status_and_type_for_mirroring_in_healthy_status(
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
        Validate on primary VR/VGR a status message is updated to reflect the current mirroring status.

        for mirroring image in healthy status:
        reason: Replicating
        status: "True"
        type: Replicating

        for mirroring image in down status:
        message: 'volume group replication status is unknown: rpc error: code = FailedPrecondition
        desc = failed to get last sync info: no snapshot details: last sync time not
        found'
        reason: Replicating
        status: "Unknown"
        type: Replicating

        is displayed.

        This test will run twice both via CLI and UI

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
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Check vr created on the primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_resource_state(
            kind=constants.VOLUME_REPLICATION,
            state="primary",
            namespace=namespace,
        )

        # Fetch status and type from the vr status
        dr_helpers.fetch_status_and_type_reflecting_on_vr_or_vgr(
            namespace,
        )

        # Fetch mirroring image status from secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )

        # assert mirroting health is OK
        assert (
            mirroring_health_secondary == "OK"
        ), "mirroring image health is not not 'OK'"

        # validate vr.status displays volume is replicating successfully
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
            namespace, mirroring_health_secondary
        )

        logger.info(
            "Validate vr status reflects mirroring status when image health=down"
        )
        # bring mirroring down on the secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT,
            replica_count=0,
        ), "Failed to scale down mirroring deployment to 0"
        logger.info("Successfully scaled down rbd mirroring deployment to 0")
        sleep(120)

        # Fetch mirroring image status from secondary cluster
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        # assert mirroting health is UNKNOWN
        assert (
            mirroring_health_secondary == "UNKNOWN"
        ), "mirroring image health is not not 'UNKNOWN'"

        # validate vr.status displays volume replication status as 'unknown'
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
            namespace, mirroring_health_secondary
        )

        # bring mirroring back up on the secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT,
            replica_count=1,
        ), "Failed to scale down mirroring deployment to 1"
        logger.info("Successfully scaled up rbd mirroring deployment to 1")
        sleep(120)

        # Fetch mirroring image status from secondary cluster
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        # assert mirroting health is OK
        assert (
            mirroring_health_secondary == "OK"
        ), "mirroring image health is not not 'OK'"

    params = [
        pytest.param(
            True,  # primary_cluster_down = True
            constants.CEPHBLOCKPOOL,
            False,  # via_ui = False
        ),
        pytest.param(
            True,  # primary_cluster_down = True
            constants.CEPHBLOCKPOOL,
            True,  # via_ui = True
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
        Tests current mirroring status message starts to display on the previously secondary cluster after successfully
        completing failover of a workload from primary to secondary. And after relocating the workload it will again
        move to the primary cluster.

        During failover while we promote the other cluster and demote the current cluster
        until rysnc is issues the state would be up+error.

        for mirroring image in error status:
        message: 'volume group replication status is unknown: rpc error: code = FailedPrecondition
        desc = failed to get last sync info: no snapshot details: last sync time not
        found'
        reason: Replicating
        status: "Unknown"
        type: Replicating

        will be displayed
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

        # Check vr status details is not displayed on secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)

        # Fetch mirroring image status from secondary cluster
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        print("######Amrita#######")
        print(f"After failover mirroring image status: {mirroring_health_secondary}")
        # assert mirroring health is OK
        assert (
            mirroring_health_secondary == "WARNING"
        ), "mirroring image health is not not 'WARNING'"

        # assert vr resource not available on secondary cluster
        for wl in workloads:
            dr_helpers.check_resource_existence(
                kind=constants.VOLUME_REPLICATION,
                should_exist=False,
                timeout=120,
                resource_name=wl.workload_namespace,
            )
            assert False, "vr resource created on secondary cluster"

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

        # Verify resources deletion from primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Fetch mirroring image status from previously primary cluster
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            primary_cluster_name
        )
        print("######Amrita#######")
        print(f"After failover mirroring image status: {mirroring_health_secondary}")
        # assert mirroting health is OK
        assert (
            mirroring_health_secondary == "OK"
        ), "mirroring image health is not not 'OK'"

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )
            # Fetch status and type from the vr status
            dr_helpers.fetch_status_and_type_reflecting_on_vr_or_vgr(
                wl.workload_namespace,
            )

            # validate vr.status displays volume is replicating successfully
            dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
                wl.workload_namespace, mirroring_health_secondary
            )

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

        # Fetch mirroring image status from previously primary cluster
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        print("######Amrita#######")
        print(f"After relocate mirroring image status: {mirroring_health_secondary}")
        # assert mirroting health is OK
        assert (
            mirroring_health_secondary == "OK"
        ), "mirroring image health is not not 'OK'"

        # Verify resources creation on primary cluster (preferredCluster)
        config.switch_to_cluster_by_name(primary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )
            # Fetch status and type from the vr status
            dr_helpers.fetch_status_and_type_reflecting_on_vr_or_vgr(
                wl.workload_namespace,
            )

            # validate vr.status displays volume is replicating successfully
            dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
                wl.workload_namespace, mirroring_health_secondary
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
