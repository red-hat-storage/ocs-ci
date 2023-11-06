import logging
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier4b
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_node_objs,
    unschedule_nodes,
    drain_nodes,
    schedule_nodes,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running, get_pods_having_label
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@tier4b
class TestNodeDrainDuringFailoverRelocate:
    """
    Tests to verify that the failover and relocate operations are not affected by node drain

    """

    @pytest.mark.parametrize(
        argnames=["workload_type", "pod_to_select_node"],
        argvalues=[
            pytest.param(
                *[constants.SUBSCRIPTION, "rbd_mirror"],
                marks=pytest.mark.polarion_id("OCS-4441"),
            ),
            pytest.param(
                *[constants.SUBSCRIPTION, "ramen_dr_cluster_operator"],
                marks=pytest.mark.polarion_id("OCS-4443"),
            ),
            pytest.param(
                *[constants.APPLICATION_SET, "rbd_mirror"],
                marks=pytest.mark.polarion_id("OCS-4442"),
            ),
            pytest.param(
                *[constants.APPLICATION_SET, "ramen_dr_cluster_operator"],
                marks=pytest.mark.polarion_id("OCS-4444"),
            ),
        ],
    )
    def test_node_drain_during_failover_and_relocate(
        self,
        dr_workload,
        workload_type,
        pod_to_select_node,
        nodes_multicluster,
        node_restart_teardown,
        node_drain_teardown,
    ):
        """
        Tests cases to verify that the failover and relocate operations are not affected when node is drained

        """
        if workload_type == constants.SUBSCRIPTION:
            rdr_workload = dr_workload(num_of_subscription=1)[0]
        else:
            rdr_workload = dr_workload(num_of_subscription=0, num_of_appset=1)[0]

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload.workload_namespace, workload_type
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace, workload_type
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace, workload_type
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Stop primary cluster nodes
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
        nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        # Select node on secondary cluster(failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        if pod_to_select_node == "ramen_dr_cluster_operator":
            node_name = (
                get_pods_having_label(
                    label=constants.RAMEN_DR_CLUSTER_OPERATOR_APP_LABEL,
                    namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
                )[0]
                .get("spec")
                .get("nodeName")
            )
        else:
            node_name = (
                get_pods_having_label(
                    label=constants.RBD_MIRROR_APP_LABEL,
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
                .get("spec")
                .get("nodeName")
            )

        # Unschedule and start drain node operation
        unschedule_nodes([node_name])
        executor = ThreadPoolExecutor(max_workers=1)
        node_drain_operaton = executor.submit(drain_nodes, [node_name])

        # Failover operation
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.failover(
            secondary_cluster_name,
            rdr_workload.workload_namespace,
            workload_type,
            rdr_workload.appset_placement_name
            if workload_type != constants.SUBSCRIPTION
            else None,
        )

        # Verify resources creation on secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        # Verify the result of node drain operation
        node_drain_operaton.result()

        # Make the node in the secondary cluster schedule-able
        schedule_nodes([node_name])

        # Verify resources deletion from primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        # Start node on primary cluster
        logger.info(
            f"Waiting for {wait_time} minutes before starting nodes of primary cluster: {primary_cluster_name}"
        )
        sleep(wait_time * 60)
        nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
        wait_for_nodes_status([node.name for node in primary_cluster_nodes])
        logger.info("Wait for all the pods in storage namespace to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=rdr_workload.workload_pvc_count
        )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Select node on primary cluster(preferredCluster)
        if pod_to_select_node == "ramen_dr_cluster_operator":
            node_name = (
                get_pods_having_label(
                    label=constants.RAMEN_DR_CLUSTER_OPERATOR_APP_LABEL,
                    namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
                )[0]
                .get("spec")
                .get("nodeName")
            )
        else:
            node_name = (
                get_pods_having_label(
                    label=constants.RBD_MIRROR_APP_LABEL,
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
                .get("spec")
                .get("nodeName")
            )

        # Unschedule and start drain node operation
        unschedule_nodes([node_name])
        executor = ThreadPoolExecutor(max_workers=1)
        node_drain_operaton = executor.submit(drain_nodes, [node_name])

        # Perform relocate
        dr_helpers.relocate(
            primary_cluster_name,
            rdr_workload.workload_namespace,
            workload_type,
            rdr_workload.appset_placement_name
            if workload_type != constants.SUBSCRIPTION
            else None,
        )

        # Verify resources deletion from secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        # Verify resources creation on primary cluster (preferredCluster)
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=rdr_workload.workload_pvc_count
        )

        # Verify the result of node drain operation
        node_drain_operaton.result()

        # Make the node in the primary cluster schedule-able
        schedule_nodes([node_name])
