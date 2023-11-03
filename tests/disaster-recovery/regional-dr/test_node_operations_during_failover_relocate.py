import logging
import os
from datetime import datetime
from time import sleep
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier4b
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_active_acm_index
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler

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
                *[constants.SUBSCRIPTION, "odr_operator"],
                marks=pytest.mark.polarion_id("OCS-4443"),
            ),
            pytest.param(
                *[constants.APPLICATION_SET, "rbd_mirror"],
                marks=pytest.mark.polarion_id("OCS-4441"),
            ),
            pytest.param(
                *[constants.APPLICATION_SET, "odr_operator"],
                marks=pytest.mark.polarion_id("OCS-4443"),
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
    ):
        """
        Tests cases to verify that the failover and relocate operations are not affected when node is drained

        """
        if workload_type == constants.SUBSCRIPTION:
            rdr_workload = dr_workload(num_of_subscription=1)[0]
            drpc_obj = DRPC(namespace=rdr_workload.workload_namespace)
        else:
            rdr_workload = dr_workload(num_of_subscription=0, num_of_appset=1)[0]
            drpc_obj = DRPC(
                namespace=constants.GITOPS_CLUSTER_NAMESPACE,
                resource_name=f"{rdr_workload.appset_placement_name}-drpc",
            )

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

        # Set cluster_kubeconfig value for 'drpc_obj' object to fetch the details without switching the cluster context
        acm_cluster_kubeconfig = os.path.join(
            config.clusters[get_active_acm_index()].ENV_DATA["cluster_path"],
            config.clusters[get_active_acm_index()].RUN.get("kubeconfig_location"),
        )
        drpc_obj.cluster_kubeconfig = acm_cluster_kubeconfig

        # Get lastGroupSyncTime before failover
        drpc_data = drpc_obj.get()
        last_group_sync_time = drpc_data.get("status").get("lastGroupSyncTime")
        logger.info(
            f"The value of lastGroupSyncTime before failover is {last_group_sync_time}."
        )

        # Verify lastGroupSyncTime before failover
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        last_group_sync_time_formatted = datetime.strptime(
            last_group_sync_time, time_format
        )
        current_time = datetime.strptime(
            datetime.utcnow().strftime(time_format), time_format
        )
        time_since_last_sync = (
            current_time - last_group_sync_time_formatted
        ).total_seconds() / 60
        logger.info(
            f"Before failover - Time in minutes since the last sync {time_since_last_sync}"
        )
        assert (
            time_since_last_sync < 2 * scheduling_interval
        ), "Before failover - Time since last sync is two times greater than the scheduling interval."
        logger.info("Verified lastGroupSyncTime before failover.")

        # Stop primary cluster nodes
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
        nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        # Failover operation
        dr_helpers.failover(
            secondary_cluster_name,
            rdr_workload.workload_namespace,
            workload_type,
            rdr_workload.appset_placement_name
            if workload_type != constants.SUBSCRIPTION
            else None,
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        # Verify resources deletion from primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)

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

        # Verify resources deletion from primary cluster
        dr_helpers.wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=rdr_workload.workload_pvc_count
        )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Get lastGroupSyncTime after failover.
        # The parameter lastGroupSyncTime may not be present for some time after failover.
        for drpc_data in TimeoutSampler(300, 5, drpc_obj.get):
            post_failover_last_group_sync_time = drpc_data.get("status").get(
                "lastGroupSyncTime"
            )
            if post_failover_last_group_sync_time:
                logger.info("After failover - Obtained lastGroupSyncTime.")
                # Adding an additional check to make sure that the old value is not populated again.
                if post_failover_last_group_sync_time != last_group_sync_time:
                    logger.info(
                        "After failover - Verified: lastGroupSyncTime after failover is different from initial value."
                    )
                    break
            logger.info(
                "The value of lastGroupSyncTime in drpc is not updated after failover. Retrying."
            )
        logger.info(
            f"The value of lastGroupSyncTime after failover is {post_failover_last_group_sync_time}."
        )

        # Verify lastGroupSyncTime after failover
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        post_failover_last_group_sync_time_formatted = datetime.strptime(
            post_failover_last_group_sync_time, time_format
        )
        current_time = datetime.strptime(
            datetime.utcnow().strftime(time_format), time_format
        )
        time_since_last_sync = (
            current_time - post_failover_last_group_sync_time_formatted
        ).total_seconds() / 60
        logger.info(
            f"After failover - Time in minutes since the last sync is {time_since_last_sync}"
        )
        assert (
            time_since_last_sync < 3 * scheduling_interval
        ), "After failover - Time since last sync is three times greater than the scheduling interval."
        logger.info("Verified lastGroupSyncTime after failover.")

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

        # Get lastGroupSyncTime after relocate. The parameter lastGroupSyncTime may not be present in drpc yaml for
        # some time after relocate. So the wait time given is more than the scheduling interval.
        for drpc_data in TimeoutSampler(
            (scheduling_interval * 60) + 300, 15, drpc_obj.get
        ):
            post_relocate_last_group_sync_time = drpc_data.get("status").get(
                "lastGroupSyncTime"
            )
            if post_relocate_last_group_sync_time:
                logger.info("After relocate - Obtained lastGroupSyncTime.")
                # Adding an additional check to make sure that the old value is not populated again.
                if (
                    post_relocate_last_group_sync_time
                    != post_failover_last_group_sync_time
                ):
                    logger.info(
                        "After relocate - Verified: lastGroupSyncTime after relocate is different from the previous "
                        "value."
                    )
                    break
            logger.info(
                "The value of lastGroupSyncTime in drpc is not updated after relocate. Retrying."
            )
        logger.info(
            f"The value of lastGroupSyncTime after relocate is {post_relocate_last_group_sync_time}."
        )

        # Verify lastGroupSyncTime after relocate
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        post_relocate_last_group_sync_time_formatted = datetime.strptime(
            post_relocate_last_group_sync_time, time_format
        )
        current_time = datetime.strptime(
            datetime.utcnow().strftime(time_format), time_format
        )
        time_since_last_sync = (
            current_time - post_relocate_last_group_sync_time_formatted
        ).total_seconds() / 60
        logger.info(
            f"After relocate - Time in minutes since the last sync is {time_since_last_sync}"
        )
        assert (
            time_since_last_sync < 3 * scheduling_interval
        ), "After relocate - Time since last sync is three times greater than the scheduling interval."
        logger.info("Verified lastGroupSyncTime after relocate.")
