import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@tier1
class TestSequentialFailover:
    """
    Test Sequential Failover actions

    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            pytest.param(
                False, marks=pytest.mark.polarion_id("OCS-4771"), id="primary_up"
            ),
            pytest.param(
                True, marks=pytest.mark.polarion_id("OCS-4770"), id="primary_down"
            ),
        ],
    )
    def test_sequential_failover_to_secondary(
        self,
        primary_cluster_down,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify failover action for multiple workloads one after another from primary to secondary cluster
        when primary cluster is Up/Down

        """
        workloads = dr_workload(num_of_subscription=5)

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workloads[0].workload_namespace
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workloads[0].workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        time.sleep(wait_time * 60)

        # Stop primary cluster nodes
        if primary_cluster_down:
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        # Initiate failover for all the workloads one after another
        config.switch_acm_ctx()
        failover_results = []
        with ThreadPoolExecutor() as executor:
            for wl in workloads:
                failover_results.append(
                    executor.submit(
                        dr_helpers.failover,
                        failover_cluster=secondary_cluster_name,
                        namespace=wl.workload_namespace,
                    )
                )
                time.sleep(5)

        # Wait for failover results
        for failover in failover_results:
            failover.result()

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
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} minutes before starting nodes of primary cluster: {primary_cluster_name}"
            )
            time.sleep(wait_time * 60)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
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

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum([wl.workload_pvc_count for wl in workloads])
        )
