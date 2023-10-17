import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from ocs_ci.framework import config

# from ocs_ci.framework.testlib import tier1, tier4a
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility import version
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)

logger = logging.getLogger(__name__)

polarion_id_primary_up = "OCS-4771"
polarion_id_primary_down = "OCS-4770"
# tier_name = tier1
if config.RUN.get("rdr_failover_via_ui"):
    polarion_id_primary_up = "OCS-5032"
    polarion_id_primary_down = "OCS-5033"
    # tier_name = tier4a


class TestSequentialFailover:
    """
    Test Sequential Failover actions

    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            pytest.param(
                False,
                # marks=[polarion_id(polarion_id_primary_up), tier_name],
                id="primary_up",
            ),
            pytest.param(
                True,
                # marks=[polarion_id(polarion_id_primary_down), tier_name],
                id="primary_down",
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

        This test is also compatible to be run from ACM UI. Pass the yaml conf/ocsci/dr_ui.yaml to test from UI.

        """
        if config.RUN.get("rdr_failover_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error("ODF/ACM version isn't supported for Failover operation")
                raise NotImplementedError
        acm_obj = AcmAddClusters()
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

        if config.RUN.get("rdr_failover_via_ui"):
            logger.info("Start the process of Failover from ACM UI")
            config.switch_acm_ctx()
            dr_submariner_validation_from_ui(acm_obj)

        # Stop primary cluster nodes
        if primary_cluster_down:
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        config.switch_acm_ctx()
        if config.RUN.get("rdr_failover_via_ui"):
            if primary_cluster_down:
                # Verify that the cluster is marked unknown in the ACM console
                if config.RUN.get("rdr_failover_via_ui"):
                    config.switch_acm_ctx()
                    check_cluster_status_on_acm_console(
                        acm_obj,
                        down_cluster_name=primary_cluster_name,
                        expected_text="Unknown",
                    )
            else:
                # Verify that the cluster is marked Ready in the ACM console
                check_cluster_status_on_acm_console(acm_obj)

        # Initiate failover for all the workloads one after another
        failover_results = []
        with ThreadPoolExecutor() as executor:
            for wl in workloads:
                if config.RUN.get("rdr_failover_via_ui"):
                    # Sequential failover process via ACM UI
                    failover_relocate_ui(
                        acm_obj,
                        scheduling_interval=scheduling_interval,
                        workload_to_move=f"{wl.workload_name}-1",
                        policy_name=wl.dr_policy_name,
                        failover_or_preferred_cluster=secondary_cluster_name,
                        action=constants.ACTION_FAILOVER,
                    )
                else:
                    # Failover process via CLI
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

        if config.RUN.get("rdr_failover_via_ui"):
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(
                acm_obj=acm_obj, action=constants.ACTION_FAILOVER
            )
