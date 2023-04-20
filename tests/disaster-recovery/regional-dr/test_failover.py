import logging
import pytest

from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)
from ocs_ci.helpers.dr_helpers import get_current_primary_cluster_name
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


@acceptance
@tier1
class TestFailover:
    """
    Test Failover action

    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            pytest.param(
                False, marks=pytest.mark.polarion_id("OCS-4429"), id="primary_up"
            ),
            pytest.param(
                True, marks=pytest.mark.polarion_id("OCS-4426"), id="primary_down"
            ),
        ],
    )
    def test_failover(
        self,
        setup_acm_ui,
        primary_cluster_down,
        nodes_multicluster,
        rdr_workload,
        node_restart_teardown,
    ):
        """
        Tests to verify application failover between managed clusters
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is UP
            2) Failover to secondary cluster when primary cluster is DOWN

        This test is also compatible to be run from ACM UI,
        pass the yaml conf/ocsci/rdr_ui.yaml to trigger it.

        """
        acm_obj = AcmAddClusters(setup_acm_ui)

        dr_helpers.set_current_primary_cluster_context(rdr_workload.workload_namespace)
        primary_cluster_index = config.cur_index
        node_objs = get_node_objs()

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        primary_cluster_name = get_current_primary_cluster_name(
            rdr_workload.workload_namespace
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )

        if config.ENV_DATA.get("rdr_failover_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version >= version.VERSION_4_13:
                logger.info("Start the process of failover from ACM UI")
                config.switch_acm_ctx()
                dr_submariner_validation_from_ui(acm_obj)
            else:
                logger.error("ODF/ACM version isn't supported for Failover operation")
                raise NotImplementedError

        # Stop primary cluster nodes
        if primary_cluster_down:
            logger.info("Stopping primary cluster nodes")
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

            # Verify if cluster is marked unknown on ACM console
            if config.ENV_DATA.get("rdr_failover_via_ui"):
                config.switch_acm_ctx()
                check_cluster_status_on_acm_console(
                    acm_obj,
                    down_cluster_name=primary_cluster_name,
                    expected_text="Unknown",
                )
        elif config.ENV_DATA.get("rdr_failover_via_ui"):
            check_cluster_status_on_acm_console(acm_obj)

        if config.ENV_DATA.get("rdr_failover_via_ui"):
            # Failover via ACM UI
            failover_relocate_ui(
                acm_obj,
                scheduling_interval=scheduling_interval,
                workload_to_move=f"{rdr_workload.workload_name}-1",
                policy_name=rdr_workload.dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
            )
        else:
            # Failover action via CLI
            dr_helpers.failover(secondary_cluster_name, rdr_workload.workload_namespace)

        # Verify resources creation on new primary cluster (failoverCluster)
        dr_helpers.set_current_primary_cluster_context(rdr_workload.workload_namespace)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        # Verify resources deletion from previous primary or current secondary cluster
        dr_helpers.set_current_secondary_cluster_context(
            rdr_workload.workload_namespace
        )
        # Start nodes if cluster is down
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} minutes before starting nodes of previous primary cluster"
            )
            sleep(120)
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            wait_for_nodes_status([node.name for node in node_objs])
            logger.info(
                "Wait for all the pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking for Ceph Health OK")
            ceph_health_check()
        dr_helpers.wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        dr_helpers.wait_for_mirroring_status_ok()

        if config.ENV_DATA.get("rdr_relocate_via_ui"):
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(acm_obj)

        # TODO: Add data integrity checks
