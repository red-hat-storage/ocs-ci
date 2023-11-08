import logging
from time import sleep

import pytest

from ocs_ci.framework import config
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
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility import version
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)

polarion_id_primary_up = "OCS-4429"
polarion_id_primary_down = "OCS-4426"
if config.RUN.get("rdr_failover_via_ui"):
    polarion_id_primary_up = "OCS-4741"
    polarion_id_primary_down = "OCS-4742"


@acceptance
@tier1
class TestFailover:
    """
    Test Failover action

    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface"],
        argvalues=[
            pytest.param(
                *[False, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id(polarion_id_primary_up),
                id="primary_up",
            ),
            pytest.param(
                *[True, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id(polarion_id_primary_down),
                id="primary_down",
            ),
            pytest.param(
                *[False, constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id(""),
                id="primary_up_cephfs",
            ),
            pytest.param(
                *[True, constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id(""),
                id="primary_down_cephfs",
            ),
        ],
    )
    def test_failover(
        self,
        primary_cluster_down,
        pvc_interface,
        setup_acm_ui,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify application failover between managed clusters
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is UP
            2) Failover to secondary cluster when primary cluster is DOWN

        This test is also compatible to be run from ACM UI,
        pass the yaml conf/ocsci/dr_ui.yaml to trigger it.

        """
        if config.RUN.get("rdr_failover_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error("ODF/ACM version isn't supported for Failover operation")
                raise NotImplementedError

        acm_obj = AcmAddClusters()
        rdr_workload = dr_workload(num_of_subscription=1, pvc_interface=pvc_interface)[
            0
        ]

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload.workload_namespace
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        if config.RUN.get("rdr_failover_via_ui"):
            logger.info("Start the process of Failover from ACM UI")
            config.switch_acm_ctx()
            dr_submariner_validation_from_ui(acm_obj)

        # Stop primary cluster nodes
        if primary_cluster_down:
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

            # Verify if cluster is marked unknown on ACM console
            if config.RUN.get("rdr_failover_via_ui"):
                config.switch_acm_ctx()
                check_cluster_status_on_acm_console(
                    acm_obj,
                    down_cluster_name=primary_cluster_name,
                    expected_text="Unknown",
                )
        elif config.RUN.get("rdr_failover_via_ui"):
            check_cluster_status_on_acm_console(acm_obj)

        if config.RUN.get("rdr_failover_via_ui"):
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

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
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
            logger.info(
                "Wait for all the pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking for Ceph Health OK")
            ceph_health_check()
        dr_helpers.wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=rdr_workload.workload_pvc_count
        )

        if config.RUN.get("rdr_failover_via_ui"):
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(acm_obj)

        # TODO: Add data integrity checks
