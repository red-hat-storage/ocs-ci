"""
RHSTOR-8722: Test failover happy-path automation via Protected Applications UI.

Console flow covered:
  Protected applications → kebab → Test failover → Start
  → DR status (Action needed) → Clean up test  (or Convert to real failover)

How to run once the PR is ready (hub with RDR + ACM UI already deployed):

  # From ocs-ci checkout, with cluster-dir pointed at the hub
  run-ci \\
    -k test_test_failover_protected_apps_ui \\
    --ocsci-conf conf/ocsci/dr_ui.yaml \\
    --cluster-name <hub-cluster-name> \\
    --cluster-path <path-to-cluster-dir> \\
    tests/functional/disaster-recovery/regional-dr/test_test_failover_protected_apps_ui.py

Notes:
  - Requires ODF >= 4.22 (Test failover UI) and a working Selenium/ACM login
    (setup_acm_ui fixture).
  - Clean-up path keeps primary UP. Convert path stops primary nodes first.
  - Prefer a Deployed AppSet (not Subscription / Metro). AppSet must expose
    applicationRef so the Test failover kebab action is offered.
  - Related API-only coverage lives in am-agrawa #15057 (dryRun CLI); this
    file exercises the console path instead.
"""

import logging
from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import (
    clean_up_test_failover_ui,
    convert_test_failover_ui,
    test_failover_ui,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)

# DRPC progression while a Test failover (dryRun) hold is active.
# Kept local until shared constants land (see am-agrawa #15057).
STATUS_TESTING_FAILOVER = "TestingFailover"


def _abort_deployed_state_dryrun(drpc_obj):
    """
    Best-effort CLI abort when a Deployed-state test failover is left hanging.

    Matches the Deployed abort shape from the dryRun design
    (clear action + failoverCluster, dryRun=false).
    """
    config.switch_acm_ctx()
    try:
        if drpc_obj.get_progression_status() != STATUS_TESTING_FAILOVER:
            return
        logger.info(
            f"Aborting leftover Test failover on DRPC {drpc_obj.resource_name}"
        )
        assert drpc_obj.patch(
            params='{"spec":{"action":null,"failoverCluster":null,"dryRun":false}}',
            format_type="merge",
        ), f"Failed to abort dryRun on {drpc_obj.resource_name}"
        drpc_obj.wait_for_progression_status(
            constants.STATUS_COMPLETED, timeout=900
        )
    except Exception:
        logger.exception(
            f"Could not abort leftover Test failover on {drpc_obj.resource_name}"
        )


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.22")
class TestTestFailoverProtectedAppsUI:
    """
    Test failover (dryRun) happy path from the Protected Applications page.
    """

    def test_test_failover_cleanup_appset_rbd(
        self,
        request,
        setup_acm_ui,
        dr_workload,
    ):
        """
        AppSet + RBD: Start Test failover via UI, then Clean up test.

        Primary stays UP. Workload must remain on the original primary after
        cleanup, and DRPC progression must return to Completed.
        """
        logger.info("Deploy AppSet RBD workload for Test failover UI clean-up path")
        workload = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )[-1]
        ui_name = workload.workload_namespace
        drpc = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workload.appset_placement_name}-drpc",
        )
        request.addfinalizer(lambda: _abort_deployed_state_dryrun(drpc))

        primary_cluster = dr_helpers.get_current_primary_cluster_name(
            workload.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            workload.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        logger.info(
            f"DR clusters - Primary: {primary_cluster}, Secondary: {secondary_cluster}"
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for initial sync")
        sleep(wait_time * 60)

        progression = drpc.get_progression_status()
        assert progression == constants.STATUS_COMPLETED, (
            f"DRPC {drpc.resource_name} progression is {progression}, "
            f"expected {constants.STATUS_COMPLETED}"
        )
        dr_helpers.verify_last_group_sync_time(drpc, scheduling_interval)

        acm_obj = AcmAddClusters()
        config.switch_acm_ctx()

        logger.info("=" * 60)
        logger.info("UI: Start Test failover from Protected Applications kebab")
        logger.info("=" * 60)
        test_failover_ui(acm_obj, workload_name=ui_name)
        drpc.wait_for_progression_status(STATUS_TESTING_FAILOVER, timeout=900)
        logger.info("DRPC reached TestingFailover (Action needed in UI)")

        logger.info("=" * 60)
        logger.info("UI: Clean up test from DR status popover")
        logger.info("=" * 60)
        clean_up_test_failover_ui(acm_obj, workload_name=ui_name)
        drpc.wait_for_progression_status(constants.STATUS_COMPLETED, timeout=900)

        # Primary production workload must still be intact after clean-up
        config.switch_to_cluster_by_name(primary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )
        logger.info(
            "Test failover clean-up verified: progression Completed and "
            "primary workload intact"
        )

    def test_test_failover_convert_appset_rbd(
        self,
        request,
        setup_acm_ui,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        AppSet + RBD: Start Test failover via UI, stop primary, Convert to real
        failover, then verify FailedOver on the secondary cluster.
        """
        logger.info("Deploy AppSet RBD workload for Test failover UI convert path")
        workload = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )[-1]
        ui_name = workload.workload_namespace
        drpc = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workload.appset_placement_name}-drpc",
        )
        request.addfinalizer(lambda: _abort_deployed_state_dryrun(drpc))

        primary_cluster = dr_helpers.get_current_primary_cluster_name(
            workload.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            workload.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        config.switch_to_cluster_by_name(primary_cluster)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for initial sync")
        sleep(wait_time * 60)

        progression = drpc.get_progression_status()
        assert progression == constants.STATUS_COMPLETED, (
            f"DRPC {drpc.resource_name} progression is {progression}, "
            f"expected {constants.STATUS_COMPLETED}"
        )
        dr_helpers.verify_last_group_sync_time(drpc, scheduling_interval)

        acm_obj = AcmAddClusters()
        config.switch_acm_ctx()

        logger.info("UI: Start Test failover")
        test_failover_ui(acm_obj, workload_name=ui_name)
        drpc.wait_for_progression_status(STATUS_TESTING_FAILOVER, timeout=900)

        config.switch_to_cluster_by_name(primary_cluster)
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster}")
        nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        config.switch_acm_ctx()
        logger.info("UI: Convert test to real failover")
        convert_test_failover_ui(acm_obj, workload_name=ui_name)
        drpc.wait_for_phase(constants.STATUS_FAILEDOVER, timeout=900)

        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            performed_dr_action=True,
        )

        # Bring primary back and clean leftover resources
        config.switch_to_cluster_by_name(primary_cluster)
        logger.info(f"Starting nodes of primary cluster: {primary_cluster}")
        nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
        wait_for_nodes_status(target_status=constants.NODE_READY)
        wait_for_pods_to_be_running(timeout=600)
        ceph_health_check(tries=60, delay=15)
        dr_helpers.wait_for_all_resources_deletion(workload.workload_namespace)

        logger.info(
            "Test failover convert verified: DRPC FailedOver and workload "
            "running on secondary"
        )
