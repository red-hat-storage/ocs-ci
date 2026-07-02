"""
UI tests for Disaster recovery Topology view (ODF 4.22+).
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import (
    close_dr_topology_sidebar,
    navigate_to_dr_topology_tab,
    open_dr_topology_cluster_sidebar,
    open_dr_topology_drpc_sidebar,
    open_dr_topology_policy_sidebar,
    verify_df_cluster_listing_on_topology_view,
    verify_dr_topology_applications_sidebar,
    verify_dr_topology_cluster_sidebar_open,
    verify_dr_topology_cluster_name_search_filter,
    verify_dr_topology_policy_search_filter,
    verify_dr_topology_policy_sidebar_open,
    wait_for_dr_topology_workloads_healthy,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.utils import enable_mco_console_plugin

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.22")
class TestDRTopologyViewUI:
    """
    Test class for Disaster recovery Topology view UI validation
    """

    @pytest.mark.polarion_id("OCS-8026")
    def test_dr_topology_view_ui(
        self,
        setup_acm_ui,
        dr_workload,
        discovered_apps_dr_workload,
    ):
        """
        End-to-end DR Topology view UI validation.

        Phase 1:
            1. Verify only Data Foundation clusters are displayed on the Topology tab
            2. Verify clicking a cluster node opens the cluster sidebar
            3. Verify clicking a DRPolicy opens the policy sidebar
            4. Verify Cluster name and Policy search filters on the topology view
            5. Deploy and DR protect one ApplicationSet and one discovered application
            6. Wait until DR setup is healthy
            7. Verify clicking DRPC status opens the Applications sidebar

        """
        enable_mco_console_plugin()

        expected_df_clusters = sorted(
            name
            for name in dr_helpers.get_dr_topology_clusters()
            if name != constants.ACM_LOCAL_CLUSTER
        )
        logger.info(
            f"Expected Data Foundation clusters from CLI: {expected_df_clusters}"
        )

        page_nav = ValidationUI()
        page_nav.refresh_web_console()

        acm_obj = AcmAddClusters()
        navigate_to_dr_topology_tab(acm_obj)
        verified_clusters = verify_df_cluster_listing_on_topology_view(
            acm_obj,
            expected_clusters=expected_df_clusters,
        )
        assert (
            verified_clusters == expected_df_clusters
        ), "Data Foundation clusters on Topology UI do not match CLI output"
        logger.info(
            f"DR Topology lists expected Data Foundation clusters: {verified_clusters}"
        )

        cluster_name = verified_clusters[0]
        open_dr_topology_cluster_sidebar(acm_obj, cluster_name)
        verify_dr_topology_cluster_sidebar_open(acm_obj, cluster_name)
        close_dr_topology_sidebar(acm_obj)
        logger.info(f"Cluster sidebar validation completed for '{cluster_name}'")

        policy_details = dr_helpers.get_dr_topology_policy_details()
        open_dr_topology_policy_sidebar(acm_obj, policy_details["name"])
        verify_dr_topology_policy_sidebar_open(
            acm_obj,
            policy_details["name"],
            policy_details["connected_clusters"],
            policy_details["scheduling_interval"],
        )
        close_dr_topology_sidebar(acm_obj)
        logger.info(
            f"Policy sidebar validation completed for '{policy_details['name']}'"
        )

        hidden_clusters = [name for name in verified_clusters if name != cluster_name]
        verify_dr_topology_cluster_name_search_filter(
            acm_obj, cluster_name, hidden_clusters
        )
        verify_dr_topology_policy_search_filter(
            acm_obj,
            policy_details["name"],
            policy_details["connected_clusters"],
        )
        logger.info("DR Topology search and filter validation completed")

        appset_workloads = dr_workload(num_of_subscription=0, num_of_appset=1)
        discovered_workloads = discovered_apps_dr_workload(kubeobject=1)
        workloads = appset_workloads + discovered_workloads
        logger.info(
            f"Deployed DR workloads: appset={len(appset_workloads)}, "
            f"discovered={len(discovered_workloads)}"
        )
        scheduling_interval = dr_helpers.get_scheduling_interval(
            appset_workloads[0].workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        dr_helpers.set_current_primary_cluster_context(
            appset_workloads[0].workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        wait_for_dr_topology_workloads_healthy(
            workloads=workloads,
            scheduling_interval=scheduling_interval,
        )

        protected_apps = dr_helpers.get_dr_topology_protected_apps(workloads)
        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            appset_workloads[0].workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        open_dr_topology_drpc_sidebar(acm_obj, primary_cluster_name)
        verify_dr_topology_applications_sidebar(acm_obj, protected_apps)
        close_dr_topology_sidebar(acm_obj)
        logger.info(
            f"Applications sidebar validation completed for cluster "
            f"'{primary_cluster_name}'"
        )
