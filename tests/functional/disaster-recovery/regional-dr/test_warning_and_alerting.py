import logging
import pytest

from time import sleep

from selenium.common.exceptions import NoSuchElementException, NoAlertPresentException

from ocs_ci.framework import config
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
    rdr_ui_failover_config_required,
)
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_drpolicy_ui,
    check_cluster_operator_status,
    application_count_on_ui,
    health_and_peer_connection_check_on_ui,
    check_apps_running_on_selected_cluster,
    clusters_in_dr_relationship,
    protected_volume_count_per_cluster,
)
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.utils import enable_mco_console_plugin
from ocs_ci.utility.utils import ceph_health_check, get_ocp_version

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@rdr_ui_failover_config_required
@skipif_ocs_version("<4.16")
class TestRDRWarningAndAlerting:
    """
    Test class for RDR Warning and Alerting

    """

    @pytest.mark.polarion_id("xxxx")
    def test_rdr_warning_and_alerting(
        self, setup_acm_ui, dr_workload, nodes_multicluster, node_restart_teardown
    ):
        """
        Test to verify
            1. "Inconsistent data on target cluster" warning alert is seen on the Failover/Relocate modal when
            the lastGroupSyncTime is lagging behind 2x the sync interval or more for a particular DR protected workload

            2. VolumeSynchronizationDelay alert is fired for each workload where the sync is lagging behind:

        """

        # Enable MCO console plugin needed for DR dashboard
        enable_mco_console_plugin()

        rdr_workload_rbd = dr_workload(
            num_of_subscription=1,
            num_of_appset=0,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        rdr_workload_cephfs = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )

        rdr_workload_total = rdr_workload_rbd + rdr_workload_cephfs

        dr_helpers.set_current_primary_cluster_context(
            rdr_workload_rbd[0].workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload_rbd[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload_rbd[0].workload_namespace
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload_rbd[0].workload_namespace, rdr_workload_rbd[0].workload_type
        )

        acm_obj = AcmAddClusters()
        page_nav = ValidationUI()

        ocp_version = get_ocp_version()
        acm_loc = locators[ocp_version]["acm_page"]

        page_nav.refresh_web_console()
        # check_cluster_status_on_acm_console(acm_obj)
        # verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
        nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        # Verify if cluster is marked unknown on ACM console
        config.switch_acm_ctx()
        check_cluster_status_on_acm_console(
            acm_obj,
            down_cluster_name=primary_cluster_name,
            expected_text="Unknown",
        )
        workload_names = []
        workload_number = 1
        for workload in rdr_workload_total:
            logger.info(f"Workload name is {workload.workload_name}")
            workload_name = f"{workload.workload_name}-{workload_number}"
            workload_names.append(workload_name)
        logger.info(f"Workload names are {workload_names}")

        # Failover via ACM UI
        for workload in rdr_workload_total:
            if workload.workload_type == constants.SUBSCRIPTION:
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=workload_names[0],
                    policy_name=workload.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                    do_not_trigger=True,
                )
            else:
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=workload_names[1],
                    policy_name=workload.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                    workload_type=constants.APPLICATION_SET,
                    do_not_trigger=True,
                )

        warning_alert_found = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["inconsistent-warning-alert"],
            expected_text="Inconsistent data on target cluster",
            timeout=180,
        )
        if warning_alert_found:
            logger.info("Warning alert 'Inconsistent data on target cluster' found")
            logger.info("Click on 'Cancel' on the action modal")
            acm_obj.do_click(
                acm_loc["cancel-action-modal"], enable_screenshot=True, avoid_stale=True
            )
            logger.info(
                f"Action modal successfully closed for {constants.SUBSCRIPTION} type workload"
            )

            logger.info("Checking for VolumeSynchronizationDelay alert")

            warning_alert_found = acm_obj.wait_until_expected_text_is_found(
                locator=acm_loc["inconsistent-warning-alert"],
                expected_text="Inconsistent data on target cluster",
                timeout=180,
            )
        else:
            logger.error(
                "Warning alert 'Inconsistent data on target cluster' not found"
            )
            raise NoAlertPresentException
        sleep(wait_time * 60)
        nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
        wait_for_nodes_status([node.name for node in primary_cluster_nodes])
        logger.info("Wait for 180 seconds for pods to stabilize")
        sleep(180)
        logger.info("Wait for all the pods in openshift-storage to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()
        for workload in rdr_workload:
            dr_helpers.wait_for_all_resources_deletion(workload.workload_namespace)

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=total_protected_pvc_count
        )
        config.switch_acm_ctx()
        check_cluster_status_on_acm_console(acm_obj)
        verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)
        assert check_apps_running_on_selected_cluster(
            acm_obj, cluster_name=secondary_cluster_name, app_names=workload_names
        ), f"Apps {workload_names} not found on cluster {secondary_cluster_name} after failover operation"
        acm_obj.take_screenshot()
        logger.info(
            f"After failover, workloads {workload_names} successfully moved to cluster {secondary_cluster_name} "
            f"on DR dashboard"
        )
        assert clusters_in_dr_relationship(
            acm_obj,
            locator=acm_loc["2-healthy-dr-clusters"],
            timeout=120,
            expected_text="2 healthy",
        ), "Did not find 2 clusters in a healthy DR relationship"
        assert check_cluster_operator_status(
            acm_obj, timeout=120
        ), "Cluster operator status is degraded"
        acm_obj.take_screenshot()
        assert all(
            count == rdr_workload_count for count in application_count_on_ui(acm_obj)
        ), (
            f"Not all element count in list {application_count_on_ui(acm_obj)} "
            f"is equal to {rdr_workload_count}"
        )
        acm_obj.take_screenshot()
        assert health_and_peer_connection_check_on_ui(
            acm_obj, cluster1=primary_cluster_name, cluster2=secondary_cluster_name
        ), "Cluster and Operator health aren't healthy, check failed"
        assert total_protected_pvc_count == protected_volume_count_per_cluster(
            acm_obj, cluster_name=secondary_cluster_name
        ), (
            f"DR protected PVC count did not match on CLI and UI after failover operation on cluster "
            f"{secondary_cluster_name}"
        )
        logger.info(
            f"DR Protected PVC count on UI matches CLI count of {total_protected_pvc_count}"
        )
