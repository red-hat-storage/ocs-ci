import logging
import time

import pytest

from time import sleep

from selenium.common.exceptions import NoAlertPresentException

from ocs_ci.framework import config
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
    polarion_id,
)
from ocs_ci.helpers import dr_helpers, helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    failover_relocate_ui,
    verify_drpolicy_ui,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ui.base_ui import wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version
from ocs_ci.ocs.utils import (
    enable_mco_console_plugin,
    get_primary_cluster_config,
    get_non_acm_cluster_config,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


def modify_deployment_count(status=""):
    """
    We scale down rbd-mirror daemon deployment on the secondary managed cluster and mds daemons on the primary managed
    cluster and scale them back to their original count

    Args:
        status (str): "down" by default sets replica count to 0, anything else like "up" will set it back to 1
    """
    if not status:
        status = "down"
    replica_count = 0 if status == "down" else 1
    primary_config = get_primary_cluster_config()
    primary_index = primary_config.MULTICLUSTER.get("multicluster_index")
    secondary_index = [
        s.MULTICLUSTER["multicluster_index"]
        for s in get_non_acm_cluster_config()
        if s.MULTICLUSTER["multicluster_index"] != primary_index
    ][0]

    logger.info(
        "Change replica count for rbd-mirror deployment on the secondary cluster "
        "and mds deployments on the primary cluster"
    )
    config.switch_ctx(secondary_index)
    helpers.modify_deployment_replica_count(
        deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT,
        replica_count=replica_count,
    )
    if status != "down":
        ceph_health_check(tries=10, delay=30)
    config.switch_ctx(primary_index)
    helpers.modify_deployment_replica_count(
        deployment_name=constants.MDS_DAEMON_DEPLOYMENT_ONE, replica_count=replica_count
    )
    helpers.modify_deployment_replica_count(
        deployment_name=constants.MDS_DAEMON_DEPLOYMENT_TWO, replica_count=replica_count
    )
    if status != "down":
        ceph_health_check(tries=10, delay=30)
    logger.info("Replica count updated successfully")


@pytest.fixture
def scale_up_deployment(request):
    def teardown():
        modify_deployment_count(status="up")

    request.addfinalizer(teardown)


@rdr
@tier1
@turquoise_squad
class TestRDRWarningAndAlerting:
    """
    Test class for RDR Warning and Alerting

    """

    @pytest.mark.parametrize(
        argnames=[
            "action",
        ],
        argvalues=[
            pytest.param(
                constants.ACTION_FAILOVER,
                marks=pytest.mark.polarion_id("xxx"),
            ),
            pytest.param(
                constants.ACTION_RELOCATE,
                marks=pytest.mark.polarion_id("yyy"),
            ),
        ],
    )
    # TODO: Update polarion IDs
    @skipif_ocs_version("<4.18")
    def test_rdr_inconsistent_data_warning_alert(
        self, action, setup_acm_ui, dr_workload, scale_up_deployment
    ):
        """
        Test to verify that "Inconsistent data on target cluster" warning alert is seen on the Failover/Relocate modal
        when the lastGroupSyncTime is lagging behind 2x the sync interval or more for a particular DR protected workload

        No DR action is performed in this test case. We scale down rbd-mirror daemon deployment on the secondary cluster
        and mds daemons on the primary cluster and scale them up back to their original count.

        """

        config.switch_acm_ctx()
        # Enable MCO console plugin needed for DR dashboard
        enable_mco_console_plugin()

        workload_names = []
        rdr_workload = dr_workload(
            num_of_subscription=1,
            num_of_appset=0,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        workload_names.append(f"{rdr_workload[0].workload_name}-1")
        dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )
        workload_names.append(f"{rdr_workload[1].workload_name}-1-cephfs")

        logger.info(f"Workload names are {workload_names}")

        dr_helpers.set_current_primary_cluster_context(
            rdr_workload[0].workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload[0].workload_namespace
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload[0].workload_namespace, rdr_workload[0].workload_type
        )

        config.switch_acm_ctx()
        acm_obj = AcmAddClusters()
        page_nav = ValidationUI()

        acm_loc = locators_for_current_ocp_version()["acm_page"]

        page_nav.refresh_web_console()
        config.switch_to_cluster_by_name(primary_cluster_name)
        drpc_subscription = DRPC(namespace=rdr_workload[0].workload_namespace)
        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{rdr_workload[1].appset_placement_name}-drpc",
        )
        drpc_objs = [drpc_subscription, drpc_appset]
        before_failover_last_group_sync_time = []
        for obj in drpc_objs:
            before_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime")
        modify_deployment_count()
        config.switch_acm_ctx()
        logger.info(
            f"Waiting for {wait_time * 60} seconds to allow warning alert to appear"
        )
        sleep(wait_time * 60)
        # Navigate to failover modal via ACM UI
        logger.info("Navigate to failover modal via ACM UI")
        for workload in rdr_workload:
            if workload.workload_type == constants.SUBSCRIPTION:
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=workload_names[0],
                    policy_name=workload.dr_policy_name,
                    action=action,
                    failover_or_preferred_cluster=secondary_cluster_name,
                    do_not_trigger=True,
                )
                warning_alert_found = acm_obj.wait_until_expected_text_is_found(
                    locator=acm_loc["inconsistent-warning-alert"],
                    expected_text="Inconsistent data on target cluster",
                    timeout=300,
                )
                if warning_alert_found:
                    logger.info(
                        "Warning alert 'Inconsistent data on target cluster' found on the Failover modal"
                    )
                    logger.info("Click on 'Cancel' on the action modal")
                    acm_obj.do_click(
                        acm_loc["cancel-action-modal"],
                        enable_screenshot=True,
                        avoid_stale=True,
                    )
                    logger.info("Action modal closed successfully")
                else:
                    logger.error(
                        "Warning alert 'Inconsistent data on target cluster' not found on the Failover modal"
                    )
                    raise NoAlertPresentException
            else:
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=workload_names[1],
                    policy_name=workload.dr_policy_name,
                    action=action,
                    failover_or_preferred_cluster=secondary_cluster_name,
                    workload_type=constants.APPLICATION_SET,
                    do_not_trigger=True,
                )
                warning_alert_found = acm_obj.wait_until_expected_text_is_found(
                    locator=acm_loc["inconsistent-warning-alert"],
                    expected_text="Inconsistent data on target cluster",
                    timeout=300,
                )
                if warning_alert_found:
                    logger.info(
                        "Warning alert 'Inconsistent data on target cluster' found on the Relocate modal"
                    )
                    logger.info("Click on 'Cancel' on the action modal")
                    acm_obj.do_click(
                        acm_loc["cancel-action-modal"],
                        enable_screenshot=True,
                        avoid_stale=True,
                    )
                    logger.info("Action modal closed successfully")
                else:
                    logger.error(
                        "Warning alert 'Inconsistent data on target cluster' not found on the Relocate modal"
                    )
                    raise NoAlertPresentException

        modify_deployment_count(status="up")
        logger.info(
            f"Waiting for {wait_time * 60} seconds to allow warning alert to disappear"
        )
        sleep(wait_time * 60)

        for obj, initial_last_group_sync_time in zip(
            drpc_objs, before_failover_last_group_sync_time
        ):
            dr_helpers.verify_last_group_sync_time(
                obj, scheduling_interval, initial_last_group_sync_time
            )
        logger.info("lastGroupSyncTime updated after pods are recovered")

        config.switch_acm_ctx()
        # Navigate to failover/relocate modal via ACM UI
        logger.info("Navigate to failover/relocate modal via ACM UI")
        for workload in rdr_workload:
            if workload.workload_type == constants.SUBSCRIPTION:
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=workload_names[0],
                    policy_name=workload.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                    do_not_trigger=True,
                )
                # Allow additional time for warning alert to disappear
                logger.info("Allowing additional time for warning alert to disappear")
                time.sleep(120)
                warning_alert_found = acm_obj.wait_until_expected_text_is_found(
                    locator=acm_loc["inconsistent-warning-alert"],
                    expected_text="Inconsistent data on target cluster",
                    timeout=60,
                )
                if warning_alert_found:
                    logger.error(
                        "Warning alert 'Inconsistent data on target cluster' still exists after successful sync on the "
                        "Failover modal"
                    )
                    raise UnexpectedBehaviour
                else:
                    logger.info(
                        "Warning alert 'Inconsistent data on target cluster' disappeared after successful sync on the "
                        "Failover modal"
                    )
                    logger.info("Click on 'Cancel' on the action modal")
                    acm_obj.do_click(
                        acm_loc["cancel-action-modal"],
                        enable_screenshot=True,
                        avoid_stale=True,
                    )
                    logger.info("Action modal closed successfully")
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
                # Allow additional time for alerts to disappear
                time.sleep(120)
                warning_alert_found = acm_obj.wait_until_expected_text_is_found(
                    locator=acm_loc["inconsistent-warning-alert"],
                    expected_text="Inconsistent data on target cluster",
                    timeout=60,
                )
                if warning_alert_found:
                    logger.error(
                        "Warning alert 'Inconsistent data on target cluster' still exists after successful sync on the "
                        "Relocate modal"
                    )
                    raise UnexpectedBehaviour
                else:
                    logger.info(
                        "Warning alert 'Inconsistent data on target cluster' disappeared after successful sync on the "
                        "Relocate modal"
                    )
                    logger.info("Click on 'Cancel' on the action modal")
                    acm_obj.do_click(
                        acm_loc["cancel-action-modal"],
                        enable_screenshot=True,
                        avoid_stale=True,
                    )
                    logger.info("Action modal closed successfully")

    @polarion_id("OCS-5348")
    @skipif_ocs_version("<4.14")
    def test_rdr_volumesyncronizationdelayalert(
        self, setup_acm_ui, dr_workload, scale_up_deployment
    ):
        """
        Test to verify that "VolumeSynchronizationDelay" warning and critical level alert is fired on the DR dashboard.

        Warning level alert is fired when lastGroupSyncTime is lagging behind 2x the sync interval
        from current time in UTC but is less than 3x

        Critical level alert is fired when lastGroupSyncTime is lagging behind 3x the sync interval
        from current time in UTC or more

        No DR action is performed in this test case. We scale down rbd-mirror daemon deployment on the secondary cluster
        and mds daemons on the primary cluster and scale them up back to their original count.

        """

        config.switch_acm_ctx()
        # Enable MCO console plugin needed for DR dashboard
        enable_mco_console_plugin()

        workload_names = []
        rdr_workload = dr_workload(
            num_of_subscription=1,
            num_of_appset=0,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        workload_names.append(f"{rdr_workload[0].workload_name}-1")
        dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )
        workload_names.append(f"{rdr_workload[1].workload_name}-1-cephfs")

        logger.info(f"Workload names are {workload_names}")

        dr_helpers.set_current_primary_cluster_context(
            rdr_workload[0].workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload[0].workload_namespace
        )

        buffer_time = 1 + (2 * scheduling_interval)

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload[0].workload_namespace
        )

        config.switch_acm_ctx()
        acm_obj = AcmAddClusters()
        page_nav = ValidationUI()

        acm_loc = locators_for_current_ocp_version()["acm_page"]

        page_nav.refresh_web_console()
        config.switch_to_cluster_by_name(primary_cluster_name)

        modify_deployment_count()
        config.switch_acm_ctx()
        verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)

        logger.info("Click on Warning alerts")
        warning_alert = acm_obj.find_an_element_by_xpath(
            "//*[@id='alert-toggle-warning']"
        ).get_attribute("aria-expanded")
        logger.info(f"State of Warning alert option is: {warning_alert}")
        if warning_alert == "false":
            logger.info("Expand Warning alert option on the DR dashboard")
            warning_alert = wait_for_element_to_be_clickable(acm_loc["warning-alert"])
            acm_obj.driver.execute_script("arguments[0].click();", warning_alert)
            acm_obj.take_screenshot()
            logger.info(
                "Successfully expanded Warning alert option on the DR dashboard"
            )
        logger.info(
            f"Wait for {buffer_time} minutes for VolumeSynchronizationDelay Warning alert to be fired"
        )
        alert_1 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelaywarningalert1"],
            expected_text="VolumeSynchronizationDelay",
            timeout=buffer_time * 60,
        )
        if alert_1:
            logger.info(
                "Warning level first 'VolumeSynchronizationDelay' alert found on the DR dashboard"
            )
        else:
            logger.error(
                "First Warning level 'VolumeSynchronizationDelay' alert not found on the DR dashboard"
            )
            raise NoAlertPresentException

        alert_2 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelaywarningalert2"],
            expected_text="VolumeSynchronizationDelay",
            timeout=180,
        )
        if alert_2:
            logger.info(
                "Warning level second 'VolumeSynchronizationDelay' alert found on the DR dashboard"
            )
        else:
            logger.error(
                "Second Warning level 'VolumeSynchronizationDelay' alert not found on the DR dashboard"
            )
            raise NoAlertPresentException
        if warning_alert == "true":
            logger.info("Close Warning alert option on the DR dashboard")
            warning_alert = wait_for_element_to_be_clickable(acm_loc["warning-alert"])
            acm_obj.driver.execute_script("arguments[0].click();", warning_alert)
            acm_obj.take_screenshot()
            logger.info("Successfully closed Warning alert option on the DR dashboard")
        logger.info("Click on Critical alerts")
        critical_alert = acm_obj.find_an_element_by_xpath(
            "//*[@id='alert-toggle-critical']"
        ).get_attribute("aria-expanded")
        logger.info(f"State of Critical alert option is: {critical_alert}")
        if critical_alert == "false":
            logger.info("Expand Critical alert option on the DR dashboard")
            critical_alert = wait_for_element_to_be_clickable(acm_loc["critical-alert"])
            acm_obj.driver.execute_script("arguments[0].click();", critical_alert)
            acm_obj.take_screenshot()
            logger.info(
                "Successfully expanded Critical alert option on the DR dashboard"
            )
        time.sleep(60)
        logger.info(
            f"Wait for {scheduling_interval} minutes for VolumeSynchronizationDelay Critical alert to be fired"
        )
        alert_1 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelayalert1"],
            expected_text="VolumeSynchronizationDelay",
            timeout=scheduling_interval * 60,
        )
        if alert_1:
            logger.info(
                "Critical level first 'VolumeSynchronizationDelay' alert found on the DR dashboard"
            )
        else:
            logger.error(
                "First Critical level 'VolumeSynchronizationDelay' alert not found on the DR dashboard"
            )
            raise NoAlertPresentException

        alert_2 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelayalert2"],
            expected_text="VolumeSynchronizationDelay",
            timeout=180,
        )
        if alert_2:
            logger.info(
                "Critical level second 'VolumeSynchronizationDelay' alert found on the DR dashboard"
            )
        else:
            logger.error(
                "Second Critical level 'VolumeSynchronizationDelay' alert not found on the DR dashboard"
            )
            raise NoAlertPresentException

        modify_deployment_count(status="up")
        logger.info(
            f"Waiting for {2 * scheduling_interval} minutes to allow data sync to complete"
            f" so that VolumeSyncronizationDelay alert disappears"
        )
        sleep(2 * scheduling_interval * 60)
        verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)
        logger.info("Click on Critical alerts")
        critical_alert = acm_obj.find_an_element_by_xpath(
            "//*[@id='alert-toggle-critical']"
        ).get_attribute("aria-expanded")
        logger.info(f"State of Critical alert option is: {critical_alert}")
        if critical_alert == "false":
            logger.info("Expand Critical alert option on the DR dashboard")
            critical_alert = wait_for_element_to_be_clickable(acm_loc["critical-alert"])
            acm_obj.driver.execute_script("arguments[0].click();", critical_alert)
            acm_obj.take_screenshot()
            logger.info(
                "Successfully expanded Critical alert option on the DR dashboard"
            )
        alert_1 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelayalert1"],
            expected_text="VolumeSynchronizationDelay",
            timeout=15,
        )
        if alert_1:
            logger.error(
                "First Critical level 'VolumeSynchronizationDelay' alert is still being fired on the DR dashboard"
            )
            raise UnexpectedBehaviour
        else:
            logger.info(
                "First Critical level 'VolumeSynchronizationDelay' alert disappeared from the DR dashboard"
            )

        alert_2 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelayalert2"],
            expected_text="VolumeSynchronizationDelay",
            timeout=15,
        )
        if alert_2:
            logger.error(
                "Second Critical level 'VolumeSynchronizationDelay' alert is still being fired on the DR dashboard"
            )
            raise UnexpectedBehaviour
        else:
            logger.info(
                "Second Critical level 'VolumeSynchronizationDelay' alert disappeared from the DR dashboard"
            )
        logger.info(
            "VolumeSynchronizationDelay alert disappeared successfully on the DR dashboard"
        )
