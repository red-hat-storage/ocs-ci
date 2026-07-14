import logging
import math

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import skipif_ibm_cloud_managed
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    ui,
    ignore_leftovers,
    black_squad,
    skipif_ocp_version,
    skipif_ocs_version,
    skipif_mcg_only,
    skipif_external_mode,
    polarion_id,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.osd_resize import basic_resize_osd
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_mgr_pods,
    get_ceph_tools_pod,
    delete_pods,
    get_prometheus_pods,
)
from ocs_ci.ocs.resources.storage_cluster import get_storage_size
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


POD_OBJ = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])


@tier2
@ui
@ignore_leftovers
@black_squad
@skipif_ocp_version("<4.17")
@skipif_ocs_version("<4.17")
@skipif_mcg_only
@skipif_ibm_cloud_managed
@skipif_external_mode
class TestConsumptionTrendUI(ManageTest):
    """
    Few test cases in this class requires manual calculations for 'average of storage consumption'
    and 'estimated days until full'. We derived values from UI, also calculated manually and compared them.

    Note:
    The values obtained from manual calculations and the UI may exhibit slight differences
     due to back filling data or background activities.
    To accommodate this, a tolerance of 10% difference between the manual and UI values is allowed.

    For example, if the manual calculation yields a value of 10 and the UI shows 11 as the average,
    the test will still pass.
    However, if the UI value is 12 or greater, the test will fail due to exceeding the 10% tolerance.

    """

    def get_active_mgr(self):
        logger.info("Get mgr pods objs")
        mgr_objs = get_mgr_pods()
        toolbox = get_ceph_tools_pod()
        active_mgr_pod_output = toolbox.exec_cmd_on_pod("ceph mgr stat")
        active_mgr_pod_suffix = active_mgr_pod_output.get("active_name")
        logger.info(f"The active MGR pod is {active_mgr_pod_suffix}")
        active_mgr_deployment_name = "rook-ceph-mgr-" + active_mgr_pod_suffix
        for obj in mgr_objs:
            if active_mgr_pod_suffix in obj.name:
                active_mgr_pod = obj.name
        return (active_mgr_deployment_name, active_mgr_pod)

    @polarion_id("OCS-6259")
    def test_consumption_trend_card_ui(self, setup_ui_class):
        """
        Verify the widget for “Consumption trend”
            1. Login to the ODF dashboard and check the widget for “Consumption trend” is displayed or not.
            2. Verify the text information on the widget

        """
        block_and_file_page = PageNavigator().nav_storage_cluster_default_page()
        block_and_file_page.validate_block_and_file_tab_active()

        collected_tpl_of_days_and_avg = (
            block_and_file_page.odf_storagesystems_consumption_trend()
        )
        avg_txt = "Average storage consumption"
        est_days_txt = "Estimated days until full"
        logger.info(
            f"Estimated days text information from the wizard is: {collected_tpl_of_days_and_avg[0]}"
        )
        logger.info(
            f"Average information from the wizard is: {collected_tpl_of_days_and_avg[1]}"
        )
        assert (
            est_days_txt in collected_tpl_of_days_and_avg[0]
        ), f"Text information for Estimated days is wrong in {collected_tpl_of_days_and_avg[0]}"
        assert (
            avg_txt in collected_tpl_of_days_and_avg[1]
        ), f"Text information for Average is wrong in {collected_tpl_of_days_and_avg[1]}"

    @polarion_id("OCS-6260")
    def test_estimated_days_until_full_ui(self, setup_ui_class):
        """
        Verify the accuracy of ‘Estimated days until full’  in the widget
            1. Get the value of storage utilised, total storage, age of the cluster
                and calculate Average of storage consumption and 'Estimated days until full'.
            2. Compare the above calculated value with the ‘Estimated days until full’ in the widget (UI

        """
        validation_ui_obj = ValidationUI()
        block_and_file_page = PageNavigator().nav_storage_cluster_default_page()
        block_and_file_page.validate_block_and_file_tab_active()

        est_days = block_and_file_page.get_est_days_from_ui()
        average = block_and_file_page.get_avg_consumption_from_ui()
        logger.info(f"From the UI, Estimated Days: {est_days} and Average: {average}")
        days_avg_tpl = validation_ui_obj.calculate_est_days_and_average_manually()
        first_validation = est_days == days_avg_tpl[0]
        # rel_tol 0.1 means upto 10% tolerance, which means that difference between  manual and UI est days is up to 10%
        second_validation = math.isclose(est_days, days_avg_tpl[0], rel_tol=0.1)
        if first_validation:
            logger.info(
                "Manually calculated and UI values are exactly matched for 'Estimated days until full'"
            )
        elif second_validation:
            logger.warning(
                "Manually calculated and UI values are matched with upto 10% difference for 'Estimated days until full'"
            )
        assert (
            first_validation or second_validation
        ), "'Estimated days until full' is wrongly displayed in UI"

    @polarion_id("OCS-6261")
    def test_average_of_storage_consumption_ui(self, setup_ui_class):
        """
        Verify the accuracy of ‘Average of storage consumption per day’  in the widget
            1. Get the value of Average, total storage, age of the cluster
                and calculate Average of storage consumption.
            2. Compare the above calculated value with the ‘Average’ in the widget (UI)

        """
        validation_ui_obj = ValidationUI()
        block_and_file_page = PageNavigator().nav_storage_cluster_default_page()
        block_and_file_page.validate_block_and_file_tab_active()

        average = block_and_file_page.get_avg_consumption_from_ui()
        logger.info(f"From the UI, Average: {average}")
        days_avg_tpl = validation_ui_obj.calculate_est_days_and_average_manually()
        first_validation = average == days_avg_tpl[1]
        # rel_tol 0.1 means upto 10% tolerance, which means that difference between  manual and UI est days is up to 10%
        second_validation = math.isclose(average, days_avg_tpl[1], rel_tol=0.1)
        if first_validation:
            logger.info(
                "Manually calculated and UI values are exactly matched for 'Average'"
            )
        elif second_validation:
            logger.warning(
                "Manually calculated and UI values are matched with upto 10% difference for 'Average'"
            )
        assert (
            first_validation or second_validation
        ), "'Average' is wrongly displayed in UI"

    @polarion_id("OCS-6262")
    def test_consumption_trend_with_prometheus_failures(self, setup_ui_class):
        """
        Fail prometheus and verify the Consumption trend in the ODF dashboard to make sure
        ‘Estimated days until full’ and 'Average' reflects accurate value.

            1. Fail the prometheus pods by deleting them. New pods will be created automatically.
            2. When the new prometheus pods  up, Consumption trend should be displayed in the dashboard.
            3. Check the values for  ‘Estimated days until full’ and ‘Average of storage consumption per day’
            4. Should show close to the values before deleting the prometheus pod

        """
        block_and_file_page = PageNavigator().nav_storage_cluster_default_page()
        block_and_file_page.validate_block_and_file_tab_active()

        logger.info("Get the value of 'Estimated days until full' from UI")
        est_days_before = block_and_file_page.get_est_days_from_ui()
        logger.info(
            f"The value of 'Estimated days until full' from UI is {est_days_before} before failing prometheus"
        )
        average_before = block_and_file_page.get_avg_consumption_from_ui()
        logger.info(
            f"'Average of storage consumption per day' from UI is {average_before} before failing prometheus"
        )
        logger.info("Bring down the prometheus")
        list_of_prometheus_pod_obj = get_prometheus_pods()
        delete_pods(list_of_prometheus_pod_obj)
        est_days_after = None
        for est_days_after in TimeoutSampler(
            timeout=300, sleep=30, func=block_and_file_page.get_est_days_from_ui
        ):
            if est_days_after > 0:
                break
            else:
                logger.info("dashboard is not ready yet")

        logger.info(
            f"From the UI, Estimated Days: {est_days_after} after prometheus recovered from failure"
        )
        average_after = block_and_file_page.get_avg_consumption_from_ui()
        logger.info(
            f"'Average of storage consumption' from UI is {average_after} after prometheus recovered from failure"
        )
        # rel_tol 0.1 means upto 10% tolerance which means that difference between  manual and UI est days is up to 10%
        assert math.isclose(
            est_days_before, est_days_after, rel_tol=0.1
        ), "Estimated days until full did not match before and after prometheus fail"
        assert math.isclose(
            average_before, average_after, rel_tol=0.1
        ), "'Average of storage consumption per day' did not match before and after prometheus fail"

    @polarion_id("OCS-6263")
    def test_consumption_trend_with_mgr_failover(self, setup_ui_class):
        """
        Verify storage consumption trend with Mgr failover
            1. Failover active mgr pod, the other mgr will become active now.
            2. Test storage consumption trend from UI is accurate after mgr failover.

        """
        validation_ui_obj = ValidationUI()
        block_and_file_page = PageNavigator().nav_storage_cluster_default_page()
        block_and_file_page.validate_block_and_file_tab_active()

        (
            active_mgr_deployment_name_before_failover,
            active_mgr_pod_before_failover,
        ) = self.get_active_mgr()

        logger.info(f"Scale down {active_mgr_deployment_name_before_failover} to 0")
        helpers.modify_deployment_replica_count(
            deployment_name=active_mgr_deployment_name_before_failover, replica_count=0
        )
        POD_OBJ.wait_for_delete(resource_name=active_mgr_pod_before_failover)

        def check_failover():
            if active_mgr_deployment_name_before_failover != self.get_active_mgr()[0]:
                logger.info("Mgr Failover succeed")

        TimeoutSampler(timeout=120, sleep=15, func=check_failover)
        logger.info(f"Scale down {active_mgr_deployment_name_before_failover} to 1")
        helpers.modify_deployment_replica_count(
            deployment_name=active_mgr_deployment_name_before_failover, replica_count=1
        )
        assert (
            len(get_mgr_pods()) == 2
        ), "one of the mgr pod is still down after scale down and up"
        logger.info("Mgr failovered successfully")

        logger.info("Now the testing will begin for consumption trend UI")
        est_days = block_and_file_page.get_est_days_from_ui()
        average = block_and_file_page.get_avg_consumption_from_ui()
        logger.info(f"From the UI, Estimated Days: {est_days} and Average: {average}")
        days_avg_tpl = validation_ui_obj.calculate_est_days_and_average_manually()
        first_validation = est_days == days_avg_tpl[0]
        # rel_tol 0.1 means upto 10% tolerance, which means that difference between  manual and UI est days is up to 10%
        second_validation = math.isclose(est_days, days_avg_tpl[0], rel_tol=0.1)
        if first_validation:
            logger.info(
                "Manually calculated and UI values are exactly matched for 'Estimated days until full'"
            )
        elif second_validation:
            logger.warning(
                "Manually calculated and UI values are matched with upto 10% difference for 'Estimated days until full'"
            )
        assert (
            first_validation or second_validation
        ), "'Estimated days until full' is wrongly displayed in UI"

    @polarion_id("OCS-6264")
    def test_consumption_trend_after_osd_resize(self, setup_ui_class):
        """
        Verify consumption trend after OSD resize
        1. Get the value of 'Estimated days until full' value from the UI.
        2. Perform OSD resize.
        3. Verify the new size is reflecting in the consumption trend dashboard or not.
        4. 'Estimated days until full' value in the UI, should increase after OSD resize.

        """
        block_and_file_page = PageNavigator().nav_storage_cluster_default_page()
        block_and_file_page.validate_block_and_file_tab_active()

        logger.info("Get the value of 'Estimated days until full' from UI")
        est_days_before = block_and_file_page.get_est_days_from_ui()
        logger.info("Performing OSD resize")
        basic_resize_osd(get_storage_size())
        logger.info("After OSD resize, checking consumption trend UI")
        est_days_after = None
        for est_days_after in TimeoutSampler(
            timeout=300, sleep=30, func=block_and_file_page.get_est_days_from_ui
        ):
            logger.info(
                "Get the value of 'Estimated days until full' from UI after OSD resize"
            )
            if est_days_after > est_days_before:
                break
            else:
                logger.warning("dashboard is not ready yet")
        else:
            raise AssertionError(
                f"'Estimated days until full' {est_days_after} did not increase after OSD resize."
            )
