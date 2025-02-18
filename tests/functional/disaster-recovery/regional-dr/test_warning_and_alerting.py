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
)
from ocs_ci.helpers import dr_helpers, helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    failover_relocate_ui,
    verify_drpolicy_ui,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.utils import (
    enable_mco_console_plugin,
    get_primary_cluster_config,
    get_non_acm_cluster_config,
)
from ocs_ci.utility.utils import get_ocp_version, ceph_health_check
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


@pytest.fixture
def scale_up_deployment(request):
    def teardown():
        primary_config = get_primary_cluster_config()
        primary_index = primary_config.MULTICLUSTER.get("multicluster_index")
        secondary_index = [
            s.MULTICLUSTER["multicluster_index"]
            for s in get_non_acm_cluster_config()
            if s.MULTICLUSTER["multicluster_index"] != primary_index
        ][0]

        logger.info(
            "Scale up rbd-mirror deployment on the secondary cluster and mds deployments on the primary cluster"
        )
        config.switch_ctx(secondary_index)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=1
        )
        ceph_health_check(tries=10, delay=30)
        config.switch_ctx(primary_index)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_ONE, replica_count=1
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_TWO, replica_count=1
        )
        ceph_health_check(tries=10, delay=30)

    request.addfinalizer(teardown)


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.18")
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

        ocp_version = get_ocp_version()
        acm_loc = locators[ocp_version]["acm_page"]

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
        logger.info(
            "Scale down rbd-mirror deployment on the secondary cluster and mds deployments on the primary cluster"
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=0
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_ONE, replica_count=0
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_TWO, replica_count=0
        )
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

        logger.info(
            "Scale up rbd-mirror deployment on the secondary cluster and mds deployments on the primary cluster"
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=1
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_ONE, replica_count=1
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_TWO, replica_count=1
        )
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

    def test_rdr_volumesyncronizationdelayalert(
        self, setup_acm_ui, dr_workload, scale_up_deployment
    ):
        """
        Test to verify that "VolumeSynchronizationDelay" critical level alert is fired on the DR dashboard
        when the lastGroupSyncTime is lagging behind 3x the sync interval or more for a particular DR protected workload

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

        ocp_version = get_ocp_version()
        acm_loc = locators[ocp_version]["acm_page"]

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
        logger.info(
            "Scale down rbd-mirror deployment on the secondary cluster and mds deployments on the primary cluster"
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=0
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_ONE, replica_count=0
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_TWO, replica_count=0
        )
        config.switch_acm_ctx()
        logger.info(f"Waiting for {wait_time * 60} seconds to allow alert to appear")
        sleep(wait_time * 60)
        verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)
        logger.info("Click on Critical alerts")
        critical_alert = acm_loc["critical-alert"].get_attribute("aria-expanded")
        logger.info(f"Critical alert state: {critical_alert}")
        if not critical_alert:
            self.do_click(
                locator=acm_loc["critical-alert"],
                avoid_stale=True,
                enable_screenshot=True,
            )
        alert_1 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelayalert1"],
            expected_text="VolumeSynchronizationDelay",
            timeout=120,
        )
        if alert_1:
            logger.info(
                "Critical level 'VolumeSynchronizationDelay' alert found on the DR dashboard"
            )
        else:
            logger.error(
                "Critical level 'VolumeSynchronizationDelay' alert not found on the DR dashboard"
            )
            raise NoAlertPresentException

        alert_2 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelayalert2"],
            expected_text="VolumeSynchronizationDelay",
            timeout=120,
        )
        if alert_2:
            logger.info(
                "Critical level 'VolumeSynchronizationDelay' alert found on the DR dashboard"
            )
        else:
            logger.error(
                "Critical level 'VolumeSynchronizationDelay' alert not found on the DR dashboard"
            )
            raise NoAlertPresentException

        logger.info(
            "Scale up rbd-mirror deployment on the secondary cluster and mds deployments on the primary cluster"
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=1
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_ONE, replica_count=1
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT_TWO, replica_count=1
        )
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
        # Allow additional time for alert to disappear
        logger.info("Allowing additional time for alert to disappear")
        time.sleep(120)
        verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)
        logger.info("Click on Critical alerts")
        critical_alert = acm_loc["critical-alert"].get_attribute("aria-expanded")
        logger.info(f"Critical alert state: {critical_alert}")
        if not critical_alert:
            self.do_click(
                locator=acm_loc["critical-alert"],
                avoid_stale=True,
                enable_screenshot=True,
            )
        alert_1 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelayalert1"],
            expected_text="VolumeSynchronizationDelay",
            timeout=120,
        )
        if alert_1:
            logger.error(
                "Critical level 'VolumeSynchronizationDelay' alert is still being fired on the DR dashboard"
            )
            raise UnexpectedBehaviour
        else:
            logger.info(
                "Critical level 'VolumeSynchronizationDelay' alert disappeared from the DR dashboard"
            )

        alert_2 = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["volsyncdelayalert2"],
            expected_text="VolumeSynchronizationDelay",
            timeout=120,
        )
        if alert_2:
            logger.error(
                "Critical level 'VolumeSynchronizationDelay' alert is still being fired on the DR dashboard"
            )
            raise UnexpectedBehaviour
        else:
            logger.info(
                "Critical level 'VolumeSynchronizationDelay' alert disappeared from the DR dashboard"
            )
