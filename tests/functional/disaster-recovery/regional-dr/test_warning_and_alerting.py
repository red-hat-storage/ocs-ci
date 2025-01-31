import logging
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
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.utils import enable_mco_console_plugin, get_primary_cluster_config
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


@pytest.fixture
def scale_up_deployment(request):
    def teardown(dr_workload):
        # rdr_workload_rbd = dr_workload(
        #     num_of_subscription=1,
        #     num_of_appset=0,
        #     pvc_interface=constants.CEPHBLOCKPOOL,
        # )
        # primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
        #     rdr_workload_rbd[0].workload_namespace
        # )
        # config.switch_to_cluster_by_name(primary_cluster_name)
        # # Validate replica count is set to 1
        # config_obj = ocp.OCP(
        #     kind=constants.DEPLOYMENT,
        #     namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
        # )
        # replica_count = config_obj.get().get("spec").get("replicas")
        # if replica_count != 2:
        #     modify_registry_pod_count(count=2)
        primary_config = get_primary_cluster_config()
        primary_index = primary_config.MULTICLUSTER.get("multicluster_index")
        config.switch_ctx(primary_index)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=1
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT, replica_count=1
        )

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
                False,
                constants.ACTION_FAILOVER,
                marks=pytest.mark.polarion_id("xxx"),
            ),
            pytest.param(
                True,
                constants.ACTION_RELOCATE,
                marks=pytest.mark.polarion_id("yyy"),
            ),
        ],
    )
    # TODO: Update polarion IDs
    def test_rdr_inconsistent_data_warning_alert(
        self, action, setup_acm_ui, dr_workload
    ):
        """
        Test to verify that "Inconsistent data on target cluster" warning alert is seen on the Failover/Relocate modal
        when the lastGroupSyncTime is lagging behind 2x the sync interval or more for a particular DR protected workload

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
        config.switch_to_cluster_by_name(primary_cluster_name)
        drpc_subscription = DRPC(namespace=rdr_workload_total[0].workload_namespace)
        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{rdr_workload_total[1].appset_placement_name}-drpc",
        )
        drpc_objs = [drpc_subscription, drpc_appset]
        before_failover_last_group_sync_time = []
        for obj in drpc_objs:
            before_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime")

        config.switch_to_cluster_by_name(primary_cluster_name)
        # Scale down rbd-mirror and mds deployments to zero
        logger.info(
            f"Scale down rbd-mirror and mds deployments to zero on primary cluster: {primary_cluster_name}"
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=0
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT, replica_count=0
        )
        workload_names = []
        workload_number = 1
        for workload in rdr_workload_total:
            logger.info(f"Workload name is {workload.workload_name}")
            workload_name = f"{workload.workload_name}-{workload_number}"
            workload_names.append(workload_name)
        logger.info(f"Workload names are {workload_names}")

        logger.info(
            f"Waiting for {wait_time * 60} seconds to allow warning alert to appear"
        )
        sleep(wait_time * 60)

        config.switch_acm_ctx()
        # Navigate to failover modal via ACM UI
        logger.info("Navigate to failover modal via ACM UI")
        for workload in rdr_workload_total:
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
                "Warning alert 'Inconsistent data on target cluster' found on the Failover modal"
            )
            logger.info("Click on 'Cancel' on the action modal")
            acm_obj.do_click(
                acm_loc["cancel-action-modal"], enable_screenshot=True, avoid_stale=True
            )
            logger.info("Action modal closed successfully")
        else:
            logger.error(
                "Warning alert 'Inconsistent data on target cluster' not found on the Failover modal"
            )
            raise NoAlertPresentException

        config.switch_to_cluster_by_name(primary_cluster_name)
        # Scale up rbd-mirror and mds deployments to one
        logger.info(
            f"Scale up rbd-mirror and mds deployments to one on primary cluster: {primary_cluster_name}"
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT, replica_count=1
        )
        helpers.modify_deployment_replica_count(
            deployment_name=constants.MDS_DAEMON_DEPLOYMENT, replica_count=1
        )
        logger.info(
            f"Waiting for {scheduling_interval * 60} seconds to allow warning alert to disappear"
        )
        sleep(scheduling_interval * 60)

        for obj, initial_last_group_sync_time in zip(
            drpc_objs, before_failover_last_group_sync_time
        ):
            dr_helpers.verify_last_group_sync_time(
                obj, scheduling_interval, initial_last_group_sync_time
            )
        logger.info("lastGroupSyncTime updated after cluster is online")

        config.switch_acm_ctx()
        # Navigate to failover modal via ACM UI
        logger.info("Navigate to failover modal via ACM UI")
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
                acm_loc["cancel-action-modal"], enable_screenshot=True, avoid_stale=True
            )
            logger.info("Action modal closed successfully")
