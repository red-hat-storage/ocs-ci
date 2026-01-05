import logging
from time import sleep

import pytest

from ocs_ci.framework.testlib import tier1, skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import (
    verify_protected_applications_list_view,
    verify_protected_app_kebab_menu_actions,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.acm.acm import AcmAddClusters

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.21")
class TestProtectedApplicationListView:
    """
    Test class for Protected Application list view feature.
    Validates that both managed (AppSet) and discovered applications
    are displayed on the Protected Applications page in ACM UI.
    """

    @pytest.mark.polarion_id("OCS-7425")
    @pytest.mark.parametrize(
        argnames=["pvc_interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL,
                id="rbd",
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                id="cephfs",
            ),
        ],
    )
    def test_protected_application_list_view(
        self,
        pvc_interface,
        setup_acm_ui,
        dr_workload,
        discovered_apps_dr_workload,
    ):
        """
        Test to verify Protected Application list view displays both
        managed (AppSet) and discovered applications.
        Also verifies kebab menu actions for managed applications.
        """
        storage_type = "RBD" if pvc_interface == constants.CEPHBLOCKPOOL else "CephFS"
        logger.info(f"Running test with storage interface: {storage_type}")

        # Deploy AppSet based workload
        appset_workload = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=pvc_interface,
        )
        logger.info(
            f"AppSet workload deployed in namespace: {appset_workload[0].workload_namespace}"
        )
        logger.info(
            f"AppSet placement name: {appset_workload[0].appset_placement_name}"
        )

        # Deploy discovered apps workload
        discovered_workload = discovered_apps_dr_workload(
            kubeobject=1,
            recipe=0,
            pvc_interface=pvc_interface,
        )
        logger.info(
            f"Discovered Apps workload deployed in namespace: "
            f"{discovered_workload[0].workload_namespace}"
        )

        # Get DRPC objects for both workloads
        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{appset_workload[0].appset_placement_name}-drpc",
        )
        drpc_discovered = DRPC(namespace=constants.DR_OPS_NAMESPACE)
        drpc_objs = [drpc_appset, drpc_discovered]

        # Wait for initial sync
        scheduling_interval = dr_helpers.get_scheduling_interval(
            appset_workload[0].workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes for initial sync to complete...")
        sleep(wait_time * 60)

        # Verify DRPC progression status
        for drpc_obj in drpc_objs:
            progression_status = drpc_obj.get_progression_status()
            logger.info(
                f"DRPC {drpc_obj.resource_name} PROGRESSION status: {progression_status}"
            )
            assert progression_status == constants.STATUS_COMPLETED, (
                f"DRPC {drpc_obj.resource_name} PROGRESSION status is {progression_status}, "
                f"expected {constants.STATUS_COMPLETED}"
            )

        # Verify lastGroupSyncTime
        for drpc_obj in drpc_objs:
            dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        # Verify applications on Protected Applications page in ACM UI
        acm_obj = AcmAddClusters()
        verify_protected_applications_list_view(
            acm_obj=acm_obj,
            appset_workloads=appset_workload,
            discovered_workloads=discovered_workload,
            timeout=120,
        )

        # Verify kebab menu actions for managed (AppSet) application
        appset_namespace = appset_workload[0].workload_namespace
        appset_app_name = (
            appset_namespace[len("appset-"):]
            if appset_namespace.startswith("appset-")
            else appset_namespace
        )
        verify_protected_app_kebab_menu_actions(
            acm_obj=acm_obj,
            app_name=appset_app_name,
            expected_actions=[
                "Edit configuration",
                "Failover",
                "Relocate",
                "Manage disaster recovery",
            ],
            timeout=60,
        )
        logger.info(f"Verified kebab menu actions for app: {appset_app_name}")
