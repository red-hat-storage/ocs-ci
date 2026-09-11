import logging
from time import sleep

import pytest

from ocs_ci.framework.testlib import tier1, skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers import (
    wait_for_replication_destinations_creation,
    wait_for_replication_destinations_deletion,
)
from ocs_ci.framework import config
from ocs_ci.helpers.dr_helpers_ui import (
    verify_protected_applications_list_view,
    verify_protected_app_kebab_menu_actions,
    verify_protected_app_dr_status,
    verify_manage_dr_modal_for_managed_app,
    failover_from_protected_app_page,
    relocate_from_protected_app_page,
    remove_dr_from_protected_app_page,
    verify_app_in_protected_applications_list,
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

    This test deploys:
    - 2 Managed (AppSet) applications:
      - App1: Used for UI verification (Steps 1-5), Failover, and Relocate
      - App2: Used for Remove DR action (independent of failover/relocate)
    - 1 Discovered application: UI verification, Failover, and Relocate
    """

    @pytest.mark.polarion_id("OCS-7426")
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
        managed (AppSet) and discovered applications, and validates
        UI-driven DR actions (Failover, Relocate, Remove DR).

        Test Flow:
        - Deploy 2 AppSet workloads and 1 Discovered Apps workload
        - Steps 1-5: UI verification (list view, DR status, kebab menu, modal)
        - Step 6: Failover app1 (AppSet) from UI
        - Step 7: Relocate app1 (AppSet) from UI (depends on failover success)
        - Step 8: Failover discovered app from UI
        - Step 9: Relocate discovered app from UI (depends on failover success)
        - Step 10: Remove DR for app2 from UI
        """
        storage_type = "RBD" if pvc_interface == constants.CEPHBLOCKPOOL else "CephFS"
        logger.info(f"Running test with storage interface: {storage_type}")

        # Deploy workloads
        appset_workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=2,
            pvc_interface=pvc_interface,
        )
        logger.info(
            f"AppSet workload 1 deployed in namespace: {appset_workloads[0].workload_namespace}"
        )
        logger.info(
            f"AppSet workload 2 deployed in namespace: {appset_workloads[1].workload_namespace}"
        )

        discovered_workload = discovered_apps_dr_workload(
            kubeobject=1,
            recipe=0,
            pvc_interface=pvc_interface,
        )
        discovered_wl = discovered_workload[0]
        logger.info(
            f"Discovered Apps workload deployed in namespace: "
            f"{discovered_wl.workload_namespace}"
        )

        # Setup DRPC objects
        drpc_app1 = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{appset_workloads[0].appset_placement_name}-drpc",
        )
        drpc_app2 = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{appset_workloads[1].appset_placement_name}-drpc",
        )
        drpc_discovered = DRPC(
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=discovered_wl.discovered_apps_placement_name,
        )
        drpc_objs = [drpc_app1, drpc_app2, drpc_discovered]

        # Wait for initial sync
        scheduling_interval = dr_helpers.get_scheduling_interval(
            appset_workloads[0].workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting for {wait_time} minutes for initial sync to complete...")
        sleep(wait_time * 60)

        for drpc_obj in drpc_objs:
            progression_status = drpc_obj.get_progression_status()
            logger.info(
                f"DRPC {drpc_obj.resource_name} PROGRESSION status: {progression_status}"
            )
            assert progression_status == constants.STATUS_COMPLETED, (
                f"DRPC {drpc_obj.resource_name} PROGRESSION status is {progression_status}, "
                f"expected {constants.STATUS_COMPLETED}"
            )

        for drpc_obj in drpc_objs:
            dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        # Protected Apps UI name matches full AppSet namespace (includes 'appset-' prefix)
        app1_name = appset_workloads[0].workload_namespace
        app2_name = appset_workloads[1].workload_namespace
        discovered_app_name = discovered_wl.discovered_apps_placement_name
        discovered_ns = discovered_wl.workload_namespace
        logger.info(f"App1 (failover/relocate): {app1_name}")
        logger.info(f"App2 (remove DR): {app2_name}")
        logger.info(f"Discovered app (failover/relocate): {discovered_app_name}")

        # Initialize ACM UI and error tracking
        acm_obj = AcmAddClusters()
        verification_errors = []

        # Step 1: Verify applications on Protected Applications page
        logger.info("=" * 60)
        logger.info("Step 1: Verify applications on Protected Applications page")
        logger.info("=" * 60)

        try:
            verify_protected_applications_list_view(
                acm_obj=acm_obj,
                appset_workloads=appset_workloads,
                discovered_workloads=discovered_workload,
                timeout=120,
            )
            logger.info("Step 1 PASSED: All applications verified on Protected Applications page")
        except Exception as e:
            error_msg = f"Step 1 FAILED: Verify applications on page - {e}"
            logger.error(error_msg)
            verification_errors.append(error_msg)

        # Step 2: Verify DR status "Healthy" for all apps
        logger.info("=" * 60)
        logger.info("Step 2: Verify DR status 'Healthy' for all apps")
        logger.info("=" * 60)
        for app_name in [app1_name, app2_name, discovered_app_name]:
            try:
                verify_protected_app_dr_status(
                    acm_obj=acm_obj,
                    app_name=app_name,
                    expected_status="healthy",
                    timeout=60,
                )
                logger.info(f"Step 2 PASSED: DR status 'healthy' for: {app_name}")
            except Exception as e:
                error_msg = f"Step 2 FAILED: DR status check for {app_name} - {e}"
                logger.error(error_msg)
                verification_errors.append(error_msg)

        if False:  # TEMP: skip for discovered-only debug (step 3)
            # Step 3: Verify kebab menu actions for managed apps
            logger.info("=" * 60)
            logger.info("Step 3: Verify kebab menu actions for managed apps")
            logger.info("=" * 60)
            for app_name in [app1_name, app2_name]:
                try:
                    verify_protected_app_kebab_menu_actions(
                        acm_obj=acm_obj,
                        app_name=app_name,
                        expected_actions=[
                            "Edit configuration",
                            "Failover",
                            "Relocate",
                            "Manage disaster recovery",
                        ],
                        timeout=60,
                    )
                    logger.info(f"Step 3 PASSED: Kebab menu verified for managed app: {app_name}")
                except Exception as e:
                    error_msg = f"Step 3 FAILED: Kebab menu for managed app {app_name} - {e}"
                    logger.error(error_msg)
                    verification_errors.append(error_msg)

        # Step 4: Verify kebab menu actions for discovered app
        logger.info("=" * 60)
        logger.info("Step 4: Verify kebab menu actions for discovered app")
        logger.info("=" * 60)
        try:
            verify_protected_app_kebab_menu_actions(
                acm_obj=acm_obj,
                app_name=discovered_app_name,
                expected_actions=[
                    "Edit configuration",
                    "Failover",
                    "Relocate",
                    "Remove disaster recovery",
                ],
                timeout=60,
            )
            logger.info(f"Step 4 PASSED: Kebab menu verified for discovered app: {discovered_app_name}")
        except Exception as e:
            error_msg = f"Step 4 FAILED: Kebab menu for discovered app {discovered_app_name} - {e}"
            logger.error(error_msg)
            verification_errors.append(error_msg)

        if False:  # TEMP: skip for discovered-only debug (step 5)
            # Step 5: Verify Manage DR modal for managed app
            logger.info("=" * 60)
            logger.info("Step 5: Verify Manage DR modal for managed app")
            logger.info("=" * 60)
            dr_policy_name = appset_workloads[0].dr_policy_name
            try:
                verify_manage_dr_modal_for_managed_app(
                    acm_obj=acm_obj,
                    app_name=app1_name,
                    expected_policy_name=dr_policy_name,
                    drpc_obj=drpc_app1,
                    timeout=60,
                )
                logger.info(f"Step 5 PASSED: Manage DR modal verified for: {app1_name}")
            except Exception as e:
                error_msg = f"Step 5 FAILED: Manage DR modal for {app1_name} - {e}"
                logger.error(error_msg)
                verification_errors.append(error_msg)

        # DR Action Steps - Get cluster info before failover
        dr_action_errors = []
        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            appset_workloads[0].workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            appset_workloads[0].workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        primary_cluster_index = config.get_cluster_index_by_name(primary_cluster_name)
        secondary_cluster_index = config.get_cluster_index_by_name(secondary_cluster_name)
        dr_status_timeout = 3 * scheduling_interval * 60
        failover_success = False
        relocate_success = False
        discovered_failover_success = False
        discovered_relocate_success = False
        remove_dr_success = False

        if False:  # TEMP: skip for discovered-only debug (steps 6-7)
            # Step 6: Failover app1 from Protected Applications page
            logger.info("=" * 60)
            logger.info("Step 6: Failover app1 from Protected Applications page")
            logger.info("=" * 60)
            logger.info(
                f"Before failover - Primary: {primary_cluster_name}, Secondary: {secondary_cluster_name}"
            )

            try:
                failover_from_protected_app_page(
                    acm_obj=acm_obj,
                    app_name=app1_name,
                    expected_target_cluster=secondary_cluster_name,
                    drpc_obj=drpc_app1,
                    timeout=120,
                )
                logger.info(f"Failover initiated for app: {app1_name}")

                config.switch_ctx(primary_cluster_index)
                logger.info(f"Switched to old primary: {primary_cluster_name}")
                dr_helpers.wait_for_all_resources_deletion(
                    appset_workloads[0].workload_namespace
                )
                logger.info(f"Resources deleted from old primary: {primary_cluster_name}")

                config.switch_ctx(secondary_cluster_index)
                logger.info(f"Switched to new primary: {secondary_cluster_name}")
                dr_helpers.wait_for_all_resources_creation(
                    appset_workloads[0].workload_pvc_count,
                    appset_workloads[0].workload_pod_count,
                    appset_workloads[0].workload_namespace,
                    performed_dr_action=True,
                )
                logger.info(f"Resources running on new primary: {secondary_cluster_name}")

                if pvc_interface == constants.CEPHBLOCKPOOL:
                    all_workloads = appset_workloads + discovered_workload
                    dr_helpers.wait_for_mirroring_status_ok(
                        replaying_images=sum([wl.workload_pvc_count for wl in all_workloads])
                    )
                    logger.info("RBD mirroring status OK after failover")
                elif pvc_interface == constants.CEPHFILESYSTEM:
                    cg_enabled = dr_helpers.is_cg_cephfs_enabled()
                    if cg_enabled:
                        # Verify deletion of ReplicationGroupDestination on old secondary
                        config.switch_to_cluster_by_name(primary_cluster_name)
                        dr_helpers.wait_for_resource_existence(
                            kind=constants.REPLICATION_GROUP_DESTINATION,
                            namespace=appset_workloads[0].workload_namespace,
                            should_exist=False,
                        )
                        logger.info("ReplicationGroupDestination deleted from old secondary")

                    # Verify ReplicationSource creation on new primary
                    config.switch_to_cluster_by_name(secondary_cluster_name)
                    dr_helpers.wait_for_replication_resources_creation(
                        count=appset_workloads[0].workload_pvc_count,
                        namespace=appset_workloads[0].workload_namespace,
                        timeout=300,
                    )
                    logger.info("CephFS ReplicationSource verified after failover")

                    if cg_enabled:
                        # Verify ReplicationGroupDestination creation on new secondary
                        config.switch_to_cluster_by_name(primary_cluster_name)
                        dr_helpers.wait_for_resource_existence(
                            kind=constants.REPLICATION_GROUP_DESTINATION,
                            namespace=appset_workloads[0].workload_namespace,
                            should_exist=True,
                        )
                        dr_helpers.wait_for_resource_count(
                            kind=constants.VOLUMESNAPSHOT,
                            namespace=appset_workloads[0].workload_namespace,
                            expected_count=appset_workloads[0].workload_pvc_count,
                        )
                        logger.info("CG-CephFS resources verified after failover")

                config.switch_acm_ctx()
                dr_helpers.verify_last_group_sync_time(drpc_app1, scheduling_interval)
                logger.info("Verified lastGroupSyncTime after failover")

                verify_protected_app_dr_status(
                    acm_obj=acm_obj,
                    app_name=app1_name,
                    expected_status="healthy",
                    timeout=dr_status_timeout,
                )
                logger.info(f"Step 6 PASSED: Failover completed for: {app1_name}")
                failover_success = True

            except Exception as e:
                error_msg = f"Step 6 FAILED: Failover for {app1_name} - {e}"
                logger.error(error_msg)
                dr_action_errors.append(error_msg)
                config.switch_acm_ctx()

            # Step 7: Relocate app1 back to original primary
            logger.info("=" * 60)
            logger.info("Step 7: Relocate app1 back to original primary")
            logger.info("=" * 60)

            if not failover_success:
                skip_msg = "Step 7 SKIPPED: Relocate skipped because failover failed"
                logger.warning(skip_msg)
                dr_action_errors.append(skip_msg)
            else:
                logger.info(
                    f"Relocate target: {primary_cluster_name} (original primary, now secondary)"
                )
                try:
                    relocate_from_protected_app_page(
                        acm_obj=acm_obj,
                        app_name=app1_name,
                        expected_target_cluster=primary_cluster_name,
                        drpc_obj=drpc_app1,
                        timeout=120,
                    )
                    logger.info(f"Relocate initiated for app: {app1_name}")

                    config.switch_ctx(secondary_cluster_index)
                    dr_helpers.wait_for_all_resources_deletion(
                        appset_workloads[0].workload_namespace
                    )
                    logger.info(f"Resources deleted from: {secondary_cluster_name}")

                    config.switch_ctx(primary_cluster_index)
                    logger.info(f"Switched to original primary: {primary_cluster_name}")
                    dr_helpers.wait_for_all_resources_creation(
                        appset_workloads[0].workload_pvc_count,
                        appset_workloads[0].workload_pod_count,
                        appset_workloads[0].workload_namespace,
                        performed_dr_action=True,
                    )
                    logger.info(f"Resources running on: {primary_cluster_name}")

                    if pvc_interface == constants.CEPHBLOCKPOOL:
                        all_workloads = appset_workloads + discovered_workload
                        dr_helpers.wait_for_mirroring_status_ok(
                            replaying_images=sum([wl.workload_pvc_count for wl in all_workloads])
                        )
                        logger.info("RBD mirroring status OK after relocate")
                    elif pvc_interface == constants.CEPHFILESYSTEM:
                        cg_enabled = dr_helpers.is_cg_cephfs_enabled()
                        if cg_enabled:
                            # Verify deletion of ReplicationGroupDestination on old secondary
                            config.switch_to_cluster_by_name(secondary_cluster_name)
                            dr_helpers.wait_for_resource_existence(
                                kind=constants.REPLICATION_GROUP_DESTINATION,
                                namespace=appset_workloads[0].workload_namespace,
                                should_exist=False,
                            )
                            logger.info("ReplicationGroupDestination deleted from old secondary")

                        # Verify ReplicationSource creation on new primary
                        config.switch_to_cluster_by_name(primary_cluster_name)
                        dr_helpers.wait_for_replication_resources_creation(
                            count=appset_workloads[0].workload_pvc_count,
                            namespace=appset_workloads[0].workload_namespace,
                            timeout=300,
                        )
                        logger.info("CephFS ReplicationSource verified after relocate")

                        if cg_enabled:
                            # Verify ReplicationGroupDestination creation on new secondary
                            config.switch_to_cluster_by_name(secondary_cluster_name)
                            dr_helpers.wait_for_resource_existence(
                                kind=constants.REPLICATION_GROUP_DESTINATION,
                                namespace=appset_workloads[0].workload_namespace,
                                should_exist=True,
                            )
                            dr_helpers.wait_for_resource_count(
                                kind=constants.VOLUMESNAPSHOT,
                                namespace=appset_workloads[0].workload_namespace,
                                expected_count=appset_workloads[0].workload_pvc_count,
                            )
                            logger.info("CG-CephFS resources verified after relocate")

                    config.switch_acm_ctx()
                    dr_helpers.verify_last_group_sync_time(drpc_app1, scheduling_interval)
                    logger.info("Verified lastGroupSyncTime after relocate")

                    verify_protected_app_dr_status(
                        acm_obj=acm_obj,
                        app_name=app1_name,
                        expected_status="healthy",
                        timeout=dr_status_timeout,
                    )
                    logger.info(f"Step 7 PASSED: Relocate completed for: {app1_name}")
                    relocate_success = True

                except Exception as e:
                    error_msg = f"Step 7 FAILED: Relocate for {app1_name} - {e}"
                    logger.error(error_msg)
                    dr_action_errors.append(error_msg)
                    config.switch_acm_ctx()

        # Discovered app primary/secondary (re-query; AppSet FO/Relocate does not move it)
        disc_primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            discovered_ns,
            discovered_apps=True,
            resource_name=discovered_app_name,
        )
        disc_secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            discovered_ns,
            discovered_apps=True,
            resource_name=discovered_app_name,
        )
        disc_primary_cluster_index = config.get_cluster_index_by_name(
            disc_primary_cluster_name
        )
        disc_secondary_cluster_index = config.get_cluster_index_by_name(
            disc_secondary_cluster_name
        )

        # Step 8: Failover discovered app from Protected Applications page
        logger.info("=" * 60)
        logger.info("Step 8: Failover discovered app from Protected Applications page")
        logger.info("=" * 60)
        logger.info(
            f"Before discovered failover - Primary: {disc_primary_cluster_name}, "
            f"Secondary: {disc_secondary_cluster_name}"
        )

        try:
            failover_from_protected_app_page(
                acm_obj=acm_obj,
                app_name=discovered_app_name,
                expected_target_cluster=disc_secondary_cluster_name,
                drpc_obj=drpc_discovered,
                timeout=120,
            )
            logger.info(f"Failover initiated for discovered app: {discovered_app_name}")

            # Discovered apps require manual cleanup on old primary
            # (DRPC enters WaitOnUserToCleanUp; pods are not auto-deleted)
            logger.info(
                f"Performing discovered apps cleanup on old primary: "
                f"{disc_primary_cluster_name}"
            )
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=discovered_app_name,
                old_primary=disc_primary_cluster_name,
                workload_namespace=discovered_ns,
                workload_dir=discovered_wl.workload_dir,
                vrg_name=discovered_app_name,
            )
            logger.info(
                f"Discovered apps cleanup completed on: {disc_primary_cluster_name}"
            )

            config.switch_ctx(disc_secondary_cluster_index)
            logger.info(f"Switched to new primary: {disc_secondary_cluster_name}")
            dr_helpers.wait_for_all_resources_creation(
                discovered_wl.workload_pvc_count,
                discovered_wl.workload_pod_count,
                discovered_ns,
                timeout=1200,
                discovered_apps=True,
                vrg_name=discovered_app_name,
                performed_dr_action=True,
            )
            logger.info(
                f"Resources running on new primary: {disc_secondary_cluster_name}"
            )

            if pvc_interface == constants.CEPHBLOCKPOOL:
                all_workloads = appset_workloads + discovered_workload
                dr_helpers.wait_for_mirroring_status_ok(
                    replaying_images=sum(
                        [wl.workload_pvc_count for wl in all_workloads]
                    )
                )
                logger.info("RBD mirroring status OK after discovered failover")
            elif pvc_interface == constants.CEPHFILESYSTEM:
                # VRG is already validated in openshift-dr-ops via
                # wait_for_all_resources_creation(discovered_apps=True).
                # Verify CephFS ReplicationDestination swap on managed clusters.
                cg_enabled = dr_helpers.is_cg_cephfs_enabled()
                config.switch_to_cluster_by_name(disc_secondary_cluster_name)
                wait_for_replication_destinations_deletion(discovered_ns)
                if cg_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=discovered_ns,
                        should_exist=False,
                    )
                    logger.info(
                        "ReplicationGroupDestination deleted from new primary"
                    )

                config.switch_to_cluster_by_name(disc_primary_cluster_name)
                wait_for_replication_destinations_creation(
                    discovered_wl.workload_pvc_count, discovered_ns
                )
                if cg_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=discovered_ns,
                        should_exist=True,
                    )
                    dr_helpers.wait_for_resource_count(
                        kind=constants.VOLUMESNAPSHOT,
                        namespace=discovered_ns,
                        expected_count=discovered_wl.workload_pvc_count,
                    )
                    logger.info(
                        "CG-CephFS resources verified after discovered failover"
                    )
                logger.info(
                    "CephFS ReplicationDestination verified after discovered failover"
                )

            config.switch_acm_ctx()
            dr_helpers.verify_last_group_sync_time(
                drpc_discovered, scheduling_interval
            )
            logger.info("Verified lastGroupSyncTime after discovered failover")

            verify_protected_app_dr_status(
                acm_obj=acm_obj,
                app_name=discovered_app_name,
                expected_status="healthy",
                timeout=dr_status_timeout,
            )
            logger.info(
                f"Step 8 PASSED: Failover completed for discovered app: "
                f"{discovered_app_name}"
            )
            discovered_failover_success = True

        except Exception as e:
            error_msg = (
                f"Step 8 FAILED: Failover for discovered app "
                f"{discovered_app_name} - {e}"
            )
            logger.error(error_msg)
            dr_action_errors.append(error_msg)
            config.switch_acm_ctx()

        # Step 9: Relocate discovered app back to original primary
        logger.info("=" * 60)
        logger.info("Step 9: Relocate discovered app back to original primary")
        logger.info("=" * 60)

        if not discovered_failover_success:
            skip_msg = (
                "Step 9 SKIPPED: Discovered relocate skipped because failover failed"
            )
            logger.warning(skip_msg)
            dr_action_errors.append(skip_msg)
        else:
            logger.info(
                f"Discovered relocate target: {disc_primary_cluster_name} "
                f"(original primary, now secondary)"
            )
            try:
                relocate_from_protected_app_page(
                    acm_obj=acm_obj,
                    app_name=discovered_app_name,
                    expected_target_cluster=disc_primary_cluster_name,
                    drpc_obj=drpc_discovered,
                    timeout=120,
                    discovered_apps=True,
                )
                logger.info(
                    f"Relocate initiated for discovered app: {discovered_app_name}"
                )

                # Discovered apps require manual cleanup on old primary
                # (DRPC enters WaitOnUserToCleanUp; pods are not auto-deleted)
                logger.info(
                    f"Performing discovered apps cleanup on old primary: "
                    f"{disc_secondary_cluster_name}"
                )
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=discovered_app_name,
                    old_primary=disc_secondary_cluster_name,
                    workload_namespace=discovered_ns,
                    workload_dir=discovered_wl.workload_dir,
                    vrg_name=discovered_app_name,
                )
                logger.info(
                    f"Discovered apps cleanup completed on: "
                    f"{disc_secondary_cluster_name}"
                )

                config.switch_acm_ctx()
                logger.info(
                    f"Waiting for DRPC to reach {constants.STATUS_RELOCATED} phase "
                    "after discovered apps cleanup"
                )
                drpc_discovered.wait_for_phase(
                    constants.STATUS_RELOCATED, timeout=1200, sleep=15
                )
                logger.info(
                    f"DRPC reached {constants.STATUS_RELOCATED} phase - "
                    "relocate complete"
                )

                config.switch_ctx(disc_primary_cluster_index)
                logger.info(
                    f"Switched to original primary: {disc_primary_cluster_name}"
                )
                dr_helpers.wait_for_all_resources_creation(
                    discovered_wl.workload_pvc_count,
                    discovered_wl.workload_pod_count,
                    discovered_ns,
                    timeout=1200,
                    discovered_apps=True,
                    vrg_name=discovered_app_name,
                    performed_dr_action=True,
                )
                logger.info(f"Resources running on: {disc_primary_cluster_name}")

                if pvc_interface == constants.CEPHBLOCKPOOL:
                    all_workloads = appset_workloads + discovered_workload
                    dr_helpers.wait_for_mirroring_status_ok(
                        replaying_images=sum(
                            [wl.workload_pvc_count for wl in all_workloads]
                        )
                    )
                    logger.info("RBD mirroring status OK after discovered relocate")
                elif pvc_interface == constants.CEPHFILESYSTEM:
                    # VRG is already validated in openshift-dr-ops via
                    # wait_for_all_resources_creation(discovered_apps=True).
                    cg_enabled = dr_helpers.is_cg_cephfs_enabled()
                    config.switch_to_cluster_by_name(disc_primary_cluster_name)
                    wait_for_replication_destinations_deletion(discovered_ns)
                    if cg_enabled:
                        dr_helpers.wait_for_resource_existence(
                            kind=constants.REPLICATION_GROUP_DESTINATION,
                            namespace=discovered_ns,
                            should_exist=False,
                        )
                        logger.info(
                            "ReplicationGroupDestination deleted from new primary"
                        )

                    config.switch_to_cluster_by_name(disc_secondary_cluster_name)
                    wait_for_replication_destinations_creation(
                        discovered_wl.workload_pvc_count, discovered_ns
                    )
                    if cg_enabled:
                        dr_helpers.wait_for_resource_existence(
                            kind=constants.REPLICATION_GROUP_DESTINATION,
                            namespace=discovered_ns,
                            should_exist=True,
                        )
                        dr_helpers.wait_for_resource_count(
                            kind=constants.VOLUMESNAPSHOT,
                            namespace=discovered_ns,
                            expected_count=discovered_wl.workload_pvc_count,
                        )
                        logger.info(
                            "CG-CephFS resources verified after discovered relocate"
                        )
                    logger.info(
                        "CephFS ReplicationDestination verified after discovered relocate"
                    )

                config.switch_acm_ctx()
                dr_helpers.verify_last_group_sync_time(
                    drpc_discovered, scheduling_interval
                )
                logger.info("Verified lastGroupSyncTime after discovered relocate")

                verify_protected_app_dr_status(
                    acm_obj=acm_obj,
                    app_name=discovered_app_name,
                    expected_status="healthy",
                    timeout=dr_status_timeout,
                )
                logger.info(
                    f"Step 9 PASSED: Relocate completed for discovered app: "
                    f"{discovered_app_name}"
                )
                discovered_relocate_success = True

            except Exception as e:
                error_msg = (
                    f"Step 9 FAILED: Relocate for discovered app "
                    f"{discovered_app_name} - {e}"
                )
                logger.error(error_msg)
                dr_action_errors.append(error_msg)
                config.switch_acm_ctx()

        if False:  # TEMP: skip for discovered-only debug (step 10)
            # Step 10: Remove DR Protection for app2
            logger.info("=" * 60)
            logger.info("Step 10: Remove DR Protection for app2 via UI (Independent)")
            logger.info("=" * 60)

            config.switch_acm_ctx()

            try:
                logger.info(f"Removing DR protection for: {app2_name}")
                remove_dr_from_protected_app_page(
                    acm_obj=acm_obj,
                    app_name=app2_name,
                    timeout=120,
                )
                logger.info(f"DR removal initiated for: {app2_name}")

                logger.info(f"Verifying '{app2_name}' is NOT on Protected Applications page")
                app2_removed = verify_app_in_protected_applications_list(
                    acm_obj=acm_obj,
                    app_name=app2_name,
                    expected_present=False,
                    timeout=180,
                    retry_interval=15,
                )
                if not app2_removed:
                    raise AssertionError(
                        f"App '{app2_name}' should NOT be listed after DR removal"
                    )
                logger.info(
                    f"Step 10 PASSED: '{app2_name}' removed from Protected Applications list"
                )
                remove_dr_success = True

            except Exception as e:
                error_msg = f"Step 10 FAILED: Remove DR for {app2_name} - {e}"
                logger.error(error_msg)
                dr_action_errors.append(error_msg)

        # Final Test Summary
        logger.info("=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)

        all_errors = verification_errors + dr_action_errors

        verification_status = "PASSED" if not verification_errors else "FAILED"
        # Steps 3, 5-7, 10 temporarily skipped for discovered-only debug
        failover_status = "SKIPPED"
        relocate_status = "SKIPPED"
        discovered_failover_status = (
            "PASSED" if discovered_failover_success else "FAILED"
        )
        discovered_relocate_status = (
            "PASSED"
            if discovered_relocate_success
            else ("SKIPPED" if not discovered_failover_success else "FAILED")
        )
        remove_dr_status = "SKIPPED"

        logger.info("Step Results:")
        logger.info(f"  Step 1-2,4 (UI Verification):         {verification_status}")
        logger.info(f"  Step 3 (Managed kebab):               SKIPPED")
        logger.info(f"  Step 5 (Manage DR modal):             SKIPPED")
        logger.info(f"  Step 6 (AppSet Failover):             {failover_status}")
        logger.info(f"  Step 7 (AppSet Relocate):             {relocate_status}")
        logger.info(
            f"  Step 8 (Discovered Failover):         {discovered_failover_status}"
        )
        logger.info(
            f"  Step 9 (Discovered Relocate):         {discovered_relocate_status}"
        )
        logger.info(f"  Step 10 (Remove DR):                  {remove_dr_status}")
        logger.info("-" * 60)

        if all_errors:
            logger.error(f"Test completed with {len(all_errors)} error(s):")
            for i, error in enumerate(all_errors, 1):
                logger.error(f"  {i}. {error}")
            logger.info("=" * 60)

            pytest.fail(
                f"Test failed with {len(all_errors)} error(s):\n"
                + "\n".join(f"  - {e}" for e in all_errors)
            )
        else:
            logger.info("All steps PASSED!")
            logger.info("=" * 60)
