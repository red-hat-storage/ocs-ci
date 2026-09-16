import logging
from time import sleep


from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import (
    batch_failover_relocate_protected_applications,
    navigate_to_protected_applications_page,
    select_all_protected_applications,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


def _build_workload_entry(workload, is_discovered, pvc_interface):
    """
    Build metadata dict for a protected application under test.

    AppSet UI name is the full workload namespace (includes appset- prefix).
    Discovered UI name is discovered_apps_placement_name.
    """
    if is_discovered:
        ui_name = workload.discovered_apps_placement_name
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=ui_name,
        )
    else:
        ui_name = workload.workload_namespace
        drpc_obj = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workload.appset_placement_name}-drpc",
        )
    return {
        "workload": workload,
        "ui_name": ui_name,
        "namespace": workload.workload_namespace,
        "pvc_count": workload.workload_pvc_count,
        "pod_count": workload.workload_pod_count,
        "is_discovered": is_discovered,
        "pvc_interface": pvc_interface,
        "drpc": drpc_obj,
        "vrg_name": ui_name if is_discovered else "",
    }


def _wait_for_drpc_phase(workload_entries, phase, timeout=600):
    for entry in workload_entries:
        logger.info(
            f"Waiting for DRPC {entry['drpc'].resource_name} to reach phase {phase}"
        )
        entry["drpc"].wait_for_phase(phase, timeout=timeout)


def _cleanup_discovered_apps_on_source(workload_entries, old_primary):
    """
    Manually clean up discovered apps on the source cluster after Failover/Relocate.

    AppSet workloads are cleaned automatically by GitOps; discovered apps require
    explicit cleanup once DRPC reaches WaitOnUserToCleanUp.
    """
    for entry in workload_entries:
        if not entry["is_discovered"]:
            continue
        workload = entry["workload"]
        logger.info(
            f"Manual cleanup for discovered app {entry['ui_name']} "
            f"on source cluster {old_primary}"
        )
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=entry["ui_name"],
            old_primary=old_primary,
            workload_namespace=entry["namespace"],
            workload_dir=workload.workload_dir,
            vrg_name=entry["vrg_name"],
        )


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.23")
class TestBatchFailoverRelocateProtectedApps:
    """
    Batch Failover and Relocate for a group of protected applications (OCS 4.23+).

    Deploys 2 AppSet (RBD + CephFS) and 2 Discovered (RBD + CephFS) workloads,
    then performs batch Failover and Relocate from the Protected Applications page.
    """

    def test_batch_failover_relocate_protected_apps(
        self,
        setup_acm_ui,
        dr_workload,
        discovered_apps_dr_workload,
    ):
        """
        Verify batch Failover and Relocate from Protected Applications page for
        AppSet and Discovered applications with mixed RBD and CephFS storage.
        """
        logger.info("Deploy AppSet workloads (RBD and CephFS)")
        # Factory returns a cumulative list; take [-1] for the newly created workload
        appset_rbd = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )[-1]
        appset_cephfs = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )[-1]

        logger.info("Deploy Discovered workloads (RBD and CephFS)")
        discovered_rbd = discovered_apps_dr_workload(
            kubeobject=1,
            recipe=0,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )[-1]
        discovered_cephfs = discovered_apps_dr_workload(
            kubeobject=1,
            recipe=0,
            pvc_interface=constants.CEPHFILESYSTEM,
        )[-1]

        workload_entries = [
            _build_workload_entry(
                appset_rbd, is_discovered=False, pvc_interface=constants.CEPHBLOCKPOOL
            ),
            _build_workload_entry(
                appset_cephfs,
                is_discovered=False,
                pvc_interface=constants.CEPHFILESYSTEM,
            ),
            _build_workload_entry(
                discovered_rbd,
                is_discovered=True,
                pvc_interface=constants.CEPHBLOCKPOOL,
            ),
            _build_workload_entry(
                discovered_cephfs,
                is_discovered=True,
                pvc_interface=constants.CEPHFILESYSTEM,
            ),
        ]

        scheduling_interval = dr_helpers.get_scheduling_interval(
            appset_rbd.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for initial sync")
        sleep(wait_time * 60)

        for entry in workload_entries:
            progression = entry["drpc"].get_progression_status()
            assert progression == constants.STATUS_COMPLETED, (
                f"DRPC {entry['drpc'].resource_name} progression is {progression}, "
                f"expected {constants.STATUS_COMPLETED}"
            )
            dr_helpers.verify_last_group_sync_time(entry["drpc"], scheduling_interval)

        primary_cluster = dr_helpers.get_current_primary_cluster_name(
            appset_rbd.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            appset_rbd.workload_namespace,
            workload_type=constants.APPLICATION_SET,
        )
        logger.info(
            f"DR clusters - Primary: {primary_cluster}, Secondary: {secondary_cluster}"
        )

        acm_obj = AcmAddClusters()

        # Batch Failover from Protected Applications page
        logger.info("=" * 60)
        logger.info("Batch Failover: select all apps and initiate Failover")
        logger.info("=" * 60)
        navigate_to_protected_applications_page(acm_obj)
        select_all_protected_applications(acm_obj, expected_count=len(workload_entries))
        batch_failover_relocate_protected_applications(
            acm_obj,
            action=constants.ACTION_FAILOVER,
            timeout=120,
        )
        _wait_for_drpc_phase(workload_entries, constants.STATUS_FAILEDOVER)
        _cleanup_discovered_apps_on_source(workload_entries, primary_cluster)
        # Allow time for sync/protection to become healthy after FO + cleanup
        logger.info(
            f"Waiting {wait_time} minutes for post-failover sync to stabilize"
        )
        sleep(wait_time * 60)
        dr_helpers.verify_post_dr_action(
            workload_entries,
            primary_cluster,
            secondary_cluster,
            scheduling_interval,
            action_label="failover",
        )
        logger.info("Batch Failover completed and verified for all applications")

        # Batch Relocate from Protected Applications page
        logger.info("=" * 60)
        logger.info("Batch Relocate: select all apps and initiate Relocate")
        logger.info("=" * 60)
        navigate_to_protected_applications_page(acm_obj)
        select_all_protected_applications(acm_obj, expected_count=len(workload_entries))
        batch_failover_relocate_protected_applications(
            acm_obj,
            action=constants.ACTION_RELOCATE,
            timeout=120,
        )
        _wait_for_drpc_phase(workload_entries, constants.STATUS_RELOCATED)
        _cleanup_discovered_apps_on_source(workload_entries, secondary_cluster)
        logger.info(
            f"Waiting {wait_time} minutes for post-relocate sync to stabilize"
        )
        sleep(wait_time * 60)
        dr_helpers.verify_post_dr_action(
            workload_entries,
            secondary_cluster,
            primary_cluster,
            scheduling_interval,
            action_label="relocate",
        )
        logger.info("Batch Relocate completed and verified for all applications")
