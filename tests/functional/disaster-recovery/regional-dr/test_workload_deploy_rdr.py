from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time

import pytest

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1, skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestWorkloadDeployRDR:
    """
    Verify that all supported RDR workload types can be successfully deployed,
    failed over, and relocated between managed clusters.

    Workload types covered:
        - Subscription: BusyBox (RBD + CephFS)
        - ApplicationSet: BusyBox (RBD + CephFS)
        - Discovered Apps: BusyBox (RBD + CephFS)
        - Discovered Apps: MongoDB (RBD)
        - Discovered Apps: FileBrowser (RBD)
        - CNV / KubeVirt VM (PVC-backed, Subscription + AppSet push + AppSet pull)

    Teardown is handled automatically by each fixture's finalizer.
    """

    @acceptance
    @pytest.mark.polarion_id("OCS-XXXX")
    @skipif_ocs_version("<4.19")
    def test_all_workloads_deploy(
        self,
        dr_workload,
        discovered_apps_dr_workload,
        cnv_dr_workload,
    ):
        """
        Deploy all supported workload types simultaneously, fail them over
        to the secondary cluster, verify their health, and relocate them back
        to the primary cluster.

        Steps:
            1.  Deploy Subscription BusyBox workload (RBD).
            2.  Deploy Subscription BusyBox workload (CephFS).
            3.  Deploy ApplicationSet BusyBox workload (RBD).
            4.  Deploy ApplicationSet BusyBox workload (CephFS).
            5.  Deploy Discovered Apps BusyBox workload (RBD).
            6.  Deploy Discovered Apps BusyBox workload (CephFS).
            7.  Deploy Discovered Apps MongoDB workload (RBD).
            8.  Deploy Discovered Apps FileBrowser workload (RBD).
            9.  Download virtctl binary.
            10. Deploy CNV VM workloads (Subscription + AppSet push + AppSet pull).
            11. Identify primary and secondary clusters.
            12. Verify initial deployment on primary cluster for all workloads.
            13. Wait for data sync to complete.
            14. Failover all workloads to secondary cluster in parallel.
            15. Verify all workloads are running on secondary cluster in parallel.
            16. Verify all workloads are deleted from primary cluster in parallel.
            17. Wait for sync to complete before relocate.
            18. Relocate all workloads back to primary cluster in parallel.
            19. Verify all workloads are deleted from secondary cluster in parallel.
            20. Verify all workloads are running on primary cluster in parallel.
        """

        # ── Helper: run a list of callables in parallel, collect errors ─────────
        def _run_parallel(tasks, stage, errors):
            """
            tasks: list of (callable, workload_id) tuples.
            Runs each callable in its own thread, appending (stage, wl_id, str(e))
            to errors on failure.  Returns the set of wl_ids that succeeded.
            """
            succeeded = set()
            with ThreadPoolExecutor(max_workers=max(len(tasks), 1)) as pool:
                future_map = {pool.submit(fn): wl_id for fn, wl_id in tasks}
                for fut in as_completed(future_map):
                    wl_id = future_map[fut]
                    try:
                        fut.result()
                        logger.info(f"[{stage}] succeeded for {wl_id}")
                        succeeded.add(wl_id)
                    except Exception as exc:
                        logger.error(f"[{stage}] failed for {wl_id}: {exc}")
                        errors.append((stage, wl_id, str(exc)))
            return succeeded

        # ── 1-4. Deploy Subscription and ApplicationSet workloads serially ──────
        # These deployments switch the global cluster context (oc operations) so
        # they must run one after another to avoid context-switch races.
        logger.info(
            "Deploying Subscription + ApplicationSet BusyBox workloads (RBD + CephFS)"
        )
        sub_appset_rbd = dr_workload(
            num_of_subscription=1,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
            skip_mirroring_validation=True,
        )
        sub_appset_cephfs = dr_workload(
            num_of_subscription=1,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
            skip_mirroring_validation=True,
        )

        all_sub_appset = sub_appset_rbd + sub_appset_cephfs

        # ── 5-8. Deploy Discovered Apps workloads serially ───────────────────────
        # Same reason: discovered_apps_dr_workload uses the global cluster context.
        logger.info("Deploying all Discovered Apps workloads")
        da_busybox_rbd = discovered_apps_dr_workload(
            pvc_interface=constants.CEPHBLOCKPOOL, kubeobject=1, recipe=0
        )
        da_busybox_cephfs = discovered_apps_dr_workload(
            pvc_interface=constants.CEPHFILESYSTEM, kubeobject=1, recipe=0
        )
        da_mongodb_rbd = discovered_apps_dr_workload(
            pvc_interface=constants.CEPHBLOCKPOOL,
            kubeobject=1,
            recipe=0,
            workloads="mongodb",
        )
        da_filebrowser_rbd = discovered_apps_dr_workload(
            pvc_interface=constants.CEPHBLOCKPOOL,
            kubeobject=1,
            recipe=0,
            workloads="filebrowser",
        )

        all_discovered_workloads = (
            da_busybox_rbd + da_busybox_cephfs + da_mongodb_rbd + da_filebrowser_rbd
        )

        # ── 9-10. CNV VM workloads (only when CNV operator is installed) ────────
        cnv_workloads = []
        cnv_installer = CNVInstaller()
        if cnv_installer.cnv_hyperconverged_installed():
            logger.info("CNV operator detected — deploying VM workloads")
            cnv_installer.download_and_extract_virtctl_binary()
            cnv_workloads = cnv_dr_workload(
                num_of_vm_subscription=1,
                num_of_vm_appset_push=1,
                num_of_vm_appset_pull=1,
                vm_type=constants.VM_VOLUME_PVC,
                skip_mirroring_validation=True,
            )
        else:
            logger.warning(
                "CNV HyperConverged operator is not installed — "
                "skipping CNV workload deployment"
            )

        # ── 11. Identify primary cluster ──────────────────────────────────────
        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            sub_appset_rbd[0].workload_namespace
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            sub_appset_rbd[0].workload_namespace
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.get_cluster_index_by_name(primary_cluster_name)
        secondary_cluster_index = config.get_cluster_index_by_name(
            secondary_cluster_name
        )
        logger.info(
            f"Primary cluster: {primary_cluster_name} (index {primary_cluster_index}), "
            f"Secondary cluster: {secondary_cluster_name} (index {secondary_cluster_index})"
        )

        # ── 12. Verify all workloads on primary (in parallel) ────────────────
        errors = []
        logger.info("Verifying all workloads on primary cluster in parallel")

        def _verify_sub_appset(wl):
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                skip_replication_resources=True,
            )

        def _verify_discovered_app(wl):
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
                skip_replication_resources=True,
            )

        def _verify_cnv(cnv_wl):
            dr_helpers.wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
                skip_replication_resources=True,
            )
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        initial_tasks = []
        for wl in all_sub_appset:
            initial_tasks.append(
                (
                    lambda w=wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_sub_appset(w),
                    ),
                    wl.workload_namespace,
                )
            )
        for wl in all_discovered_workloads:
            initial_tasks.append(
                (
                    lambda w=wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_discovered_app(w),
                    ),
                    f"DA:{wl.workload_namespace}",
                )
            )
        for cnv_wl in cnv_workloads:
            initial_tasks.append(
                (
                    lambda w=cnv_wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_cnv(w),
                    ),
                    f"CNV:{cnv_wl.workload_namespace}",
                )
            )

        _run_parallel(initial_tasks, "Initial Primary Verification", errors)
        logger.info("Completed initial verification for all workloads on primary")

        # ── 12b. Verify mirroring health before the sync wait ────────────────
        # Count RBD PVCs only: CephFS PVCs are not mirrored via RBD mirroring.
        # CNV workloads are always backed by RBD block volumes.
        rbd_pvc_count = sum(
            wl.workload_pvc_count
            for wl in all_sub_appset + all_discovered_workloads
            if wl.pvc_interface == constants.CEPHBLOCKPOOL
        ) + sum(cnv_wl.workload_pvc_count for cnv_wl in cnv_workloads)
        logger.info(
            f"Checking mirroring health with {rbd_pvc_count} expected replaying images"
        )
        dr_helpers.wait_for_mirroring_status_ok(replaying_images=rbd_pvc_count)

        # ── 13. Wait for data sync to complete ───────────────────────────────
        scheduling_interval = dr_helpers.get_scheduling_interval(
            sub_appset_rbd[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval
        logger.info(
            f"Waiting for {wait_time} minutes for initial data sync to complete"
        )
        time.sleep(wait_time * 60)

        # ── 14. Failover all workloads to secondary cluster (in parallel) ────
        logger.info("Failing over all workloads to secondary cluster in parallel")

        def _failover_sub_appset(wl):
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=wl.workload_namespace,
                workload_type=wl.workload_type,
                workload_placement_name=(
                    wl.appset_placement_name
                    if wl.workload_type == constants.APPLICATION_SET
                    else None
                ),
            )

        def _failover_discovered(wl):
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=wl.workload_namespace,
                discovered_apps=True,
                workload_placement_name=wl.discovered_apps_placement_name,
                old_primary=primary_cluster_name,
            )
            logger.info(
                f"Cleaning up Discovered Apps workload {wl.workload_namespace} on {primary_cluster_name}"
            )
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=wl.discovered_apps_placement_name,
                old_primary=primary_cluster_name,
                workload_namespace=wl.workload_namespace,
                workload_dir=wl.workload_dir,
                vrg_name=wl.discovered_apps_placement_name,
            )

        def _failover_cnv(cnv_wl):
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=(
                    cnv_wl.cnv_workload_placement_name
                    if cnv_wl.workload_type != constants.SUBSCRIPTION
                    else None
                ),
            )

        failover_tasks = []
        for wl in all_sub_appset:
            failover_tasks.append(
                (lambda w=wl: _failover_sub_appset(w), wl.workload_namespace)
            )
        for wl in all_discovered_workloads:
            failover_tasks.append(
                (lambda w=wl: _failover_discovered(w), f"DA:{wl.workload_namespace}")
            )
        for cnv_wl in cnv_workloads:
            failover_tasks.append(
                (lambda w=cnv_wl: _failover_cnv(w), f"CNV:{cnv_wl.workload_namespace}")
            )

        failed_over_ids = _run_parallel(failover_tasks, "Failover", errors)

        failed_over_sub_appset = [
            wl for wl in all_sub_appset if wl.workload_namespace in failed_over_ids
        ]
        failed_over_discovered = [
            wl
            for wl in all_discovered_workloads
            if f"DA:{wl.workload_namespace}" in failed_over_ids
        ]
        failed_over_cnv = [
            wl
            for wl in cnv_workloads
            if f"CNV:{wl.workload_namespace}" in failed_over_ids
        ]

        # ── 15. Verify resources creation on secondary cluster (in parallel) ─
        config.switch_to_cluster_by_name(secondary_cluster_name)
        logger.info("Verifying all workloads on secondary cluster in parallel")

        def _verify_secondary_sub_appset(wl):
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                performed_dr_action=True,
            )

        def _verify_secondary_discovered(wl):
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
                performed_dr_action=True,
            )

        def _verify_secondary_cnv(cnv_wl):
            dr_helpers.wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
                performed_dr_action=True,
            )
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        secondary_verify_tasks = []
        for wl in failed_over_sub_appset:
            secondary_verify_tasks.append(
                (
                    lambda w=wl, idx=secondary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_secondary_sub_appset(w),
                    ),
                    wl.workload_namespace,
                )
            )
        for wl in failed_over_discovered:
            secondary_verify_tasks.append(
                (
                    lambda w=wl, idx=secondary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_secondary_discovered(w),
                    ),
                    f"DA:{wl.workload_namespace}",
                )
            )
        for cnv_wl in failed_over_cnv:
            secondary_verify_tasks.append(
                (
                    lambda w=cnv_wl, idx=secondary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_secondary_cnv(w),
                    ),
                    f"CNV:{cnv_wl.workload_namespace}",
                )
            )

        verified_secondary_ids = _run_parallel(
            secondary_verify_tasks, "Verify Failover Secondary", errors
        )

        verified_secondary_sub_appset = [
            wl
            for wl in failed_over_sub_appset
            if wl.workload_namespace in verified_secondary_ids
        ]
        verified_secondary_discovered = [
            wl
            for wl in failed_over_discovered
            if f"DA:{wl.workload_namespace}" in verified_secondary_ids
        ]
        verified_secondary_cnv = [
            wl
            for wl in failed_over_cnv
            if f"CNV:{wl.workload_namespace}" in verified_secondary_ids
        ]

        # ── 16. Verify resources deletion from primary cluster (in parallel) ─
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info(
            "Verifying workload resources deletion from primary cluster in parallel"
        )

        deletion_tasks = []
        for wl in failed_over_sub_appset:
            deletion_tasks.append(
                (
                    lambda w=wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        dr_helpers.wait_for_all_resources_deletion(
                            w.workload_namespace
                        ),
                    ),
                    wl.workload_namespace,
                )
            )
        for wl in failed_over_discovered:
            deletion_tasks.append(
                (
                    lambda w=wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        dr_helpers.wait_for_all_resources_deletion(
                            w.workload_namespace
                        ),
                    ),
                    f"DA:{wl.workload_namespace}",
                )
            )
        for cnv_wl in failed_over_cnv:
            deletion_tasks.append(
                (
                    lambda w=cnv_wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        dr_helpers.wait_for_all_resources_deletion(
                            w.workload_namespace
                        ),
                    ),
                    f"CNV:{cnv_wl.workload_namespace}",
                )
            )

        _run_parallel(deletion_tasks, "Verify Primary Deletion", errors)

        # ── 17. Wait for sync to complete before relocate ────────────────────
        if (
            verified_secondary_sub_appset
            or verified_secondary_discovered
            or verified_secondary_cnv
        ):
            logger.info(
                f"Waiting for {wait_time} minutes for sync to complete before relocate"
            )
            time.sleep(wait_time * 60)
        else:
            logger.warning(
                "No workloads successfully failed over and verified on secondary; "
                "skipping relocate sync wait"
            )

        # ── 18. Relocate workloads back to primary cluster (in parallel) ─────
        logger.info("Relocating all workloads back to primary cluster in parallel")

        def _relocate_sub_appset(wl):
            dr_helpers.relocate(
                preferred_cluster=primary_cluster_name,
                namespace=wl.workload_namespace,
                workload_type=wl.workload_type,
                workload_placement_name=(
                    wl.appset_placement_name
                    if wl.workload_type == constants.APPLICATION_SET
                    else None
                ),
            )

        def _relocate_discovered(wl):
            dr_helpers.relocate(
                preferred_cluster=primary_cluster_name,
                namespace=wl.workload_namespace,
                workload_placement_name=wl.discovered_apps_placement_name,
                discovered_apps=True,
                old_primary=secondary_cluster_name,
                workload_instance=wl,
                vm_auto_cleanup=(
                    version.get_semantic_ocs_version_from_config()
                    >= version.VERSION_4_22
                ),
            )

        def _relocate_cnv(cnv_wl):
            dr_helpers.relocate(
                preferred_cluster=primary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=(
                    cnv_wl.cnv_workload_placement_name
                    if cnv_wl.workload_type != constants.SUBSCRIPTION
                    else None
                ),
            )

        relocate_tasks = []
        for wl in all_sub_appset:
            if wl not in verified_secondary_sub_appset:
                logger.warning(
                    f"Skipping relocate for workload {wl.workload_namespace} "
                    "because failover or secondary verification failed"
                )
                continue
            relocate_tasks.append(
                (lambda w=wl: _relocate_sub_appset(w), wl.workload_namespace)
            )
        for wl in all_discovered_workloads:
            if wl not in verified_secondary_discovered:
                logger.warning(
                    f"Skipping relocate for Discovered App {wl.workload_namespace} "
                    "because failover or secondary verification failed"
                )
                continue
            relocate_tasks.append(
                (lambda w=wl: _relocate_discovered(w), f"DA:{wl.workload_namespace}")
            )
        for cnv_wl in cnv_workloads:
            if cnv_wl not in verified_secondary_cnv:
                logger.warning(
                    f"Skipping relocate for CNV {cnv_wl.workload_namespace} "
                    "because failover or secondary verification failed"
                )
                continue
            relocate_tasks.append(
                (
                    lambda w=cnv_wl: _relocate_cnv(w),
                    f"CNV:{cnv_wl.workload_namespace}",
                )
            )

        relocated_ids = _run_parallel(relocate_tasks, "Relocate", errors)

        relocated_sub_appset = [
            wl
            for wl in all_sub_appset
            if wl in verified_secondary_sub_appset
            and wl.workload_namespace in relocated_ids
        ]
        relocated_discovered = [
            wl
            for wl in all_discovered_workloads
            if wl in verified_secondary_discovered
            and f"DA:{wl.workload_namespace}" in relocated_ids
        ]
        relocated_cnv = [
            wl
            for wl in cnv_workloads
            if wl in verified_secondary_cnv
            and f"CNV:{wl.workload_namespace}" in relocated_ids
        ]

        # ── 19. Verify resources deletion from secondary cluster (in parallel) ─
        config.switch_to_cluster_by_name(secondary_cluster_name)
        logger.info(
            "Verifying workload resources deletion from secondary cluster in parallel"
        )

        sec_deletion_tasks = []
        for wl in relocated_sub_appset:
            sec_deletion_tasks.append(
                (
                    lambda w=wl, idx=secondary_cluster_index: (
                        config.switch_ctx(idx),
                        dr_helpers.wait_for_all_resources_deletion(
                            w.workload_namespace
                        ),
                    ),
                    wl.workload_namespace,
                )
            )
        for wl in relocated_discovered:
            sec_deletion_tasks.append(
                (
                    lambda w=wl, idx=secondary_cluster_index: (
                        config.switch_ctx(idx),
                        dr_helpers.wait_for_all_resources_deletion(
                            w.workload_namespace
                        ),
                    ),
                    f"DA:{wl.workload_namespace}",
                )
            )
        for cnv_wl in relocated_cnv:
            sec_deletion_tasks.append(
                (
                    lambda w=cnv_wl, idx=secondary_cluster_index: (
                        config.switch_ctx(idx),
                        dr_helpers.wait_for_all_resources_deletion(
                            w.workload_namespace
                        ),
                    ),
                    f"CNV:{cnv_wl.workload_namespace}",
                )
            )

        _run_parallel(sec_deletion_tasks, "Verify Secondary Deletion", errors)

        # ── 20. Verify resources creation on primary cluster (in parallel) ───
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info("Verifying all workloads back on primary cluster in parallel")

        def _verify_primary_sub_appset(wl):
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                performed_dr_action=True,
            )

        def _verify_primary_discovered(wl):
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
                performed_dr_action=True,
            )

        def _verify_primary_cnv(cnv_wl):
            dr_helpers.wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
                performed_dr_action=True,
            )
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        primary_verify_tasks = []
        for wl in relocated_sub_appset:
            primary_verify_tasks.append(
                (
                    lambda w=wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_primary_sub_appset(w),
                    ),
                    wl.workload_namespace,
                )
            )
        for wl in relocated_discovered:
            primary_verify_tasks.append(
                (
                    lambda w=wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_primary_discovered(w),
                    ),
                    f"DA:{wl.workload_namespace}",
                )
            )
        for cnv_wl in relocated_cnv:
            primary_verify_tasks.append(
                (
                    lambda w=cnv_wl, idx=primary_cluster_index: (
                        config.switch_ctx(idx),
                        _verify_primary_cnv(w),
                    ),
                    f"CNV:{cnv_wl.workload_namespace}",
                )
            )

        _run_parallel(primary_verify_tasks, "Verify Relocate Primary", errors)

        # ── 21. Check for any failures across all stages ─────────────────────
        if errors:
            error_report = "\n".join(
                f"  - [{stage}] {wl_id}: {err}" for stage, wl_id, err in errors
            )
            logger.error(
                f"RDR workload deploy test encountered {len(errors)} failure(s):\n{error_report}"
            )
            pytest.fail(
                f"RDR workload deploy test encountered {len(errors)} failure(s):\n{error_report}"
            )
        logger.info(
            "All workloads completed deployment, failover, and relocate successfully"
        )
