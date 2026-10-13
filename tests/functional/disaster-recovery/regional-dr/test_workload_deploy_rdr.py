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
            14. Failover all workloads to secondary cluster.
            15. Verify all workloads are running on secondary cluster.
            16. Verify all workloads are deleted from primary cluster.
            17. Wait for sync to complete before relocate.
            18. Relocate all workloads back to primary cluster.
            19. Verify all workloads are deleted from secondary cluster.
            20. Verify all workloads are running on primary cluster.
        """
        # ── 1-4. Subscription and ApplicationSet workloads (RBD + CephFS) ───
        logger.info("Deploying Subscription BusyBox workload (RBD)")
        sub_rbd_workloads = dr_workload(
            num_of_subscription=1,
            num_of_appset=0,
            pvc_interface=constants.CEPHBLOCKPOOL,
            skip_mirroring_validation=True,
        )

        logger.info("Deploying Subscription BusyBox workload (CephFS)")
        sub_cephfs_workloads = dr_workload(
            num_of_subscription=1,
            num_of_appset=0,
            pvc_interface=constants.CEPHFILESYSTEM,
            skip_mirroring_validation=True,
        )

        logger.info("Deploying ApplicationSet BusyBox workload (RBD)")
        appset_rbd_workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
            skip_mirroring_validation=True,
        )

        logger.info("Deploying ApplicationSet BusyBox workload (CephFS)")
        appset_cephfs_workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
            skip_mirroring_validation=True,
        )

        gitops_appset_workloads = appset_rbd_workloads + appset_cephfs_workloads

        # ── 5-8. Discovered Apps workloads ───────────────────────────────────
        logger.info("Deploying Discovered Apps BusyBox workload (RBD)")
        da_busybox_rbd = discovered_apps_dr_workload(
            pvc_interface=constants.CEPHBLOCKPOOL, kubeobject=1, recipe=0
        )

        logger.info("Deploying Discovered Apps BusyBox workload (CephFS)")
        da_busybox_cephfs = discovered_apps_dr_workload(
            pvc_interface=constants.CEPHFILESYSTEM, kubeobject=1, recipe=0
        )

        logger.info("Deploying Discovered Apps MongoDB workload (RBD)")
        da_mongodb_rbd = discovered_apps_dr_workload(
            pvc_interface=constants.CEPHBLOCKPOOL,
            kubeobject=1,
            recipe=0,
            workloads="mongodb",
        )

        logger.info("Deploying Discovered Apps FileBrowser workload (RBD)")
        da_filebrowser_rbd = discovered_apps_dr_workload(
            pvc_interface=constants.CEPHBLOCKPOOL,
            kubeobject=1,
            recipe=0,
            workloads="filebrowser",
        )

        all_discovered_workloads = (
            da_busybox_rbd + da_busybox_cephfs + da_mongodb_rbd + da_filebrowser_rbd
        )

        # ── 9-10. CNV VM workloads (only when CNV operator is installed) ─────
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
            sub_rbd_workloads[0].workload_namespace
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            sub_rbd_workloads[0].workload_namespace
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info(
            f"Primary cluster: {primary_cluster_name}, "
            f"Secondary cluster: {secondary_cluster_name}"
        )

        all_sub_appset = (
            sub_rbd_workloads + sub_cephfs_workloads + gitops_appset_workloads
        )

        # ── 12. Verify all workloads on primary ──────────────────────────────
        logger.info("Verifying Subscription and ApplicationSet workloads on primary")
        for wl in all_sub_appset:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                skip_replication_resources=True,
            )
        logger.info("All subscription and appset workloads are running on primary")

        logger.info("Verifying Discovered Apps workloads on primary")
        for wl in all_discovered_workloads:
            da_primary = dr_helpers.get_current_primary_cluster_name(
                wl.workload_namespace,
                discovered_apps=True,
                resource_name=wl.discovered_apps_placement_name,
            )
            config.switch_to_cluster_by_name(da_primary)
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
                skip_replication_resources=True,
            )
        logger.info("All discovered apps workloads are running on primary")

        if cnv_workloads:
            logger.info("Verifying CNV VM workloads on primary")
            config.switch_to_cluster_by_name(primary_cluster_name)
            for cnv_wl in cnv_workloads:
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
            logger.info("All CNV workloads are running on primary")

        # ── 13. Wait for data sync to complete ───────────────────────────────
        scheduling_interval = dr_helpers.get_scheduling_interval(
            sub_rbd_workloads[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval
        logger.info(
            f"Waiting for {wait_time} minutes for initial data sync to complete"
        )
        time.sleep(wait_time * 60)

        # ── 14. Failover all workloads to secondary cluster ──────────────────
        logger.info("Failing over Subscription and ApplicationSet workloads")
        for wl in all_sub_appset:
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

        logger.info("Failing over Discovered Apps workloads")
        for wl in all_discovered_workloads:
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

        if cnv_workloads:
            logger.info("Failing over CNV VM workloads")
            for cnv_wl in cnv_workloads:
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

        # ── 15. Verify resources creation on secondary cluster ───────────────
        config.switch_to_cluster_by_name(secondary_cluster_name)
        logger.info("Verifying Subscription and ApplicationSet workloads on secondary")
        for wl in all_sub_appset:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                performed_dr_action=True,
            )

        logger.info("Verifying Discovered Apps workloads on secondary")
        for wl in all_discovered_workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
                performed_dr_action=True,
            )

        if cnv_workloads:
            logger.info("Verifying CNV VM workloads on secondary")
            for cnv_wl in cnv_workloads:
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
        logger.info("All workloads are running on secondary cluster after failover")

        # ── 16. Verify resources deletion from primary cluster ───────────────
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info("Verifying workload resources deletion from primary cluster")
        for wl in all_sub_appset:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)
        for wl in all_discovered_workloads:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_deletion(cnv_wl.workload_namespace)
        logger.info("All workload resources deleted from primary cluster")

        # ── 17. Wait for sync to complete before relocate ────────────────────
        logger.info(
            f"Waiting for {wait_time} minutes for sync to complete before relocate"
        )
        time.sleep(wait_time * 60)

        # ── 18. Relocate all workloads back to primary cluster ───────────────
        logger.info(
            "Relocating Subscription and ApplicationSet workloads back to primary"
        )
        for wl in all_sub_appset:
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

        logger.info("Relocating Discovered Apps workloads back to primary")
        for wl in all_discovered_workloads:
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

        if cnv_workloads:
            logger.info("Relocating CNV VM workloads back to primary")
            for cnv_wl in cnv_workloads:
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

        # ── 19. Verify resources deletion from secondary cluster ─────────────
        config.switch_to_cluster_by_name(secondary_cluster_name)
        logger.info("Verifying workload resources deletion from secondary cluster")
        for wl in all_sub_appset:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)
        for wl in all_discovered_workloads:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_deletion(cnv_wl.workload_namespace)
        logger.info("All workload resources deleted from secondary cluster")

        # ── 20. Verify resources creation on primary cluster ─────────────────
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info("Verifying Subscription and ApplicationSet workloads on primary")
        for wl in all_sub_appset:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                performed_dr_action=True,
            )

        logger.info("Verifying Discovered Apps workloads on primary")
        for wl in all_discovered_workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
                performed_dr_action=True,
            )

        if cnv_workloads:
            logger.info("Verifying CNV VM workloads on primary")
            for cnv_wl in cnv_workloads:
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
        logger.info(
            "All workloads are running on primary cluster after relocate"
        )
