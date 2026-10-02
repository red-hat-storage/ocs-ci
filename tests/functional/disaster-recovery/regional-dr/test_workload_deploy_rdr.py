import logging

import pytest

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1, skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestWorkloadDeployRDR:
    """
    Verify that all supported RDR workload types can be successfully deployed
    in a single test and reach a healthy state on the primary managed cluster.

    Workload types covered:
        - Subscription: BusyBox (RBD + CephFS)
        - ApplicationSet: BusyBox (RBD + CephFS)
        - Discovered Apps: BusyBox (RBD + CephFS)
        - Discovered Apps: MongoDB (RBD)
        - Discovered Apps: FileBrowser (RBD)
        - CNV / KubeVirt VM (PVC-backed, Subscription + AppSet push + AppSet pull)

    No failover, relocate, or DR protection checks are performed.
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
        Deploy all supported workload types simultaneously and verify every
        workload's pods and PVCs reach a healthy state on the primary cluster.

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
            11. Identify the primary cluster from the first subscription workload.
            12. Verify all subscription and appset workload pods/PVCs are running.
            13. Verify all discovered apps workload pods/PVCs are running.
            14. Verify all CNV VMs and their pods/PVCs are running.
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
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info(f"Primary cluster identified: {primary_cluster_name}")

        # ── 12. Verify subscription and appset workloads ──────────────────────
        logger.info("Verifying Subscription and ApplicationSet workloads on primary")
        all_sub_appset = (
            sub_rbd_workloads + sub_cephfs_workloads + gitops_appset_workloads
        )
        for wl in all_sub_appset:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                skip_replication_resources=True,
            )
        logger.info("All subscription and appset workloads are running")

        # ── 13. Verify discovered apps workloads ──────────────────────────────
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
        logger.info("All discovered apps workloads are running")

        # ── 14. Verify CNV workloads ──────────────────────────────────────────
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
        logger.info("All CNV workloads are running")
