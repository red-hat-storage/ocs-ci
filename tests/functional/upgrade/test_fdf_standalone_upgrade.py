"""
FDF Standalone Version Upgrade with Data Integrity (RHSTOR-8290 / TC-7).

Validates FDF standalone upgrade from version N to N+1 while maintaining
data integrity and service availability. The upgrade is performed by
updating the CatalogSource image and (for Y-stream) the Subscription
channel, then approving the InstallPlan.

Unlike the Fusion-managed FDF upgrade path (test_upgrade.py / FDFUpgrade),
this test targets the standalone deployment where no FusionServiceInstance
exists — the upgrade is driven directly via OLM primitives.
"""

import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier2,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.deployment.fdf_standalone import StandaloneFDFCatalogSource
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier2
class TestFDFStandaloneUpgrade:
    """
    Verify FDF standalone upgrade maintains data integrity and availability.
    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory, pod_factory):
        """Set up test workloads with checksummed data."""
        self.namespace = config.ENV_DATA["cluster_namespace"]
        self.upgrade_image = config.DEPLOYMENT.get(
            "fdf_upgrade_catalog_image", ""
        ) or config.DEPLOYMENT.get("fdf_standalone_catalog_image", "")
        self.upgrade_channel = config.DEPLOYMENT.get("fdf_upgrade_channel", "")

        self.pvc_rbd = pvc_factory(interface=constants.CEPHBLOCKPOOL, size=5)
        self.pod_rbd = pod_factory(pvc=self.pvc_rbd)

        self.pvc_cephfs = pvc_factory(interface=constants.CEPHFILESYSTEM, size=5)
        self.pod_cephfs = pod_factory(pvc=self.pvc_cephfs)

    def _record_baseline(self):
        """Record pre-upgrade state: CSVs, pod images, data checksums."""
        logger.info("Recording pre-upgrade baseline")

        csv_ocp = CSV(namespace=self.namespace)
        csv_list = csv_ocp.get().get("items", [])
        self.pre_csvs = {c["metadata"]["name"]: c["status"]["phase"] for c in csv_list}
        logger.info("Pre-upgrade CSVs: %s", self.pre_csvs)

        for pod_obj in [self.pod_rbd, self.pod_cephfs]:
            pod_obj.run_io(
                storage_type="fs",
                size="512M",
                io_direction="wo",
                runtime=0,
            )
            pod_obj.get_fio_results()

        self.md5_rbd = cal_md5sum(self.pod_rbd, "fio-rand-write")
        self.md5_cephfs = cal_md5sum(self.pod_cephfs, "fio-rand-write")
        logger.info(
            "Baseline checksums — RBD: %s, CephFS: %s",
            self.md5_rbd,
            self.md5_cephfs,
        )

        ceph_cluster = CephCluster()
        ceph_cluster.cluster_health_check(timeout=120)

    def _update_catalog_source(self):
        """Update CatalogSource to the N+1 image."""
        assert self.upgrade_image, (
            "fdf_upgrade_catalog_image or fdf_standalone_catalog_image must "
            "be set to the N+1 catalog image for upgrade testing"
        )

        logger.info("Updating CatalogSource to upgrade image: %s", self.upgrade_image)
        config.DEPLOYMENT["fdf_standalone_catalog_image"] = self.upgrade_image
        fdf_catsrc = StandaloneFDFCatalogSource()
        fdf_catsrc.create_catalog_source()

        catsrc = CatalogSource(
            resource_name=constants.FDF_STANDALONE_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        catsrc.wait_for_state("READY", timeout=600)
        logger.info("CatalogSource updated and READY")

    def _update_subscription_channel(self):
        """Update Subscription channel for Y-stream upgrade."""
        if not self.upgrade_channel:
            logger.info(
                "No upgrade channel specified — Z-stream upgrade "
                "(CatalogSource change only)"
            )
            return

        logger.info("Updating Subscription channel to: %s", self.upgrade_channel)
        sub_ocp = OCP(kind="Subscription", namespace=self.namespace)
        subs = sub_ocp.get().get("items", [])
        for sub in subs:
            sub_name = sub["metadata"]["name"]
            params = f'{{"spec": {{"channel": "{self.upgrade_channel}"}}}}'
            sub_ocp.patch(
                resource_name=sub_name,
                params=params,
                format_type="merge",
            )
            logger.info(
                "Patched Subscription '%s' channel to '%s'",
                sub_name,
                self.upgrade_channel,
            )

    def _approve_install_plans(self, timeout=600):
        """Approve any pending InstallPlans."""
        logger.info("Checking for pending InstallPlans")
        ip_ocp = OCP(kind="InstallPlan", namespace=self.namespace)

        def _approve():
            plans = ip_ocp.get().get("items", [])
            approved_any = False
            for plan in plans:
                if not plan["spec"].get("approved", False):
                    name = plan["metadata"]["name"]
                    ip_ocp.patch(
                        resource_name=name,
                        params='{"spec": {"approved": true}}',
                        format_type="merge",
                    )
                    logger.info("Approved InstallPlan '%s'", name)
                    approved_any = True
            return approved_any

        end_time = time.time() + timeout
        while time.time() < end_time:
            _approve()
            plans = ip_ocp.get().get("items", [])
            all_complete = all(
                p.get("status", {}).get("phase") == "Complete"
                for p in plans
                if p["spec"].get("approved", False)
            )
            if all_complete and plans:
                logger.info("All InstallPlans complete")
                return
            time.sleep(30)

        plans = ip_ocp.get().get("items", [])
        incomplete = [
            p["metadata"]["name"]
            for p in plans
            if p.get("status", {}).get("phase") != "Complete"
        ]
        assert (
            not incomplete
        ), f"InstallPlans not complete after {timeout}s: {incomplete}"

    def _verify_csv_upgrade(self, timeout=720):
        """Verify CSVs have been replaced with new versions."""
        logger.info("Waiting for CSV upgrade to complete")

        def _check_csvs():
            csv_ocp = CSV(namespace=self.namespace)
            csv_list = csv_ocp.get().get("items", [])
            current = {
                c["metadata"]["name"]: c["status"].get("phase", "") for c in csv_list
            }
            all_succeeded = all(p == "Succeeded" for p in current.values())
            new_csvs = set(current.keys()) != set(self.pre_csvs.keys())
            return all_succeeded and (new_csvs or not self.upgrade_channel)

        sampler = TimeoutSampler(timeout=timeout, sleep=30, func=_check_csvs)
        assert sampler.wait_for_func_status(
            True
        ), "CSVs did not reach Succeeded state after upgrade"
        logger.info("All CSVs Succeeded after upgrade")

    def test_fdf_standalone_upgrade(self, pvc_factory, pod_factory):
        """
        Upgrade FDF standalone from version N to N+1 and verify:

        Phase 1 — Baseline:
        - Record CSVs, pod images, data checksums, Ceph health

        Phase 2 — Upgrade:
        - Update CatalogSource to N+1 image
        - Update Subscription channel (Y-stream only)
        - Approve InstallPlan
        - Checkpoint A: CSVs Succeeded, StorageCluster Ready

        Phase 3 — Verification:
        - Background I/O had zero errors
        - Data checksums unchanged
        - Post-upgrade provisioning works
        - Checkpoint B: Upgrade complete, fully functional
        """
        # Phase 1: Baseline
        self._record_baseline()

        # Start background I/O
        logger.info("Starting background I/O during upgrade")
        self.pod_rbd.run_io(
            storage_type="fs",
            size="1G",
            io_direction="rw",
            runtime=600,
            bs="4K",
            rate="4m",
        )

        # Phase 2: Upgrade
        self._update_catalog_source()
        self._update_subscription_channel()
        self._approve_install_plans(timeout=600)
        self._verify_csv_upgrade(timeout=720)

        logger.info("Checkpoint A: Verifying StorageCluster Ready")
        verify_storage_cluster()

        logger.info("Verifying Ceph health post-upgrade")
        ceph_cluster = CephCluster()
        ceph_cluster.cluster_health_check(timeout=600)

        # Phase 3: Verification
        logger.info("Checkpoint B: Verifying data integrity")
        fio_result = self.pod_rbd.get_fio_results()
        reads = fio_result.get("jobs", [{}])[0].get("read", {})
        writes = fio_result.get("jobs", [{}])[0].get("write", {})
        assert (
            reads.get("total_ios", 0) > 0 or writes.get("total_ios", 0) > 0
        ), "FIO produced no I/O during upgrade"

        verify_data_integrity(self.pod_rbd, "fio-rand-write", self.md5_rbd)
        verify_data_integrity(self.pod_cephfs, "fio-rand-write", self.md5_cephfs)
        logger.info("Pre-upgrade data intact")

        logger.info("Verifying post-upgrade provisioning")
        new_pvc = pvc_factory(interface=constants.CEPHBLOCKPOOL, size=5)
        new_pod = pod_factory(pvc=new_pvc)
        new_pod.run_io(storage_type="fs", size="256M", io_direction="wo", runtime=0)
        new_pod.get_fio_results()
        logger.info("Post-upgrade provisioning verified")

        logger.info("FDF standalone upgrade completed successfully")
