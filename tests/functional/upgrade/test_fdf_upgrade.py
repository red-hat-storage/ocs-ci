"""
ODF -> FDF (IBM Fusion Data Foundation) upgrade test.

Run with:
    run-ci tests/ecosystem/upgrade/test_fdf_upgrade.py \
        --cluster-name <n> \
        --cluster-path <path> \
        -m 'pre_fdf_upgrade or fdf_upgrade or post_fdf_upgrade' \
        --ocsci-conf conf/upgrade/fdf_upgrade.yaml \
        --ocsci-conf conf/ocsci/manual_subscription_plan_approval.yaml
"""
import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    pre_fdf_upgrade,
    fdf_upgrade,
    post_fdf_upgrade,
)
from ocs_ci.ocs.cluster import CephCluster, ceph_health_check
from ocs_ci.ocs.fdf_upgrade import run_fdf_upgrade
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded
from ocs_ci.deployment.fusion_data_foundation import storagecluster_health_check

logger = logging.getLogger(__name__)


@ignore_leftovers
@pre_fdf_upgrade
class TestPreFDFUpgrade(ManageTest):
    """Sanity checks before the ODF -> FDF upgrade."""

    def test_ceph_health_pre_fdf_upgrade(self):
        """Verify Ceph cluster is healthy before upgrade."""
        ceph_health_check(tries=20, delay=30)
        logger.info("Pre-FDF-upgrade: Ceph cluster health is OK.")

    def test_all_csvs_succeeded_pre_fdf_upgrade(self):
        namespace = config.ENV_DATA["cluster_namespace"]
        assert check_all_csvs_are_succeeded(namespace=namespace)


@ignore_leftovers
@fdf_upgrade
class TestFDFUpgrade(ManageTest):
    """
    Main ODF -> FDF upgrade test.
    Reads config.UPGRADE['fdf_registry_image'] for the target catalog image.
    """

    def test_fdf_upgrade(self):
        """
        Full ODF -> FDF upgrade:
          1. Pre-flight pod health check.
          2. Create ISF FDF CatalogSource.
          3. Patch ODF subscriptions to the new catalog.
          4. Approve the pending InstallPlan (Manual strategy).
          5. Wait for all CSVs to reach Succeeded.
          6. Post-upgrade OCS install verification.
        """
        fdf_image = config.UPGRADE.get("fdf_registry_image")
        assert fdf_image, (
            "fdf_registry_image must be set in config.UPGRADE. "
            "Pass via --ocsci-conf conf/upgrade/fdf_upgrade.yaml."
        )
        logger.info(f"Starting ODF -> FDF upgrade. Catalog image: {fdf_image}")
        run_fdf_upgrade()
        logger.info("TestFDFUpgrade.test_fdf_upgrade: PASSED")


@ignore_leftovers
@post_fdf_upgrade
class TestPostFDFUpgrade(ManageTest):
    """Sanity checks after the ODF -> FDF upgrade completes."""

    def test_ceph_health_post_fdf_upgrade(self):
        """Verify Ceph cluster is healthy after upgrade."""
        ceph_health_check(tries=40, delay=30)   # more tries post-upgrade
        logger.info("Post-FDF-upgrade: Ceph cluster health is OK.")

    def test_all_csvs_succeeded_post_fdf_upgrade(self):
        namespace = config.ENV_DATA["cluster_namespace"]
        assert check_all_csvs_are_succeeded(namespace=namespace)

    def test_storagecluster_health_post_fdf_upgrade(self):
        """Verify StorageCluster is Ready and Ceph is HEALTH_OK after FDF upgrade."""
        storagecluster_health_check()