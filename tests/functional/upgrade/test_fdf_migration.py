"""
ODF -> FDF (IBM Fusion Data Foundation) migration test.

This is distinct from the FDF upgrade test (test_upgrade.py::test_fdf_upgrade)
which handles FDF-to-FDF version upgrades.  This test handles the initial
migration from Red Hat ODF to IBM Fusion Data Foundation by switching the
catalog source and re-pointing subscriptions.

Run with:
    run-ci tests/functional/upgrade/test_fdf_migration.py \
        --cluster-name <n> \
        --cluster-path <path> \
        -m 'pre_fdf_migration or fdf_migration or post_fdf_migration' \
        --ocsci-conf conf/upgrade/fdf_migration.yaml \
        --ocsci-conf conf/ocsci/manual_subscription_plan_approval.yaml
"""

import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    pre_fdf_migration,
    fdf_migration,
    post_fdf_migration,
)
from ocs_ci.framework.pytest_customization.marks import purple_squad
from ocs_ci.ocs.cluster import ceph_health_check
from ocs_ci.ocs.fdf_upgrade import FDFUpgrade
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded
from ocs_ci.deployment.fusion_data_foundation import storagecluster_health_check

logger = logging.getLogger(__name__)


@purple_squad
@ignore_leftovers
@pre_fdf_migration
class TestPreFDFMigration(ManageTest):
    """Sanity checks before the ODF -> FDF migration."""

    def test_ceph_health_pre_fdf_migration(self):
        """Verify Ceph cluster is healthy before migration."""
        ceph_health_check(tries=20, delay=30)
        logger.info("Pre-FDF-migration: Ceph cluster health is OK.")

    def test_all_csvs_succeeded_pre_fdf_migration(self):
        """Verify all CSVs are in Succeeded state before migration."""
        namespace = config.ENV_DATA["cluster_namespace"]
        logger.assertion(f"All CSVs in namespace '{namespace}' are Succeeded")
        assert check_all_csvs_are_succeeded(namespace=namespace)


@purple_squad
@ignore_leftovers
@fdf_migration
class TestFDFMigration(ManageTest):
    """
    Main ODF -> FDF migration test.

    Reads ``config.UPGRADE['fdf_registry_image']`` for the target catalog
    image.
    """

    def test_fdf_migration(self):
        """
        Full ODF -> FDF migration:
          1. Pre-flight pod health check.
          2. Create ISF FDF CatalogSource.
          3. Patch ODF subscriptions to the new catalog.
          4. Approve the pending InstallPlan (Manual strategy).
          5. Wait for all CSVs to reach Succeeded.
          6. Post-migration OCS install verification.
        """
        fdf_image = config.UPGRADE.get("fdf_registry_image")
        assert fdf_image, (
            "fdf_registry_image must be set in config.UPGRADE. "
            "Pass via --ocsci-conf conf/upgrade/fdf_migration.yaml."
        )
        namespace = config.ENV_DATA["cluster_namespace"]
        odf_version = config.ENV_DATA.get("ocs_version")

        logger.test_step("Starting ODF -> FDF migration")
        logger.info(f"Catalog image: {fdf_image}")

        fdf_migration_obj = FDFUpgrade(
            namespace=namespace,
            version_before_upgrade=odf_version,
        )
        fdf_migration_obj.run_migration()


@purple_squad
@ignore_leftovers
@post_fdf_migration
class TestPostFDFMigration(ManageTest):
    """Sanity checks after the ODF -> FDF migration completes."""

    def test_ceph_health_post_fdf_migration(self):
        """Verify Ceph cluster is healthy after migration."""
        ceph_health_check(tries=40, delay=30)
        logger.info("Post-FDF-migration: Ceph cluster health is OK.")

    def test_all_csvs_succeeded_post_fdf_migration(self):
        """Verify all CSVs are in Succeeded state after migration."""
        namespace = config.ENV_DATA["cluster_namespace"]
        logger.assertion(f"All CSVs in namespace '{namespace}' are Succeeded")
        assert check_all_csvs_are_succeeded(namespace=namespace)

    def test_storagecluster_health_post_fdf_migration(self):
        """Verify StorageCluster is Ready after FDF migration."""
        storagecluster_health_check()
        logger.info("Post-FDF-migration: StorageCluster is healthy.")
