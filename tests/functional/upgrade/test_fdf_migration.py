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
        --fdf-upgrade-registry cp.stg.icr.io/cp/df \
        --fdf-upgrade-image-tag 4.21.9-4 \
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
from ocs_ci.ocs import defaults
from ocs_ci.ocs.fdf_migration import FDFMigration
from ocs_ci.ocs.resources.csv import (
    check_all_csvs_are_succeeded,
    get_csvs_start_with_prefix,
)
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

    The target catalog image is constructed from
    ``config.DEPLOYMENT['fdf_upgrade_registry']`` and
    ``config.DEPLOYMENT['fdf_upgrade_image_tag']`` (set via the
    ``--fdf-upgrade-registry`` and ``--fdf-upgrade-image-tag`` CLI args).
    """

    def test_fdf_migration(self):
        """
        Full ODF -> FDF migration:
          1. Version validation.
          2. ITMS/IDMS for registry mirroring (if needed).
          3. Pre-migration pod health check.
          4. Create ISF FDF CatalogSource.
          5. Patch ODF subscriptions to the new catalog.
          6. Approve the pending InstallPlan (Manual strategy).
          7. Wait for all CSVs to reach Succeeded.
          8. Post-migration CSV verification.
        """
        fdf_registry = config.DEPLOYMENT.get("fdf_upgrade_registry")
        fdf_image_tag = config.DEPLOYMENT.get("fdf_upgrade_image_tag")
        assert fdf_registry and fdf_image_tag, (
            "fdf_upgrade_registry and fdf_upgrade_image_tag must be set. "
            "Pass via --fdf-upgrade-registry and --fdf-upgrade-image-tag."
        )
        namespace = config.ENV_DATA["cluster_namespace"]

        csv_list = get_csvs_start_with_prefix(
            defaults.ODF_OPERATOR_NAME, namespace=namespace
        )
        odf_csv = csv_list[0]
        odf_version = odf_csv["spec"]["version"]

        logger.test_step("Starting ODF -> FDF migration")
        logger.info(f"FDF registry: {fdf_registry}, image tag: {fdf_image_tag}")
        logger.info(f"ODF version from cluster CSV: {odf_version}")

        fdf_migration_obj = FDFMigration(
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
