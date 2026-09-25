"""
ODF-to-FDF standalone migration tests (RHSTOR-8840 / OCS-8192).

Validates migrating an existing ODF installation to FDF standalone by
switching the CatalogSource (Z-stream), with data integrity verification
throughout.

The tests require:
- A cluster with ODF already installed and StorageCluster Ready.
- ``fdf_standalone_catalog_image`` or ``ocs_registry_image`` set in config
  pointing to the FDF catalog image.

Pre-existing ODF workloads with RBD + CephFS PVCs are created by the
test fixtures and verified for data integrity across the migration.
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    purple_squad,
    tier1,
)
from ocs_ci.ocs.fdf_migration import FDFMigration

logger = logging.getLogger(__name__)


@purple_squad
@tier1
@pytest.mark.polarion_id("OCS-8192")
class TestODFToFDFZStreamMigration:
    """
    ODF to FDF Standalone Migration — Z-Stream (catalog source change only).

    Validates the migration path where the ODF subscription source is
    switched from ``redhat-operators`` to the FDF ``ibm-operators``
    catalog within the same ``major.minor`` version.

    Checkpoints:
        A: New CSVs reach Succeeded, old CSVs replaced, no stale CSVs.
        B: StorageCluster Ready, zero I/O errors, data intact, new
           PVC provisioning works post-migration.
    """

    @pytest.fixture(autouse=True)
    def setup_migration(self, pvc_factory, pod_factory):
        """
        Set up FDFMigration helper with PVC/pod factories.
        """
        self.migration = FDFMigration(
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            bg_io_runtime=600,
        )
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory

    def test_odf_to_fdf_zstream_migration(self, pvc_factory, pod_factory):
        """
        Migrate ODF to FDF standalone via Z-stream (catalog source change).

        Steps:
            Phase 1 — Pre-Migration Baseline:
            1. Record CSV names/versions, StorageCluster status, Ceph health.
            2. Record data checksums on test PVCs.
            3. Start background I/O workload (FIO, continuous random write).

            Phase 2 — Z-Stream Migration:
            4. Create FDF CatalogSource (same major.minor as current ODF).
            5. Patch all ODF Subscriptions: change spec.source to FDF catalog.
            6. Approve InstallPlans.

            Phase 3 — Verification:
            7. [Checkpoint A] New CSVs reach Succeeded, no stale CSVs in
               Replacing or Pending state, no orphaned InstallPlans.
            8. [Checkpoint B] StorageCluster Ready, background I/O had zero
               errors, data checksums unchanged, new PVC post-migration
               works.
        """
        logger.info("=== Phase 1: Pre-Migration Baseline ===")
        self.migration.prepare_pre_migration_state()
        self.migration.start_background_io()

        logger.info("=== Phase 2: Z-Stream Migration ===")
        self.migration.create_fdf_catalog_source()
        self.migration.patch_subscriptions_source()
        self.migration.approve_install_plans()

        logger.info("=== Phase 3: Verification ===")

        logger.info("--- Checkpoint A: CSV replacement ---")
        self.migration.verify_csv_replacement()
        self.migration.verify_no_orphaned_installplans()
        self.migration.verify_subscription_source()

        logger.info("--- Checkpoint B: Data integrity and cluster health ---")
        self.migration.verify_storage_cluster_ready()
        self.migration.verify_background_io()
        self.migration.verify_post_migration_provisioning(pvc_factory, pod_factory)

        logger.info("=== Z-Stream migration completed successfully ===")
