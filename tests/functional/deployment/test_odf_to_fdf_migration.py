"""
ODF-to-FDF standalone migration tests (RHSTOR-8840 / OCS-8192).

Validates migrating an existing ODF installation to FDF standalone by
switching the CatalogSource (Z-stream) or CatalogSource + channel
(Y-stream), with data integrity verification throughout.

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


@purple_squad
@tier1
@pytest.mark.polarion_id("OCS-8192")
class TestODFToFDFYStreamMigration:
    """
    ODF to FDF Standalone Migration — Y-Stream (catalog + channel change).

    Validates the migration path where both the subscription catalog
    source AND channel are changed, representing a version bump
    (e.g., ODF 4.22 to FDF 4.23).

    Checkpoints:
        C: Y-stream migration complete — new CSVs Succeeded, all
           component pods rolled to new images, data intact.
    """

    @pytest.fixture(autouse=True)
    def setup_migration(self, pvc_factory, pod_factory):
        """
        Set up FDFMigration helper with PVC/pod factories.
        """
        self.migration = FDFMigration(
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            bg_io_runtime=900,
        )
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory

    def test_odf_to_fdf_ystream_migration(
        self, pvc_factory, pod_factory, fdf_target_channel
    ):
        """
        Migrate ODF to FDF standalone via Y-stream (catalog + channel).

        Steps:
            Phase 1 — Pre-Migration Baseline:
            1. Record CSV names/versions, StorageCluster status, Ceph health.
            2. Start background I/O with checksummed integrity data.

            Phase 2 — Y-Stream Migration:
            3. Create FDF CatalogSource.
            4. Patch Subscriptions: change spec.source to FDF catalog AND
               spec.channel to new channel.
            5. Approve InstallPlans.
            6. Verify all component pods roll to new images.

            Phase 3 — Verification:
            7. [Checkpoint C] Y-stream migration complete — CSVs Succeeded,
               no stale CSVs, StorageCluster Ready, I/O zero errors,
               data intact, new PVC works.

        Args:
            fdf_target_channel: Pytest fixture providing the target FDF
                channel (e.g., ``"stable-4.23"``).
        """
        logger.info("=== Phase 1: Pre-Migration Baseline ===")
        self.migration.prepare_pre_migration_state()
        self.migration.start_background_io()

        logger.info("=== Phase 2: Y-Stream Migration ===")
        self.migration.create_fdf_catalog_source()
        self.migration.patch_subscriptions_channel(channel=fdf_target_channel)
        self.migration.approve_install_plans(timeout=900)

        logger.info("=== Phase 3: Verification ===")

        logger.info("--- Checkpoint C: Y-Stream migration complete ---")
        self.migration.verify_csv_replacement(timeout=900)
        self.migration.verify_no_orphaned_installplans()
        self.migration.verify_subscription_source()
        self.migration.verify_storage_cluster_ready()
        self.migration.verify_background_io()
        self.migration.verify_post_migration_provisioning(pvc_factory, pod_factory)

        logger.info("=== Y-Stream migration completed successfully ===")
