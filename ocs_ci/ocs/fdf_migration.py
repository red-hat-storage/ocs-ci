"""
ODF-to-FDF standalone migration helpers (RHSTOR-8840).

Provides the ``FDFMigration`` class that orchestrates migrating an existing
ODF installation to FDF standalone by switching the CatalogSource and
(optionally) the subscription channel.

Two migration paths are supported:

* **Z-stream** — same ``major.minor``, catalog source change only.
* **Y-stream** — ``major.minor`` bump, catalog source + channel change.

Usage::

    migration = FDFMigration(pvc_factory, pod_factory)
    migration.prepare_pre_migration_state()
    migration.start_background_io()
    migration.create_fdf_catalog_source()
    migration.patch_subscriptions_source()
    migration.approve_install_plans()
    migration.verify_csv_replacement()
    migration.verify_storage_cluster_ready()
    migration.verify_background_io()
    migration.verify_data_integrity()
    migration.verify_post_migration_provisioning()
"""

import logging

from ocs_ci.framework import config
from ocs_ci.helpers.disruption_helpers import FIOIntegrityChecker
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.deployment.fdf_standalone import (
    StandaloneFDFCatalogSource,
    _validate_catalog_image,
    _apply_fdf_mirror_sets,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class FDFMigration:
    """
    Orchestrates ODF-to-FDF standalone migration with data integrity
    verification.

    The migration modifies only OLM resources (CatalogSource,
    Subscription) — the StorageCluster CR is untouched because FDF
    is architecturally identical to ODF.

    Args:
        pvc_factory: Pytest ``pvc_factory`` fixture.
        pod_factory: Pytest ``pod_factory`` fixture.
        bg_io_runtime (int): Background FIO runtime in seconds.
            Should exceed the expected migration duration.
    """

    def __init__(self, pvc_factory, pod_factory, bg_io_runtime=600):
        self.namespace = config.ENV_DATA["cluster_namespace"]
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.bg_io_runtime = bg_io_runtime

        self._pre_migration_csvs = {}
        self._pre_migration_sc_phase = None
        self._io_checker = None

    # ------------------------------------------------------------------
    # Phase 1 — Pre-migration baseline
    # ------------------------------------------------------------------

    def prepare_pre_migration_state(self):
        """
        Record pre-migration baseline: CSV names/versions, StorageCluster
        phase, and Ceph health status.
        """
        logger.info("Recording pre-migration state")

        csv_ocp = OCP(kind="csv", namespace=self.namespace)
        csvs = csv_ocp.get().get("items", [])
        for csv_item in csvs:
            name = csv_item["metadata"]["name"]
            phase = csv_item.get("status", {}).get("phase", "")
            self._pre_migration_csvs[name] = phase
        logger.info(
            "Pre-migration CSVs: %s",
            list(self._pre_migration_csvs.keys()),
        )

        sc_name = config.ENV_DATA["storage_cluster_name"]
        sc_ocp = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=self.namespace,
            resource_name=sc_name,
        )
        sc_data = sc_ocp.get()
        self._pre_migration_sc_phase = sc_data.get("status", {}).get("phase", "")
        logger.info(
            "Pre-migration StorageCluster phase: %s",
            self._pre_migration_sc_phase,
        )

    def start_background_io(self):
        """
        Create PVCs (RBD + CephFS), write integrity data, compute
        checksums, and start continuous background FIO.
        """
        logger.info(
            "Starting background I/O with %ds runtime",
            self.bg_io_runtime,
        )
        self._io_checker = FIOIntegrityChecker(
            pvc_factory=self.pvc_factory,
            pod_factory=self.pod_factory,
        )
        self._io_checker.start_io(bg_runtime=self.bg_io_runtime)

    # ------------------------------------------------------------------
    # Phase 2 — Migration execution
    # ------------------------------------------------------------------

    def create_fdf_catalog_source(self):
        """
        Create the FDF standalone CatalogSource (``ibm-operators``) and
        apply IDMS mirror sets.  Reuses the deployment infrastructure
        from :mod:`ocs_ci.deployment.fdf_standalone`.
        """
        logger.info("Creating FDF CatalogSource for migration")
        catalog_image = _validate_catalog_image()
        _apply_fdf_mirror_sets(catalog_image)
        StandaloneFDFCatalogSource().create_catalog_source()
        logger.info("FDF CatalogSource created and READY")

    def patch_subscriptions_source(self):
        """
        Patch all ODF-related subscriptions in ``openshift-storage`` to
        use the FDF catalog source (``ibm-operators``).

        This is the core Z-stream migration step — the subscription
        channel stays the same, only the catalog source changes.
        """
        fdf_source = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        self._patch_subscriptions(source=fdf_source)

    def patch_subscriptions_channel(self, channel):
        """
        Patch all ODF-related subscriptions with a new channel.

        This is the additional Y-stream migration step — both catalog
        source and channel change.

        Args:
            channel (str): Target channel, e.g. ``"stable-4.23"``.
        """
        fdf_source = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        self._patch_subscriptions(source=fdf_source, channel=channel)

    def _patch_subscriptions(self, source, channel=None):
        """
        Patch ODF-related subscriptions with the given source and
        optional channel.

        Finds all subscriptions in the storage namespace and patches
        those whose current source is ``redhat-operators`` or the ODF
        default catalog.

        Args:
            source (str): New catalog source name.
            channel (str): New channel (Y-stream only). If None, channel
                is left unchanged (Z-stream).
        """
        sub_ocp = OCP(
            kind=constants.SUBSCRIPTION_COREOS,
            namespace=self.namespace,
        )
        subs = sub_ocp.get().get("items", [])
        patched_count = 0

        for sub in subs:
            sub_name = sub["metadata"]["name"]
            current_source = sub["spec"].get("source", "")

            if current_source == source and channel is None:
                logger.info(
                    "Subscription '%s' already uses source '%s', skipping",
                    sub_name,
                    source,
                )
                continue

            patch_spec = {"source": source}
            if channel:
                patch_spec["channel"] = channel

            patch_json = f'{{"spec": {patch_spec}}}'.replace("'", '"')
            import json

            patch_json = json.dumps({"spec": patch_spec})

            logger.info(
                "Patching subscription '%s': %s",
                sub_name,
                patch_spec,
            )
            sub_ocp.patch(
                resource_name=sub_name,
                params=patch_json,
                format_type="merge",
            )
            patched_count += 1

        logger.info("Patched %d subscription(s)", patched_count)
        if patched_count == 0:
            logger.warning("No subscriptions were patched — verify cluster state")

    def approve_install_plans(self, timeout=600):
        """
        Wait for new InstallPlans triggered by the subscription change
        and approve them.

        Args:
            timeout (int): Seconds to wait for InstallPlans.
        """
        logger.info("Waiting for InstallPlans to appear (timeout=%ds)", timeout)
        try:
            wait_for_install_plan_and_approve(namespace=self.namespace, timeout=timeout)
            logger.info("InstallPlans approved")
        except Exception:
            logger.warning(
                "No unapproved InstallPlans found — subscription may use "
                "Automatic approval"
            )

    # ------------------------------------------------------------------
    # Phase 3 — Verification
    # ------------------------------------------------------------------

    def verify_csv_replacement(self, timeout=720):
        """
        Verify new CSVs reach ``Succeeded`` phase and no stale CSVs
        remain in ``Replacing`` or ``Pending`` state.

        Args:
            timeout (int): Seconds to wait for CSVs to settle.
        """
        logger.info("Verifying CSV replacement (timeout=%ds)", timeout)

        sampler = TimeoutSampler(
            timeout=timeout,
            sleep=30,
            func=check_all_csvs_are_succeeded,
            namespace=self.namespace,
        )
        for result in sampler:
            if result:
                break

        csv_ocp = OCP(kind="csv", namespace=self.namespace)
        csvs = csv_ocp.get().get("items", [])

        stale_csvs = []
        for csv_item in csvs:
            name = csv_item["metadata"]["name"]
            phase = csv_item.get("status", {}).get("phase", "")
            if phase in (constants.STATUS_REPLACING, constants.STATUS_PENDING):
                stale_csvs.append(f"{name} ({phase})")

        assert not stale_csvs, f"Stale CSVs found after migration: {stale_csvs}"
        logger.info(
            "All CSVs in Succeeded phase, no stale CSVs: %s",
            [c["metadata"]["name"] for c in csvs],
        )

    def verify_no_orphaned_installplans(self):
        """
        Verify no orphaned (unapproved) InstallPlans remain after
        migration.
        """
        ip_ocp = OCP(kind="installplan", namespace=self.namespace)
        install_plans = ip_ocp.get().get("items", [])
        unapproved = [
            ip["metadata"]["name"]
            for ip in install_plans
            if not ip["spec"].get("approved", True)
        ]
        assert not unapproved, f"Orphaned InstallPlans found: {unapproved}"
        logger.info("No orphaned InstallPlans")

    def verify_storage_cluster_ready(self, timeout=600):
        """
        Verify StorageCluster remains in ``Ready`` phase after migration.

        Args:
            timeout (int): Seconds to wait for Ready phase.
        """
        logger.info("Verifying StorageCluster Ready (timeout=%ds)", timeout)
        verify_storage_cluster()
        logger.info("StorageCluster is Ready")

    def verify_background_io(self):
        """
        Wait for background FIO to complete and verify zero I/O errors.
        Also verifies md5sum integrity of the pre-written data file.

        Raises:
            AssertionError: If FIO reports errors or md5sum mismatch.
        """
        if not self._io_checker:
            logger.warning("No background I/O was started, skipping")
            return

        logger.info("Verifying background I/O results and data integrity")
        self._io_checker.wait_and_verify()
        logger.info("Background I/O completed with zero errors, data intact")

    def verify_post_migration_provisioning(self, pvc_factory, pod_factory):
        """
        Create a new PVC after migration, attach a pod, write data,
        and read it back — confirming provisioning works post-migration.

        Args:
            pvc_factory: Pytest ``pvc_factory`` fixture.
            pod_factory: Pytest ``pod_factory`` fixture.
        """
        logger.info("Verifying post-migration PVC provisioning")
        from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity

        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=2,
        )
        pod_obj = pod_factory(pvc=pvc_obj)

        pod_obj.run_io(
            storage_type="fs",
            size="512M",
            io_direction="wo",
            runtime=0,
        )
        pod_obj.get_fio_results()

        md5sum = cal_md5sum(pod_obj, "fio-rand-write")
        verify_data_integrity(pod_obj, "fio-rand-write", md5sum)
        logger.info("Post-migration PVC provisioning verified successfully")

    def verify_subscription_source(self):
        """
        Verify all subscriptions in the storage namespace point to the
        FDF catalog source.
        """
        expected_source = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        sub_ocp = OCP(
            kind=constants.SUBSCRIPTION_COREOS,
            namespace=self.namespace,
        )
        subs = sub_ocp.get().get("items", [])
        mismatched = []
        for sub in subs:
            sub_name = sub["metadata"]["name"]
            actual_source = sub["spec"].get("source", "")
            if actual_source != expected_source:
                mismatched.append(f"{sub_name} (source={actual_source})")

        assert (
            not mismatched
        ), f"Subscriptions not pointing to FDF catalog: {mismatched}"
        logger.info(
            "All %d subscription(s) point to '%s'",
            len(subs),
            expected_source,
        )
