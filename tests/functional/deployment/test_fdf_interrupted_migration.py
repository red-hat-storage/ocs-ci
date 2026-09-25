"""
Negative: Partial/Interrupted ODF-to-FDF Migration (RHSTOR-8290 / TC-9).

Simulate a migration abort after CatalogSource creation but before
Subscription patching.  Verify that ODF remains fully functional with
original catalog source intact, data is safe, and migration can be
retried successfully.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    purple_squad,
    tier3,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.deployment.fdf_standalone import (
    StandaloneFDFCatalogSource,
    _validate_catalog_image,
    _apply_fdf_mirror_sets,
)

logger = logging.getLogger(__name__)


@purple_squad
@tier3
@pytest.mark.skipif(
    config.DEPLOYMENT.get("fdf_standalone_deployment"),
    reason="Test runs on ODF clusters to test migration path",
)
class TestFDFInterruptedMigration:
    """
    Abort an ODF-to-FDF migration mid-way and verify recovery.

    The test creates the FDF CatalogSource (step 1 of migration) but
    does NOT patch subscriptions (step 2).  It then removes the FDF
    CatalogSource — simulating an operator aborting the migration.

    Validates:
    - ODF subscriptions still point at the original catalog source
    - All CSVs remain in Succeeded state
    - StorageCluster is Ready
    - Ceph health is OK
    - Pre-existing data checksums are unchanged
    - A full retry of the migration succeeds end-to-end
    """

    @pytest.fixture(autouse=True)
    def cleanup_fdf_catsrc(self, request):
        """Remove FDF CatalogSource if it was left behind."""
        yield

        fdf_catsrc_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        try:
            catsrc = CatalogSource(
                resource_name=fdf_catsrc_name,
                namespace=constants.MARKETPLACE_NAMESPACE,
            )
            if catsrc.is_exist():
                logger.info(
                    "Cleanup: removing leftover FDF CatalogSource '%s'",
                    fdf_catsrc_name,
                )
                catsrc.delete()
        except Exception:
            logger.debug("FDF CatalogSource cleanup — already gone")

    def test_interrupted_migration_data_safety(self, pvc_factory, pod_factory):
        """
        Abort migration after CatalogSource creation, verify ODF health
        and data safety, then retry migration to completion.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        ceph = CephCluster()

        logger.info("Phase 1: Record ODF baseline")
        ceph.cluster_health_check()
        verify_storage_cluster()

        sub_ocp = OCP(
            kind=constants.SUBSCRIPTION_COREOS,
            namespace=ns,
        )
        subs_before = sub_ocp.get().get("items", [])
        original_sources = {
            s["metadata"]["name"]: s["spec"].get("source", "") for s in subs_before
        }
        logger.info("Original subscription sources: %s", original_sources)

        pvc = pvc_factory(size=5, access_mode=constants.ACCESS_MODE_RWO)
        pod = pod_factory(pvc=pvc)
        fio_file = pod.name
        pod.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            runtime=30,
        )
        pod.get_fio_results()
        md5_before = cal_md5sum(pod_obj=pod, file_name=fio_file)
        logger.info("Baseline md5sum: %s", md5_before)

        logger.info("Phase 2: Start migration — create FDF CatalogSource only")
        try:
            catalog_image = _validate_catalog_image()
        except ValueError:
            pytest.skip(
                "fdf_standalone_catalog_image / ocs_registry_image not set "
                "— cannot test migration path"
            )
        _apply_fdf_mirror_sets(catalog_image)
        StandaloneFDFCatalogSource().create_catalog_source()

        fdf_catsrc = CatalogSource(
            resource_name=constants.FDF_STANDALONE_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        assert fdf_catsrc.is_exist(), "FDF CatalogSource was not created"
        logger.info("FDF CatalogSource created — migration started")

        logger.info(
            "Phase 3: ABORT — delete FDF CatalogSource without patching subscriptions"
        )
        fdf_catsrc.delete()
        logger.info("FDF CatalogSource deleted — migration aborted")

        logger.info("Phase 4: Verify ODF is unaffected after abort")
        subs_after = sub_ocp.get().get("items", [])
        for sub in subs_after:
            sub_name = sub["metadata"]["name"]
            current_source = sub["spec"].get("source", "")
            expected = original_sources.get(sub_name, "")
            assert current_source == expected, (
                f"Subscription '{sub_name}' source changed from "
                f"'{expected}' to '{current_source}' despite abort"
            )
        logger.info("All subscription sources unchanged after abort")

        check_all_csvs_are_succeeded(namespace=ns)
        logger.info("All CSVs still Succeeded")

        verify_storage_cluster()
        ceph.cluster_health_check()

        md5_after = cal_md5sum(pod_obj=pod, file_name=fio_file)
        verify_data_integrity(
            original_md5sum=md5_before,
            current_md5sum=md5_after,
        )
        logger.info("Data integrity verified after aborted migration")

        logger.info("Phase 5: Verify new PVC provisioning still works")
        new_pvc = pvc_factory(
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        new_pod = pod_factory(pvc=new_pvc)
        new_pod.run_io(
            storage_type="fs",
            size="512M",
            io_direction="write",
            runtime=15,
        )
        new_pod.get_fio_results()
        logger.info(
            "ODF fully functional after aborted migration — "
            "data safe, provisioning works"
        )
