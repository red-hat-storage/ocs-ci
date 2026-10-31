"""
Negative: Invalid CatalogSource Handling (RHSTOR-8290 / TC-12 Phase 1).

Verify FDF handles invalid CatalogSource image gracefully — clear error
state, no crash, and recovery when corrected.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier3,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.deployment.fdf_standalone import StandaloneFDFCatalogSource

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier3
class TestFDFInvalidCatalogSource:
    """
    Verify FDF handles invalid CatalogSource gracefully.
    """

    @pytest.fixture()
    def restore_catalogsource(self):
        """Restore the original CatalogSource after test."""
        yield
        logger.info("Restoring original FDF CatalogSource")
        fdf_catsrc = StandaloneFDFCatalogSource()
        fdf_catsrc.create_catalog_source()
        catsrc = CatalogSource(
            resource_name=constants.FDF_STANDALONE_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        catsrc.wait_for_state("READY", timeout=600)
        logger.info("Original CatalogSource restored")

    def test_invalid_catalog_image_recovery(self, restore_catalogsource):
        """
        Create CatalogSource with non-existent image, verify error state,
        then fix the image and verify recovery.

        Checkpoint A: Invalid catalog produces clear error, no crash.
        """
        catsrc_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        marketplace_ns = constants.MARKETPLACE_NAMESPACE

        # Apply CatalogSource with a non-existent image
        logger.info("Creating CatalogSource with non-existent image")
        bad_image = "quay.io/nonexistent/fake-catalog:does-not-exist-99.99"
        config.DEPLOYMENT["fdf_standalone_catalog_image"] = bad_image
        fdf_catsrc = StandaloneFDFCatalogSource()
        fdf_catsrc.create_catalog_source()

        # Verify it enters an error state (not READY)
        import time

        logger.info("Waiting for CatalogSource to show error state")
        catsrc_ocp = OCP(kind="CatalogSource", namespace=marketplace_ns)
        error_detected = False
        end_time = time.time() + 120
        while time.time() < end_time:
            catsrc_data = catsrc_ocp.get(resource_name=catsrc_name)
            status = catsrc_data.get("status", {})
            state = status.get("connectionState", {}).get("lastObservedState", "")
            if state and state != "READY":
                logger.info("CatalogSource state: '%s' (expected non-READY)", state)
                error_detected = True
                break
            time.sleep(10)

        assert (
            error_detected
        ), "CatalogSource did not enter error state with invalid image"
        logger.info("Checkpoint A: Invalid catalog handled — state: %s", state)

        # Verify no operator pods crashed
        logger.info("Verifying no operator pods in CrashLoopBackOff")
        pod_ocp = OCP(kind="pod", namespace=config.ENV_DATA["cluster_namespace"])
        pods = pod_ocp.get().get("items", [])
        crash_pods = [
            p["metadata"]["name"]
            for p in pods
            if any(
                cs.get("state", {}).get("waiting", {}).get("reason")
                == "CrashLoopBackOff"
                for cs in p.get("status", {}).get("containerStatuses", [])
            )
        ]
        assert not crash_pods, (
            f"Operator pods in CrashLoopBackOff after invalid catalog: " f"{crash_pods}"
        )

        # Fix: restore valid image — done by restore_catalogsource fixture
        logger.info("Invalid CatalogSource error handling verified")
