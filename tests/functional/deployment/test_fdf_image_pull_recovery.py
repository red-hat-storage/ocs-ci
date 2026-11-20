"""
FDF Installation with Image Pull Issues (RHSTOR-8290 / TC-19).

Verify FDF provides clear errors when images cannot be pulled (bad
CatalogSource image) and recovers automatically when the image is
corrected.  Unlike test_fdf_invalid_catalogsource which tests a
completely nonexistent image, this test uses a real registry with
an unreachable tag to trigger ImagePullBackOff on the catalog pod.
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
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

BAD_IMAGE = "cp.stg.icr.io/cp/df/isf-data-foundation-catalog:does-not-exist-99.99"


@fdf_standalone_required
@purple_squad
@tier3
class TestFDFImagePullRecovery:
    """
    Corrupt the FDF CatalogSource image reference, verify the cluster
    reports clear errors without crashing, then restore and verify
    recovery.

    Steps:
        1. Record healthy baseline (CatalogSource image, operator UIDs).
        2. Patch CatalogSource with an unreachable image tag.
        3. Verify catalog pod enters ImagePullBackOff / error state.
        4. Verify existing operator pods remain Running (no cascade crash).
        5. Verify StorageCluster and Ceph health are still OK.
        6. Restore the original CatalogSource image.
        7. Verify CatalogSource returns to READY.
        8. Verify operator pods are still healthy.
    """

    @pytest.fixture(autouse=True)
    def restore_catalogsource(self, request):
        """Save original CatalogSource image and restore on teardown."""
        self.ns = config.ENV_DATA["cluster_namespace"]
        catsrc_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        catsrc = CatalogSource(
            resource_name=catsrc_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        assert (
            catsrc.is_exist()
        ), f"CatalogSource '{catsrc_name}' not found — not an FDF cluster?"

        catsrc_data = catsrc.get()
        self.original_image = catsrc_data["spec"]["image"]
        logger.info("Saved original CatalogSource image: %s", self.original_image)

        yield

        logger.info(
            "Teardown: restoring CatalogSource image to '%s'", self.original_image
        )
        patch = f'{{"spec": {{"image": "{self.original_image}"}}}}'
        catsrc_ocp = OCP(
            kind="CatalogSource",
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        catsrc_ocp.patch(
            resource_name=catsrc_name,
            params=patch,
            format_type="merge",
        )
        catsrc.wait_for_state("READY", timeout=600)
        logger.info("CatalogSource restored and READY")

    def test_image_pull_error_and_recovery(self):
        """Corrupt CatalogSource image, verify error, restore, verify recovery."""
        catsrc_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        ceph = CephCluster()

        logger.info("Step 1: Record baseline operator pod UIDs")
        operator_pods_before = get_pods_having_label(
            label=constants.OPERATOR_LABEL,
            namespace=self.ns,
        )
        operator_uids = {
            p["metadata"]["name"]: p["metadata"]["uid"] for p in operator_pods_before
        }
        logger.info("Baseline operator pods: %s", list(operator_uids.keys()))

        logger.info(
            "Step 2: Patch CatalogSource with unreachable image '%s'",
            BAD_IMAGE,
        )
        catsrc_ocp = OCP(
            kind="CatalogSource",
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        patch = f'{{"spec": {{"image": "{BAD_IMAGE}"}}}}'
        catsrc_ocp.patch(
            resource_name=catsrc_name,
            params=patch,
            format_type="merge",
        )

        logger.info("Step 3: Verify catalog pod enters error state")
        catalog_pod_label = f"olm.catalogSource={catsrc_name}"
        error_detected = False
        for pods in TimeoutSampler(
            timeout=300,
            sleep=15,
            func=get_pods_having_label,
            label=catalog_pod_label,
            namespace=constants.MARKETPLACE_NAMESPACE,
        ):
            if not pods:
                continue
            for pod in pods:
                container_statuses = pod.get("status", {}).get("containerStatuses", [])
                for cs in container_statuses:
                    waiting = cs.get("state", {}).get("waiting", {})
                    reason = waiting.get("reason", "")
                    if reason in (
                        "ImagePullBackOff",
                        "ErrImagePull",
                        "CrashLoopBackOff",
                    ):
                        logger.info(
                            "Catalog pod '%s' entered '%s' state as expected",
                            pod["metadata"]["name"],
                            reason,
                        )
                        error_detected = True
                        break
                if error_detected:
                    break
            if error_detected:
                break

        assert error_detected, (
            "Catalog pod did not enter ImagePullBackOff/ErrImagePull "
            "within timeout — bad image may have been pulled somehow"
        )

        logger.info("Step 4: Verify existing operator pods remain Running")
        operator_pods_current = get_pods_having_label(
            label=constants.OPERATOR_LABEL,
            namespace=self.ns,
        )
        for pod in operator_pods_current:
            pod_name = pod["metadata"]["name"]
            phase = pod.get("status", {}).get("phase", "")
            container_statuses = pod.get("status", {}).get("containerStatuses", [])
            has_crash = any(
                cs.get("state", {}).get("waiting", {}).get("reason")
                == "CrashLoopBackOff"
                for cs in container_statuses
            )
            assert not has_crash, (
                f"Operator pod '{pod_name}' entered CrashLoopBackOff "
                "due to bad CatalogSource image — should not cascade"
            )
            assert (
                phase == "Running"
            ), f"Operator pod '{pod_name}' is '{phase}', expected Running"
        logger.info("All operator pods remain Running")

        logger.info("Step 5: Verify StorageCluster and Ceph health unaffected")
        verify_storage_cluster()
        ceph.cluster_health_check()
        logger.info("Cluster health OK despite bad CatalogSource image")

        logger.info(
            "Step 6: Restore CatalogSource to original image '%s'",
            self.original_image,
        )
        restore_patch = f'{{"spec": {{"image": "{self.original_image}"}}}}'
        catsrc_ocp.patch(
            resource_name=catsrc_name,
            params=restore_patch,
            format_type="merge",
        )

        logger.info("Step 7: Verify CatalogSource returns to READY")
        catsrc = CatalogSource(
            resource_name=catsrc_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        catsrc.wait_for_state("READY", timeout=600)
        logger.info("CatalogSource is READY again")

        logger.info("Step 8: Verify operator pods still healthy after recovery")
        wait_for_pods_to_be_running(
            namespace=self.ns,
            pod_count=len(operator_pods_before),
            timeout=300,
        )

        operator_pods_after = get_pods_having_label(
            label=constants.OPERATOR_LABEL,
            namespace=self.ns,
        )
        after_uids = {
            p["metadata"]["name"]: p["metadata"]["uid"] for p in operator_pods_after
        }
        for name, uid in operator_uids.items():
            if name in after_uids:
                assert after_uids[name] == uid, (
                    f"Operator pod '{name}' was restarted (UID changed) "
                    "during image pull recovery"
                )

        logger.info(
            "Image pull error recovery verified — CatalogSource READY, "
            "operators untouched"
        )
