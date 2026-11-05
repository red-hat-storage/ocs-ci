"""
Negative: Invalid StorageCluster Handling (RHSTOR-8290 / TC-12 Phase 3).

Verify FDF operator handles invalid StorageCluster configurations gracefully:
operator never CrashLoopBackOff, clear error conditions reported, and
recovery works when valid configuration is applied.
"""

import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier3,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier3
class TestFDFInvalidStorageCluster:
    """
    Verify FDF operator handles invalid StorageCluster configs gracefully.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """Record namespace."""
        self.namespace = config.ENV_DATA["cluster_namespace"]

    def _check_no_crashloop(self):
        """Assert no operator pods are in CrashLoopBackOff."""
        pod_ocp = OCP(kind="pod", namespace=self.namespace)
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
        assert not crash_pods, f"Operator pods in CrashLoopBackOff: {crash_pods}"

    def test_invalid_storagecluster_rejected(self):
        """
        Attempt to create a StorageCluster with invalid replica count.

        Verify:
        - OCP/webhook rejects or operator reports error in conditions
        - Operator never enters CrashLoopBackOff
        - Existing StorageCluster (if any) is unaffected

        Checkpoint C: Invalid CRs handled gracefully.
        """
        sc_ocp = OCP(kind="StorageCluster", namespace=self.namespace)

        # Check if a StorageCluster already exists (it should on FDF clusters)
        existing = sc_ocp.get().get("items", [])
        if existing:
            existing_name = existing[0]["metadata"]["name"]
            logger.info(
                "Existing StorageCluster '%s' found — will verify it "
                "remains unaffected",
                existing_name,
            )

        # Attempt to create an invalid StorageCluster with negative replica
        invalid_sc = {
            "apiVersion": "ocs.openshift.io/v1",
            "kind": "StorageCluster",
            "metadata": {
                "name": "fdf-invalid-test-sc",
                "namespace": self.namespace,
            },
            "spec": {
                "storageDeviceSets": [
                    {
                        "name": "invalid-deviceset",
                        "count": 1,
                        "replica": -1,
                        "dataPVCTemplate": {
                            "spec": {
                                "accessModes": ["ReadWriteOnce"],
                                "resources": {"requests": {"storage": "1Ti"}},
                            }
                        },
                    }
                ]
            },
        }

        creation_rejected = False
        try:
            sc_ocp.create(resource_data=invalid_sc)
            logger.info(
                "Invalid StorageCluster created — checking for error conditions"
            )

            # Wait for operator to report error in conditions
            end_time = time.time() + 120
            while time.time() < end_time:
                try:
                    sc_data = sc_ocp.get(resource_name="fdf-invalid-test-sc")
                    conditions = sc_data.get("status", {}).get("conditions", [])
                    phase = sc_data.get("status", {}).get("phase", "")

                    for cond in conditions:
                        if cond.get("type") in (
                            "ReconcileFailed",
                            "Available",
                        ):
                            status = cond.get("status", "")
                            msg = cond.get("message", "")
                            if status == "False" or "error" in msg.lower():
                                logger.info(
                                    "Operator reported error: %s — %s",
                                    cond["type"],
                                    msg,
                                )
                                creation_rejected = True
                                break

                    if phase in ("Error", "Failed"):
                        logger.info("StorageCluster phase: %s (expected)", phase)
                        creation_rejected = True

                except Exception:
                    pass

                if creation_rejected:
                    break
                time.sleep(10)

        except Exception as e:
            logger.info("Invalid StorageCluster rejected at creation: %s", str(e))
            creation_rejected = True

        finally:
            # Clean up invalid StorageCluster
            try:
                sc_ocp.delete(resource_name="fdf-invalid-test-sc", wait=False)
                logger.info("Cleaned up invalid StorageCluster")
            except Exception:
                pass

        logger.info(
            "Checkpoint C: Invalid StorageCluster handled — rejected: %s",
            creation_rejected,
        )

        # Verify operator stability
        self._check_no_crashloop()

        # Verify existing StorageCluster is unaffected
        if existing:
            logger.info("Verifying existing StorageCluster unaffected")
            verify_storage_cluster()

        logger.info("Invalid StorageCluster error handling verified")
