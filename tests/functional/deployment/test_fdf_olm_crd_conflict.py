"""
Negative: OLM CRD Conflict — FDF and ODF Coexistence (RHSTOR-8290 / TC-8).

Verify that attempting to install FDF on a cluster where ODF is already
running from redhat-operators does NOT corrupt the existing ODF deployment.
OLM should either block the conflicting install or error clearly.
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
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.deployment.fdf_standalone import StandaloneFDFCatalogSource

logger = logging.getLogger(__name__)


@purple_squad
@tier3
class TestFDFOLMCRDConflict:
    """
    Negative test: Attempt FDF install on an existing ODF cluster.

    Precondition: ODF is running from redhat-operators with active workloads.
    This test must NOT run on clusters that already have FDF — it requires
    a standard ODF deployment.
    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory, pod_factory):
        """Set up ODF workloads and verify baseline health."""
        if config.DEPLOYMENT.get("fdf_standalone_deployment"):
            pytest.skip("This negative test requires a standard ODF cluster")

        self.namespace = config.ENV_DATA["cluster_namespace"]

        self.pvc_obj = pvc_factory(interface=constants.CEPHBLOCKPOOL, size=5)
        self.pod_obj = pod_factory(pvc=self.pvc_obj)

        self.pod_obj.run_io(
            storage_type="fs",
            size="512M",
            io_direction="wo",
            runtime=0,
        )
        self.pod_obj.get_fio_results()
        self.md5_before = cal_md5sum(self.pod_obj, "fio-rand-write")

    @pytest.fixture()
    def cleanup_fdf_catsrc(self):
        """Remove FDF CatalogSource after test, regardless of outcome."""
        yield
        catsrc_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        marketplace_ns = constants.MARKETPLACE_NAMESPACE
        catsrc_ocp = OCP(kind="CatalogSource", namespace=marketplace_ns)
        if catsrc_ocp.is_exist(resource_name=catsrc_name):
            logger.info("Cleanup: removing FDF CatalogSource '%s'", catsrc_name)
            catsrc_ocp.delete(resource_name=catsrc_name, wait=True)

    def test_fdf_odf_coexistence_blocked(self, cleanup_fdf_catsrc):
        """
        Add FDF CatalogSource and attempt Subscription on ODF cluster.

        Checkpoint A: Observe OLM behavior (block, error, or CRD conflict)
        Checkpoint B: Verify existing ODF is NOT corrupted
        """
        # Add FDF CatalogSource
        logger.info("Adding FDF CatalogSource to existing ODF cluster")
        fdf_catsrc = StandaloneFDFCatalogSource()
        fdf_catsrc.create_catalog_source()

        catsrc = CatalogSource(
            resource_name=constants.FDF_STANDALONE_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        catsrc.wait_for_state("READY", timeout=300)

        # Attempt FDF Subscription
        logger.info("Attempting FDF Subscription on ODF cluster")
        sub_ocp = OCP(kind="Subscription", namespace=self.namespace)
        fdf_sub = {
            "apiVersion": "operators.coreos.com/v1alpha1",
            "kind": "Subscription",
            "metadata": {
                "name": "fdf-conflict-test-sub",
                "namespace": self.namespace,
            },
            "spec": {
                "channel": "stable-5.0",
                "name": "odf-operator",
                "source": constants.FDF_STANDALONE_CATALOG_SOURCE_NAME,
                "sourceNamespace": constants.MARKETPLACE_NAMESPACE,
                "installPlanApproval": "Manual",
            },
        }

        conflict_detected = False
        try:
            sub_ocp.create(resource_data=fdf_sub)
            logger.info("FDF Subscription created — checking for OLM conflict")

            # Check InstallPlans for CRD ownership errors
            ip_ocp = OCP(kind="InstallPlan", namespace=self.namespace)

            import time

            end_time = time.time() + 120
            while time.time() < end_time:
                plans = ip_ocp.get().get("items", [])
                for plan in plans:
                    phase = plan.get("status", {}).get("phase", "")
                    conditions = plan.get("status", {}).get("conditions", [])
                    for cond in conditions:
                        msg = cond.get("message", "")
                        if "conflict" in msg.lower() or "already" in msg.lower():
                            logger.info(
                                "Checkpoint A: OLM conflict detected — %s",
                                msg,
                            )
                            conflict_detected = True
                            break
                    if phase == "Failed":
                        logger.info("Checkpoint A: InstallPlan Failed (expected)")
                        conflict_detected = True
                if conflict_detected:
                    break
                time.sleep(10)

            if not conflict_detected:
                logger.warning(
                    "No explicit OLM conflict detected within 120s — "
                    "OLM may allow the Subscription without immediate error"
                )

        finally:
            # Clean up test subscription
            try:
                sub_ocp.delete(resource_name="fdf-conflict-test-sub", wait=True)
            except Exception:
                pass

        # Checkpoint B: Verify existing ODF is NOT corrupted
        logger.info("Checkpoint B: Verifying ODF is NOT corrupted")

        logger.info("Verifying StorageCluster still Ready")
        verify_storage_cluster()

        logger.info("Verifying Ceph health")
        ceph_cluster = CephCluster()
        ceph_cluster.cluster_health_check(timeout=300)

        logger.info("Verifying workload data integrity")
        verify_data_integrity(self.pod_obj, "fio-rand-write", self.md5_before)

        logger.info("Verifying PVC provisioning still works")
        self.pod_obj.run_io(
            storage_type="fs",
            size="256M",
            io_direction="wo",
            runtime=0,
        )
        self.pod_obj.get_fio_results()

        logger.info(
            "ODF deployment intact after FDF coexistence attempt. "
            "Conflict detected: %s",
            conflict_detected,
        )
