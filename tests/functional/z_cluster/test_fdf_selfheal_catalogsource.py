"""
FDF Standalone — CatalogSource Deletion No Impact (RHSTOR-8290 / OCS-8193).

Delete the FDF CatalogSource and verify running operators are not affected,
workload I/O continues, then recreate the CatalogSource to restore upgrade
capability.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier2,
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
from ocs_ci.deployment.fdf_standalone import StandaloneFDFCatalogSource

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier2
@pytest.mark.polarion_id("OCS-8193")
class TestFDFSelfHealCatalogSource:
    """
    Verify CatalogSource deletion does not impact running FDF operators.
    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory, pod_factory):
        """Create a test workload to verify I/O continuity."""
        self.namespace = config.ENV_DATA["cluster_namespace"]
        pvc_obj = pvc_factory(interface=constants.CEPHBLOCKPOOL, size=5)
        self.io_pod = pod_factory(pvc=pvc_obj)

    def test_catalogsource_deletion_no_impact(self):
        """
        Delete FDF CatalogSource and verify:
        - All operator pods remain Running (ODF, Rook, NooBaa)
        - Ceph cluster stays HEALTH_OK
        - Workload I/O is uninterrupted
        - CatalogSource can be recreated and returns to READY
        """
        catsrc_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        marketplace_ns = constants.MARKETPLACE_NAMESPACE

        operator_labels = [
            constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL,
            constants.OPERATOR_LABEL,
            constants.NOOBAA_OPERATOR_POD_LABEL,
        ]

        logger.info("Starting background I/O")
        self.io_pod.run_io(
            storage_type="fs",
            size="1G",
            io_direction="rw",
            runtime=180,
            bs="4K",
            rate="4m",
        )

        logger.info("Recording operator pod UIDs before CatalogSource deletion")
        pods_before = {}
        for label in operator_labels:
            pods = get_pods_having_label(label=label, namespace=self.namespace)
            pods_before[label] = {
                p["metadata"]["name"]: p["metadata"]["uid"] for p in pods
            }

        logger.info("Deleting FDF CatalogSource '%s'", catsrc_name)
        catsrc_ocp = OCP(kind="CatalogSource", namespace=marketplace_ns)
        catsrc_ocp.delete(resource_name=catsrc_name, wait=True)

        logger.info("Verifying operator pods were NOT restarted")
        for label in operator_labels:
            wait_for_pods_to_be_running(
                namespace=self.namespace, selector=label, timeout=120
            )
            pods_after = get_pods_having_label(label=label, namespace=self.namespace)
            uids_after = {
                p["metadata"]["name"]: p["metadata"]["uid"] for p in pods_after
            }
            for name, uid in pods_before[label].items():
                assert (
                    name in uids_after
                ), f"Operator pod '{name}' disappeared after CatalogSource deletion"
                assert uids_after[name] == uid, (
                    f"Operator pod '{name}' was restarted (UID changed) "
                    f"after CatalogSource deletion"
                )

        logger.info("Verifying Ceph cluster health")
        ceph_cluster = CephCluster()
        ceph_cluster.cluster_health_check(timeout=300)

        logger.info("Verifying StorageCluster reconciliation")
        verify_storage_cluster()

        logger.info("Recreating FDF CatalogSource")
        fdf_catsrc = StandaloneFDFCatalogSource()
        fdf_catsrc.create_catalog_source()
        recreated = CatalogSource(resource_name=catsrc_name, namespace=marketplace_ns)
        recreated.wait_for_state("READY", timeout=300)
        logger.info("FDF CatalogSource recreated and READY")

        logger.info("Verifying I/O completed without errors")
        fio_result = self.io_pod.get_fio_results()
        reads = fio_result.get("jobs", [{}])[0].get("read", {})
        writes = fio_result.get("jobs", [{}])[0].get("write", {})
        assert (
            reads.get("total_ios", 0) > 0 or writes.get("total_ios", 0) > 0
        ), "FIO produced no I/O during CatalogSource deletion test"
        logger.info("CatalogSource deletion no-impact verified")
