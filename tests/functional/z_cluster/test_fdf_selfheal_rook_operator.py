"""
FDF Standalone — Rook Operator Crash Recovery (RHSTOR-8290 / OCS-8193).

Delete rook-ceph-operator pod and verify it restarts,
Ceph cluster returns to HEALTH_OK, and workload I/O is uninterrupted.
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
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier2
@pytest.mark.polarion_id("OCS-8193")
class TestFDFSelfHealRookOperator:
    """
    Verify Rook operator self-heals from pod crash on FDF standalone.
    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory, pod_factory):
        """Create a test workload to verify I/O continuity."""
        self.namespace = config.ENV_DATA["cluster_namespace"]
        pvc_obj = pvc_factory(interface=constants.CEPHBLOCKPOOL, size=5)
        self.io_pod = pod_factory(pvc=pvc_obj)

    def test_rook_operator_crash_recovery(self):
        """
        Delete rook-ceph-operator pod and verify:
        - Pod restarts automatically
        - Ceph cluster returns to HEALTH_OK
        - StorageCluster reconciliation completes
        - Workload I/O is uninterrupted
        """
        logger.info("Starting background I/O")
        self.io_pod.run_io(
            storage_type="fs",
            size="1G",
            io_direction="rw",
            runtime=180,
            bs="4K",
            rate="4m",
        )

        logger.info("Deleting rook-ceph-operator pod")
        label = constants.OPERATOR_LABEL
        pods = get_pods_having_label(label=label, namespace=self.namespace)
        assert pods, f"No pods found with label '{label}'"

        pod_ocp = OCP(kind="pod", namespace=self.namespace)
        for p in pods:
            pod_ocp.delete(resource_name=p["metadata"]["name"], wait=False)

        logger.info("Waiting for rook-ceph-operator pod to restart")
        wait_for_pods_to_be_running(
            namespace=self.namespace, selector=label, timeout=300
        )

        logger.info("Verifying Ceph cluster health")
        ceph_cluster = CephCluster()
        ceph_cluster.cluster_health_check(timeout=300)

        logger.info("Verifying StorageCluster reconciliation")
        verify_storage_cluster()

        logger.info("Verifying I/O completed without errors")
        fio_result = self.io_pod.get_fio_results()
        reads = fio_result.get("jobs", [{}])[0].get("read", {})
        writes = fio_result.get("jobs", [{}])[0].get("write", {})
        assert (
            reads.get("total_ios", 0) > 0 or writes.get("total_ios", 0) > 0
        ), "FIO produced no I/O during rook operator restart"
        logger.info("Rook operator crash recovery verified")
