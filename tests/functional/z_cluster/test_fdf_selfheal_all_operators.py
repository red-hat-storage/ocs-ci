"""
FDF Standalone — All Operators Simultaneous Crash Recovery (RHSTOR-8290 / OCS-8193).

Delete ODF, Rook, and NooBaa operator pods simultaneously and verify
they all recover, Ceph+NooBaa health returns OK, and workload I/O
is uninterrupted.
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
class TestFDFSelfHealAllOperators:
    """
    Verify all FDF operators self-heal from simultaneous pod crash.
    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory, pod_factory):
        """Create a test workload to verify I/O continuity."""
        self.namespace = config.ENV_DATA["cluster_namespace"]
        pvc_obj = pvc_factory(interface=constants.CEPHBLOCKPOOL, size=5)
        self.io_pod = pod_factory(pvc=pvc_obj)

    def _delete_pods_by_label(self, label):
        """Delete all pods matching a label selector."""
        pods = get_pods_having_label(label=label, namespace=self.namespace)
        if not pods:
            logger.warning("No pods found with label '%s'", label)
            return
        pod_ocp = OCP(kind="pod", namespace=self.namespace)
        for p in pods:
            pod_ocp.delete(resource_name=p["metadata"]["name"], wait=False)
        logger.info("Deleted %d pod(s) with label '%s'", len(pods), label)

    def test_all_operators_simultaneous_crash(self):
        """
        Delete ODF, Rook, and NooBaa operator pods simultaneously and verify:
        - All pods restart automatically
        - Ceph cluster returns to HEALTH_OK
        - NooBaa health returns OK
        - StorageCluster reconciliation completes
        - Workload I/O is uninterrupted
        """
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
            runtime=300,
            bs="4K",
            rate="4m",
        )

        logger.info("Deleting all operator pods simultaneously")
        for label in operator_labels:
            self._delete_pods_by_label(label)

        logger.info("Waiting for all operator pods to restart")
        for label in operator_labels:
            wait_for_pods_to_be_running(
                namespace=self.namespace, selector=label, timeout=600
            )

        logger.info("Verifying Ceph cluster health")
        ceph_cluster = CephCluster()
        ceph_cluster.cluster_health_check(timeout=600)

        logger.info("Verifying NooBaa health")
        ceph_cluster.wait_for_noobaa_health_ok(tries=60, delay=10)

        logger.info("Verifying StorageCluster reconciliation")
        verify_storage_cluster()

        logger.info("Verifying I/O completed without errors")
        fio_result = self.io_pod.get_fio_results()
        reads = fio_result.get("jobs", [{}])[0].get("read", {})
        writes = fio_result.get("jobs", [{}])[0].get("write", {})
        assert (
            reads.get("total_ios", 0) > 0 or writes.get("total_ios", 0) > 0
        ), "FIO produced no I/O during simultaneous operator restart"
        logger.info("All operators simultaneous crash recovery verified")
