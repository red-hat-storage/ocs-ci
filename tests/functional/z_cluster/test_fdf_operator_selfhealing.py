"""
FDF Standalone Operator Self-Healing tests (RHSTOR-8840 / OCS-8193).

Validates that FDF standalone operators recover from pod crashes and
that CatalogSource deletion does not impact running workloads.

Negative tests that verify:
- Individual operator pod crash → automatic restart + reconciliation
- Simultaneous crash of all operator pods → full recovery
- CatalogSource deletion → running FDF workloads unaffected
"""

import logging
import time

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

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier2
@pytest.mark.polarion_id("OCS-8193")
class TestFDFOperatorSelfHealing:
    """
    Verify FDF operators self-heal from pod crashes and that
    CatalogSource removal does not impact running workloads.
    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory, pod_factory):
        """
        Create a test workload (RBD PVC + pod with background I/O)
        to verify I/O continuity during operator disruptions.
        """
        self.namespace = config.ENV_DATA["cluster_namespace"]

        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=5,
        )
        self.io_pod = pod_factory(pvc=self.pvc_obj)
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory

    def _start_io(self):
        """Start background I/O on the test pod."""
        self.io_pod.run_io(
            storage_type="fs",
            size="1G",
            io_direction="rw",
            runtime=120,
            bs="4K",
            rate="4m",
        )

    def _verify_io(self):
        """Wait for I/O to complete and verify no errors."""
        fio_result = self.io_pod.get_fio_results()
        fio_error = fio_result.get("jobs", [{}])[0].get("error", 0)
        assert fio_error == 0, f"FIO reported error code {fio_error}"
        reads = fio_result.get("jobs", [{}])[0].get("read", {})
        writes = fio_result.get("jobs", [{}])[0].get("write", {})
        read_errors = reads.get("io_kbytes", 0) == 0 and reads.get("total_ios", 0) == 0
        write_errors = (
            writes.get("io_kbytes", 0) == 0 and writes.get("total_ios", 0) == 0
        )
        assert not (read_errors and write_errors), "FIO produced no I/O — check pod"

    def _delete_pods_by_label(self, label, wait_running=True, timeout=300):
        """
        Delete all pods matching the label and optionally wait for
        replacements to reach Running state.
        """
        pods = get_pods_having_label(
            label=label,
            namespace=self.namespace,
        )
        assert pods, f"No pods found with label '{label}'"

        pod_names = [p["metadata"]["name"] for p in pods]
        logger.info("Deleting pods: %s", pod_names)

        pod_ocp = OCP(kind="pod", namespace=self.namespace)
        for name in pod_names:
            pod_ocp.delete(resource_name=name, wait=False)

        if wait_running:
            logger.info(
                "Waiting for pods with label '%s' to return to Running",
                label,
            )
            time.sleep(10)
            new_pods = get_pods_having_label(label=label, namespace=self.namespace)
            new_pod_names = [p["metadata"]["name"] for p in new_pods]
            wait_for_pods_to_be_running(
                namespace=self.namespace,
                pod_names=new_pod_names,
                timeout=timeout,
            )

    def test_odf_operator_crash_recovery(self):
        """
        Delete ODF operator manager pod and verify it restarts with
        StorageCluster reconciliation intact and I/O uninterrupted.
        """
        self._start_io()

        logger.info("Deleting ODF operator controller-manager pod")
        self._delete_pods_by_label(constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL)

        verify_storage_cluster()
        self._verify_io()
        logger.info("ODF operator crash recovery verified")

    def test_rook_operator_crash_recovery(self):
        """
        Delete rook-ceph-operator pod and verify Ceph remains healthy
        and I/O continues.
        """
        self._start_io()

        logger.info("Deleting rook-ceph-operator pod")
        self._delete_pods_by_label(constants.OPERATOR_LABEL)

        ceph_cluster = CephCluster()
        ceph_cluster.cluster_health_check(timeout=300)

        self._verify_io()
        logger.info("Rook operator crash recovery verified")

    def test_mcg_operator_crash_recovery(self):
        """
        Delete MCG (NooBaa) operator pod and verify NooBaa remains
        functional.
        """
        self._start_io()

        logger.info("Deleting NooBaa operator pod")
        self._delete_pods_by_label(constants.NOOBAA_OPERATOR_POD_LABEL)

        noobaa_ocp = OCP(
            kind="noobaa",
            namespace=self.namespace,
            resource_name="noobaa",
        )
        noobaa_ocp.wait_for_phase(phase="Ready", timeout=300)

        self._verify_io()
        logger.info("MCG operator crash recovery verified")

    def test_all_operators_simultaneous_crash(self):
        """
        Delete ALL operator pods simultaneously and verify full
        recovery — all pods restart, reconciliation resumes, I/O
        uninterrupted.
        """
        self._start_io()

        operator_labels = [
            constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL,
            constants.OPERATOR_LABEL,
            constants.NOOBAA_OPERATOR_POD_LABEL,
        ]

        logger.info("Deleting all operator pods simultaneously")
        pod_ocp = OCP(kind="pod", namespace=self.namespace)
        for label in operator_labels:
            pods = get_pods_having_label(label=label, namespace=self.namespace)
            for p in pods or []:
                pod_ocp.delete(resource_name=p["metadata"]["name"], wait=False)

        logger.info("Waiting for all operator pods to recover")
        time.sleep(15)
        for label in operator_labels:
            new_pods = get_pods_having_label(label=label, namespace=self.namespace)
            new_pod_names = [p["metadata"]["name"] for p in new_pods]
            wait_for_pods_to_be_running(
                namespace=self.namespace,
                pod_names=new_pod_names,
                timeout=300,
            )

        verify_storage_cluster()

        ceph_cluster = CephCluster()
        ceph_cluster.cluster_health_check(timeout=300)

        self._verify_io()
        logger.info("All operators simultaneous crash recovery verified")

    def test_catalogsource_deletion_no_impact(self):
        """
        Delete the FDF CatalogSource and verify running FDF is NOT
        impacted — operator pods continue, StorageCluster stays Ready,
        PVC provisioning works. Then recreate and verify recovery.
        """
        catsrc_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME

        logger.info("Starting I/O before CatalogSource deletion")
        self._start_io()

        logger.info("Deleting FDF CatalogSource '%s'", catsrc_name)
        catsrc_ocp = OCP(
            kind="CatalogSource",
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        catsrc_ocp.delete(resource_name=catsrc_name, wait=True)

        try:
            time.sleep(30)

            logger.info("Verifying operator pods still running")
            for label in [
                constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL,
                constants.OPERATOR_LABEL,
                constants.NOOBAA_OPERATOR_POD_LABEL,
            ]:
                pods = get_pods_having_label(label=label, namespace=self.namespace)
                assert pods, f"Operator pods with label '{label}' not found"
                for p in pods:
                    phase = p["status"]["phase"]
                    assert (
                        phase == "Running"
                    ), f"Pod {p['metadata']['name']} is {phase}, expected Running"

            logger.info("Verifying StorageCluster still Ready")
            verify_storage_cluster()

            logger.info("Verifying PVC provisioning still works")
            new_pvc = self.pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                size=2,
            )
            new_pod = self.pod_factory(pvc=new_pvc)
            new_pod.run_io(
                storage_type="fs",
                size="256M",
                io_direction="wo",
                runtime=0,
            )
            new_pod.get_fio_results()

            logger.info("Verifying original I/O completed without errors")
            self._verify_io()
        finally:
            logger.info("Recreating FDF CatalogSource")
            from ocs_ci.deployment.fdf_standalone import (
                StandaloneFDFCatalogSource,
            )

            StandaloneFDFCatalogSource().create_catalog_source()

            catsrc = CatalogSource(
                resource_name=catsrc_name,
                namespace=constants.MARKETPLACE_NAMESPACE,
            )
            catsrc.wait_for_state("READY", timeout=300)
            logger.info("CatalogSource recreated and READY — upgrade path restored")
