"""
FDF Standalone Provider/Client Mode (HCP) validation (RHSTOR-8840 / OCS-8192).

Validates the primary Provider/Client architecture with FDF standalone:
- Provider server running on the host (management) cluster
- StorageClient connects from hosted cluster
- StorageClasses propagate to the hosted cluster
- PVC lifecycle (RBD + CephFS) works from the client side
- Volume expansion, snapshot, and restore from client

Requires an HCP environment with FDF standalone on the host cluster
and at least one hosted cluster configured as a client.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    cal_md5sum,
    get_pods_having_label,
    verify_data_integrity,
)

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier1
@pytest.mark.polarion_id("OCS-8192")
class TestFDFProviderClient:
    """
    Verify FDF standalone Provider/Client architecture with HCP.

    Checkpoints:
        A: Client connects, StorageClasses propagate to hosted cluster.
        B: All volume operations (create, expand, snapshot, restore)
           work from client.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.provider_namespace = config.ENV_DATA["cluster_namespace"]

    def test_provider_server_running(self):
        """
        Verify the OCS Provider API server is running on the
        management (host) cluster with FDF standalone.
        """
        logger.info("Verifying Provider server is Running")
        provider_pods = get_pods_having_label(
            label=constants.PROVIDER_SERVER_LABEL,
            namespace=self.provider_namespace,
        )
        assert provider_pods, (
            "No Provider server pods found with label "
            f"'{constants.PROVIDER_SERVER_LABEL}'"
        )
        for pod_data in provider_pods:
            pod_name = pod_data["metadata"]["name"]
            phase = pod_data["status"]["phase"]
            logger.info("Provider server pod '%s': %s", pod_name, phase)
            assert (
                phase == "Running"
            ), f"Provider pod '{pod_name}' is '{phase}', expected 'Running'"
        logger.info("Checkpoint: Provider server Running")

    def test_storageclient_connected(self):
        """
        Verify StorageClient CR on the hosted cluster connects to the
        provider and StorageClasses propagate.
        """
        logger.info("Verifying StorageClient connection")
        sc_ocp = OCP(
            kind=constants.STORAGECLIENT,
            namespace=self.provider_namespace,
        )
        clients = sc_ocp.get().get("items", [])
        assert clients, "No StorageClient resources found"

        for client in clients:
            name = client["metadata"]["name"]
            phase = client.get("status", {}).get("phase", "")
            logger.info("StorageClient '%s': phase=%s", name, phase)
            assert phase in ("Connected", "Ready"), (
                f"StorageClient '{name}' phase is '{phase}', "
                "expected 'Connected' or 'Ready'"
            )

        logger.info("Verifying StorageClasses propagated")
        sc_ocp = OCP(kind="storageclass")
        storage_classes = sc_ocp.get().get("items", [])
        sc_names = [sc["metadata"]["name"] for sc in storage_classes]
        logger.info("Available StorageClasses: %s", sc_names)
        assert any(
            "ceph" in name for name in sc_names
        ), f"No Ceph StorageClasses found: {sc_names}"

        logger.info("Checkpoint A: Client connected, StorageClasses propagated")

    def test_pvc_lifecycle_from_client(
        self, pvc_factory, pod_factory, snapshot_factory, snapshot_restore_factory
    ):
        """
        Create PVCs (RBD + CephFS) on the hosted cluster, attach
        workloads, verify data, expand, snapshot, and restore.
        """
        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            logger.info("--- Testing %s from client ---", interface)

            logger.info("Creating PVC and pod")
            pvc_obj = pvc_factory(interface=interface, size=5)
            pod_obj = pod_factory(pvc=pvc_obj)

            logger.info("Writing data")
            pod_obj.run_io(
                storage_type="fs",
                size="1G",
                io_direction="wo",
                runtime=0,
            )
            pod_obj.get_fio_results()
            md5sum_original = cal_md5sum(pod_obj, "fio-rand-write")

            logger.info("Verifying data readback")
            verify_data_integrity(pod_obj, "fio-rand-write", md5sum_original)

            logger.info("Expanding PVC from %dGi to %dGi", 5, 10)
            pvc_obj.resize_pvc(10, True)

            logger.info("Creating VolumeSnapshot")
            snap_obj = snapshot_factory(pvc_obj)

            logger.info("Restoring from snapshot")
            restored_pvc = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                size=str(
                    pvc_obj.get()
                    .get("spec", {})
                    .get("resources", {})
                    .get("requests", {})
                    .get("storage", "10Gi")
                ),
            )
            restored_pod = pod_factory(pvc=restored_pvc)
            verify_data_integrity(restored_pod, "fio-rand-write", md5sum_original)

            logger.info("%s lifecycle complete", interface)

        logger.info("Checkpoint B: All volume operations from client succeeded")
