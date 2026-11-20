"""
FDF Standalone HCP — StorageClient Connection Validation (RHSTOR-8290 / OCS-8192).

Verify StorageClient CR on the hosted cluster connects to the FDF
provider and Ceph StorageClasses propagate to the hosted cluster.
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

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier1
@pytest.mark.polarion_id("OCS-8192")
class TestFDFHCPStorageClient:
    """
    Verify StorageClient connects and StorageClasses propagate with FDF standalone HCP.
    """

    def test_storageclient_connected(self):
        """
        Verify:
        - StorageClient CR exists and is Connected/Ready
        - Ceph StorageClasses are propagated to the hosted cluster
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info("Verifying StorageClient connection")
        sc_ocp = OCP(
            kind=constants.STORAGECLIENT,
            namespace=namespace,
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

        logger.info("Verifying StorageClasses propagated to hosted cluster")
        sc_ocp = OCP(kind="storageclass")
        storage_classes = sc_ocp.get().get("items", [])
        sc_names = [sc["metadata"]["name"] for sc in storage_classes]
        logger.info("Available StorageClasses: %s", sc_names)
        assert any(
            "ceph" in name for name in sc_names
        ), f"No Ceph StorageClasses found on hosted cluster: {sc_names}"

        logger.info(
            "StorageClient connected, %d StorageClass(es) propagated",
            len([n for n in sc_names if "ceph" in n]),
        )
