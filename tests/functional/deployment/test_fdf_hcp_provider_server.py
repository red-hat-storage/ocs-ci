"""
FDF Standalone HCP — Provider Server Validation (RHSTOR-8290 / OCS-8192).

Verify the OCS Provider API server pods are Running on the management
(host) cluster when FDF standalone is deployed in Provider/Client mode.
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
from ocs_ci.ocs.resources.pod import get_pods_having_label

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier1
@pytest.mark.polarion_id("OCS-8192")
class TestFDFHCPProviderServer:
    """
    Verify FDF standalone Provider server is running on the host cluster.
    """

    def test_provider_server_running(self):
        """
        Verify the OCS Provider API server pods exist and are in
        Running phase on the management (host) cluster.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info("Checking Provider server pods")
        provider_pods = get_pods_having_label(
            label=constants.PROVIDER_SERVER_LABEL,
            namespace=namespace,
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

        logger.info("All %d Provider server pod(s) Running", len(provider_pods))
