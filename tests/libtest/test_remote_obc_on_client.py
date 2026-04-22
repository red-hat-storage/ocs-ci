"""
Libtest to validate remote OBC functionality on client clusters.

This test validates that Object Bucket Claims (OBC) can be created
on client clusters when remote OBC is enabled via ODF CLI.
"""

import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    libtest,
    yellow_squad,
    hci_provider_and_client_required,
    tier1,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@yellow_squad
@libtest
@hci_provider_and_client_required
@tier1
class TestRemoteOBCOnClient(ManageTest):
    """
    Libtest to verify remote OBC creation and functionality on client clusters.

    This test class validates that:
    - Remote OBC can be enabled on client cluster using ODF CLI
    - OBC can be created successfully on client cluster
    - OBC reaches Bound state
    - OBC credentials are accessible
    - OBC can be deleted successfully
    """

    pytestmark = pytest.mark.usefixtures("remote_obc_setup_session")

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """
        Setup test context - save original cluster index.

        Args:
            request: pytest request object
        """
        self.orig_index = config.cur_index
        logger.info(f"Original cluster index: {self.orig_index}")

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Teardown - restore original cluster context.

        Args:
            request: pytest request object
        """

        def finalizer():
            logger.info("Restoring original cluster context")
            config.switch_ctx(self.orig_index)
            logger.info(f"Switched back to cluster index {self.orig_index}")

        request.addfinalizer(finalizer)

    def test_remote_obc_creation_on_client(self):
        """
        Test OBC creation on client cluster with remote OBC enabled.

        This test validates the complete lifecycle of an OBC on a client cluster:
        1. Switch to client cluster context
        2. Create OBC using oc command
        3. Verify OBC reaches Bound state
        4. Verify OBC credentials are created
        5. Delete OBC and verify cleanup

        """
        # Get client cluster index
        client_indices = config.get_consumer_indexes_list()
        if not client_indices:
            pytest.skip("No client clusters found in the configuration")

        client_index = client_indices[0]
        logger.info(f"Testing remote OBC on client cluster index: {client_index}")

        # Switch to client cluster
        config.switch_ctx(client_index)
        logger.info(f"Switched to client cluster index {client_index}")

        # Verify we're on a client cluster
        cluster_type = config.ENV_DATA.get("cluster_type", "").lower()
        assert (
            cluster_type == constants.HCI_CLIENT
        ), f"Expected cluster_type to be {constants.HCI_CLIENT}, got {cluster_type}"
        logger.info(f"Confirmed cluster type is {constants.HCI_CLIENT}")

        # Create unique OBC name
        obc_name = create_unique_resource_name(
            resource_description="obc", resource_type="remote-client"
        )
        logger.info(f"Creating OBC: {obc_name}")

        # Create OBC using OCP
        namespace = config.ENV_DATA["cluster_namespace"]
        obc_data = {
            "apiVersion": "objectbucket.io/v1alpha1",
            "kind": "ObjectBucketClaim",
            "metadata": {"name": obc_name, "namespace": namespace},
            "spec": {
                "generateBucketName": obc_name,
                "storageClassName": "openshift-storage.noobaa.io",
            },
        }

        obc_obj = OCP(kind="ObjectBucketClaim", namespace=namespace)
        obc_obj.create(yaml_dict=obc_data)
        logger.info(f"OBC {obc_name} created successfully")

        # Wait for OBC to reach Bound state
        logger.info(f"Waiting for OBC {obc_name} to reach Bound state")
        for sample in TimeoutSampler(
            timeout=300,
            sleep=10,
            func=self._check_obc_phase,
            obc_name=obc_name,
            namespace=namespace,
        ):
            if sample:
                logger.info(f"OBC {obc_name} reached Bound state")
                break

        # Verify OBC object and credentials
        logger.info(f"Verifying OBC {obc_name} details")
        obc_resource = OBC(obc_name)

        # Verify bucket name is set
        assert obc_resource.bucket_name, f"Bucket name not found for OBC {obc_name}"
        logger.info(f"OBC bucket name: {obc_resource.bucket_name}")

        # Verify credentials exist
        assert obc_resource.access_key_id, f"Access key ID not found for OBC {obc_name}"
        assert (
            obc_resource.access_key
        ), f"Secret access key not found for OBC {obc_name}"
        logger.info("OBC credentials verified successfully")

        # Verify ConfigMap exists
        cm_obj = OCP(kind="ConfigMap", namespace=namespace, resource_name=obc_name)
        cm_data = cm_obj.get()
        assert cm_data, f"ConfigMap not found for OBC {obc_name}"
        logger.info(f"OBC ConfigMap verified: {obc_name}")

        # Verify Secret exists
        secret_obj = OCP(kind="Secret", namespace=namespace, resource_name=obc_name)
        secret_data = secret_obj.get()
        assert secret_data, f"Secret not found for OBC {obc_name}"
        logger.info(f"OBC Secret verified: {obc_name}")

        # Delete OBC
        logger.info(f"Deleting OBC {obc_name}")
        obc_obj.delete(resource_name=obc_name)

        # Verify OBC is deleted
        logger.info(f"Verifying OBC {obc_name} deletion")
        for sample in TimeoutSampler(
            timeout=180,
            sleep=10,
            func=self._check_obc_deleted,
            obc_name=obc_name,
            namespace=namespace,
        ):
            if sample:
                logger.info(f"OBC {obc_name} deleted successfully")
                break

        logger.info("Remote OBC test completed successfully on client cluster")

    def _check_obc_phase(self, obc_name, namespace):
        """
        Check if OBC has reached Bound phase.

        Args:
            obc_name (str): Name of the OBC
            namespace (str): Namespace where OBC is created

        Returns:
            bool: True if OBC is in Bound phase, False otherwise
        """
        try:
            obc_obj = OCP(
                kind="ObjectBucketClaim", namespace=namespace, resource_name=obc_name
            )
            obc_data = obc_obj.get()
            phase = obc_data.get("status", {}).get("phase")
            logger.info(f"OBC {obc_name} phase: {phase}")
            return phase == "Bound"
        except Exception as e:
            logger.warning(f"Error checking OBC phase: {e}")
            return False

    def _check_obc_deleted(self, obc_name, namespace):
        """
        Check if OBC has been deleted.

        Args:
            obc_name (str): Name of the OBC
            namespace (str): Namespace where OBC was created

        Returns:
            bool: True if OBC is deleted, False otherwise
        """
        try:
            obc_obj = OCP(
                kind="ObjectBucketClaim", namespace=namespace, resource_name=obc_name
            )
            obc_obj.get()
            logger.info(f"OBC {obc_name} still exists")
            return False
        except Exception:
            logger.info(f"OBC {obc_name} not found (deleted)")
            return True
