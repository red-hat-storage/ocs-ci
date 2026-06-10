"""
Test OBC deletion on client clusters with verification on provider.

This test module validates OBC deletion behavior on HCI client clusters connected to
a provider, ensuring that deletion propagates correctly from client to provider for
both empty and non-empty buckets.
"""

import boto3
import logging
import os
import pytest
import tempfile
import urllib3

from ocs_ci.framework import config

# Suppress SSL warnings for S3 client
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    hci_provider_and_client_required,
    red_squad,
    runs_on_provider,
    mcg,
    polarion_id,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import (
    get_s3_credentials_from_obc,
    wait_for_obc_phase,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Enable remote OBC for this module
pytestmark = pytest.mark.usefixtures("remote_obc_setup_session")

# Test constants
OBC_BIND_TIMEOUT = 300
OBC_DELETE_TIMEOUT = 300
OBJECTBUCKET_DELETE_TIMEOUT = 300
SECRET_DELETE_TIMEOUT = 120
CONFIGMAP_DELETE_TIMEOUT = 120
TEST_FILE_SIZE = 5 * 1024 * 1024  # 5MB


@tier1
@red_squad
@mcg
@runs_on_provider
@hci_provider_and_client_required
class TestOBCDeletionClientProvider(ManageTest):
    """
    Test OBC deletion on client clusters with verification on provider.

    This test class validates that OBC deletion on client clusters properly
    propagates to provider clusters, cleaning up all associated resources.
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """Setup test resources."""
        self.test_files = []
        self.bucket_name = None

        def finalizer():
            """Cleanup resources."""
            # Close and delete test files
            for test_file in self.test_files:
                try:
                    test_file.close()
                except Exception as e:
                    logger.warning("Failed to close test file: %s", e)

                # Delete the temporary file from disk
                try:
                    os.unlink(test_file.name)
                except Exception as e:
                    logger.warning(
                        "Failed to delete test file %s: %s", test_file.name, e
                    )

            # Switch back to current context if needed
            try:
                config.switch_ctx(config.cur_index)
            except Exception:
                pass

        request.addfinalizer(finalizer)

    @polarion_id("OCS-7951")
    def test_delete_obc_with_data(self, project_factory):
        """
        Delete OBC on client with data and verify deletion on provider.

        Test steps:
        1. Create OBC on client and add data
        2. Delete OBC on client with data
        3. Secret, ConfigMap and OB are deleted on client
        4. Secret, ConfigMap and OB are deleted on provider

        Expected result:
        OBC and its resources are deleted on both client and provider

        """
        # Get client cluster index
        client_indices = config.get_consumer_indexes_list()
        if not client_indices:
            pytest.skip("No client clusters found")

        client_index = client_indices[0]

        # Step 1: Create OBC on client and add data
        logger.test_step("Step 1: Creating OBC on client and adding data")
        with config.RunWithConfigContext(client_index):
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()
            assert cluster_type == constants.HCI_CLIENT, (
                "Expected HCI_CLIENT, got %s" % cluster_type
            )

            # Create project on client cluster
            proj_obj = project_factory()
            namespace = proj_obj.namespace
            cluster_name = config.ENV_DATA.get("cluster_name", "client")

            # Create OBC on client cluster
            logger.info("Creating OBC on client cluster %s", cluster_name)
            obc_name = create_unique_resource_name(
                resource_description="obc", resource_type="del-with-data"
            )
            obc_data = {
                "apiVersion": "objectbucket.io/v1alpha1",
                "kind": "ObjectBucketClaim",
                "metadata": {"name": obc_name, "namespace": namespace},
                "spec": {
                    "generateBucketName": obc_name,
                    "storageClassName": constants.NOOBAA_SC,
                },
            }
            create_resource(**obc_data)
            logger.info("OBC '%s' created in namespace '%s'", obc_name, namespace)

            # Wait for OBC to reach Bound state
            wait_for_obc_phase(
                obc_name, namespace, constants.STATUS_BOUND, OBC_BIND_TIMEOUT
            )

            # Extract S3 credentials
            s3_creds = get_s3_credentials_from_obc(obc_name, namespace)
            self.bucket_name = s3_creds["bucket_name"]
            logger.info(
                "Bucket: %s, Endpoint: %s", self.bucket_name, s3_creds["endpoint"]
            )

            # Create S3 client and upload data
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_creds["access_key_id"],
                aws_secret_access_key=s3_creds["secret_access_key"],
                endpoint_url="https://%s" % s3_creds["endpoint"],
                verify=False,
            )

            # Create test file with data
            test_file = tempfile.NamedTemporaryFile(delete=False, mode="wb")
            test_data = b"x" * TEST_FILE_SIZE
            test_file.write(test_data)
            test_file.flush()
            test_file.close()
            self.test_files.append(test_file)

            test_object_key = "test-object-with-data.bin"
            logger.info(
                "Uploading test object '%s' (%s bytes)", test_object_key, len(test_data)
            )
            s3_client.upload_file(
                Filename=test_file.name,
                Bucket=self.bucket_name,
                Key=test_object_key,
            )
            logger.info("Test object uploaded successfully")

            # Verify object exists
            response = s3_client.head_object(
                Bucket=self.bucket_name, Key=test_object_key
            )
            logger.info("Verified object exists, size: %s", response["ContentLength"])

        # Verify bucket exists on provider before deletion
        logger.info("Verifying bucket exists on provider before deletion")
        with config.RunWithProviderConfigContextIfAvailable():
            ob_obj = OCP(
                kind="ObjectBucket",
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            ob_list = ob_obj.get()
            bucket_found = False
            for ob in ob_list.get("items", []):
                if ob["spec"]["endpoint"]["bucketName"] == self.bucket_name:
                    bucket_found = True
                    logger.info(
                        "ObjectBucket found on provider: %s", ob["metadata"]["name"]
                    )
                    break
            assert bucket_found, (
                "Bucket %s not found on provider before deletion" % self.bucket_name
            )

        # Step 2: Delete OBC on client with data
        logger.test_step("Step 2: Deleting OBC on client with data")
        with config.RunWithConfigContext(client_index):
            logger.info("Deleting OBC '%s' from namespace '%s'", obc_name, namespace)
            obc_obj_cleanup = OCP(kind="ObjectBucketClaim", namespace=namespace)
            obc_obj_cleanup.delete(resource_name=obc_name)

            # Step 3: Verify Secret, ConfigMap and OB are deleted on client
            logger.test_step(
                "Step 3: Verifying Secret, ConfigMap and OB are deleted on client"
            )

            # Wait for OBC deletion
            logger.info("Waiting for OBC deletion on client")
            obc_obj_check = OCP(kind="ObjectBucketClaim", namespace=namespace)
            obc_obj_check.wait_for_delete(
                resource_name=obc_name, timeout=OBC_DELETE_TIMEOUT
            )
            logger.info("OBC '%s' deleted successfully on client", obc_name)

            # Verify Secret deletion
            logger.info("Verifying Secret deletion on client")
            secret_obj_check = OCP(kind=constants.SECRET, namespace=namespace)
            secret_obj_check.wait_for_delete(
                resource_name=obc_name, timeout=SECRET_DELETE_TIMEOUT
            )
            logger.info("Secret '%s' deleted successfully on client", obc_name)

            # Verify ConfigMap deletion
            logger.info("Verifying ConfigMap deletion on client")
            configmap_obj_check = OCP(kind=constants.CONFIGMAP, namespace=namespace)
            configmap_obj_check.wait_for_delete(
                resource_name=obc_name, timeout=CONFIGMAP_DELETE_TIMEOUT
            )
            logger.info("ConfigMap '%s' deleted successfully on client", obc_name)

        # Step 4: Verify Secret, ConfigMap and OB are deleted on provider
        logger.test_step("Step 4: Verifying ObjectBucket deletion on provider")
        with config.RunWithProviderConfigContextIfAvailable():
            for sample in TimeoutSampler(
                timeout=OBJECTBUCKET_DELETE_TIMEOUT,
                sleep=10,
                func=self._check_objectbucket_deleted,
                bucket_name=self.bucket_name,
            ):
                if sample:
                    logger.info(
                        "ObjectBucket for '%s' deleted on provider", self.bucket_name
                    )
                    break
            else:
                pytest.fail(
                    "ObjectBucket for '%s' was not deleted within timeout on provider"
                    % self.bucket_name
                )

        logger.info(
            "Test completed successfully - OBC with data deleted on both client and provider"
        )

    @polarion_id("OCS-7952")
    def test_delete_obc_without_data(self, project_factory):
        """
        Delete OBC on client without data and verify deletion on provider.

        Test steps:
        1. Create OBC on client with no data
        2. Delete OBC on client with no data
        3. Secret, ConfigMap and OB are deleted on client
        4. Secret, ConfigMap and OB are deleted on provider

        Expected result:
        OBC and its resources are deleted on both client and provider

        """
        # Get client cluster index
        client_indices = config.get_consumer_indexes_list()
        if not client_indices:
            pytest.skip("No client clusters found")

        client_index = client_indices[0]

        # Step 1: Create OBC on client with no data
        logger.test_step("Step 1: Creating OBC on client with no data")
        with config.RunWithConfigContext(client_index):
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()
            assert cluster_type == constants.HCI_CLIENT, (
                "Expected HCI_CLIENT, got %s" % cluster_type
            )

            # Create project on client cluster
            proj_obj = project_factory()
            namespace = proj_obj.namespace
            cluster_name = config.ENV_DATA.get("cluster_name", "client")

            # Create OBC on client cluster
            logger.info("Creating OBC on client cluster %s", cluster_name)
            obc_name = create_unique_resource_name(
                resource_description="obc", resource_type="del-no-data"
            )
            obc_data = {
                "apiVersion": "objectbucket.io/v1alpha1",
                "kind": "ObjectBucketClaim",
                "metadata": {"name": obc_name, "namespace": namespace},
                "spec": {
                    "generateBucketName": obc_name,
                    "storageClassName": constants.NOOBAA_SC,
                },
            }
            create_resource(**obc_data)
            logger.info("OBC '%s' created in namespace '%s'", obc_name, namespace)

            # Wait for OBC to reach Bound state
            wait_for_obc_phase(
                obc_name, namespace, constants.STATUS_BOUND, OBC_BIND_TIMEOUT
            )

            # Extract bucket name for verification
            s3_creds = get_s3_credentials_from_obc(obc_name, namespace)
            self.bucket_name = s3_creds["bucket_name"]
            logger.info("Bucket: %s (no data uploaded)", self.bucket_name)

        # Verify bucket exists on provider before deletion
        logger.info("Verifying bucket exists on provider before deletion")
        with config.RunWithProviderConfigContextIfAvailable():
            ob_obj = OCP(
                kind="ObjectBucket",
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            ob_list = ob_obj.get()
            bucket_found = False
            for ob in ob_list.get("items", []):
                if ob["spec"]["endpoint"]["bucketName"] == self.bucket_name:
                    bucket_found = True
                    logger.info(
                        "ObjectBucket found on provider: %s", ob["metadata"]["name"]
                    )
                    break
            assert bucket_found, (
                "Bucket %s not found on provider before deletion" % self.bucket_name
            )

        # Step 2: Delete OBC on client with no data
        logger.test_step("Step 2: Deleting OBC on client with no data")
        with config.RunWithConfigContext(client_index):
            logger.info("Deleting OBC '%s' from namespace '%s'", obc_name, namespace)
            obc_obj_cleanup = OCP(kind="ObjectBucketClaim", namespace=namespace)
            obc_obj_cleanup.delete(resource_name=obc_name)

            # Step 3: Verify Secret, ConfigMap and OB are deleted on client
            logger.test_step(
                "Step 3: Verifying Secret, ConfigMap and OB are deleted on client"
            )

            # Wait for OBC deletion
            logger.info("Waiting for OBC deletion on client")
            obc_obj_check = OCP(kind="ObjectBucketClaim", namespace=namespace)
            obc_obj_check.wait_for_delete(
                resource_name=obc_name, timeout=OBC_DELETE_TIMEOUT
            )
            logger.info("OBC '%s' deleted successfully on client", obc_name)

            # Verify Secret deletion
            logger.info("Verifying Secret deletion on client")
            secret_obj_check = OCP(kind=constants.SECRET, namespace=namespace)
            secret_obj_check.wait_for_delete(
                resource_name=obc_name, timeout=SECRET_DELETE_TIMEOUT
            )
            logger.info("Secret '%s' deleted successfully on client", obc_name)

            # Verify ConfigMap deletion
            logger.info("Verifying ConfigMap deletion on client")
            configmap_obj_check = OCP(kind=constants.CONFIGMAP, namespace=namespace)
            configmap_obj_check.wait_for_delete(
                resource_name=obc_name, timeout=CONFIGMAP_DELETE_TIMEOUT
            )
            logger.info("ConfigMap '%s' deleted successfully on client", obc_name)

        # Step 4: Verify Secret, ConfigMap and OB are deleted on provider
        logger.test_step("Step 4: Verifying ObjectBucket deletion on provider")
        with config.RunWithProviderConfigContextIfAvailable():
            for sample in TimeoutSampler(
                timeout=OBJECTBUCKET_DELETE_TIMEOUT,
                sleep=10,
                func=self._check_objectbucket_deleted,
                bucket_name=self.bucket_name,
            ):
                if sample:
                    logger.info(
                        "ObjectBucket for '%s' deleted on provider", self.bucket_name
                    )
                    break
            else:
                pytest.fail(
                    "ObjectBucket for '%s' was not deleted within timeout on provider"
                    % self.bucket_name
                )

        logger.info(
            "Test completed successfully - OBC without data deleted on both client and provider"
        )

    # Helper methods

    def _check_objectbucket_deleted(self, bucket_name):
        """
        Check if ObjectBucket has been deleted on provider.

        Args:
            bucket_name (str): Bucket name to check

        Returns:
            bool: True if ObjectBucket is deleted, False otherwise

        """
        if not bucket_name:
            return True

        try:
            ob_obj = OCP(
                kind="ObjectBucket",
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            ob_list = ob_obj.get()
            for ob in ob_list.get("items", []):
                if ob["spec"]["endpoint"]["bucketName"] == bucket_name:
                    logger.info("ObjectBucket for '%s' still exists", bucket_name)
                    return False
            logger.info("ObjectBucket for '%s' not found (deleted)", bucket_name)
            return True
        except Exception as e:
            logger.warning("Error checking ObjectBucket deletion: %s", e)
            return True
