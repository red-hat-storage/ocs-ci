"""
Test OBC isolation between client clusters.

This test module validates that OBCs are properly isolated between different
HCI client clusters connected to a provider, ensuring proper multi-tenancy.
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

logger = logging.getLogger(__name__)

# Enable remote OBC for this module
pytestmark = pytest.mark.usefixtures("remote_obc_setup_session")

# Test constants
OBC_BIND_TIMEOUT = 300


@tier1
@red_squad
@mcg
@runs_on_provider
@hci_provider_and_client_required
class TestOBCIsolationClientProvider(ManageTest):
    """
    Test OBC isolation between client clusters.

    This test class validates that OBCs on different client clusters are
    properly isolated from each other, ensuring proper multi-tenancy.
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """Setup test resources."""
        self.test_files = []

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

    @polarion_id("OCS-7953")
    def test_obc_isolation_between_clients(self, project_factory):
        """
        Test OBC isolation between different client clusters.

        Test steps:
        1. Create OBC on first client cluster
        2. Check that the OBC is not visible on second client cluster
        3. Create OBC with the same name and the same namespace on second client cluster
        4. Upload different data to both buckets
        5. Check that correct data is in correct bucket

        Expected result:
        OBCs are separated among clients and correct data is in correct buckets

        """
        # Get client cluster indices
        client_indices = config.get_consumer_indexes_list()
        if len(client_indices) < 2:
            pytest.skip("Test requires at least 2 client clusters")

        client1_index = client_indices[0]
        client2_index = client_indices[1]

        # Shared OBC name to test isolation
        shared_obc_name = create_unique_resource_name(
            resource_description="obc", resource_type="isolation-test"
        )

        # Variables to store bucket information
        client1_bucket_name = None
        client2_bucket_name = None
        client1_s3_client = None
        client2_s3_client = None

        # Step 1: Create OBC on first client cluster
        logger.test_step("Step 1: Creating OBC on first client cluster")
        with config.RunWithConfigContext(client1_index):
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()
            assert cluster_type == constants.HCI_CLIENT, (
                "Expected HCI_CLIENT, got %s" % cluster_type
            )

            # Create project on client1
            proj1_obj = project_factory()
            namespace1 = proj1_obj.namespace
            cluster1_name = config.ENV_DATA.get("cluster_name", "client1")
            logger.info(
                "Created namespace '%s' on client cluster %s", namespace1, cluster1_name
            )

            # Create OBC on client1
            logger.info(
                "Creating OBC '%s' on client cluster %s", shared_obc_name, cluster1_name
            )
            obc_data = {
                "apiVersion": "objectbucket.io/v1alpha1",
                "kind": "ObjectBucketClaim",
                "metadata": {"name": shared_obc_name, "namespace": namespace1},
                "spec": {
                    "generateBucketName": shared_obc_name,
                    "storageClassName": constants.NOOBAA_SC,
                },
            }
            create_resource(**obc_data)
            logger.info(
                "OBC '%s' created in namespace '%s'", shared_obc_name, namespace1
            )

            # Wait for OBC to reach Bound state
            wait_for_obc_phase(
                shared_obc_name, namespace1, constants.STATUS_BOUND, OBC_BIND_TIMEOUT
            )

            # Extract S3 credentials from client1
            s3_creds1 = get_s3_credentials_from_obc(shared_obc_name, namespace1)
            client1_bucket_name = s3_creds1["bucket_name"]
            logger.info(
                "Client1 bucket: %s, Endpoint: %s",
                client1_bucket_name,
                s3_creds1["endpoint"],
            )

            # Create S3 client for client1
            client1_s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_creds1["access_key_id"],
                aws_secret_access_key=s3_creds1["secret_access_key"],
                endpoint_url="https://%s" % s3_creds1["endpoint"],
                verify=False,
            )

        # Step 2: Check that the OBC is not visible on second client cluster
        logger.test_step(
            "Step 2: Verifying OBC is not visible on second client cluster"
        )
        with config.RunWithConfigContext(client2_index):
            cluster2_name = config.ENV_DATA.get("cluster_name", "client2")
            logger.info("Checking on client cluster %s", cluster2_name)

            # Create namespace on client2 to fully test isolation
            proj2_obj = project_factory()
            namespace2 = proj2_obj.namespace
            logger.info(
                "Created namespace '%s' on client cluster %s", namespace2, cluster2_name
            )

            # Check OBC does not exist on client2
            obc_obj = OCP(kind="ObjectBucketClaim", namespace=namespace2)
            assert not obc_obj.is_exist(
                resource_name=shared_obc_name
            ), "OBC '%s' should not exist on client2 namespace '%s'" % (
                shared_obc_name,
                namespace2,
            )
            logger.info("Verified: OBC '%s' does not exist on client2", shared_obc_name)

        # Step 3: Create OBC with the same name on second client cluster
        logger.test_step("Step 3: Creating OBC with same name on second client cluster")
        with config.RunWithConfigContext(client2_index):
            # Create OBC on client2 with same name
            logger.info(
                "Creating OBC '%s' on client cluster %s", shared_obc_name, cluster2_name
            )
            obc_data2 = {
                "apiVersion": "objectbucket.io/v1alpha1",
                "kind": "ObjectBucketClaim",
                "metadata": {"name": shared_obc_name, "namespace": namespace2},
                "spec": {
                    "generateBucketName": shared_obc_name,
                    "storageClassName": constants.NOOBAA_SC,
                },
            }
            create_resource(**obc_data2)
            logger.info(
                "OBC '%s' created in namespace '%s'", shared_obc_name, namespace2
            )

            # Wait for OBC to reach Bound state
            wait_for_obc_phase(
                shared_obc_name, namespace2, constants.STATUS_BOUND, OBC_BIND_TIMEOUT
            )

            # Extract S3 credentials from client2
            s3_creds2 = get_s3_credentials_from_obc(shared_obc_name, namespace2)
            client2_bucket_name = s3_creds2["bucket_name"]
            logger.info(
                "Client2 bucket: %s, Endpoint: %s",
                client2_bucket_name,
                s3_creds2["endpoint"],
            )

            # Verify bucket names are different
            assert (
                client1_bucket_name != client2_bucket_name
            ), "Bucket names should be different: client1=%s, client2=%s" % (
                client1_bucket_name,
                client2_bucket_name,
            )
            logger.info(
                "Verified: Bucket names are different (client1: %s, client2: %s)",
                client1_bucket_name,
                client2_bucket_name,
            )

            # Create S3 client for client2
            client2_s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_creds2["access_key_id"],
                aws_secret_access_key=s3_creds2["secret_access_key"],
                endpoint_url="https://%s" % s3_creds2["endpoint"],
                verify=False,
            )

        # Step 4: Upload different data to both buckets
        logger.test_step("Step 4: Uploading different data to both buckets")

        # Upload to client1 bucket
        with config.RunWithConfigContext(client1_index):
            client1_object_key = "client1-data.txt"
            client1_data = b"This is data from CLIENT 1"

            test_file1 = tempfile.NamedTemporaryFile(delete=False, mode="wb")
            test_file1.write(client1_data)
            test_file1.flush()
            test_file1.close()
            self.test_files.append(test_file1)

            logger.info(
                "Uploading '%s' to client1 bucket '%s'",
                client1_object_key,
                client1_bucket_name,
            )
            client1_s3_client.upload_file(
                Filename=test_file1.name,
                Bucket=client1_bucket_name,
                Key=client1_object_key,
            )
            logger.info("Upload to client1 bucket completed")

        # Upload to client2 bucket
        with config.RunWithConfigContext(client2_index):
            client2_object_key = "client2-data.txt"
            client2_data = b"This is data from CLIENT 2"

            test_file2 = tempfile.NamedTemporaryFile(delete=False, mode="wb")
            test_file2.write(client2_data)
            test_file2.flush()
            test_file2.close()
            self.test_files.append(test_file2)

            logger.info(
                "Uploading '%s' to client2 bucket '%s'",
                client2_object_key,
                client2_bucket_name,
            )
            client2_s3_client.upload_file(
                Filename=test_file2.name,
                Bucket=client2_bucket_name,
                Key=client2_object_key,
            )
            logger.info("Upload to client2 bucket completed")

        # Step 5: Check that correct data is in correct bucket
        logger.test_step("Step 5: Verifying correct data is in correct buckets")

        # Verify client1 bucket has only client1 data
        with config.RunWithConfigContext(client1_index):
            logger.info("Verifying client1 bucket contains client1 data")

            # Check client1 object exists
            response1 = client1_s3_client.head_object(
                Bucket=client1_bucket_name, Key=client1_object_key
            )
            logger.info("Client1 object exists, size: %s", response1["ContentLength"])

            # Download and verify client1 data
            download1 = tempfile.NamedTemporaryFile(delete=False, mode="wb")
            download1.close()
            self.test_files.append(download1)

            client1_s3_client.download_file(
                Bucket=client1_bucket_name,
                Key=client1_object_key,
                Filename=download1.name,
            )

            with open(download1.name, "rb") as f:
                downloaded_data1 = f.read()

            assert (
                downloaded_data1 == client1_data
            ), "Client1 data mismatch. Expected: %s, Got: %s" % (
                client1_data,
                downloaded_data1,
            )
            logger.info("Verified: Client1 bucket contains correct client1 data")

            # Verify client2 object does not exist in client1 bucket
            try:
                client1_s3_client.head_object(
                    Bucket=client1_bucket_name, Key=client2_object_key
                )
                pytest.fail(
                    "Client2 object '%s' should not exist in client1 bucket"
                    % client2_object_key
                )
            except Exception as e:
                if "404" in str(e) or "NoSuchKey" in str(e) or "Not Found" in str(e):
                    logger.info(
                        "Verified: Client2 object does not exist in client1 bucket"
                    )
                else:
                    raise

        # Verify client2 bucket has only client2 data
        with config.RunWithConfigContext(client2_index):
            logger.info("Verifying client2 bucket contains client2 data")

            # Check client2 object exists
            response2 = client2_s3_client.head_object(
                Bucket=client2_bucket_name, Key=client2_object_key
            )
            logger.info("Client2 object exists, size: %s", response2["ContentLength"])

            # Download and verify client2 data
            download2 = tempfile.NamedTemporaryFile(delete=False, mode="wb")
            download2.close()
            self.test_files.append(download2)

            client2_s3_client.download_file(
                Bucket=client2_bucket_name,
                Key=client2_object_key,
                Filename=download2.name,
            )

            with open(download2.name, "rb") as f:
                downloaded_data2 = f.read()

            assert (
                downloaded_data2 == client2_data
            ), "Client2 data mismatch. Expected: %s, Got: %s" % (
                client2_data,
                downloaded_data2,
            )
            logger.info("Verified: Client2 bucket contains correct client2 data")

            # Verify client1 object does not exist in client2 bucket
            try:
                client2_s3_client.head_object(
                    Bucket=client2_bucket_name, Key=client1_object_key
                )
                pytest.fail(
                    "Client1 object '%s' should not exist in client2 bucket"
                    % client1_object_key
                )
            except Exception as e:
                if "404" in str(e) or "NoSuchKey" in str(e) or "Not Found" in str(e):
                    logger.info(
                        "Verified: Client1 object does not exist in client2 bucket"
                    )
                else:
                    raise

        logger.info(
            "Test completed successfully - OBCs are isolated between clients with correct data"
        )
