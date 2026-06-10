"""
Test NooBaa Object Browser UI on client clusters in provider mode.

This test module validates object browser functionality on HCI client clusters,
ensuring proper isolation and folder navigation in the UI.
"""

import logging
import os
import pytest
import time

from selenium.webdriver.common.by import By

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    hci_provider_and_client_required,
    red_squad,
    runs_on_provider,
    mcg,
    polarion_id,
    ui,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import (
    get_s3_credentials_from_obc,
    wait_for_obc_phase,
)
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab
from ocs_ci.ocs.ui.page_objects.s3_login_form import S3LoginForm
import boto3
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# Enable remote OBC and object browser CA cert setup for this module
pytestmark = [
    pytest.mark.usefixtures("remote_obc_setup_session"),
    pytest.mark.usefixtures("object_browser_ca_cert_setup_client"),
]

# Test constants
OBC_BIND_TIMEOUT = 300


@tier1
@ui
@red_squad
@mcg
@runs_on_provider
@hci_provider_and_client_required
class TestObjectBrowserClientProviderUI(ManageTest):
    """
    Test NooBaa Object Browser UI on client clusters.

    This test class validates object browser functionality with proper
    isolation between client clusters and folder navigation.
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """Setup test resources."""
        self.test_files = []
        self.obcs_to_delete = []

        def finalizer():
            """Cleanup resources."""
            # Close test file handles and delete files
            for test_file in self.test_files:
                try:
                    test_file.close()
                except Exception as e:
                    logger.warning("Failed to close test file: %s", e)

                try:
                    os.unlink(test_file.name)
                except Exception as e:
                    logger.warning(
                        "Failed to delete test file %s: %s", test_file.name, e
                    )

            # Delete OBCs
            for obc_info in self.obcs_to_delete:
                try:
                    with config.RunWithConfigContext(obc_info["cluster_index"]):
                        obc_obj = OCP(
                            kind="ObjectBucketClaim", namespace=obc_info["namespace"]
                        )
                        obc_obj.delete(resource_name=obc_info["obc_name"])
                        logger.info(
                            "Deleted OBC '%s' from cluster index %d",
                            obc_info["obc_name"],
                            obc_info["cluster_index"],
                        )
                except Exception as e:
                    logger.warning("Failed to delete OBC: %s", e)

            # Close browser
            try:
                close_browser()
            except Exception as e:
                logger.warning("Failed to close browser: %s", e)

            # Switch back to current context
            try:
                config.switch_ctx(config.cur_index)
            except Exception:
                pass

        request.addfinalizer(finalizer)

    @polarion_id("OCS-7990")
    def test_object_browser_list_objects_with_folders(self, project_factory):
        """
        Test object browser can list objects and navigate folders on client clusters.

        Test steps:
        1. Create buckets on 2 client clusters
        2. Upload objects with folder-like paths to both buckets
        3. Login to object browser on client 1
        4. List objects and verify correct objects are shown
        5. Verify folder navigation works (paths shown as folders)
        6. Login to object browser on client 2
        7. Verify client 2 only sees its own objects, not client 1's

        Expected result:
        - Correct objects are shown in the test bucket
        - Paths are shown as folders allowing navigation
        - Other clients don't see resources created on different client
        """
        # Get client cluster indices
        client_indices = config.get_consumer_indexes_list()
        if len(client_indices) < 2:
            pytest.skip("Test requires at least 2 client clusters")

        client1_index = client_indices[0]
        client2_index = client_indices[1]

        # Store OBC and bucket information
        client1_obc_name = None
        client2_obc_name = None
        client1_bucket_name = None
        client2_bucket_name = None
        client1_namespace = None
        client2_namespace = None
        client1_s3_client = None
        client2_s3_client = None
        client1_secret_name = None
        client2_secret_name = None

        # Step 1: Create OBC and upload objects on first client cluster
        logger.test_step(
            "Step 1: Creating OBC and uploading objects on first client cluster"
        )
        with config.RunWithConfigContext(client1_index):
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()
            assert cluster_type == constants.HCI_CLIENT, (
                "Expected HCI_CLIENT, got %s" % cluster_type
            )

            # Create project on client1
            proj1_obj = project_factory()
            client1_namespace = proj1_obj.namespace
            cluster1_name = config.ENV_DATA.get("cluster_name", "client1")
            client1_url = config.ENV_DATA.get("console_url")
            logger.info(
                "Created namespace '%s' on client cluster %s (URL: %s)",
                client1_namespace,
                cluster1_name,
                client1_url,
            )

            # Create OBC on client1
            client1_obc_name = create_unique_resource_name(
                resource_description="obc", resource_type="browser-test"
            )
            obc_data = {
                "apiVersion": "objectbucket.io/v1alpha1",
                "kind": "ObjectBucketClaim",
                "metadata": {"name": client1_obc_name, "namespace": client1_namespace},
                "spec": {
                    "generateBucketName": client1_obc_name,
                    "storageClassName": constants.NOOBAA_SC,
                },
            }
            create_resource(**obc_data)
            logger.info(
                "OBC '%s' created in namespace '%s'",
                client1_obc_name,
                client1_namespace,
            )

            # Track for cleanup
            self.obcs_to_delete.append(
                {
                    "obc_name": client1_obc_name,
                    "namespace": client1_namespace,
                    "cluster_index": client1_index,
                }
            )

            # Wait for OBC to reach Bound state
            wait_for_obc_phase(
                client1_obc_name,
                client1_namespace,
                constants.STATUS_BOUND,
                OBC_BIND_TIMEOUT,
            )

            # Extract S3 credentials
            s3_creds1 = get_s3_credentials_from_obc(client1_obc_name, client1_namespace)
            client1_bucket_name = s3_creds1["bucket_name"]
            client1_secret_name = client1_obc_name  # Secret has same name as OBC
            logger.info(
                "Client1 bucket: %s, Secret: %s",
                client1_bucket_name,
                client1_secret_name,
            )

            # Create S3 client for client1
            client1_s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_creds1["access_key_id"],
                aws_secret_access_key=s3_creds1["secret_access_key"],
                endpoint_url="https://%s" % s3_creds1["endpoint"],
                verify=False,
            )

            # Upload test objects with folder-like paths
            logger.info(
                "Uploading test objects with folder-like paths to client1 bucket"
            )
            test_objects = [
                "folder1/file1.txt",
                "folder1/file2.txt",
                "folder1/subfolder/file3.txt",
                "folder2/file4.txt",
                "root-file.txt",
            ]

            for obj_key in test_objects:
                test_data = f"Client 1 test data for {obj_key}".encode()
                client1_s3_client.put_object(
                    Bucket=client1_bucket_name, Key=obj_key, Body=test_data
                )
                logger.info("Uploaded object: %s", obj_key)

        # Step 1b: Create OBC and upload objects on second client cluster
        logger.test_step(
            "Step 1b: Creating OBC and uploading objects on second client cluster"
        )
        with config.RunWithConfigContext(client2_index):
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()
            assert cluster_type == constants.HCI_CLIENT, (
                "Expected HCI_CLIENT, got %s" % cluster_type
            )

            # Create project on client2
            proj2_obj = project_factory()
            client2_namespace = proj2_obj.namespace
            cluster2_name = config.ENV_DATA.get("cluster_name", "client2")
            client2_url = config.ENV_DATA.get("console_url")
            logger.info(
                "Created namespace '%s' on client cluster %s (URL: %s)",
                client2_namespace,
                cluster2_name,
                client2_url,
            )

            # Create OBC on client2
            client2_obc_name = create_unique_resource_name(
                resource_description="obc", resource_type="browser-test"
            )
            obc_data2 = {
                "apiVersion": "objectbucket.io/v1alpha1",
                "kind": "ObjectBucketClaim",
                "metadata": {"name": client2_obc_name, "namespace": client2_namespace},
                "spec": {
                    "generateBucketName": client2_obc_name,
                    "storageClassName": constants.NOOBAA_SC,
                },
            }
            create_resource(**obc_data2)
            logger.info(
                "OBC '%s' created in namespace '%s'",
                client2_obc_name,
                client2_namespace,
            )

            # Track for cleanup
            self.obcs_to_delete.append(
                {
                    "obc_name": client2_obc_name,
                    "namespace": client2_namespace,
                    "cluster_index": client2_index,
                }
            )

            # Wait for OBC to reach Bound state
            wait_for_obc_phase(
                client2_obc_name,
                client2_namespace,
                constants.STATUS_BOUND,
                OBC_BIND_TIMEOUT,
            )

            # Extract S3 credentials
            s3_creds2 = get_s3_credentials_from_obc(client2_obc_name, client2_namespace)
            client2_bucket_name = s3_creds2["bucket_name"]
            client2_secret_name = client2_obc_name  # Secret has same name as OBC
            logger.info(
                "Client2 bucket: %s, Secret: %s",
                client2_bucket_name,
                client2_secret_name,
            )

            # Create S3 client for client2
            client2_s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_creds2["access_key_id"],
                aws_secret_access_key=s3_creds2["secret_access_key"],
                endpoint_url="https://%s" % s3_creds2["endpoint"],
                verify=False,
            )

            # Upload different test objects to client2 bucket
            logger.info(
                "Uploading test objects with folder-like paths to client2 bucket"
            )
            test_objects2 = [
                "client2-folder/file-a.txt",
                "client2-folder/file-b.txt",
                "client2-data.txt",
            ]

            for obj_key in test_objects2:
                test_data = f"Client 2 test data for {obj_key}".encode()
                client2_s3_client.put_object(
                    Bucket=client2_bucket_name, Key=obj_key, Body=test_data
                )
                logger.info("Uploaded object: %s", obj_key)

        # Step 2-5: Login to object browser on client 1 and verify objects
        logger.test_step("Step 2-5: Testing object browser on client 1")
        with config.RunWithConfigContext(client1_index):
            # Get client1 console URL and credentials
            console_url = config.ENV_DATA.get("console_url")
            logger.info("Logging into client1 console at: %s", console_url)

            # Login to OpenShift console
            login_ui()
            time.sleep(3)

            # Navigate to Object Storage page
            logger.info("Navigating to Object Storage page")
            bucket_ui = BucketsTab()
            bucket_ui.nav_object_storage_page()
            time.sleep(3)

            # Sign in with S3 credentials
            logger.info(
                "Signing in with S3 secret: %s/%s",
                client1_namespace,
                client1_secret_name,
            )
            s3_login = S3LoginForm()
            s3_login.sign_in_with_secret(
                namespace=client1_namespace,
                secret_name=client1_secret_name,
            )
            time.sleep(2)

            # Verify S3 sign-in success
            assert s3_login.is_signed_in(), "S3 login failed on client1"
            logger.info("Successfully signed in to object browser on client1")

            # Navigate to the bucket (click on bucket name)
            logger.info("Navigating to bucket: %s", client1_bucket_name)
            bucket_ui.do_click(
                (f"//a[contains(text(), '{client1_bucket_name}')]", By.XPATH)
            )
            time.sleep(3)

            # Verify objects/folders are visible using existing locators
            logger.info("Verifying objects/folders are visible in bucket")

            # Check if we can find folder1 using the file_name_text locator
            folder1_locator = format_locator(
                bucket_ui.bucket_tab["file_name_text"], "folder1"
            )
            folder1_elements = bucket_ui.get_elements(folder1_locator)
            assert folder1_elements, "folder1 not found in object list"
            logger.info("✓ Found folder1 in object list")

            # Check if we can find root-file.txt
            root_file_locator = format_locator(
                bucket_ui.bucket_tab["file_name_text"], "root-file.txt"
            )
            root_file_elements = bucket_ui.get_elements(root_file_locator)
            assert root_file_elements, "root-file.txt not found in object list"
            logger.info("✓ Found root-file.txt in object list")

            # Navigate into folder1 by clicking on its name
            logger.info("Attempting folder navigation into folder1")
            bucket_ui.do_click(("//a[contains(text(), 'folder1')]", By.XPATH))
            time.sleep(2)
            logger.info("✓ Clicked on folder1 link - navigation attempted")

            # Verify we're inside folder1 by looking for file1.txt
            file1_locator = format_locator(
                bucket_ui.bucket_tab["file_name_text"], "file1.txt"
            )
            file1_elements = bucket_ui.get_elements(file1_locator)
            assert file1_elements, "file1.txt not found after navigating into folder1"
            logger.info("✓ Successfully navigated into folder - found file1.txt")

        # Step 6-7: Login to object browser on client 2 and verify isolation
        logger.test_step(
            "Step 6-7: Testing object browser on client 2 and verifying isolation"
        )
        with config.RunWithConfigContext(client2_index):
            # Get client2 console URL
            console_url = config.ENV_DATA.get("console_url")
            logger.info("Logging into client2 console at: %s", console_url)

            # Close previous browser and login to client2
            close_browser()
            time.sleep(2)
            login_ui()
            time.sleep(3)

            # Navigate to Object Storage page
            logger.info("Navigating to Object Storage page")
            bucket_ui2 = BucketsTab()
            bucket_ui2.nav_object_storage_page()
            time.sleep(3)

            # Sign in with S3 credentials for client2
            logger.info(
                "Signing in with S3 secret: %s/%s",
                client2_namespace,
                client2_secret_name,
            )
            s3_login2 = S3LoginForm()
            s3_login2.sign_in_with_secret(
                namespace=client2_namespace,
                secret_name=client2_secret_name,
            )
            time.sleep(2)

            # Verify S3 sign-in success
            assert s3_login2.is_signed_in(), "S3 login failed on client2"
            logger.info("Successfully signed in to object browser on client2")

            # Verify client2 sees its own bucket
            buckets_list = bucket_ui2.get_buckets_list()
            logger.info("Buckets visible on client2: %s", buckets_list)
            assert (
                client2_bucket_name in buckets_list
            ), f"Client2 bucket '{client2_bucket_name}' not found in bucket list"

            # Verify client2 does NOT see client1's bucket
            assert (
                client1_bucket_name not in buckets_list
            ), f"Client1 bucket '{client1_bucket_name}' should not be visible on client2"
            logger.info("Verified: Client2 cannot see client1's bucket")

            # Navigate to client2 bucket
            logger.info("Navigating to client2 bucket: %s", client2_bucket_name)
            bucket_ui2.do_click(
                (f"//a[contains(text(), '{client2_bucket_name}')]", By.XPATH)
            )
            time.sleep(3)

            # Verify client2 objects are listed in UI
            logger.info("Verifying client2 objects are visible in bucket")

            # Check if we can find client2-folder
            client2_folder_locator = format_locator(
                bucket_ui2.bucket_tab["file_name_text"], "client2-folder"
            )
            client2_folder_elements = bucket_ui2.get_elements(client2_folder_locator)
            assert client2_folder_elements, "client2-folder not found in object list"
            logger.info("✓ Found client2-folder in object list")

            # Check if we can find client2-data.txt
            client2_data_locator = format_locator(
                bucket_ui2.bucket_tab["file_name_text"], "client2-data.txt"
            )
            client2_data_elements = bucket_ui2.get_elements(client2_data_locator)
            assert client2_data_elements, "client2-data.txt not found in object list"
            logger.info("✓ Found client2-data.txt in object list")

        logger.info("Test completed successfully - Object browser isolation verified")
