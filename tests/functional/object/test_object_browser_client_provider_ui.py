"""
Test NooBaa Object Browser UI on client clusters in provider mode.

This test module validates object browser functionality on HCI client clusters,
ensuring proper isolation and folder navigation in the UI.
"""

import logging
import os
import pytest
import requests
import shutil
import tempfile

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    hci_provider_and_client_required,
    red_squad,
    runs_on_provider,
    mcg,
    polarion_id,
    ui,
    jira,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import (
    get_s3_credentials_from_obc,
    wait_for_obc_phase,
)
from ocs_ci.ocs.ui.base_ui import (
    accept_s3_endpoint_certificate,
    login_ui,
    close_browser,
)
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

            try:
                close_browser()
            except Exception as e:
                logger.warning("Failed to close browser: %s", e)

            try:
                config.switch_ctx(config.cur_index)
            except Exception:
                pass

        request.addfinalizer(finalizer)

    def _create_obc_on_client(self, client_index, project_factory, resource_type):
        """
        Create an OBC on a client cluster and return S3 credentials.

        Args:
            client_index (int): Cluster index of the client.
            project_factory: Pytest fixture for project creation.
            resource_type (str): Resource type suffix for naming.

        Returns:
            dict: Keys: obc_name, namespace, bucket_name, s3_client, s3_creds.

        """
        with config.RunWithConfigContext(client_index):
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()
            assert cluster_type == constants.HCI_CLIENT, (
                "Expected HCI_CLIENT, got %s" % cluster_type
            )

            proj_obj = project_factory()
            namespace = proj_obj.namespace
            logger.info(
                "Created namespace '%s' on client cluster %s (URL: %s)",
                namespace,
                config.ENV_DATA.get("cluster_name", "unknown"),
                config.ENV_DATA.get("console_url"),
            )

            obc_name = create_unique_resource_name(
                resource_description="obc", resource_type=resource_type
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

            self.obcs_to_delete.append(
                {
                    "obc_name": obc_name,
                    "namespace": namespace,
                    "cluster_index": client_index,
                }
            )

            wait_for_obc_phase(
                obc_name, namespace, constants.STATUS_BOUND, OBC_BIND_TIMEOUT
            )

            s3_creds = get_s3_credentials_from_obc(obc_name, namespace)
            bucket_name = s3_creds["bucket_name"]
            logger.info("Bucket: %s, Secret: %s", bucket_name, obc_name)

            s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_creds["access_key_id"],
                aws_secret_access_key=s3_creds["secret_access_key"],
                endpoint_url="https://%s" % s3_creds["endpoint"],
                verify=False,
            )

            return {
                "obc_name": obc_name,
                "namespace": namespace,
                "bucket_name": bucket_name,
                "s3_client": s3_client,
                "s3_creds": s3_creds,
            }

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
        client_indices = config.get_consumer_indexes_list()
        if len(client_indices) < 2:
            pytest.skip("Test requires at least 2 client clusters")

        client1_index = client_indices[0]
        client2_index = client_indices[1]

        # Step 1: Create OBC and upload objects on first client cluster
        logger.test_step(
            "Step 1: Creating OBC and uploading objects on first client cluster"
        )
        c1 = self._create_obc_on_client(client1_index, project_factory, "browser-test")

        with config.RunWithConfigContext(client1_index):
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
                test_data = "Client 1 test data for %s" % obj_key
                c1["s3_client"].put_object(
                    Bucket=c1["bucket_name"],
                    Key=obj_key,
                    Body=test_data.encode(),
                )
                logger.info("Uploaded object: %s", obj_key)

        # Step 1b: Create OBC and upload objects on second client cluster
        logger.test_step(
            "Step 1b: Creating OBC and uploading objects on second client cluster"
        )
        c2 = self._create_obc_on_client(client2_index, project_factory, "browser-test")

        with config.RunWithConfigContext(client2_index):
            logger.info(
                "Uploading test objects with folder-like paths to client2 bucket"
            )
            test_objects2 = [
                "client2-folder/file-a.txt",
                "client2-folder/file-b.txt",
                "client2-data.txt",
            ]

            for obj_key in test_objects2:
                test_data = "Client 2 test data for %s" % obj_key
                c2["s3_client"].put_object(
                    Bucket=c2["bucket_name"],
                    Key=obj_key,
                    Body=test_data.encode(),
                )
                logger.info("Uploaded object: %s", obj_key)

        # Step 2-5: Login to object browser on client 1 and verify objects
        # RunWithConfigContext sets config.ENV_DATA (including console_url),
        # so login_ui() connects to the correct client cluster UI.
        logger.test_step("Step 2-5: Testing object browser on client 1")
        with config.RunWithConfigContext(client1_index):
            logger.info(
                "Logging into client1 console at: %s",
                config.ENV_DATA.get("console_url"),
            )
            login_ui()

            accept_s3_endpoint_certificate(c1["s3_creds"]["endpoint"])

            logger.info("Navigating to Object Storage page")
            bucket_ui = BucketsTab()
            bucket_ui.nav_object_storage_page()

            logger.info(
                "Signing in with S3 secret: %s/%s",
                c1["namespace"],
                c1["obc_name"],
            )
            s3_login = S3LoginForm()
            s3_login.sign_in_with_secret(
                namespace=c1["namespace"],
                secret_name=c1["obc_name"],
            )

            assert s3_login.is_signed_in(), "S3 login failed on client1"
            logger.info("Successfully signed in to object browser on client1")

            logger.info("Navigating to bucket: %s", c1["bucket_name"])
            bucket_ui.navigate_to_bucket_by_name(c1["bucket_name"])

            logger.info("Verifying objects/folders are visible in bucket")

            folder1_locator = format_locator(
                bucket_ui.bucket_tab["file_name_text"], "folder1"
            )
            folder1_elements = bucket_ui.get_elements(folder1_locator)
            assert folder1_elements, "folder1 not found in object list"
            logger.info("Found folder1 in object list")

            root_file_locator = format_locator(
                bucket_ui.bucket_tab["file_name_text"], "root-file.txt"
            )
            root_file_elements = bucket_ui.get_elements(root_file_locator)
            assert root_file_elements, "root-file.txt not found in object list"
            logger.info("Found root-file.txt in object list")

            logger.info("Attempting folder navigation into folder1")
            folder1_link = format_locator(bucket_ui.bucket_tab["item_link"], "folder1")
            bucket_ui.do_click(folder1_link)
            logger.info("Clicked on folder1 link - navigation attempted")

            file1_locator = format_locator(
                bucket_ui.bucket_tab["file_name_text"], "file1.txt"
            )
            file1_elements = bucket_ui.get_elements(file1_locator)
            assert file1_elements, "file1.txt not found after navigating into folder1"
            logger.info("Successfully navigated into folder - found file1.txt")

        # Step 6-7: Login to object browser on client 2 and verify isolation
        # Close the browser from client1 session, then open a new one for client2.
        logger.test_step(
            "Step 6-7: Testing object browser on client 2 and verifying isolation"
        )
        with config.RunWithConfigContext(client2_index):
            logger.info(
                "Logging into client2 console at: %s",
                config.ENV_DATA.get("console_url"),
            )
            close_browser()
            login_ui()

            accept_s3_endpoint_certificate(c2["s3_creds"]["endpoint"])

            logger.info("Navigating to Object Storage page")
            bucket_ui2 = BucketsTab()
            bucket_ui2.nav_object_storage_page()

            logger.info(
                "Signing in with S3 secret: %s/%s",
                c2["namespace"],
                c2["obc_name"],
            )
            s3_login2 = S3LoginForm()
            s3_login2.sign_in_with_secret(
                namespace=c2["namespace"],
                secret_name=c2["obc_name"],
            )

            assert s3_login2.is_signed_in(), "S3 login failed on client2"
            logger.info("Successfully signed in to object browser on client2")

            buckets_list = bucket_ui2.get_buckets_list()
            logger.info("Buckets visible on client2: %s", buckets_list)
            assert c2["bucket_name"] in buckets_list, (
                "Client2 bucket '%s' not found in bucket list" % c2["bucket_name"]
            )

            assert c1["bucket_name"] not in buckets_list, (
                "Client1 bucket '%s' should not be visible on client2"
                % c1["bucket_name"]
            )
            logger.info("Verified: Client2 cannot see client1's bucket")

            logger.info("Navigating to client2 bucket: %s", c2["bucket_name"])
            bucket_ui2.navigate_to_bucket_by_name(c2["bucket_name"])

            logger.info("Verifying client2 objects are visible in bucket")

            client2_folder_locator = format_locator(
                bucket_ui2.bucket_tab["file_name_text"], "client2-folder"
            )
            client2_folder_elements = bucket_ui2.get_elements(client2_folder_locator)
            assert client2_folder_elements, "client2-folder not found in object list"
            logger.info("Found client2-folder in object list")

            client2_data_locator = format_locator(
                bucket_ui2.bucket_tab["file_name_text"], "client2-data.txt"
            )
            client2_data_elements = bucket_ui2.get_elements(client2_data_locator)
            assert client2_data_elements, "client2-data.txt not found in object list"
            logger.info("Found client2-data.txt in object list")

        logger.info("Test completed successfully - Object browser isolation verified")

    @polarion_id("OCS-7994")
    @jira("DFBUGS-7973")
    def test_object_browser_upload_download_folder(self, project_factory):
        """
        Test upload and download folder with objects via object browser.

        Test steps:
        1. Upload a folder with objects via UI
        2. Download an object and check integrity via S3
        3. Download the folder contents via S3
        4. Delete an object from folder via S3
        5. Delete the folder contents via S3

        Steps 2-5 use S3 API because the object browser UI does not expose
        download or delete actions for individual objects.

        Expected result:
        - Upload, download integrity and deletion operations succeed
        """
        client_indices = config.get_consumer_indexes_list()
        if len(client_indices) < 1:
            pytest.skip("Test requires at least 1 client cluster")

        client_index = client_indices[0]

        temp_folder = tempfile.mkdtemp(prefix="test-folder-")
        test_file1 = os.path.join(temp_folder, "file1.txt")
        test_file2 = os.path.join(temp_folder, "file2.txt")
        test_data1 = b"Test data for file1"
        test_data2 = b"Test data for file2"

        with open(test_file1, "wb") as f:
            f.write(test_data1)
        with open(test_file2, "wb") as f:
            f.write(test_data2)

        folder_name = os.path.basename(temp_folder)
        logger.info("Created temporary folder: %s", temp_folder)

        try:
            # Step 1: Create OBC on client cluster
            logger.test_step("Step 1: Creating OBC on client cluster")
            obc = self._create_obc_on_client(
                client_index, project_factory, "folder-test"
            )

            # RunWithConfigContext sets config.ENV_DATA (including console_url),
            # so login_ui() connects to the correct client cluster UI.
            with config.RunWithConfigContext(client_index):
                # Step 2: Upload folder via UI
                logger.test_step("Step 2: Uploading folder via UI")
                login_ui()

                accept_s3_endpoint_certificate(obc["s3_creds"]["endpoint"])

                bucket_ui = BucketsTab()
                bucket_ui.nav_object_storage_page()

                s3_login = S3LoginForm()
                s3_login.sign_in_with_secret(
                    namespace=obc["namespace"], secret_name=obc["obc_name"]
                )
                assert s3_login.is_signed_in(), "S3 login failed"
                logger.info("Successfully signed in to object browser")

                bucket_ui.navigate_to_bucket_by_name(obc["bucket_name"])

                logger.info("Uploading folder: %s", temp_folder)
                bucket_ui.upload_folder_to_bucket(temp_folder)

                folder_locator = format_locator(
                    bucket_ui.bucket_tab["file_name_text"], folder_name
                )
                folder_elements = bucket_ui.get_elements(folder_locator)
                assert folder_elements, (
                    "Folder '%s' not found after upload" % folder_name
                )
                logger.info("Folder uploaded successfully")

                # Step 3: Download object and verify integrity via S3
                logger.test_step("Step 3: Downloading object and verifying integrity")
                download_path = os.path.join(
                    tempfile.gettempdir(), "downloaded_file1.txt"
                )
                obc["s3_client"].download_file(
                    obc["bucket_name"],
                    "%s/file1.txt" % folder_name,
                    download_path,
                )
                logger.info("Downloaded file to: %s", download_path)

                with open(download_path, "rb") as f:
                    downloaded_data = f.read()
                assert (
                    downloaded_data == test_data1
                ), "Downloaded file content does not match original"
                logger.info("File integrity verified")
                os.unlink(download_path)

                # Step 4: Download entire folder via S3
                logger.test_step("Step 4: Downloading entire folder")
                download_folder = tempfile.mkdtemp(prefix="downloaded-folder-")
                try:
                    response = obc["s3_client"].list_objects_v2(
                        Bucket=obc["bucket_name"],
                        Prefix="%s/" % folder_name,
                    )
                    if "Contents" in response:
                        for obj in response["Contents"]:
                            key = obj["Key"]
                            file_path = os.path.join(
                                download_folder, os.path.basename(key)
                            )
                            obc["s3_client"].download_file(
                                obc["bucket_name"], key, file_path
                            )
                            logger.info("Downloaded: %s", key)

                    downloaded_file1 = os.path.join(download_folder, "file1.txt")
                    downloaded_file2 = os.path.join(download_folder, "file2.txt")
                    assert os.path.exists(
                        downloaded_file1
                    ), "file1.txt not in downloaded folder"
                    assert os.path.exists(
                        downloaded_file2
                    ), "file2.txt not in downloaded folder"

                    with open(downloaded_file1, "rb") as f:
                        assert f.read() == test_data1, "file1.txt content mismatch"
                    with open(downloaded_file2, "rb") as f:
                        assert f.read() == test_data2, "file2.txt content mismatch"
                    logger.info("Folder downloaded and verified successfully")
                finally:
                    shutil.rmtree(download_folder)

                # Step 5: Delete object from folder via S3
                logger.test_step("Step 5: Deleting object from folder via S3")
                obc["s3_client"].delete_object(
                    Bucket=obc["bucket_name"],
                    Key="%s/file1.txt" % folder_name,
                )
                logger.info("Deleted file1.txt from folder")

                response = obc["s3_client"].list_objects_v2(
                    Bucket=obc["bucket_name"],
                    Prefix="%s/" % folder_name,
                )
                remaining_objects = [obj["Key"] for obj in response.get("Contents", [])]
                assert (
                    "%s/file1.txt" % folder_name not in remaining_objects
                ), "file1.txt should be deleted"
                assert (
                    "%s/file2.txt" % folder_name in remaining_objects
                ), "file2.txt should still exist"
                logger.info("Object deleted successfully")

                # Step 6: Delete folder contents via S3
                logger.test_step("Step 6: Deleting folder contents via S3")
                for key in remaining_objects:
                    obc["s3_client"].delete_object(Bucket=obc["bucket_name"], Key=key)
                    logger.info("Deleted: %s", key)

                response = obc["s3_client"].list_objects_v2(
                    Bucket=obc["bucket_name"],
                    Prefix="%s/" % folder_name,
                )
                assert (
                    "Contents" not in response
                ), "Folder should be empty after deletion"
                logger.info("Folder deleted successfully")

        finally:
            if os.path.exists(temp_folder):
                shutil.rmtree(temp_folder)
                logger.info("Cleaned up temporary folder")

        logger.info("Test completed successfully")

    @polarion_id("OCS-7995")
    def test_object_browser_share_object(self, project_factory):
        """
        Test sharing an object via presigned URL in object browser.

        Test steps:
        1. Create OBC with object on client
        2. Get object presigned URL
        3. Validate that the URL is accessible

        Expected result:
        - Object URL is accessible and returns correct content
        """
        client_indices = config.get_consumer_indexes_list()
        if len(client_indices) < 1:
            pytest.skip("Test requires at least 1 client cluster")

        client_index = client_indices[0]

        logger.test_step("Step 1: Creating OBC with object on client")
        obc = self._create_obc_on_client(client_index, project_factory, "share-test")

        with config.RunWithConfigContext(client_index):
            test_object_key = "shared-test-file.txt"
            test_object_data = b"This is a shared test file"
            obc["s3_client"].put_object(
                Bucket=obc["bucket_name"],
                Key=test_object_key,
                Body=test_object_data,
            )
            logger.info("Uploaded test object: %s", test_object_key)

            logger.test_step("Step 2: Generating presigned URL")
            presigned_url = obc["s3_client"].generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": obc["bucket_name"],
                    "Key": test_object_key,
                },
                ExpiresIn=3600,
            )
            logger.info("Generated presigned URL: %s", presigned_url)

            logger.test_step("Step 3: Validating presigned URL is accessible")
            response = requests.get(presigned_url, verify=False, timeout=60)
            assert response.status_code == 200, (
                "Presigned URL returned status %s" % response.status_code
            )
            assert (
                response.content == test_object_data
            ), "Downloaded content does not match original"
            logger.info("Presigned URL is accessible and returns correct content")

        logger.info("Test completed successfully")
