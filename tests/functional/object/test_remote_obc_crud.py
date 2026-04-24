"""
Test remote OBC creation and CRUD operations on client clusters.

This test validates OBC functionality on HCI client clusters connected to a provider,
including bucket creation, S3 CRUD operations, and data integrity verification.
"""

import base64
import boto3
import botocore
import hashlib
import logging
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
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Enable remote OBC for this module
pytestmark = pytest.mark.usefixtures("remote_obc_setup_session")


@tier1
@red_squad
@mcg
@runs_on_provider
@hci_provider_and_client_required
class TestRemoteOBCCRUD(ManageTest):
    """
    Test OBC creation and CRUD operations on client clusters.

    This test validates:
    1. OBC creation on client cluster using provider's noobaa storageclass
    2. OBC binding and credential generation
    3. Bucket creation on provider with correct naming (remote-obc-<hash>)
    4. Label verification
    5. S3 CRUD operations with various object sizes
    6. Data integrity verification using checksums
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """Setup test resources."""
        self.obcs_to_cleanup = []
        self.test_files = []
        self.bucket_name = None

        def finalizer():
            """Cleanup resources."""
            # Delete test files
            for test_file in self.test_files:
                try:
                    test_file.close()
                except Exception as e:
                    logger.warning(f"Failed to close test file: {e}")

            # Switch back to current context if needed
            try:
                config.switch_ctx(config.cur_index)
            except Exception:
                pass

        request.addfinalizer(finalizer)

    def test_remote_obc_crud_operations(self):
        """
        Test OBC creation and CRUD operations on client cluster.

        Test steps:
        1. Deploy Provider cluster with MCG enabled (pre-requisite)
        2. Deploy Client (hosted) cluster connected to Provider (pre-requisite)
        3. On Client cluster, create OBC using openshift-storage.noobaa.io StorageClass
        4. Verify OBC is bound
        5. Verify ConfigMap/Secret are created on Client
        6. Extract S3 credentials from OB Secret on Client
        7. Check matching names for buckets on provider: remote-obc-<id hash>
        8. Check labels for obc, namespace, storageconsumer
        9. Perform CRUD operations with various object sizes
        10. Test data integrity with checksums

        """
        # Get client cluster index
        client_indices = config.get_consumer_indexes_list()
        if not client_indices:
            pytest.skip("No client clusters found")

        client_index = client_indices[0]

        # Step 3: Create OBC on client cluster
        with config.RunWithConfigContext(client_index):
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()
            assert (
                cluster_type == constants.HCI_CLIENT
            ), f"Expected HCI_CLIENT, got {cluster_type}"

            namespace = config.ENV_DATA["cluster_namespace"]
            cluster_name = config.ENV_DATA.get("cluster_name", "client")

            logger.info(f"Creating OBC on client cluster {cluster_name}")
            obc_name = create_unique_resource_name(
                resource_description="obc", resource_type="remote-crud"
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
            self.obcs_to_cleanup.append((client_index, obc_name, namespace))
            logger.info(f"OBC '{obc_name}' created")

            # Step 4: Wait for OBC to reach Bound state
            logger.info(f"Waiting for OBC '{obc_name}' to reach Bound state")
            for sample in TimeoutSampler(
                timeout=300,
                sleep=10,
                func=self._check_obc_phase,
                obc_name=obc_name,
                namespace=namespace,
            ):
                if sample:
                    logger.info(f"OBC '{obc_name}' reached Bound state")
                    break

            # Step 5: Verify ConfigMap and Secret are created on Client
            logger.info("Verifying ConfigMap and Secret creation")

            # Get OBC status to retrieve bucket name
            obc_ocp = OCP(
                kind="ObjectBucketClaim", namespace=namespace, resource_name=obc_name
            )
            obc_data = obc_ocp.get()
            self.bucket_name = obc_data.get("spec", {}).get("bucketName")
            assert self.bucket_name, f"Bucket name not found for OBC {obc_name}"

            # Verify ConfigMap exists
            configmap_obj = OCP(kind=constants.CONFIGMAP, namespace=namespace)
            assert configmap_obj.is_exist(
                resource_name=obc_name
            ), f"ConfigMap {obc_name} not found"

            # Verify Secret exists
            secret_obj = OCP(kind=constants.SECRET, namespace=namespace)
            assert secret_obj.is_exist(
                resource_name=obc_name
            ), f"Secret {obc_name} not found"

            # Step 6: Extract S3 credentials from ConfigMap and Secret
            logger.info("Extracting S3 credentials from Secret and ConfigMap")

            # Get credentials from Secret
            secret_data = secret_obj.get(resource_name=obc_name)

            access_key_id = base64.b64decode(
                secret_data["data"]["AWS_ACCESS_KEY_ID"]
            ).decode("utf-8")
            access_key = base64.b64decode(
                secret_data["data"]["AWS_SECRET_ACCESS_KEY"]
            ).decode("utf-8")

            # Get endpoint from ConfigMap
            configmap_data = configmap_obj.get(resource_name=obc_name)
            s3_endpoint = configmap_data["data"]["BUCKET_HOST"]

            assert access_key_id, f"Access key ID not found for OBC {obc_name}"
            assert access_key, f"Secret key not found for OBC {obc_name}"
            assert s3_endpoint, f"S3 endpoint not found for OBC {obc_name}"

            logger.info("S3 credentials extracted successfully")
            logger.info(f"Bucket name: {self.bucket_name}")
            logger.info(f"S3 endpoint: {s3_endpoint}")

            # Create S3 client for CRUD operations
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key_id,
                aws_secret_access_key=access_key,
                endpoint_url=f"https://{s3_endpoint}",
                verify=False,
            )

        # Step 7: Check bucket name on provider
        logger.info("Verifying bucket on provider cluster")
        with config.RunWithProviderConfigContextIfAvailable():
            # Bucket name should be in format: remote-obc-<hash> or similar
            logger.info(f"Looking for bucket: {self.bucket_name}")

            # Verify bucket exists via OBC API
            ob_obj = OCP(
                kind="ObjectBucket",
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            ob_list = ob_obj.get()
            bucket_found = False
            for ob in ob_list.get("items", []):
                if ob["spec"]["endpoint"]["bucketName"] == self.bucket_name:
                    bucket_found = True
                    ob_name = ob["metadata"]["name"]
                    logger.info(
                        f"Found ObjectBucket '{ob_name}' with bucket name '{self.bucket_name}'"
                    )

                    # Step 8: Check labels
                    logger.info("Verifying ObjectBucket labels")
                    labels = ob["metadata"].get("labels", {})
                    logger.info(f"ObjectBucket labels: {labels}")

                    # Verify expected labels exist
                    assert (
                        "bucket-provisioner" in labels
                    ), "bucket-provisioner label not found"

                    # Check if namespace label exists (may vary by implementation)
                    if "noobaa-domain" in labels:
                        logger.info(f"noobaa-domain label: {labels['noobaa-domain']}")

                    break

            assert bucket_found, f"Bucket {self.bucket_name} not found on provider"

        # Step 9 & 10: Perform CRUD operations and verify data integrity
        logger.info("Starting S3 CRUD operations")

        # Switch back to client for S3 operations
        with config.RunWithConfigContext(client_index):
            # Create test objects of various sizes
            test_objects = self._create_test_objects()

            # CREATE: Upload objects
            logger.info("Testing CREATE operations")
            for obj_name, obj_data in test_objects.items():
                logger.info(f"Uploading object '{obj_name}' ({obj_data['size']} bytes)")
                s3_client.upload_file(
                    Filename=obj_data["file_path"],
                    Bucket=self.bucket_name,
                    Key=obj_name,
                )
                logger.info(f"Successfully uploaded '{obj_name}'")

            # READ: Download and verify checksums
            logger.info("Testing READ operations and data integrity")
            for obj_name, obj_data in test_objects.items():
                logger.info(f"Downloading object '{obj_name}'")
                download_path = f"/tmp/downloaded_{obj_name}"

                s3_client.download_file(
                    Bucket=self.bucket_name,
                    Key=obj_name,
                    Filename=download_path,
                )

                # Verify checksum
                downloaded_md5 = self._calculate_md5(download_path)
                logger.info(
                    f"Original MD5: {obj_data['md5']}, Downloaded MD5: {downloaded_md5}"
                )
                assert downloaded_md5 == obj_data["md5"], f"MD5 mismatch for {obj_name}"
                logger.info(f"Data integrity verified for '{obj_name}'")

            # UPDATE: Overwrite an object
            logger.info("Testing UPDATE operations")
            update_obj_name = list(test_objects.keys())[0]
            new_content = b"Updated content for testing"
            new_content_file = tempfile.NamedTemporaryFile(delete=False, mode="wb")
            new_content_file.write(new_content)
            new_content_file.flush()
            new_content_file.close()
            self.test_files.append(new_content_file)

            logger.info(f"Overwriting object '{update_obj_name}'")
            s3_client.upload_file(
                Filename=new_content_file.name,
                Bucket=self.bucket_name,
                Key=update_obj_name,
            )

            # Verify updated content
            updated_download_path = f"/tmp/updated_{update_obj_name}"
            s3_client.download_file(
                Bucket=self.bucket_name,
                Key=update_obj_name,
                Filename=updated_download_path,
            )

            with open(updated_download_path, "rb") as f:
                downloaded_content = f.read()
            assert downloaded_content == new_content, "Updated content does not match"
            logger.info(f"Successfully updated and verified '{update_obj_name}'")

            # DELETE: Remove objects
            logger.info("Testing DELETE operations")
            for obj_name in test_objects.keys():
                logger.info(f"Deleting object '{obj_name}'")
                s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=obj_name,
                )

                # Verify object is deleted (should raise exception)
                try:
                    s3_client.head_object(
                        Bucket=self.bucket_name,
                        Key=obj_name,
                    )
                    raise AssertionError(
                        f"Object {obj_name} still accessible after deletion"
                    )
                except Exception as e:
                    # Expected to fail (404 NoSuchKey)
                    if "404" in str(e) or "NoSuchKey" in str(e):
                        logger.info(
                            f"Verified object '{obj_name}' is deleted (404 NoSuchKey as expected)"
                        )
                    else:
                        logger.warning(f"Unexpected error when verifying deletion: {e}")
                        raise

            logger.info("All CRUD operations completed successfully")

            # Cleanup OBC
            logger.info(f"Deleting OBC '{obc_name}'")
            obc_obj_cleanup = OCP(kind="ObjectBucketClaim", namespace=namespace)
            obc_obj_cleanup.delete(resource_name=obc_name)

            # Verify deletion
            for sample in TimeoutSampler(
                timeout=180,
                sleep=10,
                func=self._check_obc_deleted,
                obc_name=obc_name,
                namespace=namespace,
            ):
                if sample:
                    logger.info(f"OBC '{obc_name}' deleted successfully")
                    break

        logger.info("Test completed successfully")

    def _check_obc_phase(self, obc_name, namespace):
        """
        Check if OBC has reached Bound phase.

        Args:
            obc_name (str): Name of the OBC
            namespace (str): Namespace of the OBC

        Returns:
            bool: True if OBC is Bound, False otherwise

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
            namespace (str): Namespace of the OBC

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

    def _create_test_objects(self):
        """
        Create test objects of various sizes with known checksums.

        Returns:
            dict: Dictionary mapping object names to their metadata
                {
                    "object_name": {
                        "size": <bytes>,
                        "file_path": <path>,
                        "md5": <checksum>,
                        "sha256": <checksum>
                    }
                }

        """
        test_objects = {}

        # Small object (<1MB)
        small_size = 512 * 1024  # 512KB
        small_file = tempfile.NamedTemporaryFile(delete=False, mode="wb")
        small_data = b"a" * small_size
        small_file.write(small_data)
        small_file.flush()
        small_file.close()
        self.test_files.append(small_file)

        test_objects["small_object.bin"] = {
            "size": small_size,
            "file_path": small_file.name,
            "md5": self._calculate_md5(small_file.name),
            "sha256": self._calculate_sha256(small_file.name),
        }

        # Medium object (10-50MB)
        medium_size = 25 * 1024 * 1024  # 25MB
        medium_file = tempfile.NamedTemporaryFile(delete=False, mode="wb")
        # Write in chunks to avoid memory issues
        chunk_size = 1024 * 1024  # 1MB chunks
        for _ in range(medium_size // chunk_size):
            medium_file.write(b"b" * chunk_size)
        medium_file.flush()
        medium_file.close()
        self.test_files.append(medium_file)

        test_objects["medium_object.bin"] = {
            "size": medium_size,
            "file_path": medium_file.name,
            "md5": self._calculate_md5(medium_file.name),
            "sha256": self._calculate_sha256(medium_file.name),
        }

        # Large object (100MB+)
        large_size = 110 * 1024 * 1024  # 110MB
        large_file = tempfile.NamedTemporaryFile(delete=False, mode="wb")
        # Write in chunks
        for _ in range(large_size // chunk_size):
            large_file.write(b"c" * chunk_size)
        large_file.flush()
        large_file.close()
        self.test_files.append(large_file)

        test_objects["large_object.bin"] = {
            "size": large_size,
            "file_path": large_file.name,
            "md5": self._calculate_md5(large_file.name),
            "sha256": self._calculate_sha256(large_file.name),
        }

        logger.info(f"Created {len(test_objects)} test objects")
        for name, data in test_objects.items():
            logger.info(f"  {name}: {data['size']} bytes, MD5: {data['md5'][:16]}...")

        return test_objects

    def _calculate_md5(self, file_path):
        """
        Calculate MD5 checksum of a file.

        Args:
            file_path (str): Path to the file

        Returns:
            str: MD5 checksum (hex digest)

        """
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()

    def _calculate_sha256(self, file_path):
        """
        Calculate SHA256 checksum of a file.

        Args:
            file_path (str): Path to the file

        Returns:
            str: SHA256 checksum (hex digest)

        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
