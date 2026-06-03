"""
Test RGW STS (Security Token Service) functionality in OCS-CI
"""

import json
import logging
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    post_upgrade,
    red_squad,
    rgw,
    runs_on_provider,
    skipif_mcg_only,
    tier2,
)
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

logger = logging.getLogger(__name__)


@rgw
@red_squad
@runs_on_provider
@skipif_mcg_only
class TestRGWSTS:
    """
    Test RGW STS (Security Token Service) functionality
    """

    @pytest.fixture(scope="function")
    def rgw_iam_client_creation(self, rgw_obj, request):
        """
        Create RGW account, root user, and IAM client for STS operations

        Steps:
        1. Create RGW account using radosgw-admin (20-byte ID: RGW + 17 numeric digits)
        2. Create account root user using radosgw-admin
        3. Create boto3 IAM client with user credentials

        Cleanup:
        - Deletes the user and account after test completion

        Returns:
            tuple: (iam_client, account_info_dict)
                - iam_client: boto3 IAM client configured with account credentials
                - account_info_dict: Dictionary containing:
                    - account_id: RGW account ID
                    - user_id: User ID
                    - access_key: AWS access key
                    - secret_key: AWS secret key
        """
        cephobjectstore_name = OCP(
            kind=constants.CEPHOBJECTSTORE,
            resource_name="ocs-storagecluster-cephobjectstore",
            namespace=config.ENV_DATA["cluster_namespace"],
        ).get()["metadata"]["name"]
        toolbox = get_ceph_tools_pod()

        # Generate unique IDs to avoid conflicts
        # RGW account ID must be exactly 20 bytes long, start with "RGW", and end with numeric digits
        # Format: RGW + 17 numeric digits = 20 bytes total
        numeric_suffix = str(uuid.uuid4().int)[:17]
        account_id = f"RGW{numeric_suffix}"
        user_id = create_unique_resource_name("sts", "user")

        def finalizer():
            """
            Cleanup RGW account and user after test completion
            """
            logger.info("Starting cleanup of RGW account and user")
            toolbox = get_ceph_tools_pod()

            try:
                # Delete user first (must delete user before account)
                logger.info(f"Deleting RGW user: {user_id}")
                user_delete_cmd = f"radosgw-admin user rm --uid={user_id} --purge-data"
                toolbox.exec_cmd_on_pod(user_delete_cmd, out_yaml_format=False)
                logger.info(f"RGW user {user_id} deleted successfully")
            except Exception as e:
                logger.warning(f"Failed to delete RGW user {user_id}: {e}")

            try:
                # Delete account
                logger.info(f"Deleting RGW account: {account_id}")
                account_delete_cmd = (
                    f"radosgw-admin account rm --account-id={account_id} --purge-data"
                )
                toolbox.exec_cmd_on_pod(account_delete_cmd, out_yaml_format=False)
                logger.info(f"RGW account {account_id} deleted successfully")
            except Exception as e:
                logger.warning(f"Failed to delete RGW account {account_id}: {e}")

        request.addfinalizer(finalizer)

        # Step 1: Create RGW account
        logger.info(f"Creating RGW account with ID: {account_id}")
        account_create_cmd = (
            f"radosgw-admin account create "
            f"--account-id={account_id} "
            f"--rgw-zone={cephobjectstore_name} "
            f"--rgw-zonegroup={cephobjectstore_name} "
            f"--rgw-realm={cephobjectstore_name}"
        )
        toolbox.exec_cmd_on_pod(account_create_cmd, out_yaml_format=False)
        logger.info(f"RGW account {account_id} created successfully")

        # Step 2: Create account root user
        logger.info(f"Creating account root user: {user_id}")
        user_create_cmd = (
            f"radosgw-admin user create "
            f"--uid={user_id} "
            f'--display-name="STSUser" '
            f"--account-id={account_id} "
            f"--account-root "
            f"--rgw-zone={cephobjectstore_name} "
            f"--rgw-zonegroup={cephobjectstore_name} "
            f"--rgw-realm={cephobjectstore_name}"
        )
        user_info = toolbox.exec_cmd_on_pod(user_create_cmd, out_yaml_format=False)

        # Parse user credentials from response
        user_data = json.loads(user_info) if isinstance(user_info, str) else user_info
        access_key = user_data["keys"][0]["access_key"]
        secret_key = user_data["keys"][0]["secret_key"]

        logger.info(
            f"Account root user created successfully - Access Key: {access_key[:10]}..."
        )

        endpoint, _, _ = rgw_obj.get_credentials()

        iam_client = boto3.client(
            "iam",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=rgw_obj.region,
            verify=False,
        )

        return iam_client, {
            "account_id": account_id,
            "user_id": user_id,
            "access_key": access_key,
            "secret_key": secret_key,
        }

    def trust_policy_creation(self):
        """
        Create trust policy for IAM role

        Returns:
            dict: Trust policy document
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        return policy

    def role_permission_policy(self):
        """
        Create permission policy for IAM role

        Returns:
            dict: Permission policy document
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": ["s3:*"], "Resource": "*"}],
        }
        return policy

    def put_object(self, s3_client, bucket_name, object_key, data):
        """
        Put an object to S3 bucket using boto3 client

        Args:
            s3_client: boto3 S3 client
            bucket_name (str): Name of the bucket
            object_key (str): Object key/name
            data (bytes): Object content

        Returns:
            dict: Put object response
        """
        logger.info(f"Putting object {object_key} in bucket {bucket_name}")
        response = s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=data)
        logger.info(f"Object {object_key} created successfully")
        return response

    def get_object(self, s3_client, bucket_name, object_key):
        """
        Get an object from S3 bucket using boto3 client

        Args:
            s3_client: boto3 S3 client
            bucket_name (str): Name of the bucket
            object_key (str): Object key/name

        Returns:
            bytes: Object content
        """
        logger.info(f"Getting object {object_key} from bucket {bucket_name}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response["Body"].read()
        logger.info(f"Object {object_key} retrieved successfully")
        return content

    @tier2
    @post_upgrade
    def test_rgw_sts_configuration(
        self,
        rgw_obj,
        rgw_iam_client_creation,
    ):
        """
        Test RGW STS (Security Token Service) end-to-end workflow

        Steps:
        1. Verify EnableSTS parameter in StorageCluster is set to true by default
        2. Validate sts-key secret exists in the cluster
        3. Verify CephObjectStore has correct STS configuration
        4. Create IAM user and S3 bucket
        5. List IAM users
        6. Create IAM role with trust policy
        7. Attach permission policy to IAM role
        8. Assume role and get temporary credentials
        9. Access S3 bucket using assumed role credentials
        """

        # Unpack the fixture return values
        iam_client, rgw_account_and_user = rgw_iam_client_creation

        cephobjectstore_obj = OCP(
            kind=constants.CEPHOBJECTSTORE,
            resource_name="ocs-storagecluster-cephobjectstore",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        cephobjectstore_name = cephobjectstore_obj.get()["metadata"]["name"]

        # Step 1: Check EnableSTS parameter in StorageCluster
        logger.info("Step 1: Checking EnableSTS parameter in StorageCluster")
        sc_ocp = OCP(
            kind="StorageCluster",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        )
        sc_data = sc_ocp.get()
        enable_sts = (
            sc_data.get("spec", {})
            .get("managedResources", {})
            .get("cephObjectStores", {})
            .get("enableSTS", False)
        )

        logger.info(f"StorageCluster EnableSTS: {enable_sts}")
        assert enable_sts is True, (
            f"EnableSTS should be true by default in StorageCluster, "
            f"but got: {enable_sts}"
        )

        # Step 2: Validate sts-key secret exists
        logger.info("Step 2: Validating sts-key secret exists")
        secret_ocp = OCP(
            kind=constants.SECRET, namespace=config.ENV_DATA["cluster_namespace"]
        )

        sts_secret_name = f"sts-key-{cephobjectstore_name}"
        logger.info(f"Looking for secret: {sts_secret_name}")

        sts_secret = secret_ocp.get(resource_name=sts_secret_name)
        assert sts_secret, f"Secret {sts_secret_name} not found"

        # Verify secret has rgw_sts_key data
        secret_data = sts_secret.get("data", {})
        assert (
            "rgw_sts_key" in secret_data
        ), f"Secret {sts_secret_name} does not contain 'rgw_sts_key'"
        logger.info(f"Secret {sts_secret_name} verified successfully")

        # Step 3: Verify CephObjectStore has correct STS configuration
        logger.info("Step 3: Verifying CephObjectStore STS configuration")

        # Get CephObjectStore data
        cephobjectstore_data = cephobjectstore_obj.get()

        # Check rgwCommandFlags
        rgw_command_flags = (
            cephobjectstore_data.get("spec", {})
            .get("gateway", {})
            .get("rgwCommandFlags", {})
        )

        assert (
            "rgw_s3_auth_use_sts" in rgw_command_flags
        ), "rgw_s3_auth_use_sts not found in rgwCommandFlags"
        assert rgw_command_flags["rgw_s3_auth_use_sts"] == "true", (
            f"rgw_s3_auth_use_sts should be 'true', "
            f"got: {rgw_command_flags['rgw_s3_auth_use_sts']}"
        )

        # Check rgwConfigFromSecret
        rgw_config_from_secret = (
            cephobjectstore_data.get("spec", {})
            .get("gateway", {})
            .get("rgwConfigFromSecret", {})
        )

        assert (
            "rgw_sts_key" in rgw_config_from_secret
        ), "rgw_sts_key not found in rgwConfigFromSecret"

        sts_key_config = rgw_config_from_secret["rgw_sts_key"]
        assert sts_key_config.get("key") == "rgw_sts_key", (
            f"rgw_sts_key key should be 'rgw_sts_key', "
            f"got: {sts_key_config.get('key')}"
        )
        assert sts_key_config.get("name") == sts_secret_name, (
            f"rgw_sts_key secret name should be '{sts_secret_name}', "
            f"got: {sts_key_config.get('name')}"
        )

        logger.info("CephObjectStore STS configuration verified successfully")

        # Step 4: Create IAM user and bucket
        logger.info("Step 4: Creating IAM user and S3 bucket")
        iam_user_name = create_unique_resource_name("sts-iam", "user")
        bucket_name = create_unique_resource_name("sts-test", "bucket")

        try:
            # Create IAM user
            logger.info(f"Creating IAM user: {iam_user_name}")
            iam_client.create_user(UserName=iam_user_name)
            logger.info(f"IAM user {iam_user_name} created successfully")

            # Attach S3 full access policy to user
            logger.info(f"Attaching S3FullAccess policy to user {iam_user_name}")
            iam_client.attach_user_policy(
                UserName=iam_user_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess",
            )

            # Create access key for IAM user
            logger.info(f"Creating access key for IAM user {iam_user_name}")
            access_key_response = iam_client.create_access_key(UserName=iam_user_name)

            iam_user_access_key = access_key_response["AccessKey"]["AccessKeyId"]
            iam_user_secret_key = access_key_response["AccessKey"]["SecretAccessKey"]

            logger.info(f"IAM user access key created: {iam_user_access_key[:10]}...")

            # Get endpoint from rgw_obj
            endpoint, _, _ = rgw_obj.get_credentials()

            # Create S3 client with IAM user credentials
            iam_user_s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=iam_user_access_key,
                aws_secret_access_key=iam_user_secret_key,
                region_name=rgw_obj.region,
                verify=False,
            )

            # Create bucket
            logger.info(f"Creating S3 bucket: {bucket_name}")
            iam_user_s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} created successfully")

        except ClientError as e:
            logger.error(f"Failed to create IAM user or bucket: {e}")
            raise

        # Step 5: List IAM users
        logger.info("Step 5: Listing IAM users")
        try:
            users_response = iam_client.list_users()
            users = users_response.get("Users", [])
            user_names = [user["UserName"] for user in users]

            logger.info(f"IAM users found: {user_names}")
            assert (
                iam_user_name in user_names
            ), f"IAM user {iam_user_name} not found in user list"

        except ClientError as e:
            logger.error(f"Failed to list IAM users: {e}")
            raise

        # Step 6: Create IAM role
        logger.info("Step 6: Creating IAM role")
        role_name = create_unique_resource_name("sts-test", "role")
        trust_policy = self.trust_policy_creation()

        try:
            logger.info(f"Creating IAM role: {role_name}")
            iam_client.create_role(
                RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy)
            )
            logger.info(f"IAM role {role_name} created successfully")

        except ClientError as e:
            logger.error(f"Failed to create IAM role: {e}")
            raise

        # Step 7: Create and attach role permission policy
        logger.info("Step 7: Attaching permission policy to IAM role")
        policy_name = create_unique_resource_name("sts-s3", "policy")

        try:
            role_permission_policy = self.role_permission_policy()
            logger.info(f"Putting role policy: {policy_name} on role {role_name}")
            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(role_permission_policy),
            )
            logger.info(f"Permission policy attached to role {role_name}")

        except ClientError as e:
            logger.error(f"Failed to attach role policy: {e}")
            raise

        # Step 8: AssumeRole to get temporary credentials
        logger.info("Step 8: Assuming IAM role to get temporary credentials")
        role_arn = f"arn:aws:iam::{rgw_account_and_user['account_id']}:role/{role_name}"
        session_name = create_unique_resource_name("sts", "session")

        try:
            logger.info(f"Assuming role: {role_arn}")

            # Create STS client with IAM user credentials
            iam_user_sts_client = boto3.client(
                "sts",
                endpoint_url=endpoint,
                aws_access_key_id=iam_user_access_key,
                aws_secret_access_key=iam_user_secret_key,
                region_name=rgw_obj.region,
                verify=False,
            )

            assume_role_response = iam_user_sts_client.assume_role(
                RoleArn=role_arn, RoleSessionName=session_name
            )

            credentials = assume_role_response["Credentials"]
            assumed_access_key = credentials["AccessKeyId"]
            assumed_secret_key = credentials["SecretAccessKey"]
            assumed_session_token = credentials["SessionToken"]

            logger.info(
                f"Assumed role credentials obtained: {assumed_access_key[:10]}..."
            )
            logger.info(f"Session token length: {len(assumed_session_token)}")

            assert assumed_access_key, "Assumed role access key should not be empty"
            assert assumed_secret_key, "Assumed role secret key should not be empty"
            assert assumed_session_token, "Session token should not be empty"

        except ClientError as e:
            logger.error(f"Failed to assume role: {e}")
            raise

        # Step 9: Access bucket using assumed role credentials
        logger.info("Step 9: Accessing S3 bucket using assumed role credentials")

        try:
            # Create S3 client with assumed role credentials
            assumed_s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=assumed_access_key,
                aws_secret_access_key=assumed_secret_key,
                aws_session_token=assumed_session_token,
                region_name=rgw_obj.region,
                verify=False,
            )

            # List buckets using assumed role
            logger.info("Listing buckets with assumed role credentials")
            buckets_response = assumed_s3_client.list_buckets()
            buckets = buckets_response.get("Buckets", [])
            bucket_names = [bucket["Name"] for bucket in buckets]

            logger.info(f"Buckets accessible with assumed role: {bucket_names}")
            assert (
                bucket_name in bucket_names
            ), f"Bucket {bucket_name} should be accessible with assumed role"

            # Put an object in the bucket using assumed role
            test_object_key = "test-sts-object.txt"
            test_object_content = b"This is a test object created using STS credentials"

            self.put_object(
                assumed_s3_client, bucket_name, test_object_key, test_object_content
            )

            # Verify object exists by retrieving it
            obj_content = self.get_object(
                assumed_s3_client, bucket_name, test_object_key
            )

            assert obj_content == test_object_content, "Object content mismatch"
            logger.info(f"Object {test_object_key} verified successfully")

        except ClientError as e:
            logger.error(f"Failed to access bucket with assumed role: {e}")
            raise

        logger.info("RGW STS test completed successfully!")
