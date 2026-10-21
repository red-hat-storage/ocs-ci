"""
Test RGW STS (Security Token Service) functionality in OCS-CI
"""

import json
import logging
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError, ParamValidationError

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
from ocs_ci.utility.utils import TimeoutSampler

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
        cephobjectstore_name = constants.CEPHOBJECTSTORE_NAME
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

        iam_client = self.make_boto3_client(
            "iam", endpoint, rgw_obj.region, access_key, secret_key
        )

        return iam_client, {
            "account_id": account_id,
            "user_id": user_id,
            "access_key": access_key,
            "secret_key": secret_key,
            "endpoint": endpoint,
            "region": rgw_obj.region,
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

    def make_boto3_client(
        self, service, endpoint, region, access_key, secret_key, session_token=None
    ):
        """Create a boto3 client with common RGW connection parameters."""
        kwargs = dict(
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            verify=False,
        )
        if session_token is not None:
            kwargs["aws_session_token"] = session_token
        return boto3.client(service, **kwargs)

    def put_object(self, s3_client, bucket_name, object_key, data):
        """
        Put an object to S3 bucket using boto3 client. Automatically uses multipart
        upload when data exceeds 5 MiB (the minimum S3 multipart part size).

        Args:
            s3_client: boto3 S3 client
            bucket_name (str): Name of the bucket
            object_key (str): Object key/name
            data (bytes): Object content

        Returns:
            dict: Put object response
        """
        part_size = 5 * 1024 * 1024  # 5 MiB — minimum S3 multipart part size
        logger.info(f"Putting object {object_key} in bucket {bucket_name}")
        if len(data) <= part_size:
            response = s3_client.put_object(
                Bucket=bucket_name, Key=object_key, Body=data
            )
            logger.info(f"Object {object_key} created successfully")
            return response
        mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
        upload_id = mpu["UploadId"]
        parts = []
        offset = 0
        part_number = 1
        try:
            while offset < len(data):
                chunk = data[offset : offset + part_size]
                resp = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=object_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
                offset += part_size
                part_number += 1
            response = s3_client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            s3_client.abort_multipart_upload(
                Bucket=bucket_name, Key=object_key, UploadId=upload_id
            )
            raise
        logger.info(
            f"Object {object_key} uploaded via multipart ({part_number - 1} parts)"
        )
        return response

    def get_object(self, s3_client, bucket_name, object_key):
        """
        Get an object from S3 bucket using boto3 client. Automatically uses ranged
        GET requests for objects larger than 5 MiB.

        Args:
            s3_client: boto3 S3 client
            bucket_name (str): Name of the bucket
            object_key (str): Object key/name

        Returns:
            bytes: Object content
        """
        part_size = 5 * 1024 * 1024  # 5 MiB
        logger.info(f"Getting object {object_key} from bucket {bucket_name}")
        total_size = s3_client.head_object(Bucket=bucket_name, Key=object_key)[
            "ContentLength"
        ]
        if total_size <= part_size:
            content = s3_client.get_object(Bucket=bucket_name, Key=object_key)[
                "Body"
            ].read()
            logger.info(f"Object {object_key} retrieved successfully")
            return content
        chunk_size = 1024 * 1024  # 1 MiB per range request
        downloaded = b""
        start = 0
        while start < total_size:
            end = min(start + chunk_size - 1, total_size - 1)
            downloaded += s3_client.get_object(
                Bucket=bucket_name, Key=object_key, Range=f"bytes={start}-{end}"
            )["Body"].read()
            start = end + 1
        logger.info(
            f"Object {object_key} retrieved via ranged GET ({total_size} bytes)"
        )
        return downloaded

    def create_iam_user(self, iam_client, with_s3_policy=False):
        """
        Create an IAM user with an access key pair.

        Args:
            iam_client: boto3 IAM client
            with_s3_policy (bool): Attach AmazonS3FullAccess policy when True

        Returns:
            tuple: (user_name, access_key_id, secret_access_key)
        """
        user_name = create_unique_resource_name("sts-iam", "user")
        iam_client.create_user(UserName=user_name)
        logger.info(f"IAM user {user_name} created successfully")
        if with_s3_policy:
            iam_client.attach_user_policy(
                UserName=user_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess",
            )
            logger.info(f"S3FullAccess policy attached to IAM user {user_name}")
        access_key_response = iam_client.create_access_key(UserName=user_name)
        access_key = access_key_response["AccessKey"]["AccessKeyId"]
        secret_key = access_key_response["AccessKey"]["SecretAccessKey"]
        logger.info(f"IAM user access key created: {access_key[:10]}...")
        return user_name, access_key, secret_key

    def create_iam_role(self, iam_client, account_id, with_permission_policy=True):
        """
        Create an IAM role with trust policy and optionally attach full S3 permission policy.

        Args:
            iam_client: boto3 IAM client
            account_id (str): RGW account ID used to build the role ARN
            with_permission_policy (bool): Attach the default s3:* permission policy when True

        Returns:
            tuple: (role_name, role_arn, policy_name)
        """
        role_name = create_unique_resource_name("sts-test", "role")
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(self.trust_policy_creation()),
        )
        logger.info(f"IAM role {role_name} created successfully")
        role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
        policy_name = create_unique_resource_name("sts-s3", "policy")
        if with_permission_policy:
            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(self.role_permission_policy()),
            )
            logger.info(f"Permission policy {policy_name} attached to role {role_name}")
        return role_name, role_arn, policy_name

    def assume_role_and_get_s3_client(
        self, sts_client, role_arn, endpoint, region, duration=900
    ):
        """
        Assume an IAM role and return an S3 client using the temporary credentials.

        Args:
            sts_client: boto3 STS client
            role_arn (str): ARN of the role to assume
            endpoint (str): RGW endpoint URL
            region (str): AWS region
            duration (int): Token duration in seconds (default: 900)

        Returns:
            boto3 S3 client configured with assumed role credentials
        """
        session_name = create_unique_resource_name("sts", "session")
        credentials = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name,
            DurationSeconds=duration,
        )["Credentials"]
        logger.info(f"Assumed role {role_arn}, session: {session_name}")
        return self.make_boto3_client(
            "s3",
            endpoint,
            region,
            credentials["AccessKeyId"],
            credentials["SecretAccessKey"],
            credentials["SessionToken"],
        )

    def assert_client_error(self, operation_fn, expected_error_code, step_label):
        """
        Assert that a boto3 operation raises a ClientError with the expected error code.

        Args:
            operation_fn (callable): Zero-argument callable that performs the operation
            expected_error_code (str): Expected error code (e.g. "AccessDenied")
            step_label (str): Label used in log messages (e.g. "Step 5")
        """
        try:
            operation_fn()
            assert (
                False
            ), f"{step_label}: Expected {expected_error_code} but operation succeeded"
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.info(f"{step_label}: Got expected error - Error code: {error_code}")
            assert (
                error_code == expected_error_code
            ), f"{step_label}: Expected {expected_error_code}, got {error_code}"

    @tier2
    @post_upgrade
    def test_rgw_sts_configuration(
        self,
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
        endpoint = rgw_account_and_user["endpoint"]
        region = rgw_account_and_user["region"]

        cephobjectstore_name = constants.CEPHOBJECTSTORE_NAME
        cephobjectstore_obj = OCP(
            kind=constants.CEPHOBJECTSTORE,
            resource_name=cephobjectstore_name,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

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
        bucket_name = create_unique_resource_name("sts-test", "bucket")

        try:
            iam_user_name, iam_user_access_key, iam_user_secret_key = (
                self.create_iam_user(iam_client, with_s3_policy=True)
            )
            iam_user_s3_client = self.make_boto3_client(
                "s3", endpoint, region, iam_user_access_key, iam_user_secret_key
            )
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

        # Steps 6-7: Create IAM role with trust policy and permission policy
        logger.info("Steps 6-7: Creating IAM role and attaching permission policy")
        try:
            _, role_arn, _ = self.create_iam_role(
                iam_client, rgw_account_and_user["account_id"]
            )
        except ClientError as e:
            logger.error(f"Failed to create IAM role or attach policy: {e}")
            raise

        # Step 8: AssumeRole to get temporary credentials
        logger.info("Step 8: Assuming IAM role to get temporary credentials")
        try:
            iam_user_sts_client = self.make_boto3_client(
                "sts", endpoint, region, iam_user_access_key, iam_user_secret_key
            )
            assumed_s3_client = self.assume_role_and_get_s3_client(
                iam_user_sts_client, role_arn, endpoint, region
            )
        except ClientError as e:
            logger.error(f"Failed to assume role: {e}")
            raise

        # Step 9: Access bucket using assumed role credentials
        logger.info("Step 9: Accessing S3 bucket using assumed role credentials")
        try:
            logger.info("Listing buckets with assumed role credentials")
            bucket_names = [
                b["Name"] for b in assumed_s3_client.list_buckets()["Buckets"]
            ]
            logger.info(f"Buckets accessible with assumed role: {bucket_names}")
            assert (
                bucket_name in bucket_names
            ), f"Bucket {bucket_name} should be accessible with assumed role"

            test_object_key = "test-sts-object.txt"
            test_object_content = b"This is a test object created using STS credentials"
            self.put_object(
                assumed_s3_client, bucket_name, test_object_key, test_object_content
            )
            obj_content = self.get_object(
                assumed_s3_client, bucket_name, test_object_key
            )
            assert obj_content == test_object_content, "Object content mismatch"
            logger.info(f"Object {test_object_key} verified successfully")
        except ClientError as e:
            logger.error(f"Failed to access bucket with assumed role: {e}")
            raise

        logger.info("RGW STS test completed successfully!")

    @tier2
    def test_rgw_sts_assumerole_scenarios(
        self,
        rgw_iam_client_creation,
    ):
        """
        Test RGW STS AssumeRole with correct, incorrect, and missing RoleARN

        Steps:
            1. Create IAM role with trust policy and attach permission policy
            2. Fetch credentials using correct AssumeRole name - expect success
            3. Fetch credentials using incorrect AssumeRole name - expect failure
            4. Fetch credentials without passing RoleARN - expect failure
        """
        iam_client, rgw_account_and_user = rgw_iam_client_creation
        endpoint = rgw_account_and_user["endpoint"]

        # Step 1: Create IAM user, role, and attach permission policy
        logger.info("Step 1: Creating IAM user, role, and attaching permission policy")
        region = rgw_account_and_user["region"]
        account_id = rgw_account_and_user["account_id"]

        _, iam_user_access_key, iam_user_secret_key = self.create_iam_user(iam_client)
        sts_client = self.make_boto3_client(
            "sts", endpoint, region, iam_user_access_key, iam_user_secret_key
        )
        _, role_arn, _ = self.create_iam_role(iam_client, account_id)
        session_name = create_unique_resource_name("sts", "session")

        # Step 2: Fetch credentials using correct AssumeRole name - expect success
        logger.info("Step 2: Fetching credentials using correct AssumeRole name")
        logger.info(f"Assuming role with correct ARN: {role_arn}")

        assume_role_response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name,
        )
        credentials = assume_role_response["Credentials"]
        assert credentials["AccessKeyId"], "AccessKeyId should not be empty"
        assert credentials["SecretAccessKey"], "SecretAccessKey should not be empty"
        assert credentials["SessionToken"], "SessionToken should not be empty"
        logger.info(
            "Step 2: Credentials fetched successfully with correct AssumeRole name"
        )

        # Step 3: Fetch credentials using incorrect AssumeRole name - expect failure
        logger.info("Step 3: Fetching credentials using incorrect AssumeRole name")
        incorrect_role_arn = f"arn:aws:iam::{account_id}:role/non-existent-role"
        logger.info(f"Assuming role with incorrect ARN: {incorrect_role_arn}")

        try:
            sts_client.assume_role(
                RoleArn=incorrect_role_arn,
                RoleSessionName=session_name,
            )
            assert False, "Expected ClientError for incorrect role ARN but got success"
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.info(
                f"Step 3: Got expected error for incorrect role ARN - Error code: {error_code}"
            )
            assert (
                error_code == "NoSuchEntity"
            ), f"Unexpected error code for incorrect role ARN: {error_code}"

        # Step 4: Fetch credentials without passing RoleARN - expect failure
        logger.info("Step 4: Fetching credentials without passing RoleARN")

        try:
            sts_client.assume_role(RoleSessionName=session_name)
            assert False, "Expected error when RoleARN is missing but got success"
        except ParamValidationError as e:
            logger.info(
                f"Step 4: Got expected error when RoleARN is missing - {type(e).__name__}: {e}"
            )

        logger.info("RGW STS AssumeRole scenarios test completed successfully!")

    @tier2
    def test_rgw_sts_token_validation(
        self,
        rgw_iam_client_creation,
    ):
        """
        Test RGW STS token validation with valid and invalid credentials

        Steps:
            1. Create IAM user, STS client, IAM role with trust and permission policies
            2. Create S3 bucket using IAM user credentials
            3. Fetch credentials using correct AssumeRole name
            4. Get bucket content using credentials from step 3 - expect success
            5. Get bucket content using invalid Access Key - expect InvalidAccessKeyId
            6. AssumeRole with DurationSeconds=0 - expect ParamValidationError
            7. AssumeRole with negative DurationSeconds - expect ParamValidationError
            8. Get bucket content using invalid Secret Key - expect SignatureDoesNotMatch
            9. Get bucket content using invalid Session Token - expect InvalidArgument
            10. Wait for STS token to expire (DurationSeconds=900, waits ~910s)
            11. Get bucket content again after token expiration - expect ExpiredToken
        """
        iam_client, rgw_account_and_user = rgw_iam_client_creation
        endpoint = rgw_account_and_user["endpoint"]
        region = rgw_account_and_user["region"]
        account_id = rgw_account_and_user["account_id"]

        # Step 1: Create IAM user, STS client, role, and attach permission policy
        logger.info(
            "Step 1: Creating IAM user, STS client, role, and attaching permission policy"
        )
        _, iam_user_access_key, iam_user_secret_key = self.create_iam_user(
            iam_client, with_s3_policy=True
        )
        iam_user_s3_client = self.make_boto3_client(
            "s3", endpoint, region, iam_user_access_key, iam_user_secret_key
        )
        sts_client = self.make_boto3_client(
            "sts", endpoint, region, iam_user_access_key, iam_user_secret_key
        )
        _, role_arn, _ = self.create_iam_role(iam_client, account_id)

        # Step 2: Create S3 bucket and upload test object
        logger.info("Step 2: Creating S3 bucket and uploading test object")
        bucket_name = create_unique_resource_name("sts-test", "bucket")
        iam_user_s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} created successfully")
        test_object_key = "sts-validation-object.txt"
        test_object_data = b"Test data for STS credential validation"
        self.put_object(
            iam_user_s3_client, bucket_name, test_object_key, test_object_data
        )
        logger.info(f"Test object {test_object_key} uploaded successfully")

        # Step 3: Fetch credentials using correct AssumeRole name
        # DurationSeconds=900 is the minimum allowed by botocore and RGW
        logger.info("Step 3: Fetching credentials using correct AssumeRole name")
        session_name = create_unique_resource_name("sts", "session")
        token_duration = 900

        assume_role_response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name,
            DurationSeconds=token_duration,
        )
        credentials = assume_role_response["Credentials"]
        assumed_access_key = credentials["AccessKeyId"]
        assumed_secret_key = credentials["SecretAccessKey"]
        assumed_session_token = credentials["SessionToken"]
        logger.info(
            f"Credentials fetched successfully, token expires in {token_duration}s"
        )

        # Step 4: Put and get object using valid assumed role credentials - expect success
        logger.info(
            "Step 4: Performing put and get operations using valid assumed role credentials"
        )
        assumed_s3_client = self.make_boto3_client(
            "s3",
            endpoint,
            region,
            assumed_access_key,
            assumed_secret_key,
            assumed_session_token,
        )
        assumed_object_key = "sts-assumed-role-object.txt"
        assumed_object_data = b"Test data written using assumed role credentials"
        self.put_object(
            assumed_s3_client, bucket_name, assumed_object_key, assumed_object_data
        )
        retrieved_data = self.get_object(
            assumed_s3_client, bucket_name, assumed_object_key
        )
        assert (
            retrieved_data == assumed_object_data
        ), "Object content mismatch when reading with assumed role credentials"
        logger.info(
            "Step 4: Put and get operations succeeded with valid assumed role credentials"
        )

        # Step 5: Get bucket content using invalid Access Key - expect failure
        logger.info("Step 5: Listing bucket content using invalid Access Key")
        invalid_access_key_client = self.make_boto3_client(
            "s3",
            endpoint,
            region,
            "INVALIDACCESSKEY00000",
            assumed_secret_key,
            assumed_session_token,
        )
        self.assert_client_error(
            lambda: invalid_access_key_client.list_objects_v2(Bucket=bucket_name),
            "InvalidAccessKeyId",
            "Step 5",
        )

        # Step 6: AssumeRole with DurationSeconds=0 - expect failure
        logger.info("Step 6: AssumeRole with DurationSeconds=0")
        try:
            sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=session_name,
                DurationSeconds=0,
            )
            assert False, "Expected error with DurationSeconds=0 but got success"
        except ParamValidationError as e:
            logger.info(
                f"Step 6: Got expected error with DurationSeconds=0 - {type(e).__name__}: {e}"
            )

        # Step 7: AssumeRole with negative DurationSeconds - expect failure
        logger.info("Step 7: AssumeRole with negative DurationSeconds")
        try:
            sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=session_name,
                DurationSeconds=-1,
            )
            assert False, "Expected error with negative DurationSeconds but got success"
        except ParamValidationError as e:
            logger.info(
                f"Step 7: Got expected error with negative DurationSeconds - {type(e).__name__}: {e}"
            )

        # Step 8: Get bucket content using invalid Secret Key - expect failure
        logger.info("Step 8: Listing bucket content using invalid Secret Key")
        invalid_secret_key_client = self.make_boto3_client(
            "s3",
            endpoint,
            region,
            assumed_access_key,
            "invalidsecretkey0000000000000000000000000",  # pragma: allowlist secret
            assumed_session_token,
        )
        self.assert_client_error(
            lambda: invalid_secret_key_client.list_objects_v2(Bucket=bucket_name),
            "SignatureDoesNotMatch",
            "Step 8",
        )

        # Step 9: Get bucket content using invalid Session Token - expect failure
        logger.info("Step 9: Listing bucket content using invalid Session Token")
        invalid_token_client = self.make_boto3_client(
            "s3",
            endpoint,
            region,
            assumed_access_key,
            assumed_secret_key,
            "InvalidSessionToken00000000000000",
        )
        self.assert_client_error(
            lambda: invalid_token_client.list_objects_v2(Bucket=bucket_name),
            "InvalidArgument",
            "Step 9",
        )

        # Step 10: Wait for STS token to expire
        logger.info(f"Step 10: Waiting {token_duration + 10}s for STS token to expire")
        time.sleep(token_duration + 10)
        logger.info("Step 10: Token expiration wait complete")

        # Step 11: Get bucket content after token expiration - expect failure
        # Use TimeoutSampler to retry for up to 120s to handle infra timing jitter
        logger.info("Step 11: Listing bucket content after token expiration")

        def _is_token_expired():
            try:
                assumed_s3_client.list_objects_v2(Bucket=bucket_name)
                return False
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "ExpiredToken":
                    logger.info(
                        f"Step 11: Got expected error after token expiration - "
                        f"Error code: {error_code}"
                    )
                    return True
                raise

        for token_expired in TimeoutSampler(
            timeout=120, sleep=10, func=_is_token_expired
        ):
            if token_expired:
                break

    @tier2
    def test_rgw_sts_role_policy_enforcement(self, rgw_iam_client_creation):
        """
        Test RGW STS role policy enforcement with selective S3 operation permissions

        Steps:
            1. Create IAM user, STS client, IAM role, S3 bucket and upload test object
            2. Create role policy with only s3:GetObject
            3. AssumeRole and fetch credentials
            4. Perform get operation using credentials from step 3 - expect success
            5. Perform delete operation using credentials from step 3 - expect AccessDenied
            6. Update role policy to add s3:DeleteObject
            7. Create new AssumeRole and fetch credentials
            8. Perform delete operation using credentials from step 7 - expect success
            9. Create new role policy allowing all S3 operations with explicit deny on
               s3:DeleteObject
            10. Create new AssumeRole and fetch credentials
            11. Perform get operation using credentials from step 10 - expect success
            12. Perform delete operation using credentials from step 10 - expect AccessDenied
        """
        iam_client, rgw_account_and_user = rgw_iam_client_creation
        endpoint = rgw_account_and_user["endpoint"]
        region = rgw_account_and_user["region"]
        account_id = rgw_account_and_user["account_id"]

        # Step 1: Create IAM user, role, S3 bucket and upload test object
        logger.info(
            "Step 1: Creating IAM user, role, S3 bucket and uploading test object"
        )
        _, iam_user_access_key, iam_user_secret_key = self.create_iam_user(
            iam_client, with_s3_policy=True
        )
        iam_user_s3_client = self.make_boto3_client(
            "s3", endpoint, region, iam_user_access_key, iam_user_secret_key
        )
        sts_client = self.make_boto3_client(
            "sts", endpoint, region, iam_user_access_key, iam_user_secret_key
        )
        role_name, role_arn, policy_name = self.create_iam_role(
            iam_client, account_id, with_permission_policy=False
        )

        bucket_name = create_unique_resource_name("sts-test", "bucket")
        iam_user_s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} created successfully")

        object_key = "test-policy-object.txt"
        object_content = b"Test object for role policy enforcement"
        self.put_object(iam_user_s3_client, bucket_name, object_key, object_content)
        logger.info("Step 1: Setup complete")

        # Step 2: Create role policy with only s3:GetObject
        logger.info("Step 2: Creating role policy with only s3:GetObject")
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "*"}
                    ],
                }
            ),
        )
        logger.info("Step 2: Role policy with s3:GetObject created")

        # Step 3: AssumeRole and fetch credentials
        logger.info("Step 3: AssumeRole and fetching credentials")
        assumed_s3_client = self.assume_role_and_get_s3_client(
            sts_client, role_arn, endpoint, region
        )
        logger.info("Step 3: Credentials fetched successfully")

        # Step 4: GET operation - expect success
        logger.info("Step 4: Performing GET operation with GetObject-only policy")
        response = assumed_s3_client.get_object(Bucket=bucket_name, Key=object_key)
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "Expected GET to succeed with s3:GetObject policy"
        logger.info("Step 4: GET operation succeeded as expected")

        # Step 5: DELETE operation - expect failure (no s3:DeleteObject in policy)
        logger.info(
            "Step 5: Performing DELETE operation with GetObject-only policy "
            "- expect AccessDenied"
        )
        self.assert_client_error(
            lambda: assumed_s3_client.delete_object(Bucket=bucket_name, Key=object_key),
            "AccessDenied",
            "Step 5",
        )

        # Step 6: Update role policy to add s3:DeleteObject
        logger.info("Step 6: Updating role policy to add s3:DeleteObject")
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:GetObject", "s3:DeleteObject"],
                            "Resource": "*",
                        }
                    ],
                }
            ),
        )
        logger.info("Step 6: Role policy updated with s3:GetObject and s3:DeleteObject")

        # Step 7: New AssumeRole and fetch credentials
        logger.info("Step 7: Creating new AssumeRole and fetching credentials")
        new_assumed_s3_client = self.assume_role_and_get_s3_client(
            sts_client, role_arn, endpoint, region
        )
        logger.info("Step 7: New credentials fetched successfully")

        # Step 8: DELETE operation - expect success (s3:DeleteObject now allowed)
        logger.info(
            "Step 8: Performing DELETE operation with updated policy - expect success"
        )
        delete_response = new_assumed_s3_client.delete_object(
            Bucket=bucket_name, Key=object_key
        )
        assert (
            delete_response["ResponseMetadata"]["HTTPStatusCode"] == 204
        ), "Expected DELETE to succeed after adding s3:DeleteObject to role policy"
        logger.info("Step 8: DELETE operation succeeded as expected")

        # Re-upload object deleted in step 8 for use in steps 11-12
        self.put_object(iam_user_s3_client, bucket_name, object_key, object_content)

        # Step 9: Create new role policy allowing all operations with explicit deny on
        # s3:DeleteObject
        logger.info(
            "Step 9: Creating role policy with all S3 operations allowed "
            "and explicit deny on s3:DeleteObject"
        )
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {"Effect": "Allow", "Action": ["s3:*"], "Resource": "*"},
                        {
                            "Effect": "Deny",
                            "Action": ["s3:DeleteObject"],
                            "Resource": "*",
                        },
                    ],
                }
            ),
        )
        logger.info(
            "Step 9: Role policy updated to allow all S3 operations "
            "with explicit deny on s3:DeleteObject"
        )

        # Step 10: New AssumeRole and fetch credentials
        logger.info("Step 10: Creating new AssumeRole with deny-delete policy")
        deny_s3_client = self.assume_role_and_get_s3_client(
            sts_client, role_arn, endpoint, region
        )
        logger.info("Step 10: Credentials fetched with deny-delete policy")

        # Step 11: GET operation - expect success (s3:GetObject allowed by s3:*)
        logger.info(
            "Step 11: Performing GET operation with deny-delete policy - expect success"
        )
        get_response = deny_s3_client.get_object(Bucket=bucket_name, Key=object_key)
        assert (
            get_response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "Expected GET to succeed with Allow s3:* and Deny s3:DeleteObject policy"
        logger.info("Step 11: GET operation succeeded as expected")

        # Step 12: DELETE operation - expect failure (explicit deny overrides allow)
        logger.info(
            "Step 12: Performing DELETE operation with explicit deny policy "
            "- expect AccessDenied"
        )
        self.assert_client_error(
            lambda: deny_s3_client.delete_object(Bucket=bucket_name, Key=object_key),
            "AccessDenied",
            "Step 12",
        )

        logger.info("RGW STS role policy enforcement test completed successfully!")

    @tier2
    def test_rgw_sts_s3_operations_comprehensive(self, rgw_iam_client_creation):
        """
        Test comprehensive S3 operations using both STS and standard IAM credentials

        Steps:
            1. Create IAM role with full S3 permissions and assume role to obtain STS
               credentials; use fixture root user credentials as standard credentials
            2. Create shared bucket; list with both STS and standard credentials
            3. Perform object upload, download, delete using both STS and standard
               credentials on shared bucket
            4. Perform multipart upload, download, delete using both STS and standard
               credentials on shared bucket
        """
        iam_client, rgw_account_and_user = rgw_iam_client_creation
        endpoint = rgw_account_and_user["endpoint"]
        region = rgw_account_and_user["region"]
        account_id = rgw_account_and_user["account_id"]
        access_key = rgw_account_and_user["access_key"]
        secret_key = rgw_account_and_user["secret_key"]

        # Step 1: Create role with full S3 permissions and assume role
        logger.info("Step 1: Creating role with full S3 permissions and assuming role")
        std_s3_client = self.make_boto3_client(
            "s3", endpoint, region, access_key, secret_key
        )
        sts_client = self.make_boto3_client(
            "sts", endpoint, region, access_key, secret_key
        )

        _, role_arn, _ = self.create_iam_role(iam_client, account_id)
        sts_s3_client = self.assume_role_and_get_s3_client(
            sts_client, role_arn, endpoint, region
        )
        logger.info("Step 1: Role and STS credentials set up successfully")

        # Create one shared bucket for all operations; deleted at the end of the test
        shared_bucket = create_unique_resource_name("sts-shared", "bucket")
        std_s3_client.create_bucket(Bucket=shared_bucket)
        logger.info(f"Shared bucket {shared_bucket} created using standard credentials")

        # Step 2: List shared bucket with both STS and standard credentials
        logger.info("Step 2: Listing shared bucket with STS and standard credentials")
        std_bucket_names = [b["Name"] for b in std_s3_client.list_buckets()["Buckets"]]
        assert (
            shared_bucket in std_bucket_names
        ), f"Bucket {shared_bucket} not found when listing with standard credentials"
        logger.info(
            f"Bucket {shared_bucket} listed successfully using standard credentials"
        )

        sts_bucket_names = [b["Name"] for b in sts_s3_client.list_buckets()["Buckets"]]
        assert (
            shared_bucket in sts_bucket_names
        ), f"Bucket {shared_bucket} not found when listing with STS credentials"
        logger.info(f"Bucket {shared_bucket} listed successfully using STS credentials")
        logger.info("Step 2: Bucket list operations completed successfully")

        # Step 3: Object upload, download, delete on the shared bucket
        logger.info(
            "Step 3: Performing object upload, download, delete "
            "with STS and standard credentials"
        )

        # Upload with standard, download and delete with STS
        std_object_key = "std-object.txt"
        std_object_data = b"Object uploaded using standard IAM credentials"
        self.put_object(std_s3_client, shared_bucket, std_object_key, std_object_data)
        logger.info(f"Object {std_object_key} uploaded using standard credentials")
        retrieved = sts_s3_client.get_object(Bucket=shared_bucket, Key=std_object_key)[
            "Body"
        ].read()
        assert (
            retrieved == std_object_data
        ), "Content mismatch for object uploaded by standard and downloaded by STS"
        logger.info(
            f"Object {std_object_key} downloaded and verified using STS credentials"
        )
        sts_s3_client.delete_object(Bucket=shared_bucket, Key=std_object_key)
        logger.info(f"Object {std_object_key} deleted using STS credentials")

        # Upload with STS, download and delete with standard
        sts_object_key = "sts-object.txt"
        sts_object_data = b"Object uploaded using STS assumed role credentials"
        self.put_object(sts_s3_client, shared_bucket, sts_object_key, sts_object_data)
        logger.info(f"Object {sts_object_key} uploaded using STS credentials")
        retrieved = std_s3_client.get_object(Bucket=shared_bucket, Key=sts_object_key)[
            "Body"
        ].read()
        assert (
            retrieved == sts_object_data
        ), "Content mismatch for object uploaded by STS and downloaded by standard"
        logger.info(
            f"Object {sts_object_key} downloaded and verified using standard credentials"
        )
        std_s3_client.delete_object(Bucket=shared_bucket, Key=sts_object_key)
        logger.info(f"Object {sts_object_key} deleted using standard credentials")
        logger.info("Step 3: Object operations completed successfully")

        # Step 4: Multipart upload, download, delete on the shared bucket
        logger.info(
            "Step 4: Performing multipart upload, download, delete "
            "with STS and standard credentials"
        )
        multipart_data = b"M" * (5 * 1024 * 1024 * 2 + 1024)  # two 5 MiB parts + 1 KiB

        # Multipart upload with standard, download and delete with STS
        std_mp_key = "std-multipart-object"
        self.put_object(std_s3_client, shared_bucket, std_mp_key, multipart_data)
        logger.info(
            f"Multipart object {std_mp_key} uploaded using standard credentials"
        )
        downloaded = self.get_object(sts_s3_client, shared_bucket, std_mp_key)
        assert (
            downloaded == multipart_data
        ), "Content mismatch for multipart object uploaded by standard and downloaded by STS"
        logger.info(
            f"Multipart object {std_mp_key} downloaded and verified using STS credentials"
        )
        sts_s3_client.delete_object(Bucket=shared_bucket, Key=std_mp_key)
        logger.info(f"Multipart object {std_mp_key} deleted using STS credentials")

        # Multipart upload with STS, download and delete with standard
        sts_mp_key = "sts-multipart-object"
        self.put_object(sts_s3_client, shared_bucket, sts_mp_key, multipart_data)
        logger.info(f"Multipart object {sts_mp_key} uploaded using STS credentials")
        downloaded = self.get_object(std_s3_client, shared_bucket, sts_mp_key)
        assert (
            downloaded == multipart_data
        ), "Content mismatch for multipart object uploaded by STS and downloaded by standard"
        logger.info(
            f"Multipart object {sts_mp_key} downloaded and verified "
            "using standard credentials"
        )
        std_s3_client.delete_object(Bucket=shared_bucket, Key=sts_mp_key)
        logger.info(f"Multipart object {sts_mp_key} deleted using standard credentials")
        logger.info("Step 4: Multipart operations completed successfully")

        # Delete the shared bucket using STS credentials
        sts_s3_client.delete_bucket(Bucket=shared_bucket)
        logger.info(f"Shared bucket {shared_bucket} deleted using STS credentials")
        logger.info("RGW STS comprehensive S3 operations test completed successfully!")
