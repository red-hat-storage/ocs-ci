import logging
import time

import pytest
import botocore.exceptions as boto3exception
import json
import uuid

from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import (
    NoBucketPolicyResponse,
    InvalidStatusCode,
    UnexpectedBehaviour,
    CommandFailed,
)
from ocs_ci.framework.testlib import (
    MCGTest,
    tier1,
    tier2,
    tier3,
    skipif_ocs_version,
)
from ocs_ci.ocs.resources.bucket_policy import (
    NoobaaAccount,
    HttpResponseParser,
    gen_bucket_policy,
)
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.bucket_utils import (
    put_bucket_policy,
    get_bucket_policy,
    put_public_access_block_config,
    get_public_access_block,
    s3_put_object,
    delete_bucket_policy,
    s3_get_object,
    s3_delete_object,
    create_multipart_upload,
    s3_put_bucket_website,
    s3_get_bucket_website,
    s3_delete_bucket_website,
    s3_get_bucket_versioning,
    s3_put_bucket_versioning,
    s3_list_objects_v2,
    list_multipart_upload,
    list_uploaded_parts,
    complete_multipart_upload,
    craft_s3_command,
)
from ocs_ci.ocs.defaults import website_config, index, error
from ocs_ci.ocs.constants import (
    bucket_website_action_list,
    bucket_version_action_list,
    AWSCLI_TEST_OBJ_DIR,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    red_squad,
    runs_on_provider,
    mcg,
    provider_mode,
    post_upgrade,
    pre_upgrade,
    polarion_id,
)
from ocs_ci.ocs.resources.storage_cluster import verify_backing_store
from ocs_ci.utility import version
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


def delete_bucket_policy_verify(s3_obj, bucket_name):
    """
    Delete bucket policy and confirm it got deleted successfully, if not throwing
    UnexpectedBehaviour with an invalid error code.
    Args:
        s3_obj (obj): MCG or OBC object
        bucket_name (str): Name of the bucket
    """

    # Delete bucket policy
    logger.info(f"Delete bucket policy by admin on bucket: {bucket_name}")
    delete_policy = delete_bucket_policy(s3_obj, bucket_name)
    logger.info(f"Delete policy response: {delete_policy}")

    # Confirming again by calling get_bucket_policy
    try:
        get_bucket_policy(s3_obj, bucket_name)
    except boto3exception.ClientError as e:
        logger.info(e.response)
        response = HttpResponseParser(e.response)
        if response.error["Code"] == "NoSuchBucketPolicy":
            logger.info("Bucket policy has been deleted successfully")
        else:
            raise UnexpectedBehaviour(
                f"{e.response} received invalid error code {response.error['Code']}"
            )


def put_allow_bucket_policy(bucket_name, mcg_obj):
    """
    This utility function puts allow policy on the specified bucket
    Args:
        bucket_name (str): The bucket on which the policy should be put
        mcg_obj (obj): An object representing the current state of the MCG in the cluster
    """

    bucket_policy_generated = gen_bucket_policy(
        user_list="*",
        actions_list=["*"],
        resources_list=[bucket_name, f'{bucket_name}/{"*"}'],
    )
    bucket_policy = json.dumps(bucket_policy_generated)
    # Put bucket policy
    logger.info(f"Putting bucket policy {bucket_policy} on bucket: {bucket_name}")
    put_bucket_policy(mcg_obj, bucket_name, bucket_policy)
    # Hardcoded sleep is needed because we lack a confirmation mechanism
    # we could wait for - even the get-policy result has been observed to be
    # unreliable in confirming whether the policy is actually taking effect
    timeout = 120
    logger.info(f"Waiting for {timeout} seconds for the policy to take effect")
    time.sleep(timeout)
    get_policy = get_bucket_policy(mcg_obj, bucket_name)
    logger.info(f"Got bucket policy: {get_policy['Policy']} on bucket {bucket_name}")


def put_public_access_block_configuration(
    bucket_name, mcg_obj, public_access_block_configuration
):
    """
    This utility function puts public access block configuration on the specified bucket
    Args:
        bucket_name (str): The bucket on which the policy should be put
        mcg_obj (obj): An object representing the current state of the MCG in the cluster
        public_access_block_configuration (dict): access block configuration to put
    """
    timeout = 120
    logger.info(
        f"Putting public access block configuration {public_access_block_configuration} "
        f"on bucket: {bucket_name}"
    )
    put_public_access_block_config(
        mcg_obj, bucket_name, public_access_block_configuration
    )
    # Hardcoded sleep is needed because we lack a confirmation mechanism
    # we could wait for - even the get_public_access_block result has been observed to be
    # unreliable in confirming whether the public access block is actually taking effect
    logger.info(
        f"Waiting for {timeout} seconds for the public access block to take effect"
    )
    time.sleep(timeout)
    public_access_block_configuration_defined = get_public_access_block(
        mcg_obj, bucket_name
    )
    logger.info(
        f"Public access block configuration on bucket {bucket_name} is: "
        f"{public_access_block_configuration_defined}"
    )


def check_ls_command(
    mcg_obj,
    awscli_pod_session,
    bucket_name,
    file_name,
):
    """
    Check that 'cp' and 'ls' commands on the bucket work. If the commands succeed, nothing happens.
    If not, an exception thrown from craft_s3_command will be raised
    Args:
        mcg_obj (obj): An object representing the current state of the MCG in the cluster
        awscli_pod_session (pod): A pod running the AWSCLI tools
        bucket_name (str): Name of the bucket on which ls should be run
        file_name (str): File to be looked for
    """

    awscli_pod_session.exec_cmd_on_pod(
        command=craft_s3_command(
            f"cp {AWSCLI_TEST_OBJ_DIR}{file_name} s3://{bucket_name}/{file_name}",
            mcg_obj=mcg_obj,
        ),
        out_yaml_format=False,
    )

    awscli_pod_session.exec_cmd_on_pod(
        command=craft_s3_command(f"ls s3://{bucket_name}/{file_name}", mcg_obj=mcg_obj),
        out_yaml_format=False,
    )


def check_commands(
    mcg_obj,
    awscli_pod_session,
    bucket_name,
    path_on_bucket,
    file_name,
    allow_ls_anonymous,
):
    """
    Check that 'ls' and 'cp' commands on the bucket work as expected.
    The expected behavior of 'ls' is:
        1. Success in finding 'file_name' file with non-anonymous access
        2. Success with anonymous access when 'allow_anonymous' is True
        3. Failure with "Access Denied" on anonymous access when 'allow_anonymous' is False
    The expected behavior of 'cp' is:
        1. Success with non-anonymous access
        2. Failure with "Access Denied" on anonymous access
    Args:
        mcg_obj (obj): An object representing the current state of the MCG in the cluster
        awscli_pod_session (pod): A pod running the AWSCLI tools
        path_on_bucket (str) Target path on the bucket, to which the file is copied. If empty, the files
                is copied to the bucket root.
        bucket_name (str): Name of the bucket on which ls should be run
        file_name (str): File to be looked for
        allow_ls_anonymous (bool): Defines whether 'ls' with anonymous access is allowed
    Raises:
        UnexpectedBehaviour if the ls is not working as expected
    """

    ls_command = f"ls s3://{bucket_name}/{path_on_bucket}"
    cp_command = f"cp {AWSCLI_TEST_OBJ_DIR}{file_name} s3://{bucket_name}/{path_on_bucket}{file_name}"

    awscli_pod_session.exec_cmd_on_pod(
        command=craft_s3_command(cp_command, mcg_obj=mcg_obj),
        out_yaml_format=False,
    )

    awscli_pod_session.exec_cmd_on_pod(
        command=craft_s3_command(f"{ls_command}{file_name}", mcg_obj=mcg_obj),
        out_yaml_format=False,
    )

    try:
        awscli_pod_session.exec_cmd_on_pod(
            command=craft_s3_command(
                f"{ls_command} --no-sign-request", mcg_obj=mcg_obj
            ),
            out_yaml_format=False,
        )
        if not allow_ls_anonymous:
            raise UnexpectedBehaviour(
                "ls command with anonymous user (--no-sign-request) should not be allowed"
            )
    except CommandFailed as ex:
        if not allow_ls_anonymous and "Access Denied" in str(ex):
            logger.info(
                "ls command with anonymous user (--no-sign-request) is not allowed, continue the test"
            )
        else:
            raise

    # check copy with anonymous user, it should always fail
    try:
        awscli_pod_session.exec_cmd_on_pod(
            command=craft_s3_command(
                f"{cp_command} --no-sign-request", mcg_obj=mcg_obj
            ),
            out_yaml_format=False,
        )
        raise UnexpectedBehaviour(
            "cp command with anonymous user (--no-sign-request) should not be allowed"
        )
    except CommandFailed as ex:
        if "Access Denied" in str(ex):
            logger.info(
                "cp command with anonymous user (--no-sign-request) is not allowed, continue the test"
            )
        else:
            raise


@provider_mode
@mcg
@red_squad
@runs_on_provider
@skipif_managed_service
@skipif_ocs_version("<4.3")
class TestS3BucketPolicy(MCGTest):
    """
    Test Bucket Policies on Noobaa accounts
    """

    @pytest.mark.polarion_id("OCS-2150")
    @tier1
    def test_basic_bucket_policy_operations(self, mcg_obj, bucket_factory):
        """
        Test Add, Modify, delete bucket policies
        """
        # Creating obc and obc object to get account details, keys etc
        obc_name = bucket_factory(amount=1, interface="OC")[0].name
        obc_obj = OBC(obc_name)

        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["GetObject"],
            resources_list=[obc_obj.bucket_name],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Add Bucket Policy
        logger.info(f"Creating bucket policy on bucket: {obc_obj.bucket_name}")
        put_policy = put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)

        if put_policy is not None:
            response = HttpResponseParser(put_policy)
            if response.status_code == 200:
                logger.info("Bucket policy has been created successfully")
            else:
                raise InvalidStatusCode(f"Invalid Status code: {response.status_code}")
        else:
            raise NoBucketPolicyResponse("Put policy response is none")

        # Get bucket policy
        logger.info(f"Getting Bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Modifying bucket policy to take new policy
        logger.info("Modifying bucket policy")
        actions_list = ["ListBucket", "CreateBucket"]
        actions = list(map(lambda action: "s3:%s" % action, actions_list))

        modified_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=actions_list,
            resources_list=[obc_obj.bucket_name],
        )
        bucket_policy_modified = json.dumps(modified_policy_generated)

        put_modified_policy = put_bucket_policy(
            mcg_obj, obc_obj.bucket_name, bucket_policy_modified
        )

        if put_modified_policy is not None:
            response = HttpResponseParser(put_modified_policy)
            if response.status_code == 200:
                logger.info("Bucket policy has been modified successfully")
            else:
                raise InvalidStatusCode(f"Invalid Status code: {response.status_code}")
        else:
            raise NoBucketPolicyResponse("Put modified policy response is none")

        # Get Modified Policy
        get_modified_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        modified_policy = json.loads(get_modified_policy["Policy"])
        logger.info(f"Got modified bucket policy: {modified_policy}")

        actions_from_modified_policy = modified_policy["Statement"][0]["Action"]
        modified_actions = list(map(str, actions_from_modified_policy))
        initial_actions = actions
        logger.info(f"Actions from modified_policy: {modified_actions}")
        logger.info(f"User provided actions actions: {initial_actions}")
        if modified_actions == initial_actions:
            logger.info("Modified actions and initial actions are same")
        else:
            raise UnexpectedBehaviour(
                "Modification Failed: Action lists are not identical"
            )

        # Delete Policy
        logger.info(f"Delete bucket policy by admin on bucket: {obc_obj.bucket_name}")
        delete_policy = delete_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Delete policy response: {delete_policy}")

        if delete_policy is not None:
            response = HttpResponseParser(delete_policy)
            if response.status_code == 204:
                logger.info("Bucket policy is deleted successfully")
            else:
                raise InvalidStatusCode(f"Invalid Status code: {response.status_code}")
        else:
            raise NoBucketPolicyResponse("Delete policy response is none")

        # Confirming again by calling get_bucket_policy
        try:
            get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "NoSuchBucketPolicy":
                logger.info("Bucket policy has been deleted successfully")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )

    @provider_mode
    @pytest.mark.polarion_id("OCS-2146")
    @tier1
    def test_bucket_policy_actions(self, mcg_obj, bucket_factory):
        """
        Tests user access to Put, Get, Delete bucket policy actions
        """
        # Creating obc and obc object to get account details, keys etc
        obc_name = bucket_factory(amount=1, interface="OC")[0].name
        obc_obj = OBC(obc_name)

        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["PutBucketPolicy"],
            resources_list=[obc_obj.bucket_name],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Admin creates a policy on the user bucket, for Action: PutBucketPolicy
        logger.info(f"Creating policy by admin on bucket: {obc_obj.bucket_name}")
        put_policy = put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)
        logger.info(f"Put bucket policy response from admin: {put_policy}")

        # Verifying Put bucket policy by user by changing the actions to GetBucketPolicy & DeleteBucketPolicy
        user_generated_policy = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["GetBucketPolicy", "DeleteBucketPolicy"],
            resources_list=[obc_obj.bucket_name],
        )
        bucket_policy1 = json.dumps(user_generated_policy)

        logger.info(f"Changing bucket policy by User on bucket: {obc_obj.bucket_name}")
        put_policy_user = put_bucket_policy(
            obc_obj, obc_obj.bucket_name, bucket_policy1
        )
        logger.info(f"Put bucket policy response from user: {put_policy_user}")

        # Verifying whether user can get the bucket policy after modification
        get_policy = get_bucket_policy(obc_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Verifying whether user is not allowed Put the bucket policy after modification
        logger.info(
            f"Verifying whether user: {obc_obj.obc_account} is denied to put objects"
        )
        try:
            put_bucket_policy(obc_obj, obc_obj.bucket_name, bucket_policy1)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info(
                    f"Put bucket policy has been denied access to the user: {obc_obj.obc_account}"
                )
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )

        # Verifying whether user can Delete the bucket policy after modification
        logger.info(f"Deleting bucket policy on bucket: {obc_obj.bucket_name}")
        delete_policy = delete_bucket_policy(obc_obj, obc_obj.bucket_name)
        logger.info(f"Delete policy response: {delete_policy}")

    @pytest.mark.polarion_id("OCS-2156")
    @tier1
    def test_object_actions(self, mcg_obj, bucket_factory):
        """
        Test to verify different object actions and cross account access to buckets
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        # Creating multiple obc users (accounts)
        obc = bucket_factory(amount=1, interface="OC")
        obc_obj = OBC(obc[0].name)

        # Admin sets policy on obc bucket with obc account principal
        bucket_policy_generated = gen_bucket_policy(
            user_list=[obc_obj.obc_account],
            actions_list=(
                ["PutObject"]
                if version.get_semantic_ocs_version_from_config() <= version.VERSION_4_6
                else ["GetObject", "DeleteObject"]
            ),
            effect=(
                "Allow"
                if version.get_semantic_ocs_version_from_config() <= version.VERSION_4_6
                else "Deny"
            ),
            resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            f"Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}"
        )
        put_policy = put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)
        logger.info(f"Put bucket policy response from Admin: {put_policy}")

        # Hardcoded sleep is needed because we lack a confirmation mechanism
        # we could wait for - even the get-policy result has been observed to be
        # unreliable in confirming whether they policy is actually taking effect
        logger.info("Waiting for 120 seconds for the policy to take effect")
        time.sleep(120)

        # Get Policy
        logger.info(f"Getting Bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Verifying whether users can put object
        logger.info(
            f"Adding object on bucket: {obc_obj.bucket_name} using user: {obc_obj.obc_account}"
        )
        assert s3_put_object(
            obc_obj, obc_obj.bucket_name, object_key, data
        ), "Failed: Put Object"

        # Verifying whether Get action is not allowed
        logger.info(
            f"Verifying whether user: {obc_obj.obc_account} is denied to Get object"
        )
        try:
            s3_get_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info("Get Object action has been denied access")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )
        else:
            assert False, "Get object succeeded when it should have failed"

        if version.get_semantic_ocs_version_from_config() == version.VERSION_4_6:
            logger.info(
                f"Verifying whether the user: "
                f"{obc_obj.obc_account} is able to access Get action"
                f"irrespective of the policy set"
            )
            assert s3_get_object(
                obc_obj, obc_obj.bucket_name, object_key
            ), "Failed: Get Object"

        # Verifying whether obc account allowed to create multipart
        logger.info(
            f"Creating multipart on bucket: {obc_obj.bucket_name}"
            f" with key: {object_key} using user: {obc_obj.obc_account}"
        )
        create_multipart_upload(obc_obj, obc_obj.bucket_name, object_key)

        # Verifying whether S3 user is allowed to create multipart
        logger.info(
            f"Creating multipart on bucket: {obc_obj.bucket_name} "
            f"with key: {object_key} using user: {obc_obj.obc_account}"
        )
        create_multipart_upload(obc_obj, obc_obj.bucket_name, object_key)

        # Verifying whether obc account is denied access to delete object
        logger.info(
            f"Verifying whether user: {obc_obj.obc_account} is denied to Delete object"
        )
        try:
            s3_delete_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info("Delete action has been denied access")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )
        else:
            assert False, "Delete object succeeded when it should have failed"

    @pytest.mark.polarion_id("OCS-2145")
    @tier2
    def test_anonymous_read_only(self, mcg_obj, bucket_factory):
        """
        Tests read only access by an anonymous user
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        user_name = "noobaa-user" + str(uuid.uuid4().hex)
        email = user_name + "@mail.com"

        # Creating a s3 bucket
        s3_bucket = bucket_factory(amount=1, interface="OC")[0]

        # Creating a random user account
        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_12:
            user = NoobaaAccount(
                mcg_obj, name=user_name, email=email, buckets=[s3_bucket.name]
            )
        else:
            user = NoobaaAccount(mcg_obj, name=user_name, email=email)

        # Admin sets policy all users '*' (Public access)
        bucket_policy_generated = gen_bucket_policy(
            user_list="*",
            actions_list=["GetObject"],
            resources_list=[f'{s3_bucket.name}/{"*"}'],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            f"Creating bucket policy on bucket: {s3_bucket.name} with wildcard (*) Principal"
        )
        put_policy = put_bucket_policy(mcg_obj, s3_bucket.name, bucket_policy)
        logger.info(f"Put bucket policy response from Admin: {put_policy}")

        # Getting Policy
        logger.info(f"Getting bucket policy on bucket: {s3_bucket.name}")
        get_policy = get_bucket_policy(mcg_obj, s3_bucket.name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Admin writes an object to bucket
        logger.info(f"Writing object on bucket: {s3_bucket.name} by admin")
        assert s3_put_object(
            mcg_obj, s3_bucket.name, object_key, data
        ), "Failed: PutObject"

        # Reading the object by anonymous user
        logger.info(
            f"Getting object by user: {user.email_id} on bucket: {s3_bucket.name} "
        )
        retry_s3_get_object = retry(boto3exception.ClientError, tries=4, delay=10)(
            s3_get_object
        )
        assert retry_s3_get_object(
            user, s3_bucket.name, object_key
        ), f"Failed: Get Object by user {user.email_id}"

    @pytest.mark.polarion_id("OCS-2140")
    @tier2
    def test_bucket_website_and_policies(self, mcg_obj, bucket_factory):
        """
        Tests bucket website bucket policy actions
        """
        # Creating a OBC (account)
        obc = bucket_factory(amount=1, interface="OC")
        obc_obj = OBC(obc[0].name)

        # Admin sets policy with Put/Get bucket website actions
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=bucket_website_action_list,
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}'],
            effect="Allow",
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            f"Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}"
        )
        assert put_bucket_policy(
            mcg_obj, obc_obj.bucket_name, bucket_policy
        ), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f"Getting bucket policy for bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        logger.info(f"Adding bucket website config to: {obc_obj.bucket_name}")
        assert s3_put_bucket_website(
            s3_obj=obc_obj,
            bucketname=obc_obj.bucket_name,
            website_config=website_config,
        ), "Failed: PutBucketWebsite"
        logger.info(f"Getting bucket website config from: {obc_obj.bucket_name}")
        assert s3_get_bucket_website(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name
        ), "Failed: GetBucketWebsite"

        logger.info("Writing index and error data to the bucket")
        assert s3_put_object(
            s3_obj=obc_obj,
            bucketname=obc_obj.bucket_name,
            object_key="index.html",
            data=index,
            content_type="text/html",
        ), "Failed: PutObject"
        assert s3_put_object(
            s3_obj=obc_obj,
            bucketname=obc_obj.bucket_name,
            object_key="error.html",
            data=error,
            content_type="text/html",
        ), "Failed: PutObject"

        # Verifying whether DeleteBucketWebsite action is denied access
        logger.info(
            f"Verifying whether user: {obc_obj.obc_account} is denied to DeleteBucketWebsite"
        )
        try:
            s3_delete_bucket_website(s3_obj=obc_obj, bucketname=obc_obj.bucket_name)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info("GetObject action has been denied access")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )

        # Admin modifies policy to allow DeleteBucketWebsite action
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["DeleteBucketWebsite"],
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}'],
            effect="Allow",
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            f"Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}"
        )
        assert put_bucket_policy(
            mcg_obj, obc_obj.bucket_name, bucket_policy
        ), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f"Getting bucket policy for bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        logger.info(
            f"Deleting bucket website config from bucket: {obc_obj.bucket_name}"
        )
        assert s3_delete_bucket_website(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name
        ), "Failed: DeleteBucketWebsite"

    @pytest.mark.polarion_id("OCS-2161")
    @tier2
    def test_bucket_versioning_and_policies(self, mcg_obj, bucket_factory):
        """
        Tests bucket and object versioning on Noobaa buckets and also its related actions
        """
        # Creating a OBC user (Account)
        obc = bucket_factory(amount=1, interface="OC")
        obc_obj = OBC(obc[0].name)

        # Admin sets a policy on OBC bucket to allow versioning related actions
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=bucket_version_action_list,
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}'],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Creating policy
        logger.info(f"Creating bucket policy on bucket: {obc_obj.bucket_name} by Admin")
        assert put_bucket_policy(
            mcg_obj, obc_obj.bucket_name, bucket_policy
        ), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f"Getting bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        logger.info(
            f"Enabling bucket versioning on {obc_obj.bucket_name} using User: {obc_obj.obc_account}"
        )
        assert s3_put_bucket_versioning(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name, status="Enabled"
        ), "Failed: PutBucketVersioning"

        logger.info(
            f"Verifying whether versioning is enabled on bucket: {obc_obj.bucket_name}"
        )
        assert s3_get_bucket_versioning(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name
        ), "Failed: GetBucketVersioning"

        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["PutBucketVersioning"],
            resources_list=[obc_obj.bucket_name],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f"Creating bucket policy on bucket: {obc_obj.bucket_name} by Admin")
        assert put_bucket_policy(
            mcg_obj, obc_obj.bucket_name, bucket_policy
        ), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f"Getting bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        logger.info(
            f"Suspending bucket versioning on {obc_obj.bucket_name} using User: {obc_obj.obc_account}"
        )
        assert s3_put_bucket_versioning(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name, status="Suspended"
        ), "Failed: PutBucketVersioning"

        # Verifying whether GetBucketVersion action is denied access
        logger.info(
            f"Verifying whether user: {obc_obj.obc_account} is denied to GetBucketVersion"
        )
        try:
            s3_get_bucket_versioning(s3_obj=obc_obj, bucketname=obc_obj.bucket_name)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info("Get Object action has been denied access")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )

    @pytest.mark.polarion_id("OCS-2159")
    @tier2
    def test_bucket_policy_effect_deny(self, mcg_obj, bucket_factory):
        """
        Tests explicit "Deny" effect on bucket policy actions
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        # Creating multiple obc user (account)
        obc = bucket_factory(amount=1, interface="OC")
        obc_obj = OBC(obc[0].name)

        # Admin writes an object to bucket
        logger.info(f"Writing an object on bucket: {obc_obj.bucket_name} by Admin")
        assert s3_put_object(
            mcg_obj, obc_obj.bucket_name, object_key, data
        ), "Failed: PutObject"

        # Admin sets policy with Effect: Deny on obc bucket with obc-account principal
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["GetObject"],
            resources_list=[f"{obc_obj.bucket_name}/{object_key}"],
            effect="Deny",
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            f"Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}"
        )
        assert put_bucket_policy(
            mcg_obj, obc_obj.bucket_name, bucket_policy
        ), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f"Getting bucket policy from bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Verifying whether Get action is denied access
        logger.info(
            f"Verifying whether user: {obc_obj.obc_account} is denied to GetObject"
        )
        try:
            s3_get_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info("GetObject action has been denied access")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )

        # Admin sets a new policy on same obc bucket with same account but with different action and resource
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["DeleteObject"],
            resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
            effect="Deny",
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f"Creating bucket policy on bucket: {obc_obj.bucket_name}")
        assert put_bucket_policy(
            mcg_obj, obc_obj.bucket_name, bucket_policy
        ), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f"Getting bucket policy from bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Verifying whether delete action is denied
        logger.info(
            f"Verifying whether user: {obc_obj.obc_account} is denied to Get object"
        )
        try:
            s3_delete_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info("Get Object action has been denied access")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )

    @pytest.mark.polarion_id("OCS-2149")
    @tier2
    def test_bucket_policy_multi_statement(self, mcg_obj, bucket_factory):
        """
        Tests multiple statements in a bucket policy
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        # Creating OBC (account) and Noobaa user account
        obc = bucket_factory(amount=1, interface="OC")
        obc_obj = OBC(obc[0].name)

        # Statement_1 public read access to a bucket
        single_statement_policy = gen_bucket_policy(
            sid="statement-1",
            user_list="*",
            actions_list=["GetObject"],
            resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
            effect="Allow",
        )

        # Additional Statements; Statement_2 - PutObject permission on specific user
        # Statement_3 - Denying Permission to DeleteObject action for aultiple Users
        new_statements = {
            "statement_2": {
                "Action": "s3:PutObject",
                "Effect": "Allow",
                "Principal": {"AWS": obc_obj.obc_account},
                "Resource": [f'arn:aws:s3:::{obc_obj.bucket_name}/{"*"}'],
                "Sid": "Statement-2",
            },
            "statement_3": {
                "Action": "s3:DeleteObject",
                "Effect": "Deny",
                "Principal": {"AWS": [obc_obj.obc_account]},
                "Resource": [f'arn:aws:s3:::{"*"}'],
                "Sid": "Statement-3",
            },
        }

        for key, value in new_statements.items():
            single_statement_policy["Statement"].append(value)

        logger.info(f"New policy {single_statement_policy}")
        bucket_policy = json.dumps(single_statement_policy)

        # Creating Policy
        logger.info(
            f"Creating multi statement bucket policy on bucket: {obc_obj.bucket_name}"
        )
        assert put_bucket_policy(
            mcg_obj, obc_obj.bucket_name, bucket_policy
        ), "Failed: PutBucketPolicy "

        # Getting Policy
        logger.info(
            f"Getting multi statement bucket policy from bucket: {obc_obj.bucket_name}"
        )
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # NooBaa user writes an object to bucket
        logger.info(
            f"Writing object on bucket: {obc_obj.bucket_name} with User: {obc_obj.obc_account}"
        )
        assert s3_put_object(
            obc_obj, obc_obj.bucket_name, object_key, data
        ), "Failed: Put Object"

        # Verifying public read access
        logger.info(
            f"Reading object on bucket: {obc_obj.bucket_name} with User: {obc_obj.obc_account}"
        )
        assert s3_get_object(
            obc_obj, obc_obj.bucket_name, object_key
        ), "Failed: Get Object"

        # Verifying Delete object is denied on both Accounts
        logger.info("Verifying whether S3:DeleteObject action is denied access")
        try:
            s3_delete_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info(f"DeleteObject failed due to: {response.error['Message']}")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )

    @pytest.mark.parametrize(
        argnames="policy_name, policy_param",
        argvalues=[
            pytest.param(
                *["invalid_principal", "test-user"],
                marks=pytest.mark.polarion_id("OCS-2168"),
            ),
            pytest.param(
                *["invalid_action", "GetContent"],
                marks=pytest.mark.polarion_id("OCS-2166"),
            ),
            pytest.param(
                *["invalid_resource", "new_bucket"],
                marks=pytest.mark.polarion_id("OCS-2170"),
            ),
        ],
    )
    @tier3
    def test_bucket_policy_verify_invalid_scenarios(
        self, mcg_obj, bucket_factory, policy_name, policy_param
    ):
        """
        Test invalid bucket policy scenarios
        """
        # Creating a OBC (Account)
        obc = bucket_factory(amount=1, interface="OC")
        obc_obj = OBC(obc[0].name)

        # Policy tests invalid/non-existent principal. ie: test-user
        if policy_name == "invalid_principal":
            bucket_policy_generated = gen_bucket_policy(
                user_list=policy_param,
                actions_list=["GetObject"],
                resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
                effect="Allow",
            )
            bucket_policy = json.dumps(bucket_policy_generated)

        # Policy tests invalid/non-existent S3 Action. ie: GetContent
        elif policy_name == "invalid_action":
            bucket_policy_generated = gen_bucket_policy(
                user_list=obc_obj.obc_account,
                actions_list=[policy_param],
                resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
                effect="Allow",
            )
            bucket_policy = json.dumps(bucket_policy_generated)

        # Policy tests invalid/non-existent resource/bucket. ie: new_bucket
        elif policy_name == "invalid_resource":
            bucket_policy_generated = gen_bucket_policy(
                user_list=obc_obj.obc_account,
                actions_list=["GetObject"],
                resources_list=[policy_param],
                effect="Allow",
            )
            bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f"Verifying Malformed Policy: {policy_name}")
        try:
            put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "MalformedPolicy":
                logger.info(
                    f"PutBucketPolicy failed due to: {response.error['Message']}"
                )
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code {response.error['Code']}"
                )

    @pytest.mark.polarion_id("OCS-5767")
    @tier1
    def test_bucket_policy_elements_NotPrincipal(self, mcg_obj, bucket_factory):
        """
        Test bucket policy element of NotPrincipal and Effect: Deny
        """

        # Creating obc and obc object
        obc_bucket = bucket_factory(amount=1, interface="OC")
        obc_obj = OBC(obc_bucket[0].name)

        # Create data and object key
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        # Set bucket policy for obc_bucket
        bucket_policy_generated = gen_bucket_policy(
            principal_property="NotPrincipal",
            user_list=[obc_obj.obc_account],
            actions_list=["PutObject"],
            resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
            effect="Deny",
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Add Bucket Policy
        logger.info(f"Creating bucket policy on bucket: {obc_obj.bucket_name}")
        put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)

        # Get bucket policy
        logger.info(f"Getting Bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Verify put Object is allowed.
        logger.info(f"Put Object to the bucket: {obc_obj.bucket_name} ")
        assert s3_put_object(
            mcg_obj,
            obc_obj.bucket_name,
            object_key,
            data,
        ), f"Failed to put object to bucket {obc_obj.bucket_name}"

        # Delete policy and confirm policy got deleted.
        delete_bucket_policy_verify(obc_obj, obc_obj.bucket_name)

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[
            pytest.param(
                *["Allow"], marks=[tier1, pytest.mark.polarion_id("OCS-5768")]
            ),
            pytest.param(*["Deny"], marks=[tier1, pytest.mark.polarion_id("OCS-5769")]),
        ],
    )
    def test_bucket_policy_elements_NotAction(self, mcg_obj, bucket_factory, effect):
        """
        Test bucket policy element of NotAction with Effect: Allow/Deny
        """

        # Creating obc and obc object to get account details, keys etc
        obc_bucket = bucket_factory(amount=2, interface="OC")
        obc_obj = OBC(obc_bucket[0].name)
        obc_obj1 = OBC(obc_bucket[1].name)

        # Set bucket policy for user
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj1.obc_account,
            action_property="NotAction",
            actions_list=["DeleteBucket"],
            resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
            effect=effect,
        )
        if effect == "Allow":
            bucket_policy_generated["Statement"][0]["NotAction"][0] = "s3:ListBucket"
        bucket_policy = json.dumps(bucket_policy_generated)

        # Add Bucket Policy
        logger.info(f"Creating bucket policy on bucket: {obc_obj.bucket_name}")
        put_policy = put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)
        logger.info(f"Put bucket policy response from admin: {put_policy}")

        # Get bucket policy on the bucket
        logger.info(f"Getting Bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Verify DeleteBucket and putObject operation
        # in both scenarios: Effect=Allow/Deny
        if effect == "Allow":
            # Put Object is allowed
            logger.info("Writing index data to the bucket")
            assert s3_put_object(
                s3_obj=obc_obj1,
                bucketname=obc_obj.bucket_name,
                object_key="index.html",
                data=index,
                content_type="text/html",
            ), "Failed to put object."

            # List bucket get access denied.
            logger.info(f"Listing bucket objects {obc_obj.bucket_name}")
            try:
                s3_list_objects_v2(s3_obj=obc_obj1, bucketname=obc_obj.bucket_name)
                raise UnexpectedBehaviour(
                    "Failed: Object got listed, expect to get AccessDenied."
                )
            except boto3exception.ClientError as e:
                logger.info(e.response)
                response = HttpResponseParser(e.response)
                if response.error["Code"] == "AccessDenied":
                    logger.info(f"Bucket deleting got {response.error['Code']}")
                else:
                    raise UnexpectedBehaviour(
                        f"{e.response} received invalid error code "
                        f"{response.error['Code']}"
                    )
        if effect == "Deny":
            # Put Object get access denied.
            logger.info("Writing index data to the bucket")
            try:
                s3_put_object(
                    s3_obj=obc_obj1,
                    bucketname=obc_obj.bucket_name,
                    object_key="index.html",
                    data=index,
                    content_type="text/html",
                )
                raise UnexpectedBehaviour(
                    "Failed: Completed put object to bucket, expect to get AccessDenied."
                )
            except boto3exception.ClientError as e:
                logger.info(e.response)
                response = HttpResponseParser(e.response)
                if response.error["Code"] == "AccessDenied":
                    logger.info(f"PutObject got {response.error['Code']}")
                else:
                    raise UnexpectedBehaviour(
                        f"{e.response} received invalid error code "
                        f"{response.error['Code']}"
                    )

            # Delete bucket is allowed.
            logger.info(f"Deleting bucket {obc_obj.bucket_name}")
            assert s3_delete_bucket_website(
                s3_obj=obc_obj, bucketname=obc_obj.bucket_name
            ), "Failed to delete bucket."

        # Delete policy and confirm policy got deleted.
        delete_bucket_policy_verify(obc_obj, obc_obj.bucket_name)

    @pytest.mark.polarion_id("OCS-5770")
    @tier2
    def test_bucket_policy_elements_NotResource(self, mcg_obj, bucket_factory):
        """
        Test bucket policy element of NotResource with Effect: Deny
        """

        # Creating obc and obc object to get account details, keys etc
        obc_bucket = bucket_factory(amount=1, interface="OC")
        obc_obj = OBC(obc_bucket[0].name)

        # Create data and object key
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        # Set bucket policy for user
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["*"],
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}'],
            resource_property="NotResource",
            effect="Deny",
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Add Bucket Policy
        logger.info(f"Creating bucket policy on bucket: {obc_obj.bucket_name}")
        put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)

        # Get bucket policy
        logger.info(f"Getting Bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # Verify S3 action (putObject) is allowed.
        logger.info(
            f"Adding object on the bucket: {obc_obj.bucket_name} using user: {obc_obj.obc_account}"
        )
        assert s3_put_object(
            obc_obj, obc_obj.bucket_name, object_key, data
        ), "Failed to put Object"

        # Delete policy and confirm policy got deleted.
        delete_bucket_policy_verify(obc_obj, obc_obj.bucket_name)

    @pytest.mark.polarion_id("OCS-2451")
    @skipif_ocs_version("<4.6")
    @tier2
    def test_public_website(self, mcg_obj, bucket_factory):
        """
        Tests public bucket website access
        """
        # Creating a S3 bucket to host website
        s3_bucket = bucket_factory(amount=1, interface="OC")

        # Creating random S3 users
        users = []
        account1 = "noobaa-user1" + str(uuid.uuid4().hex)
        account2 = "noobaa-user2" + str(uuid.uuid4().hex)
        for account in account1, account2:
            if version.get_semantic_ocs_version_from_config() < version.VERSION_4_12:
                users.append(
                    NoobaaAccount(
                        mcg=mcg_obj,
                        name=account,
                        email=f"{account}@mail.com",
                        buckets=[s3_bucket[0].name],
                    )
                )
            else:
                users.append(
                    NoobaaAccount(
                        mcg=mcg_obj,
                        name=account,
                        email=f"{account}@mail.com",
                    )
                )
        logger.info(f"Adding bucket website config to: {s3_bucket[0].name}")
        assert s3_put_bucket_website(
            s3_obj=mcg_obj,
            bucketname=s3_bucket[0].name,
            website_config=website_config,
        ), "Failed: PutBucketWebsite"
        logger.info(f"Getting bucket website config from: {s3_bucket[0].name}")
        assert s3_get_bucket_website(
            s3_obj=mcg_obj, bucketname=s3_bucket[0].name
        ), "Failed: GetBucketWebsite"

        logger.info("Writing index and error data to the bucket")
        assert s3_put_object(
            s3_obj=mcg_obj,
            bucketname=s3_bucket[0].name,
            object_key="index.html",
            data=index,
            content_type="text/html",
        ), "Failed: PutObject"
        assert s3_put_object(
            s3_obj=mcg_obj,
            bucketname=s3_bucket[0].name,
            object_key="error.html",
            data=error,
            content_type="text/html",
        ), "Failed: PutObject"

        # Setting Get(read) policy action for all users(public)
        bucket_policy_generated = gen_bucket_policy(
            sid="PublicRead",
            user_list="*",
            actions_list=["GetObject"],
            resources_list=[f"{s3_bucket[0].name}/{'*'}"],
            effect="Allow",
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            f"Creating bucket policy on bucket: {s3_bucket[0].name} with public access"
        )
        assert put_bucket_policy(
            mcg_obj, s3_bucket[0].name, bucket_policy
        ), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f"Getting bucket policy for bucket: {s3_bucket[0].name}")
        get_policy = get_bucket_policy(mcg_obj, s3_bucket[0].name)
        logger.info(f"Bucket policy: {get_policy['Policy']}")

        # Verifying GetObject by reading the index of the website by anonymous users
        for user in users:
            logger.info(
                f"Getting object using user: {user.email_id} on bucket: {s3_bucket[0].name} "
            )
            retry_s3_get_object = retry(boto3exception.ClientError, tries=4, delay=10)(
                s3_get_object
            )

            assert retry_s3_get_object(
                user, s3_bucket[0].name, "index.html"
            ), f"Failed: Get Object by user {user.email_id}"

    @tier2
    @pytest.mark.polarion_id("OCS-3920")
    @skipif_ocs_version("<4.10")
    def test_multipart_with_policy(self, mcg_obj, bucket_factory):
        """
        Test Multipart upload with bucket policy set on the bucket
        """
        bucket = bucket_factory(interface="OC")[0].name
        obc_obj = OBC(bucket)
        part_body = "Random data-" + str(uuid.uuid4().hex)
        object_key = "MpuObjKey"

        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=[
                "ListBucketMultipartUploads",
                "ListMultipartUploadParts",
                "PutObject",
            ],
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}'],
            effect="Allow",
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Creates and gets policy
        logger.info(f"Creating policy on bucket: {obc_obj.bucket_name}")
        put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)

        logger.info(f"Getting Bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        logger.info(f"Initiating MP Upload on Bucket: {bucket} with Key {object_key}")
        upload_id = create_multipart_upload(obc_obj, bucket, object_key)
        logger.info(
            f"Listing the MP Upload : {list_multipart_upload(obc_obj, bucket)['Uploads']}"
        )

        # Uploading individual part with no body to the Bucket
        logger.info(f"Uploading to the bucket: {bucket}")
        part_etag = obc_obj.s3_client.upload_part(
            Bucket=bucket,
            Key=object_key,
            Body=part_body,
            UploadId=upload_id,
            PartNumber=1,
        )["ETag"]

        # Listing the Uploaded part
        logger.info(
            f"Listing the individual part: {list_uploaded_parts(obc_obj, bucket, object_key, upload_id)['Parts']}"
        )
        uploaded_part = [{"ETag": part_etag, "PartNumber": 1}]

        # Completing the Multipart Upload
        logger.info(f"Completing the MP Upload with on bucket: {bucket}")
        complete_multipart_upload(obc_obj, bucket, object_key, upload_id, uploaded_part)

    @tier1
    @pytest.mark.polarion_id("OCS-5183")
    def test_supported_bucket_policy_operations(self, mcg_obj, bucket_factory):
        """
        Test supported s3 bucket policies.
        """
        # Creating obc and obc object to get account details, keys etc
        obc_name = bucket_factory(amount=1, interface="OC")[0].name
        obc_obj = OBC(obc_name)

        actions_list = [
            "GetBucketObjectLockConfiguration",
            "GetObjectRetention",
            "GetObjectLegalHold",
            "PutBucketObjectLockConfiguration",
            "PutObjectRetention",
            "GetObjectLegalHold",
        ]
        bucket_policy_generated = gen_bucket_policy(
            user_list="*",
            actions_list=actions_list,
            resources_list=[obc_obj.bucket_name],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Add Bucket Policy
        logger.info(f"Creating bucket policy on bucket: {obc_obj.bucket_name}")
        put_policy = put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)

        assert put_policy is not None, "Put policy response is None"
        response = HttpResponseParser(put_policy)
        assert (
            response.status_code == 200
        ), f"Invalid Status code: {response.status_code}"
        logger.info("Bucket policy has been created successfully")

        # Get bucket policy
        logger.info(f"Getting Bucket policy on bucket: {obc_obj.bucket_name}")
        get_policy = get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        bucket_policy = get_policy["Policy"]
        logger.info(f"Got bucket policy: {bucket_policy}")
        bucket_policy = json.loads(bucket_policy)

        # Find the missing bucket policies
        bucket_policies = bucket_policy["Statement"][0]["Action"]
        bucket_policies = [
            action.split("s3:", 1)[1]
            for action in bucket_policies
            if action.startswith("s3:")
        ]
        actions_list = [action for action in actions_list]
        missing_policies = [
            action for action in actions_list if action not in bucket_policies
        ]
        assert (
            not missing_policies
        ), f"Some bucket_policies are not created : {missing_policies}"

    @pytest.mark.parametrize(
        argnames=["test_bucket_name", "bucketclass_dict"],
        argvalues=[
            pytest.param(*["first.bucket", {}]),
            pytest.param(
                *["", {"interface": "OC", "backingstore_dict": {"rgw": [(1, None)]}}]
            ),
            pytest.param(
                *[
                    "",
                    {
                        "interface": "OC",
                        "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                ]
            ),
            pytest.param(
                *["", {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}}]
            ),
            pytest.param(
                *["", {"interface": "OC", "backingstore_dict": {"gcp": [(1, None)]}}]
            ),
            pytest.param(
                *["", {"interface": "OC", "backingstore_dict": {"ibmcos": [(1, None)]}}]
            ),
        ],
        ids=[
            "FIRST-BUCKET",
            "RGW-OC",
            "AWS-OC",
            "AZURE-OC",
            "GCP-OC",
            "IBMCOS-OC",
        ],
    )
    @tier2
    def test_public_access_block_anonymous(
        self,
        mcg_obj,
        bucket_factory,
        awscli_pod_session,
        test_bucket_name,
        bucketclass_dict,
    ):
        """
        This test verifies that anonymous user cannot access the bucket after public access block settings were applied
        Scenario:
        1. Create a bucket (or use predefined bucket) and write file
        2. Verify that anonymous user cannot list the bucket content and cannot copy to the bucket
        3. Put "allow all" policy to the bucket
        4. Verify that anonymous user can list the bucket content but still cannot copy to the bucket
        5. Put Public Access Block with Block/Restrict=True to the bucket
        6. Verify that anonymous user cannot list the bucket content again and still cannot copy to the bucket
        7. Put Public Access Block with Block/Restrict=False to the bucket
        8. Verify that anonymous user can list the bucket content again.
        Args:
            mcg_obj (obj): An object representing the current state of the MCG in the cluster
            awscli_pod_session (pod): A pod running the AWSCLI tools
            bucket_factory: Calling this fixture creates a new bucket(s)
            test_bucket_name(str) If is not empty, use the bucket by this name in the test. If empty -- create bucket
            bucketclass_dict(dict) Parameters for bucket creation, used only if test_bucket_name is empty

        """

        bucket_name = (
            bucket_factory(1, bucketclass=bucketclass_dict)[0].name
            if not test_bucket_name
            else test_bucket_name
        )

        # Copy a file to the bucket
        standard_test_obj_list = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        file_name = standard_test_obj_list[0]
        logger.info(f"Going to copy file {file_name} to the bucket {bucket_name}")
        deep_dir_name = "dir1/dir2/dir3/"

        check_commands(mcg_obj, awscli_pod_session, bucket_name, "", file_name, False)
        check_commands(
            mcg_obj, awscli_pod_session, bucket_name, deep_dir_name, file_name, False
        )

        put_allow_bucket_policy(bucket_name, mcg_obj)

        check_commands(mcg_obj, awscli_pod_session, bucket_name, "", file_name, True)
        check_commands(
            mcg_obj, awscli_pod_session, bucket_name, deep_dir_name, file_name, True
        )

        # Put allow public access block configuration
        public_access_block_configuration = {
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }
        put_public_access_block_configuration(
            bucket_name, mcg_obj, public_access_block_configuration
        )

        check_commands(mcg_obj, awscli_pod_session, bucket_name, "", file_name, False)
        check_commands(
            mcg_obj, awscli_pod_session, bucket_name, deep_dir_name, file_name, False
        )

        # Put deny public access block configuration
        public_access_block_configuration = {
            "BlockPublicPolicy": False,
            "RestrictPublicBuckets": False,
        }
        put_public_access_block_configuration(
            bucket_name, mcg_obj, public_access_block_configuration
        )

        check_commands(mcg_obj, awscli_pod_session, bucket_name, "", file_name, True)
        check_commands(
            mcg_obj, awscli_pod_session, bucket_name, deep_dir_name, file_name, True
        )

    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                *[{"interface": "OC", "backingstore_dict": {"rgw": [(1, None)]}}]
            ),
        ],
        ids=[
            "RGW-OC",
        ],
    )
    @tier2
    def test_public_access_block_noobaa_admin(
        self,
        mcg_obj,
        bucket_factory,
        awscli_pod_session,
        bucketclass_dict,
    ):
        """
        This test verifies that it is possible to access the bucket after putting noobaa admin credentials
        Scenario:
        1. Create a bucket and write file
        2. Verify that there is the access to the bucket
        3. Put "allow all" policy to the bucket and verify that the access still exist
        5. Put Public Access Block with Block/Restrict=True to the bucket and verify that the access still exist
        6. Access the bucket with noobaa admin credentials and verify that it's accessible despite Block configuration
        Args:
            mcg_obj (obj): An object representing the current state of the MCG in the cluster
            awscli_pod_session (pod): A pod running the AWSCLI tools
            bucket_factory: Calling this fixture creates a new bucket(s)
            bucketclass_dict(dict) Parameters for bucket creation, used only if test_bucket_name is empty
        """

        bucket_name = bucket_factory(1, bucketclass=bucketclass_dict)[0].name

        # Copy a file to the bucket
        standard_test_obj_list = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        file_name = standard_test_obj_list[0]
        logger.info(f"Going to copy file {file_name} to the bucket {bucket_name}")

        check_ls_command(mcg_obj, awscli_pod_session, bucket_name, file_name)

        put_allow_bucket_policy(bucket_name, mcg_obj)

        check_ls_command(mcg_obj, awscli_pod_session, bucket_name, file_name)

        # Put public access block configuration
        public_access_block_configuration = {
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }
        put_public_access_block_configuration(
            bucket_name, mcg_obj, public_access_block_configuration
        )

        check_ls_command(mcg_obj, awscli_pod_session, bucket_name, file_name)

        # Update MCG object to use noobaa admin credentials
        mcg_obj.update_s3_creds()

        check_ls_command(mcg_obj, awscli_pod_session, bucket_name, file_name)


@mcg
@red_squad
@polarion_id("OCS-6540")
class TestNoobaaUpgradeWithBucketPolicy:
    """
    Test noobaa status post upgrade when there is bucket
    with some bucket policy.

    Bug: https://bugzilla.redhat.com/show_bug.cgi?id=2302507

    """

    @pre_upgrade
    def test_create_bucket_policy_before_upgrade(
        self,
        request,
        bucket_factory_session,
        mcg_obj_session,
    ):
        """
        Create bucket with some bucket policy before the upgrade

        """
        # Create a bucket and create obc object
        obc = bucket_factory_session(amount=1, interface="CLI")[0]
        obc_obj = OBC(obc.name)

        # Generate bucket policy
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=["PutBucketPolicy", "GetBucketPolicy", "DeleteBucketPolicy"],
            resources_list=[obc_obj.bucket_name],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            "Caching the bucket and bucket policy info for post upgrade verification"
        )

        request.config.cache.set("bucket_policy_bucket", obc.name)
        request.config.cache.set("bucket_policy", bucket_policy)

    @post_upgrade
    def test_verify_noobaa_after_upgrade(self, request, mcg_obj_session):
        """
        Verify the noobaa health and verify the bucket policy post upgrade

        """
        logger.info("Extracting the bucket and bucket policy info from the cache")
        obc_name = request.config.cache.get("bucket_policy_bucket", None)
        bucket_policy = request.config.cache.get("bucket_policy", None)

        assert (
            obc_name and bucket_policy
        ), "Seem like either pre-upgrade test for this failed or unable to cache the bucket/bucket policy info"

        # Check noobaa health
        logger.info("Verifying noobaa health")
        CephCluster().noobaa_health_check()

        # Check backing-store health
        verify_backing_store(constants.DEFAULT_NOOBAA_BACKINGSTORE)

        logger.info(f"Creating policy by admin on bucket: {obc_name}")
        put_policy = put_bucket_policy(mcg_obj_session, obc_name, bucket_policy)
        logger.info(f"Put bucket policy response from admin: {put_policy}")
