import logging
import time

import pytest
import botocore.exceptions as boto3exception
import json
import uuid

from ocs_ci.ocs.exceptions import (
    NoBucketPolicyResponse,
    InvalidStatusCode,
    UnexpectedBehaviour,
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
    list_multipart_upload,
    list_uploaded_parts,
    complete_multipart_upload,
)
from ocs_ci.ocs.defaults import website_config, index, error
from ocs_ci.ocs.constants import (
    bucket_website_action_list,
    bucket_version_action_list,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    bugzilla,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.utility import version
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


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
        logger.info("Waiting for 60 seconds for the policy to take effect")
        time.sleep(60)

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
    @tier1
    def test_anonymous_read_only(self, mcg_obj, bucket_factory):
        """
        Tests read only access by an anonymous user
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        user_name = "noobaa-user" + str(uuid.uuid4().hex)
        email = user_name + "@mail.com"

        # Creating a s3 bucket
        s3_bucket = bucket_factory(amount=1, interface="S3")[0]

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

    @pytest.mark.polarion_id("OCS-2451")
    @pytest.mark.bugzilla("1893163")
    @skipif_ocs_version("<4.6")
    @tier1
    def test_public_website(self, mcg_obj, bucket_factory):
        """
        Tests public bucket website access
        """
        # Creating a S3 bucket to host website
        s3_bucket = bucket_factory(amount=1, interface="S3")

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
    @bugzilla("2054540")
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
    @pytest.mark.bugzilla("2210289")
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
