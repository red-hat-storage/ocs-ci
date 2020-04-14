import logging

import pytest
import botocore.exceptions as boto3exception
import json
import uuid

from ocs_ci.framework.pytest_customization.marks import filter_insecure_request_warning
from ocs_ci.ocs.exceptions import NoBucketPolicyResponse, InvalidStatusCode, UnexpectedBehaviour
from ocs_ci.framework.testlib import ManageTest, tier1, tier3, skipif_ocs_version
from ocs_ci.ocs.resources.bucket_policy import OBC, NoobaaAccount, HttpResponseParser, gen_bucket_policy
from tests.manage.mcg import helpers

logger = logging.getLogger(__name__)


@skipif_ocs_version('<4.3')
@filter_insecure_request_warning
class TestS3BucketPolicy(ManageTest):
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
        obc_name = bucket_factory(amount=1, interface='OC')[0].name
        obc_obj = OBC(mcg_obj, obc=obc_name)

        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['GetObject'],
            resources_list=[obc_obj.bucket_name]
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Add Bucket Policy
        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name}')
        put_policy = helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)

        if put_policy is not None:
            response = HttpResponseParser(put_policy)
            if response.status_code == 200:
                logger.info('Bucket policy has been created successfully')
            else:
                raise InvalidStatusCode(f"Invalid Status code: {response.status_code}")
        else:
            raise NoBucketPolicyResponse("Put policy response is none")

        # Get bucket policy
        logger.info(f'Getting Bucket policy on bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f'Got bucket policy: %s' % get_policy['Policy'])

        # Modifying bucket policy to take new policy
        logger.info('Modifying bucket policy')
        actions_list = ['ListBucket', 'CreateBucket']
        actions = list(map(lambda action: "s3:%s" % action, actions_list))

        modified_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=actions_list,
            resources_list=[obc_obj.bucket_name]
        )
        bucket_policy_modified = json.dumps(modified_policy_generated)

        put_modified_policy = helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy_modified)

        if put_modified_policy is not None:
            response = HttpResponseParser(put_modified_policy)
            if response.status_code == 200:
                logger.info('Bucket policy has been modified successfully')
            else:
                raise InvalidStatusCode(f"Invalid Status code: {response.status_code}")
        else:
            raise NoBucketPolicyResponse("Put modified policy response is none")

        # Get Modified Policy
        get_modified_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        modified_policy = json.loads(get_modified_policy['Policy'])
        logger.info(f'Got modified bucket policy: {modified_policy}')

        actions_from_modified_policy = modified_policy['statement'][0]['action']
        modified_actions = list(map(str, actions_from_modified_policy))
        initial_actions = list(map(str.lower, actions))
        logger.info(f'Actions from modified_policy: {modified_actions}')
        logger.info(f'User provided actions actions: {initial_actions}')
        if modified_actions == initial_actions:
            logger.info("Modified actions and initial actions are same")
        else:
            raise UnexpectedBehaviour('Modification Failed: Action lists are not identical')

        # Delete Policy
        logger.info(f'Delete bucket policy by admin on bucket: {obc_obj.bucket_name}')
        delete_policy = helpers.delete_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info(f'Delete policy response: {delete_policy}')

        if delete_policy is not None:
            response = HttpResponseParser(delete_policy)
            if response.status_code == 204:
                logger.info('Bucket policy is deleted successfully')
            else:
                raise InvalidStatusCode(f"Invalid Status code: {response.status_code}")
        else:
            raise NoBucketPolicyResponse("Delete policy response is none")

        # Confirming again by calling get_bucket_policy
        try:
            helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'NoSuchBucketPolicy':
                logger.info('Bucket policy has been deleted successfully')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

    @pytest.mark.polarion_id("OCS-2146")
    @tier1
    def test_bucket_policy_actions(self, mcg_obj, bucket_factory):
        """
        Tests user access to Put, Get, Delete bucket policy actions
        """
        # Creating obc and obc object to get account details, keys etc
        obc_name = bucket_factory(amount=1, interface='OC')[0].name
        obc_obj = OBC(mcg_obj, obc=obc_name)

        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['PutBucketPolicy'],
            resources_list=[obc_obj.bucket_name]
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Admin creates a policy on the user bucket, for Action: PutBucketPolicy
        logger.info(f'Creating policy by admin on bucket: {obc_obj.bucket_name}')
        put_policy = helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)
        logger.info(f'Put bucket policy response from admin: {put_policy}')

        # Verifying Put bucket policy by user by changing the actions to GetBucketPolicy & DeleteBucketPolicy
        user_generated_policy = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['GetBucketPolicy', 'DeleteBucketPolicy'],
            resources_list=[obc_obj.bucket_name]
        )
        bucket_policy1 = json.dumps(user_generated_policy)

        logger.info(f'Changing bucket policy by User on bucket: {obc_obj.bucket_name}')
        put_policy_user = helpers.put_bucket_policy(obc_obj, obc_obj.bucket_name, bucket_policy1)
        logger.info(f'Put bucket policy response from user: {put_policy_user}')

        # Verifying whether user can get the bucket policy after modification
        get_policy = helpers.get_bucket_policy(obc_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s\n' % get_policy['Policy'])

        # Verifying whether user is not allowed Put the bucket policy after modification
        logger.info(f'Verifying whether user: {obc_obj.obc_account} is denied to put objects')
        try:
            helpers.put_bucket_policy(obc_obj, obc_obj.bucket_name, bucket_policy1)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'AccessDenied':
                logger.info(f'Put bucket policy has been denied access to the user: {obc_obj.obc_account}')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

        # Verifying whether user can Delete the bucket policy after modification
        logger.info(f'Deleting bucket policy on bucket: {obc_obj.bucket_name}')
        delete_policy = helpers.delete_bucket_policy(obc_obj, obc_obj.bucket_name)
        logger.info(f'Delete policy response: {delete_policy}')

    @pytest.mark.polarion_id("OCS-2156")
    @tier1
    def test_object_actions(self, mcg_obj, bucket_factory):
        """
        Test to verify different object actions and cross account access to buckets
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        # Creating multiple obc users (accounts)
        obc = bucket_factory(amount=1, interface='OC')
        obc_obj = OBC(mcg_obj, obc=obc[0].name)

        # Admin sets policy on obc bucket with obc account principal
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['PutObject'],
            resources_list=[f'{obc_obj.bucket_name}/{"*"}']
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}')
        put_policy = helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)
        logger.info(f'Put bucket policy response from Admin: {put_policy}')

        # Get Policy
        logger.info(f'Getting Bucket policy on bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        # Verifying whether obc account can put object
        logger.info(f'Adding object on bucket: {obc_obj.bucket_name}')
        assert helpers.s3_put_object(obc_obj, obc_obj.bucket_name, object_key, data), "Failed: Put Object"

        # Verifying whether Get action is not allowed
        logger.info(f'Verifying whether user: {obc_obj.obc_account} is denied to Get object')
        try:
            helpers.s3_get_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'AccessDenied':
                logger.info('Get Object action has been denied access')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

        # Verifying whether obc account allowed to create multipart
        logger.info(f'Creating multipart on bucket: {obc_obj.bucket_name} with key: {object_key}')
        helpers.create_multipart_upload(obc_obj, obc_obj.bucket_name, object_key)

        # Verifying whether obc account is denied access to delete object
        logger.info(f'Verifying whether user: {obc_obj.obc_account} is denied to Delete object')
        try:
            helpers.s3_delete_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'AccessDenied':
                logger.info('Delete action has been denied access')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

        # Creating noobaa account to access bucket belonging to obc account
        user_name = "noobaa-user" + str(uuid.uuid4().hex)
        email = user_name + "@mail.com"
        user = NoobaaAccount(mcg_obj, name=user_name, email=email, buckets=[obc_obj.bucket_name])

        # Admin sets a policy on obc-account bucket with noobaa-account principal (cross account access)
        new_policy_generated = gen_bucket_policy(
            user_list=user.email_id,
            actions_list=['GetObject', 'DeleteObject'],
            resources_list=[f'{obc_obj.bucket_name}/{"*"}']
        )
        new_policy = json.dumps(new_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}')
        put_policy = helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, new_policy)
        logger.info(f'Put bucket policy response from admin: {put_policy}')

        # Get Policy
        logger.info(f'Getting bucket policy on bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        # Verifying whether Get, Delete object is allowed
        logger.info(f'Getting object on bucket: {obc_obj.bucket_name} with user: {user.email_id}')
        assert helpers.s3_get_object(user, obc_obj.bucket_name, object_key), "Failed: Get Object"
        logger.info(f'Deleting object on bucket: {obc_obj.bucket_name} with user: {user.email_id}')
        assert helpers.s3_delete_object(user, obc_obj.bucket_name, object_key), "Failed: Delete Object"

        # Verifying whether Put object action is denied
        logger.info(f'Verifying whether user: {user.email_id} is denied to Put object after updating policy')
        try:
            helpers.s3_put_object(user, obc_obj.bucket_name, object_key, data)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'AccessDenied':
                logger.info('Put object action has been denied access')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

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
        s3_bucket = bucket_factory(amount=1, interface='S3')[0]

        # Creating a random user account
        user = NoobaaAccount(mcg_obj, name=user_name, email=email, buckets=[s3_bucket.name])

        # Admin sets policy all users '*' (Public access)
        bucket_policy_generated = gen_bucket_policy(
            user_list=["*"],
            actions_list=['GetObject'],
            resources_list=[f'{s3_bucket.name}/{"*"}']
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {s3_bucket.name} with wildcard (*) Principal')
        put_policy = helpers.put_bucket_policy(mcg_obj, s3_bucket.name, bucket_policy)
        logger.info(f'Put bucket policy response from Admin: {put_policy}')

        # Getting Policy
        logger.info(f'Getting bucket policy on bucket: {s3_bucket.name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, s3_bucket.name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        # Admin writes an object to bucket
        logger.info(f'Writing object on bucket: {s3_bucket.name} by admin')
        assert helpers.s3_put_object(mcg_obj, s3_bucket.name, object_key, data), "Failed: PutObject"

        # Reading the object by anonymous user
        logger.info(f'Getting object by user: {user.email_id} on bucket: {s3_bucket.name} ')
        assert helpers.s3_get_object(user, s3_bucket.name, object_key), f"Failed: Get Object by user {user.email_id}"

    @pytest.mark.polarion_id("OCS-2140")
    @tier1
    def test_bucket_website_and_policies(self, mcg_obj, bucket_factory):
        """
        Tests bucket website bucket policy actions
        """
        website_config = {
            'ErrorDocument': {'Key': 'error.html'},
            'IndexDocument': {'Suffix': 'index.html'},
        }
        index = "<html><body><h1>My Static Website on S3</h1></body></html>"
        error = "<html><body><h1>Oh. Something bad happened!</h1></body></html>"

        # Creating a OBC (account)
        obc = bucket_factory(amount=1, interface='OC')
        obc_obj = OBC(mcg_obj, obc=obc[0].name)

        # Admin sets policy with with Put/Get bucket website actions
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['PutBucketWebsite', 'GetBucketWebsite', 'PutObject'],
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}'],
            effect="Allow"
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}')
        assert helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f'Getting bucket policy for bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        logger.info(f"Adding bucket website config to: {obc_obj.bucket_name}")
        assert helpers.s3_put_bucket_website(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name,
            website_config=website_config
        ), "Failed: PutBucketWebsite"
        logger.info(f"Getting bucket website config from: {obc_obj.bucket_name}")
        assert helpers.s3_get_bucket_website(s3_obj=obc_obj, bucketname=obc_obj.bucket_name), "Failed: GetBucketWebsite"

        logger.info("Writing index and error data to the bucket")
        assert helpers.s3_put_object(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name,
            object_key="index.html", data=index, content_type='text/html'
        ), "Failed: PutObject"
        assert helpers.s3_put_object(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name,
            object_key="error.html", data=error, content_type='text/html'
        ), "Failed: PutObject"

        # Verifying whether DeleteBucketWebsite action is denied access
        logger.info(f'Verifying whether user: {obc_obj.obc_account} is denied to DeleteBucketWebsite')
        try:
            helpers.s3_delete_bucket_website(s3_obj=obc_obj, bucketname=obc_obj.bucket_name)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'AccessDenied':
                logger.info('GetObject action has been denied access')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

        # Admin modifies policy to allow DeleteBucketWebsite action
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['DeleteBucketWebsite'],
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}'],
            effect="Allow"
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}')
        assert helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f'Getting bucket policy for bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        logger.info(f"Deleting bucket website config from bucket: {obc_obj.bucket_name}")
        assert helpers.s3_delete_bucket_website(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name
        ), "Failed: DeleteBucketWebsite"

    @pytest.mark.polarion_id("OCS-2161")
    @tier1
    def test_bucket_versioning_and_policies(self, mcg_obj, bucket_factory):
        """
        Tests bucket and object versioning on Noobaa buckets and also its related actions
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        object_versions = []

        # Creating a OBC user (Account)
        obc = bucket_factory(amount=1, interface='OC')
        obc_obj = OBC(mcg_obj, obc=obc[0].name)

        # Admin sets a policy on OBC bucket to allow Versioning related actions
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['PutBucketVersioning', 'GetBucketVersioning'],
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}']
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        # Creating policy
        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name} by Admin')
        assert helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f'Getting bucket policy on bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        logger.info(f'Enabling bucket versioning on {obc_obj.bucket_name} using User: {obc_obj.obc_account}')
        assert helpers.s3_put_bucket_versioning(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name, status="Enabled"
        ), "Failed: PutBucketVersioning"

        logger.info(f'Verifying whether versioning is enabled on bucket: {obc_obj.bucket_name}')
        assert helpers.s3_get_bucket_versioning(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name
        ), "Failed: GetBucketVersioning"

        # Admin modifies the policy to all obc-account to write/read/delete versioned objects
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['PutObject', 'GetObjectVersion', 'DeleteObjectVersion'],
            resources_list=[obc_obj.bucket_name, f'{obc_obj.bucket_name}/{"*"}']
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name} by Admin')
        assert helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f'Getting bucket policy for bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        for key in range(5):
            logger.info(f"Writing {key} version of {object_key}")
            obj = helpers.s3_put_object(
                s3_obj=obc_obj, bucketname=obc_obj.bucket_name,
                object_key=object_key, data=data
            )
            object_versions.append(obj['VersionId'])

        for version in object_versions:
            logger.info(f"Reading version: {version} of {object_key}")
            assert helpers.s3_get_object(
                s3_obj=obc_obj, bucketname=obc_obj.bucket_name, object_key=object_key, versionid=version
            ), f"Failed: To Read object {version}"
            logger.info(f"Deleting version: {version} of {object_key}")
            assert helpers.s3_delete_object(
                s3_obj=obc_obj, bucketname=obc_obj.bucket_name, object_key=object_key, versionid=version
            ), f"Failed: To Delete object with {version}"

        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['PutBucketVersioning'],
            resources_list=[obc_obj.bucket_name]
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name} by Admin')
        assert helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f'Getting bucket policy on bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        logger.info(f"Suspending bucket versioning on {obc_obj.bucket_name} using User: {obc_obj.obc_account}")
        assert helpers.s3_put_bucket_versioning(
            s3_obj=obc_obj, bucketname=obc_obj.bucket_name, status="Suspended"
        ), "Failed: PutBucketVersioning"

        # Verifying whether GetBucketVersion action is denied access
        logger.info(f'Verifying whether user: {obc_obj.obc_account} is denied to GetBucketVersion')
        try:
            helpers.s3_get_bucket_versioning(s3_obj=obc_obj, bucketname=obc_obj.bucket_name)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'AccessDenied':
                logger.info('Get Object action has been denied access')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

    @pytest.mark.polarion_id("OCS-2159")
    @tier1
    def test_bucket_policy_effect_deny(self, mcg_obj, bucket_factory):
        """
        Tests explicit "Deny" effect on bucket policy actions
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        # Creating multiple obc user (account)
        obc = bucket_factory(amount=1, interface='OC')
        obc_obj = OBC(mcg_obj, obc=obc[0].name)

        # Admin writes an object to bucket
        logger.info(f'Writing an object on bucket: {obc_obj.bucket_name} by Admin')
        assert helpers.s3_put_object(mcg_obj, obc_obj.bucket_name, object_key, data), "Failed: PutObject"

        # Admin sets policy with Effect: Deny on obc bucket with obc-account principal
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['GetObject'],
            resources_list=[f'{obc_obj.bucket_name}/{object_key}'],
            effect="Deny"
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name} with principal: {obc_obj.obc_account}')
        assert helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f'Getting bucket policy from bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        # Verifying whether Get action is denied access
        logger.info(f'Verifying whether user: {obc_obj.obc_account} is denied to GetObject')
        try:
            helpers.s3_get_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'AccessDenied':
                logger.info('GetObject action has been denied access')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

        # Admin sets a new policy on same obc bucket with same account but with different action and resource
        bucket_policy_generated = gen_bucket_policy(
            user_list=obc_obj.obc_account,
            actions_list=['DeleteObject'],
            resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
            effect="Deny"
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f'Creating bucket policy on bucket: {obc_obj.bucket_name}')
        assert helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy), "Failed: PutBucketPolicy"

        # Getting Policy
        logger.info(f'Getting bucket policy from bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        # Verifying whether delete action is denied
        logger.info(f'Verifying whether user: {obc_obj.obc_account} is denied to Get object')
        try:
            helpers.s3_delete_object(obc_obj, obc_obj.bucket_name, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'AccessDenied':
                logger.info('Get Object action has been denied access')
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

    @pytest.mark.polarion_id("OCS-2149")
    @tier1
    def test_bucket_policy_multi_statement(self, mcg_obj, bucket_factory):
        """
        Tests multiple statements in a bucket policy
        """
        data = "Sample string content to write to a new S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        user_name = "noobaa-user" + str(uuid.uuid4().hex)
        email = user_name + "@mail.com"

        # Creating OBC (account) and Noobaa user account
        obc = bucket_factory(amount=1, interface='OC')
        obc_obj = OBC(mcg_obj, obc=obc[0].name)
        noobaa_user = NoobaaAccount(mcg_obj, name=user_name, email=email, buckets=[obc_obj.bucket_name])
        accounts = [obc_obj, noobaa_user]

        # Statement_1 public read access to a bucket
        single_statement_policy = gen_bucket_policy(
            sid="statement-1",
            user_list=["*"],
            actions_list=['GetObject'],
            resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
            effect="Allow"
        )

        # Additional Statements; Statement_2 - PutObject permission on specific user
        # Statement_3 - Denying Permission to DeleteObject action for aultiple Users
        new_statements = {
            "statement_2": {
                'Action': 's3:PutObject',
                'Effect': 'Allow',
                'Principal': noobaa_user.email_id,
                'Resource': [f'arn:aws:s3:::{obc_obj.bucket_name}/{"*"}'],
                'Sid': 'Statement-2'
            },
            "statement_3": {
                'Action': 's3:DeleteObject',
                'Effect': 'Deny',
                'Principal': [obc_obj.obc_account, noobaa_user.email_id],
                'Resource': [f'arn:aws:s3:::{"*"}'],
                'Sid': 'Statement-3'
            }
        }

        for key, value in new_statements.items():
            single_statement_policy["Statement"].append(value)

        logger.info(f"New policy {single_statement_policy}")
        bucket_policy = json.dumps(single_statement_policy)

        # Creating Policy
        logger.info(f'Creating multi statement bucket policy on bucket: {obc_obj.bucket_name}')
        assert helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy), "Failed: PutBucketPolicy "

        # Getting Policy
        logger.info(f'Getting multi statement bucket policy from bucket: {obc_obj.bucket_name}')
        get_policy = helpers.get_bucket_policy(mcg_obj, obc_obj.bucket_name)
        logger.info('Got bucket policy:%s' % get_policy['Policy'])

        # NooBaa user writes an object to bucket
        logger.info(f'Writing object on bucket: {obc_obj.bucket_name} with User: {noobaa_user.email_id}')
        assert helpers.s3_put_object(noobaa_user, obc_obj.bucket_name, object_key, data), "Failed: Put Object"

        # Verifying public read access
        logger.info(f'Reading object on bucket: {obc_obj.bucket_name} with User: {obc_obj.obc_account}')
        assert helpers.s3_get_object(obc_obj, obc_obj.bucket_name, object_key), "Failed: Get Object"

        # Verifying Delete object is denied on both Accounts
        for user in accounts:
            logger.info(f"Verifying whether S3:DeleteObject action is denied access for {user}")
            try:
                helpers.s3_delete_object(user, obc_obj.bucket_name, object_key)
            except boto3exception.ClientError as e:
                logger.info(e.response)
                response = HttpResponseParser(e.response)
                if response.error['Code'] == 'AccessDenied':
                    logger.info(f"DeleteObject failed due to: {response.error['Message']}")
                else:
                    raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")

    @pytest.mark.parametrize(
        argnames="policy_name, policy_param",
        argvalues=[
            pytest.param(
                *["invalid_principal", "test-user"], marks=pytest.mark.polarion_id("OCS-2168")
            ),
            pytest.param(
                *["invalid_action", "GetContent"], marks=pytest.mark.polarion_id("OCS-2166")
            ),
            pytest.param(
                *["invalid_resource", "new_bucket"], marks=pytest.mark.polarion_id("OCS-2170")
            )
        ]
    )
    @tier3
    def test_bucket_policy_verify_invalid_scenarios(self, mcg_obj, bucket_factory, policy_name, policy_param):
        """
        Test invalid bucket policy scenarios
        """
        # Creating a OBC (Account)
        obc = bucket_factory(amount=1, interface='OC')
        obc_obj = OBC(mcg_obj, obc=obc[0].name)

        # Policy tests invalid/non-existent principal. ie: test-user
        if policy_name == "invalid_principal":
            bucket_policy_generated = gen_bucket_policy(
                user_list=policy_param,
                actions_list=['GetObject'],
                resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
                effect="Allow"
            )
            bucket_policy = json.dumps(bucket_policy_generated)

        # Policy tests invalid/non-existent S3 Action. ie: GetContent
        elif policy_name == "invalid_action":
            bucket_policy_generated = gen_bucket_policy(
                user_list=obc_obj.obc_account,
                actions_list=[policy_param],
                resources_list=[f'{obc_obj.bucket_name}/{"*"}'],
                effect="Allow"
            )
            bucket_policy = json.dumps(bucket_policy_generated)

        # Policy tests invalid/non-existent resource/bucket. ie: new_bucket
        elif policy_name == "invalid_resource":
            bucket_policy_generated = gen_bucket_policy(
                user_list=obc_obj.obc_account,
                actions_list=['GetObject'],
                resources_list=[policy_param],
                effect="Allow"
            )
            bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(f"Verifying Malformed Policy: {policy_name}")
        try:
            helpers.put_bucket_policy(mcg_obj, obc_obj.bucket_name, bucket_policy)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error['Code'] == 'MalformedPolicy':
                logger.info(f"PutBucketPolicy failed due to: {response.error['Message']}")
            else:
                raise UnexpectedBehaviour(f"Response received invalid error code {response.error['Code']}")
