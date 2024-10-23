import time

import pytest
import logging

from botocore.exceptions import ClientError

from ocs_ci.ocs.resources.pod import wait_for_noobaa_pods_running

from uuid import uuid4
from ocs_ci.ocs.resources.bucket_policy import NoobaaAccount
from ocs_ci.ocs.bucket_utils import (
    sts_assume_role,
    s3_create_bucket,
    s3_delete_bucket,
    s3_list_buckets,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.retry import retry_until_exception
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import mcg, red_squad

logger = logging.getLogger(__name__)


@pytest.fixture()
def nb_assign_user_role_fixture(request, mcg_obj_session):

    email = None

    def factory(user_email, role_name, principal="*"):
        """
        Assign assume role policy to the user

        Args:
            user_email (str): Name/id/email of the user
            role_name (str): Name of the role

        """
        nonlocal email
        email = user_email
        noobaa_assume_role_policy = (
            f'{{"role_name": "{role_name}","assume_role_policy": '
            f'{{"version": "2024-07-16","statement": [{{"action": ["sts:AssumeRole"],'
            f'"effect": "allow","principal": ["{principal}"]}}]}}}}'
        )

        mcg_obj_session.assign_sts_role(user_email, noobaa_assume_role_policy)

    def teardown():
        """
        Remove role from the user

        """
        try:
            mcg_obj_session.remove_sts_role(email)
        except CommandFailed as e:
            if "No such account email" not in e.args[0]:
                raise

    request.addfinalizer(teardown)
    return factory


@pytest.fixture()
def nb_account_factory(request):
    """
    Fixture to create noobaa account

    """
    nb_account = []

    def factory(mcg, user_name, user_email, **kwargs):
        """
        Factory function to create the noobaa account

        Args:
            mcg (MCG): MCG object
            user_name (str): User name
            user_email (str): Email of the user

        """
        account = NoobaaAccount(
            mcg=mcg, name=f"{user_name}", email=f"{user_email}", **kwargs
        )
        nb_account.append(account)

        return account

    def teardown():
        """
        Teardown function that deletes the Noobaa account

        """
        for account in nb_account:
            account.delete_account()

    request.addfinalizer(teardown)
    return factory


@pytest.fixture()
def new_bucket(request, mcg_obj_session):
    """
    Create new bucket using s3

    """
    buckets_created = []

    def factory(bucket_name, s3_client=None):
        s3_create_bucket(mcg_obj_session, bucket_name, s3_client)
        logger.info(f"Created new-bucket {bucket_name}")
        bucket_obj = {
            "s3client": s3_client,
            "bucket": bucket_name,
            "mcg": mcg_obj_session,
        }
        buckets_created.append(bucket_obj)

    def finalizer():
        """
        Cleanup the created bucket

        """
        for bucket in buckets_created:
            logger.info(f"Deleting bucket {bucket.get('bucket')}")
            s3_delete_bucket(
                bucket.get("mcg"), bucket.get("bucket"), bucket.get("s3client")
            )
            logger.info(f"Deleted the bucket {bucket}")

    request.addfinalizer(finalizer)
    return factory


@mcg
@red_squad
class TestSTSClient:
    def test_sts_assume_role(
        self,
        mcg_obj_session,
        awscli_pod_session,
        nb_account_factory,
        nb_assign_user_role_fixture,
        new_bucket,
        add_env_vars_to_noobaa_core_class,
        add_env_vars_to_noobaa_endpoint_class,
    ):
        """
        Test sts support for Noobaa clients.
        As part of this, we test the following:
            * Assign role to an user
            * Assume the role of an user
            * Perform IO by assuming of the role of another user
            * Remove the role from the user
            * Try to assume the role

        """
        # change sts token expire time to 10 minute
        add_env_vars_to_noobaa_core_class(
            [(constants.STS_DEFAULT_SESSION_TOKEN_EXPIRY_MS, 600000)]
        )
        logger.info("Changed the sts token expiration time to 10 minutes")

        add_env_vars_to_noobaa_endpoint_class(
            [(constants.STS_DEFAULT_SESSION_TOKEN_EXPIRY_MS, 600000)]
        )
        logger.info("Changed the sts token expiration time to 10 minutes")

        # make sure all noobaa pods are running after the patch
        wait_for_noobaa_pods_running()

        # create a bucket using noobaa admin creds
        time.sleep(300)
        bucket_1 = "first-bucket"
        new_bucket(bucket_1)
        logger.info(f"Created bucket {bucket_1}")

        # create a noobaa account
        user_1 = f"user-{uuid4().hex}"
        user_email_1 = f"{user_1}@email"
        nb_account_1 = nb_account_factory(
            mcg=mcg_obj_session,
            user_name=f"{user_1}",
            user_email=f"{user_email_1}",
        )
        signed_request_creds = {
            "region": mcg_obj_session.region,
            "endpoint": mcg_obj_session.sts_internal_endpoint,
            "access_key_id": nb_account_1.access_key_id,
            "access_key": nb_account_1.access_key,
            "ssl": False,
        }
        # new_bucket("test-bucket", nb_account_1.s3_client)
        # logger.info("Created new bucket test-bucket")

        # create another noobaa account
        user_2 = f"user-{uuid4().hex}"
        user_email_2 = f"{user_2}@email"
        nb_account_2 = nb_account_factory(
            mcg=mcg_obj_session,
            user_name=f"{user_2}",
            user_email=f"{user_email_2}",
            allow_bucket_creation=False,
        )
        nb_user_access_key_id = nb_account_2.access_key_id
        logger.info(f"Created new user '{user_2}'")

        # assign assume role policy to the user-1
        role_name = "user-1-assume-role"
        nb_assign_user_role_fixture(user_email_2, role_name, principal=user_email_1)
        logger.info(f"Assigned the assume role policy to the user {user_2}")

        # noobaa admin assumes the above role
        creds_generated = sts_assume_role(
            awscli_pod_session,
            role_name,
            nb_user_access_key_id,
            signed_request_creds=signed_request_creds,
        )
        mcg_obj_session.create_s3client_assumed_role(creds_generated)
        logger.info(creds_generated)

        # perform io to validate the role assumption
        bucket_2 = "second-bucket"
        try:
            new_bucket(bucket_2, mcg_obj_session.assumed_s3_client)
            assert (
                False
            ), "Bucket was created even though assumed role user doesnt have ability to create new bucket"
        except Exception as err:
            if "AccessDenied" not in err.args[0]:
                raise
            logger.info("Bucket creation failed as expected")
        logger.info(s3_list_buckets(mcg_obj_session, mcg_obj_session.assumed_s3_client))

        # remove the role from the user
        mcg_obj_session.remove_sts_role(user_email_2)
        logger.info(f"Removed the assume role policy from the user {user_2}")

        # try to assume role after the assume role policy is removed
        assert retry_until_exception(
            exception_to_check=CommandFailed,
            tries=15,
            delay=60,
            backoff=1,
            text_in_exception="An error occurred (Unknown) when calling the AssumeRole operation: Unknown",
        )(sts_assume_role)(
            awscli_pod_session,
            role_name,
            nb_user_access_key_id,
            signed_request_creds=signed_request_creds,
        ), "AssumeRole operation expected to fail but it seems to be succeeding even after several tries"

        # perform io to validate the role assumption is no longer valid
        assert retry_until_exception(
            exception_to_check=ClientError,
            tries=20,
            delay=60,
            backoff=1,
            text_in_exception="ExpiredToken",
        )(s3_list_buckets)(
            mcg_obj_session,
            mcg_obj_session.assumed_s3_client,
        ), "Token doesn't seem to have expired."
        logger.info("Token is expired as expected.")
