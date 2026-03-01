import pytest
import logging

from botocore.exceptions import ClientError

from ocs_ci.ocs.utils import retry
from uuid import uuid4

from ocs_ci.ocs.bucket_utils import (
    sts_assume_role,
    s3_create_bucket,
    s3_delete_bucket,
    s3_list_buckets,
    create_s3client_from_assume_role_creds,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.retry import retry_until_exception
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import mcg, red_squad, tier2

logger = logging.getLogger(__name__)


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
@tier2
class TestSTSClient:
    def test_sts_assume_role(
        self,
        mcg_obj_session,
        awscli_pod_session,
        mcg_account_factory,
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

        # create a bucket using noobaa admin creds
        bucket_1 = "first-bucket"
        try:
            retry(ClientError, tries=5, delay=5)(new_bucket)(bucket_name=bucket_1)
        except ClientError as e:
            if "BucketAlreadyExists" in str(e):
                logger.info(f"Bucket {bucket_1} already exists")
            else:
                raise
        logger.info(f"Created bucket {bucket_1}")

        # create a noobaa account
        user_1 = f"user-{uuid4().hex}"
        nb_account_1 = mcg_account_factory(
            name=user_1,
        )
        signed_request_creds = {
            "region": mcg_obj_session.region,
            "endpoint": mcg_obj_session.sts_internal_endpoint,
            "access_key_id": nb_account_1.get("access_key_id"),
            "access_key": nb_account_1.get("access_key"),
            "ssl": False,
        }

        # create another noobaa account
        user_2 = f"user-{uuid4().hex}"
        nb_account_2 = mcg_account_factory(
            name=user_2,
            allow_bucket_create=False,
        )
        nb_user_access_key_id = nb_account_2.get("access_key_id")
        logger.info(f"Created new user '{user_2}'")

        # assign assume role policy to the user-1
        role_name = "user-1-assume-role"
        nb_assign_user_role_fixture(user_2, role_name, principal=user_1)
        logger.info(f"Assigned the assume role policy to the user {user_2}")

        # noobaa admin assumes the above role
        creds_generated = sts_assume_role(
            awscli_pod_session,
            role_name,
            nb_user_access_key_id,
            signed_request_creds=signed_request_creds,
        )
        assumed_user_s3client = create_s3client_from_assume_role_creds(
            mcg_obj_session, creds_generated
        )

        # perform io to validate the role assumption
        bucket_2 = "second-bucket"
        try:
            new_bucket(bucket_2, assumed_user_s3client)
            assert (
                False
            ), "Bucket was created even though assumed role user doesnt have ability to create new bucket"
        except Exception as err:
            if "AccessDenied" not in err.args[0]:
                raise
            logger.info("Bucket creation failed as expected")
        logger.info(
            f"Listing buckets with the assumed user: {s3_list_buckets(mcg_obj_session, assumed_user_s3client)}"
        )

        # remove the role from the user
        mcg_obj_session.remove_sts_role(user_2)
        logger.info(f"Removed the assume role policy from the user {user_2}")

        # try to assume role after the assume role policy is removed
        assert retry_until_exception(
            exception_to_check=CommandFailed,
            tries=15,
            delay=60,
            backoff=1,
            text_in_exception="AccessDenied",
        )(sts_assume_role)(
            awscli_pod_session,
            role_name,
            nb_user_access_key_id,
            signed_request_creds=signed_request_creds,
        ), "AssumeRole operation expected to fail but it seems to be succeeding even after several tries"

        # perform io to validate the old token is no longer valid
        assert retry_until_exception(
            exception_to_check=ClientError,
            tries=20,
            delay=60,
            backoff=1,
            text_in_exception="ExpiredToken",
        )(s3_list_buckets)(
            mcg_obj_session,
            assumed_user_s3client,
        ), "Token doesn't seem to have expired."
        logger.info("Token is expired as expected.")
