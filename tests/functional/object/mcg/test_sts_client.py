import pytest
import logging

from uuid import uuid4
from ocs_ci.ocs.resources.bucket_policy import NoobaaAccount
from ocs_ci.ocs.bucket_utils import sts_assume_role, s3_create_bucket, s3_delete_bucket
from ocs_ci.ocs.exceptions import CommandFailed

logger = logging.getLogger(__name__)


@pytest.fixture()
def nb_assign_user_role_fixture(request, mcg_obj_session):

    email = None

    def factory(user_email, role_name):
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
            f'"effect": "allow","principal": ["*"]}}]}}}}'
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
    nb_account = None

    def factory(mcg, user_name, user_email, **kwargs):
        """
        Factory function to create the noobaa account

        Args:
            mcg (MCG): MCG object
            user_name (str): User name
            user_email (str): Email of the user

        """
        nonlocal nb_account
        nb_account = NoobaaAccount(
            mcg=mcg, name=f"{user_name}", email=f"{user_email}", **kwargs
        )

        return nb_account

    def teardown():
        """
        Teardown function that deletes the Noobaa account

        """
        nb_account.delete_account()

    request.addfinalizer(teardown)
    return factory


@pytest.fixture()
def new_bucket(request, mcg_obj_session):
    """
    Create new bucket using s3

    """
    buckets_created = []

    def factory(bucket_name, s3_client=None):
        bucket_obj = {
            "s3client": s3_client,
            "bucket": bucket_name,
            "mcg": mcg_obj_session,
        }
        buckets_created.append(bucket_obj)
        s3_create_bucket(mcg_obj_session, bucket_name, s3_client)
        logger.info(f"Created new-bucket {bucket_name}")

    def finalizer():
        """
        Cleanup the created bucket

        """
        for bucket in buckets_created:
            s3_delete_bucket(
                bucket.get("mcg"), bucket.get("bucket"), bucket.get("s3client")
            )
            logger.info(f"Deleted the bucket {bucket}")

    request.addfinalizer(finalizer)
    return factory


class TestSTSClient:
    def test_sts_assume_role(
        self,
        mcg_obj_session,
        awscli_pod_session,
        nb_account_factory,
        nb_assign_user_role_fixture,
        new_bucket,
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
        # create a bucket using noobaa admin creds
        bucket_1 = "first-bucket"
        new_bucket(bucket_1)
        logger.info(f"Created bucket {bucket_1}")

        # create noobaa account
        user_name = f"user-{uuid4().hex}"
        user_email = f"user-{uuid4().hex}@email"
        nb_account = nb_account_factory(
            mcg=mcg_obj_session,
            user_name=f"{user_name}",
            user_email=f"{user_email}",
            allow_bucket_creation=False,
        )
        nb_user_access_key_id = nb_account.access_key_id
        logger.info(f"Created new user '{user_name}'")

        # assign assume role policy to the above created user
        role_name = "user-1-assume-role"
        nb_assign_user_role_fixture(user_email, role_name)
        logger.info(f"Assigned the assume role policy to the user {user_name}")

        # noobaa admin assumes the above role
        creds_generated = sts_assume_role(
            awscli_pod_session,
            role_name,
            nb_user_access_key_id,
            mcg_obj=mcg_obj_session,
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
        s3_delete_bucket(mcg_obj_session, bucket_1, mcg_obj_session.assumed_s3_client)
        logger.info(f"Deleted bucket {bucket_1}")

        # remove the role from the user
        mcg_obj_session.remove_sts_role(user_email)
        logger.info(f"Removed the assume role policy from the user {user_name}")

        # try to assume role after the assume role policy is removed
        sts_assume_role(
            awscli_pod_session,
            role_name,
            nb_user_access_key_id,
            mcg_obj=mcg_obj_session,
        )

        # perform io to validate the role assumption is no longer valid
        s3_create_bucket(mcg_obj_session, bucket_1)
        logger.info(f"Created bucket {bucket_1} again")
        s3_delete_bucket(mcg_obj_session, bucket_1)
        logger.info(f"Deleted bucket {bucket_1} again")
