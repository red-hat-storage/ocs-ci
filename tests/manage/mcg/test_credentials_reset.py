import logging
import random
import pytest
import boto3

from ocs_ci.framework.pytest_customization.marks import tier2
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework import config

from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


class TestCredentialsReset(MCGTest):
    """
    Test suite for resetting and regenerating MCG related credentials

    """

    @pytest.fixture()
    def original_noobaa_admin_password(self, request, mcg_obj_session):
        """
        Fixture to get the original password from the noobaa-admin secret

        """
        original_password = mcg_obj_session.get_noobaa_admin_credentials_from_secret()[
            "password"
        ]

        def finalizer():
            """
            Reset the password back to the original and retrieve a new RPC token

            """
            mcg_obj_session.reset_admin_pw(new_password=original_password)
            mcg_obj_session.retrieve_nb_token()

        request.addfinalizer(finalizer)
        return original_password

    @tier2
    def test_change_nb_admin_pw(self, mcg_obj_session, original_noobaa_admin_password):
        """
        Test changing the NooBaa admin password

        1. Change the noobaa-admin password
        2. Verify the password changed in the noobaa-admin secret
        3. Verify the old password fails when attempting to generate an RPC token and the new password succeeds

        """

        # Change the noobaa-admin password
        new_password = "new_nb_admin_password"
        mcg_obj_session.reset_admin_pw(new_password=new_password)

        # Verify the password changed in the noobaa-admin secret
        assert (
            new_password
            == mcg_obj_session.get_noobaa_admin_credentials_from_secret()["password"]
        )

        # Verify the original password fails when attempting to generate an RPC token
        # TODO - define a more specific exception to catch
        mcg_obj_session.password = original_noobaa_admin_password
        with pytest.raises(Exception):
            mcg_obj_session.retrieve_nb_token()
            logger.error(
                "Unexpectedly succeeded in retrieving RPC token with the original password"
            )

        # Verify the new password succeeds when attempting to generate an RPC token
        mcg_obj_session.password = new_password
        try:
            mcg_obj_session.retrieve_nb_token()
            logger.info("Successfully retrieved RPC token with new password")
        except Exception as e:
            logger.error(f"Failed to retrieve RPC token with new password: {e}")
            raise

    @tier2
    def test_regenerate_account_s3_creds(
        self, mcg_obj_session, mcg_account_factory, bucket_factory
    ):
        """
        Test regenerating S3 credentials for an MCG account

        1. Create a new noobaa account and fetch its S3 credentials
        2. Regenerate its S3 credentials using the noobaa CLI command
        3. Verify its S3 credentials have changed at the secret
        4. Verify that creating an S3 bucket fails with the old credentials and succeeds with the new credentials

        """
        acc_name = f"nsfs-integrity-test-{random.randrange(100)}"
        original_acc_credentials = mcg_account_factory(name=acc_name)
        endpoint = original_acc_credentials["endpoint"]

        # Fetch the account's S3 credentials
        ocp_secret_obj = OCP(
            kind="secret", namespace=config.ENV_DATA["cluster_namespace"]
        )

        mcg_acc_secret = ocp_secret_obj.get(resource_name=f"noobaa-account-{acc_name}")
        original_access_key = mcg_acc_secret["data"]["AWS_ACCESS_KEY_ID"]
        original_secret_key = mcg_acc_secret["data"]["AWS_SECRET_ACCESS_KEY"]

        # Regenerate the account's S3 credentials
        mcg_obj_session.exec_mcg_cmd(f"account regenerate {acc_name}", use_yes=True)

        # Verify the account's S3 credentials have changed at the secret
        mcg_acc_secret = ocp_secret_obj.get(resource_name=f"noobaa-account-{acc_name}")
        new_access_key = mcg_acc_secret["data"]["AWS_ACCESS_KEY_ID"]
        new_secret_key = mcg_acc_secret["data"]["AWS_SECRET_ACCESS_KEY"]

        assert (
            original_access_key != new_access_key
        ), f"Expected the access key to change from {original_access_key} to {new_access_key}"

        assert (
            original_secret_key != new_secret_key
        ), f"Expected the secret key to change from {original_secret_key} to {new_secret_key}"

        # Verify that creating an S3 bucket fails with the old credentials
        original_credentials_s3_resource = boto3.resource(
            "s3",
            verify=False,
            endpoint_url=endpoint,
            aws_access_key_id=original_access_key,
            aws_secret_access_key=original_secret_key,
        )

        # TODO - define a more specific exception to catch
        with pytest.raises(Exception):
            bucket_factory(
                s3resource=original_credentials_s3_resource, verify_health=False
            )
            logger.error(
                "Unexpectedly succeeded in creating an S3 bucket with the old credentials"
            )

        # Verify that creating an S3 bucket succeeds with the new credentials
        new_credentials_s3_resource = boto3.resource(
            "s3",
            verify=False,
            endpoint_url=endpoint,
            aws_access_key_id=new_access_key,
            aws_secret_access_key=new_secret_key,
        )

        try:
            bucket_factory(s3resource=new_credentials_s3_resource, verify_health=False)
            logger.info("Successfully created an S3 bucket with the new credentials")
        except Exception as e:
            logger.error(f"Failed to create an S3 bucket with the new credentials: {e}")
            raise

    @tier2
    def test_regenerate_obc_s3_creds(self, mcg_obj_session, bucket_factory):
        """
        Test regenerating S3 credentials for an OBC

        1. Create an OBC and fetch its S3 credentials
        2. Regenerate its S3 credentials using the noobaa CLI command
        3. Verify its S3 credentials have changed at the secret
        4. Verify that listing the bucket fails with the old credentials and succeeds with the new credentials
        """

        obc_name = bucket_factory(amount=1, interface="OC", timeout=120)[0].name

        # Fetch the account's S3 credentials
        ocp_secret_obj = OCP(
            kind="secret", namespace=config.ENV_DATA["cluster_namespace"]
        )

        obc_secret = ocp_secret_obj.get(resource_name=obc_name)
        original_access_key = obc_secret["data"]["AWS_ACCESS_KEY_ID"]
        original_secret_key = obc_secret["data"]["AWS_SECRET_ACCESS_KEY"]

        # Regenerate the account's S3 credentials
        mcg_obj_session.exec_mcg_cmd(f"obc regenerate {obc_name}", use_yes=True)

        obc_secret = ocp_secret_obj.get(resource_name=obc_name)
        new_access_key = obc_secret["data"]["AWS_ACCESS_KEY_ID"]
        new_secret_key = obc_secret["data"]["AWS_SECRET_ACCESS_KEY"]

        assert (
            original_access_key != new_access_key
        ), f"Expected the access key to change from {original_access_key} to {new_access_key}"

        assert (
            original_secret_key != new_secret_key
        ), f"Expected the secret key to change from {original_secret_key} to {new_secret_key}"

        # Verify that listing the bucket fails with the old credentials
        original_credentials_s3_resource = boto3.resource(
            "s3",
            verify=False,
            endpoint_url=mcg_obj_session.s3_internal_endpoint,
            aws_access_key_id=original_access_key,
            aws_secret_access_key=original_secret_key,
        )

        # TODO - define a more specific exception to catch
        with pytest.raises(Exception):
            original_credentials_s3_resource.Bucket(obc_name).load()
            logger.error(
                "Unexpectedly succeeded in listing the bucket with the old credentials"
            )

        # Verify that listing the bucket succeeds with the new credentials
        new_credentials_s3_resource = boto3.resource(
            "s3",
            verify=False,
            endpoint_url=mcg_obj_session.s3_internal_endpoint,
            aws_access_key_id=new_access_key,
            aws_secret_access_key=new_secret_key,
        )

        try:
            new_credentials_s3_resource.Bucket(obc_name).load()
            logger.info("Successfully listed the bucket with the new credentials")
        except Exception as e:
            logger.error(f"Failed to list the bucket with the new credentials: {e}")
            raise
