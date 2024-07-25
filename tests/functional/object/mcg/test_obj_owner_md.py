import json
import logging
import random
import time
import pytest
import boto3

from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    put_bucket_policy,
    write_random_test_objects_to_bucket,
    map_objects_to_owners,
)

from ocs_ci.framework.testlib import (
    MCGTest,
    tier2,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    runs_on_provider,
    mcg,
)

from ocs_ci.ocs.resources.bucket_policy import gen_bucket_policy

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
class TestObjOwnerMD(MCGTest):
    """
    Test the ownership behavior of objects in MCG
    """

    @pytest.fixture(scope="class")
    def other_acc_creds(self, mcg_account_factory_class):
        """
        Create another account and return its credentials along with its name.

        Returns:
            dict: The credentials of the new account. It's expected
            to contain the following keys:
                - name (str): The name of the account
                - access_key_id (str): The access key ID of the account
                - access_key (str): The secret access key of the account

        Note that we use the class scope fixture here to ensure the right teardown order:
        The teardown of the function scoped bucket_factory will take place before
        the teardown of the class scoped mcg_account_factory_class, thus ensuring that
        any of the new account's buckets are deleted before the account itself is deleted.

        """
        acc_name = f"non-admin-{random.randrange(100)}"
        acc_creds = mcg_account_factory_class(name=acc_name, ssl=False)
        acc_creds["name"] = acc_name

        return acc_creds

    @tier2
    def test_obj_owner_in_s3_buckets(
        self,
        mcg_obj,
        other_acc_creds,
        bucket_factory,
        awscli_pod_session,
        test_directory_setup,
    ):
        """
        Test the ownership behavior of objects in S3 buckets.
        Objects in a bucket should be owned by the account that created the bucket:

        1. Create a bucket using the noobaa admin account credentials
        2. Create a bucket using another account credentials
        3. Allow the other account access to the admin's bucket
        4. Write objects from each account to both buckets
        5. For both buckets, check that the objects are owned by the creator of the bucket

        """
        # 1. Create a bucket using the noobaa admin account credentials
        admin_bucket_name = bucket_factory()[0].name

        # 2. Create a bucket using another account credentials

        # Set MCG object with the other account's credentials
        mcg_obj.s3_resource = boto3.resource(
            "s3",
            verify=False,
            endpoint_url=mcg_obj.s3_endpoint,
            aws_access_key_id=other_acc_creds["access_key_id"],
            aws_secret_access_key=other_acc_creds["access_key"],
        )
        mcg_obj.s3_client = mcg_obj.s3_resource.meta.client

        # Since bucket_factory uses the MCG object,
        # the following command creates a bucket using the other account's creds
        non_admin_bucket_name = bucket_factory()[0].name

        # Set the mcg_obj credentials back to the noobaa admin account's
        mcg_obj.update_s3_creds()

        # 3. Allow the other account access to the admin's bucket
        bucket_policy = gen_bucket_policy(
            user_list="*",
            actions_list=["*"],
            resources_list=[admin_bucket_name, f"{admin_bucket_name}/*"],
        )
        put_bucket_policy(mcg_obj, admin_bucket_name, json.dumps(bucket_policy))

        logger.info("Waiting 30s for the bucket policy to take effect...")
        time.sleep(30)

        # 4. Write objects from each account to both buckets
        for bucket in (admin_bucket_name, non_admin_bucket_name):
            write_random_test_objects_to_bucket(
                amount=3,
                io_pod=awscli_pod_session,
                file_dir=test_directory_setup.origin_dir,
                pattern="admin-obj",
                bucket_to_write=bucket,
                mcg_obj=mcg_obj,
            )

            write_random_test_objects_to_bucket(
                amount=3,
                io_pod=awscli_pod_session,
                file_dir=test_directory_setup.origin_dir,
                pattern="non-admin-obj",
                bucket_to_write=bucket,
                s3_creds=other_acc_creds,
            )

        # 5. For both buckets, check that the objects are owned by the creator of the bucket
        bucket_to_expected_owner = {
            admin_bucket_name: mcg_obj.noobaa_user,
            non_admin_bucket_name: other_acc_creds["name"],
        }
        for bucket, expected_owner in bucket_to_expected_owner.items():
            obj_name_to_owner_data = map_objects_to_owners(mcg_obj, bucket)
            for obj, owner_data in obj_name_to_owner_data.items():
                assert owner_data["DisplayName"] == expected_owner, (
                    f"Object {obj} in bucket {bucket} is not owned by "
                    f"the creator of the bucket"
                )

    @tier2
    @skipif_disconnected_cluster
    @skipif_proxy_cluster
    @skipif_aws_creds_are_missing
    @pytest.mark.parametrize(
        argnames=["bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"aws": [(1, None)]},
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
            ),
        ],
        ids=[
            "backingstore-cli",
            "namespacestore-oc",
        ],
    )
    def test_obj_owner_in_obc_buckets(
        self,
        mcg_obj,
        bucket_factory,
        bucketclass,
        awscli_pod_session,
        test_directory_setup,
    ):
        """
        Test the ownership behavior of objects in OBC buckets.
        Objects in an OBC bucket should be owned by the noobaa operator
        account since the OBC was created by it.

        1. Create an OBC and upload some objects to it
        2. Verify that all the objets in the OBC bucket are owned by the noobaa operator account
        """
        # 1. Create an OBC and upload some objects to it
        bucket = bucket_factory(1, bucketclass=bucketclass)[0]
        write_random_test_objects_to_bucket(
            amount=3,
            io_pod=awscli_pod_session,
            file_dir=test_directory_setup.origin_dir,
            pattern="obc-obj",
            bucket_to_write=bucket.name,
            mcg_obj=mcg_obj,
        )

        # 2. Verify that all the objets in the OBC bucket are owned by the noobaa operator account
        obj_name_to_owner_data = map_objects_to_owners(mcg_obj, bucket.name)
        for obj, owner_data in obj_name_to_owner_data.items():
            assert owner_data["DisplayName"] == constants.NB_OPERATOR_ACC_NAME, (
                f"Object {obj} in bucket {bucket.name} is not owned by "
                f"the noobaa operator account"
            )
