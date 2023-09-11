import logging
import uuid
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    bugzilla,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    expire_mcg_objects,
    s3_put_object,
    s3_get_object,
    write_random_test_objects_to_bucket,
    write_random_test_objects_to_s3_path,
    wait_for_object_count_in_bucket,
)
from ocs_ci.ocs.resources.mcg_lifecycle_policies import LifecycleConfig, ExpirationRule

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
class TestObjectExpiration(MCGTest):
    """
    Tests suite for object expiration

    """

    # NOTE: This is a workaround for the fact that the lifecycle background worker
    # runs every 8 hours by default, which is too long for the tests to wait
    @pytest.fixture(scope="class", autouse=True)
    def reduce_expiration_interval(self, add_env_vars_to_noobaa_core_class):
        """
        Reduce the interval in which the lifecycle background worker is running

        """
        new_interval_in_miliseconds = 60 * 1000
        add_env_vars_to_noobaa_core_class(
            [(constants.LIFECYCLE_INTERVAL_PARAM, new_interval_in_miliseconds)]
        )

    @tier1
    @pytest.mark.polarion_id("OCS-5166")
    def test_object_expiration(
        self, mcg_obj, bucket_factory, awscli_pod_session, test_directory_setup
    ):
        """
        Test the basic functionality of the object expiration policy on MCG

        1. Set S3 expiration policy on an MCG bucket
        2. Upload random objects under a prefix that is set to expire
        3. Upload random objects under a prefix that is not set to expire
        4. Manually expire the objects under the prefix that is set to expire
        5. Wait and verify the deletion of the objects that were set to expire
        6. Verify that objects that were not set to expire were not deleted

        """

        objects_amount = 10
        prefix_to_expire = "to_expire/"
        prefix_not_to_expire = "not_to_expire/"

        # 1. Set S3 expiration policy on an MCG bucket
        logger.info("Creating S3 bucket")

        bucket = bucket_factory()[0].name

        logger.info(f"Setting object expiration on bucket: {bucket}")
        lifecycle_config = LifecycleConfig(
            ExpirationRule(days=1, prefix=prefix_to_expire)
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_config.as_dict()
        )

        # 2. Upload random objects under a prefix that is set to expire
        logger.info("Uploading random objects to the bucket for expiration")
        write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{prefix_to_expire}",
            file_dir=test_directory_setup.origin_dir,
            amount=objects_amount,
            mcg_obj=mcg_obj,
        )
        # Get the names of the objects that were just uploaded for later
        objs_to_expire = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {test_directory_setup.origin_dir}"
        ).split(" ")

        # 3. Upload random objects under a prefix that is not set to expire
        logger.info("Uploading random objects to the bucket for non-expiration")
        write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{prefix_not_to_expire}",
            file_dir=test_directory_setup.origin_dir,
            amount=objects_amount,
            mcg_obj=mcg_obj,
        )

        # 4. Manually expire objects in the target prefix (skip one object to keep the prefix alive)
        expire_mcg_objects(bucket, objs_to_expire[1:], prefix=prefix_to_expire)

        # 5. Wait and verify the deletion of the objects that were set to expire
        logger.info(f"Waiting for the expiration of s3://{bucket}/{prefix_to_expire}")
        assert wait_for_object_count_in_bucket(
            io_pod=awscli_pod_session,
            expected_count=1,
            bucket_name=bucket,
            prefix=prefix_to_expire,
            s3_obj=mcg_obj,
            timeout=600,
            sleep=30,
        ), "Objects were not expired in time!"

        # 6. Verify that objects that were not set to expire were not deleted
        logger.info(f"Verifying that s3://{bucket}/{prefix_not_to_expire} still exists")
        assert wait_for_object_count_in_bucket(
            io_pod=awscli_pod_session,
            expected_count=objects_amount,
            bucket_name=bucket,
            prefix=prefix_not_to_expire,
            s3_obj=mcg_obj,
            timeout=60,
            sleep=10,
        ), "Objects were expired when they shouldn't have been!"

    @pytest.mark.polarion_id("OCS-5167")
    @tier1
    def test_disabled_object_expiration(
        self, mcg_obj, bucket_factory, awscli_pod_session, test_directory_setup
    ):
        """
        Test that objects are not deleted when expiration is disabled

        1. Set S3 expiration policy on an MCG bucket
        2. Edit the expiration policy to disable it
        3. Upload random objects
        4. Expire the objects manually
        5. Wait and verify that the objects were not deleted

        """
        objects_amount = 3

        # 1. Set S3 expiration policy on an MCG bucket
        logger.info("Creating S3 bucket")
        bucket = bucket_factory()[0].name

        logger.info(f"Setting object expiration on bucket: {bucket}")
        lifecycle_config = LifecycleConfig(ExpirationRule(days=1))
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_config.as_dict()
        )

        # 2. Edit the expiration policy to disable it
        logger.info("Disabling the expiration policy")
        lifecycle_config.rules[0].status = "Disabled"
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_config.as_dict()
        )

        # 3. Upload random objects
        logger.info("Uploading random objects to the bucket for expiration")
        write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=bucket,
            file_dir=test_directory_setup.origin_dir,
            amount=objects_amount,
            mcg_obj=mcg_obj,
        )

        # 4. Expire the objects manually
        objs_to_expire = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {test_directory_setup.origin_dir}"
        ).split(" ")
        expire_mcg_objects(bucket, objs_to_expire)

        # 5. Wait and verify that the objects were not deleted
        logger.info("Verifying that the uploaded objects still exists")
        assert not wait_for_object_count_in_bucket(
            io_pod=awscli_pod_session,
            expected_count=0,
            bucket_name=bucket,
            s3_obj=mcg_obj,
            timeout=300,
            sleep=30,
        ), "Objects were expired when they shouldn't have been!"

    @skipif_ocs_version("<4.10")
    @bugzilla("2034661")
    @bugzilla("2029298")
    @pytest.mark.polarion_id("OCS-3929")
    @tier1
    def test_object_expiration_in_minutes(self, mcg_obj, bucket_factory):
        """
        Test object is not deleted in minutes when object is set to expire in a day

        """
        # Creating S3 bucket
        bucket = bucket_factory()[0].name
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        obj_data = "Random data" + str(uuid.uuid4().hex)
        expire_rule = {
            "Rules": [
                {
                    "Expiration": {"Days": 1, "ExpiredObjectDeleteMarker": False},
                    "Filter": {"Prefix": ""},
                    "ID": "data-expire",
                    "Status": "Enabled",
                }
            ]
        }

        logger.info(f"Setting object expiration on bucket: {bucket}")
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=expire_rule
        )

        logger.info(f"Getting object expiration configuration from bucket: {bucket}")
        logger.info(
            f"Got configuration: {mcg_obj.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)}"
        )

        logger.info(f"Writing {object_key} to bucket: {bucket}")
        assert s3_put_object(
            s3_obj=mcg_obj, bucketname=bucket, object_key=object_key, data=obj_data
        ), "Failed: Put Object"

        logger.info("Sleeping for 90 seconds")
        sleep(90)

        logger.info(f"Getting {object_key} from bucket: {bucket} after 90 seconds")
        assert s3_get_object(
            s3_obj=mcg_obj, bucketname=bucket, object_key=object_key
        ), "Failed: Get Object"
