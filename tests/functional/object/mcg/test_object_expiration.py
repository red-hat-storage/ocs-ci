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
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    LifecyclePolicy,
    ExpirationRule,
    LifecycleFilter,
)

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
        bucket = bucket_factory()[0].name

        logger.info(f"Setting object expiration on bucket: {bucket}")
        lifecycle_policy = LifecyclePolicy(
            ExpirationRule(days=1, filter=LifecycleFilter(prefix=prefix_to_expire))
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
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

    # TODO: determine tier
    def test_object_expiration_filter(
        self, mcg_obj, bucket_factory, awscli_pod_session, test_directory_setup
    ):
        """
        Test various filtering options for the object expiration policy.

        1. On an MCG bucket, Set an S3 lifecycle policy with the following rules:
            - An expiration rule with a prefix filter
            - An expiration rule with a tags filter
            - An expiration rule with a minBytes filter
            - An expiration rule with a maxBytes filter
            - An expiration rule with a filter of all of the above
        2. Upload objects in the target prefix of the first rule
        3. Upload objects that match the tags in the second rule
        4. Upload objects that biger than the minimum size of the third rule
        5. Upload objects exceept the max size of the fourth rule
        6. Upload objects that match a combination of the above filters
        7. Upload objects that don't match any of the above filters
        8. Set the creation time of all of the objects to be older than the expiration time
        9. Verify that only the objects that should have been expired were deleted

        """
        first_prefix_to_expire = "to_expire_a/"
        second_prefix_to_expire = "to_expire_b/"
        object_count_per_case = 10
        tag_a_key, tag_a_value = "tag-a", "value-a"
        tag_b_key, tag_b_value = "tag-b", "value-b"
        tag_c_key, tag_c_value = "tag-c", "value-c"
        objects_to_expire = []
        objects_not_to_expire = []

        # 1. On an MCG bucket, Set an S3 lifecycle policy with the multiple filter rules
        bucket = bucket_factory()[0].name
        expiration_rules = []
        expiration_rules.append(
            ExpirationRule(
                days=1, filter=LifecycleFilter(prefix=first_prefix_to_expire)
            )
        )
        expiration_rules.append(
            ExpirationRule(
                days=1,
                filter=LifecycleFilter(
                    tags={tag_a_key: tag_a_value, tag_b_key: tag_b_value}
                ),
            )
        )
        expiration_rules.append(
            ExpirationRule(days=1, filter=LifecycleFilter(minBytes=100))
        )
        expiration_rules.append(
            ExpirationRule(days=1, filter=LifecycleFilter(maxBytes=200))
        )
        expiration_rules.append(
            ExpirationRule(
                days=1,
                filter=LifecycleFilter(
                    prefix="to_expire_b/",
                    tags={tag_c_key: tag_c_value},
                    minBytes=100,
                    maxBytes=200,
                ),
            )
        )
        lifecycle_policy = LifecyclePolicy(expiration_rules)
        logger.info(
            f"Setting lifecycle policy to bucket {bucket}: \n {lifecycle_policy}"
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
        )

        # 2. Upload objects in the target prefix of the first rule
        logger.info("Uploading objects to match the prefix filter")
        objects_to_expire += write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{first_prefix_to_expire}",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case,
            pattern="prefixed-obj-",
            mcg_obj=mcg_obj,
        )

        # 3. Upload objects that match the tags in the second rule
        tagged_objs_a = write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case // 2,
            pattern="tag-a-obj-",
            mcg_obj=mcg_obj,
        )
        tagged_objs_b = write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case // 2,
            pattern="tag-b-obj-",
            mcg_obj=mcg_obj,
        )
        # TODO: tag the objects that start with the pattern above accordingly
        objects_to_expire += tagged_objs_a + tagged_objs_b

        # 4. Upload objects that biger than the minimum size of the third rule
        objects_to_expire += write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case,
            pattern="big-obj-",
            bs="2M",
            mcg_obj=mcg_obj,
        )

        # 5. Upload objects that are smaller than the maximum size of the fourth rule
        objects_to_expire += write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case,
            pattern="small-obj-",
            bs="500K",
            mcg_obj=mcg_obj,
        )

        # 6. Upload objects that match a combination of the above filters
        objects_to_expire += write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{second_prefix_to_expire}",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case,
            pattern="mixed-criteria-obj-",
            bs="1M",
            mcg_obj=mcg_obj,
        )
        # TODO: tag the objects that start with the pattern above accordingly

        # 7. Upload objects that don't match any of the above filters
        # 7.1 Upload objects without a prefix that don't match any of the above filters
        objects_not_to_expire += write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case,
            pattern="not-to-expire-no-prefix-obj-",
            bs="1M",
            mcg_obj=mcg_obj,
        )
        # 7.2 Upload objects in the second prefix that don't have the target tag
        objects_not_to_expire += write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{second_prefix_to_expire}",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case,
            pattern="not-to-expire-no-tag-obj-",
            bs="1M",
            mcg_obj=mcg_obj,
        )
        # 7.3 Upload objects to the second prefix and are not in the filter size range
        objects_not_to_expire += write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{second_prefix_to_expire}",
            file_dir=test_directory_setup.origin_dir,
            amount=object_count_per_case,
            pattern="not-to-expire-size-obj-",
            bs="1M",
            mcg_obj=mcg_obj,
        )
        # TODO: tag the objects that start with the pattern above accordingly

        # 8. Set the creation time of all of the objects to be older than the expiration time
        logger.info("Setting the creation time of all objects to be older than 1 day")
        expire_mcg_objects(bucket)

        # 9. Verify that only the objects that should have been expired were deleted
        # TODO: Use Uday's util function

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
        lifecycle_policy = LifecyclePolicy(ExpirationRule(days=1))
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
        )

        # 2. Edit the expiration policy to disable it
        logger.info("Disabling the expiration policy")
        lifecycle_policy.rules[0].is_enabled = False
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
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
