import logging
import json
import uuid
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier2,
    bugzilla,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    expire_objects_in_bucket,
    list_objects_from_bucket,
    s3_put_object,
    s3_get_object,
    tag_objects,
    write_random_test_objects_to_bucket,
    write_random_test_objects_to_s3_path,
    wait_for_object_count_in_bucket,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    LifecyclePolicy,
    ExpirationRule,
    LifecycleFilter,
)
from ocs_ci.utility.utils import TimeoutSampler

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
        expire_objects_in_bucket(bucket, objs_to_expire[1:], prefix=prefix_to_expire)

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

    @tier2
    @pytest.mark.polarion_id("OCS-5185")
    def test_expiration_policy_filter(
        self, mcg_obj, bucket_factory, awscli_pod_session, test_directory_setup
    ):
        """
        Test various filtering options for the object expiration policy.

        1. On an MCG bucket, Set an S3 lifecycle policy with the following rules:
            1.1 An expiration rule with a prefix filter
            1.2 Multiple expiration rules with tags filters
            1.3 An expiration rule with a combination of prefix and tags filters
        2. Upload objects to the target prefix of the first rule
        3. Upload objects and tag them to match the tag filters rules
        4. Upload objects to the target prefix of the mixed criteria rule and tag them to match the tags filter
        5. Upload objects that don't match any of the above filters
        6. Set the creation time of all of the objects to be older than the expiration time
        7. Verify that only the objects that should have been expired were deleted

        """
        first_prefix_to_expire = "to_expire_a/"
        second_prefix_to_expire = "to_expire_b/"
        object_count_per_case = 10
        tag_a_key, tag_a_value = "tag-a", "value-a"
        tag_b_key, tag_b_value = "tag-b", "value-b"
        tag_c_key, tag_c_value = "tag-c", "value-c"

        # 1. On an MCG bucket, Set an S3 lifecycle policy with the multiple filter rules
        bucket = bucket_factory()[0].name
        expiration_rules = []
        # 1.1 An expiration rule with a prefix filter
        expiration_rules.append(
            ExpirationRule(
                days=1,
                use_date=True,
                filter=LifecycleFilter(prefix=first_prefix_to_expire),
            )
        )
        # 1.2 Multiple expiration rules with tags filters
        expiration_rules.append(
            ExpirationRule(
                days=1,
                use_date=True,
                filter=LifecycleFilter(tags={tag_a_key: tag_a_value}),
            )
        )
        expiration_rules.append(
            ExpirationRule(
                days=1,
                use_date=True,
                filter=LifecycleFilter(tags={tag_b_key: tag_b_value}),
            )
        )
        expiration_rules.append(
            ExpirationRule(
                days=1,
                use_date=True,
                filter=LifecycleFilter(
                    tags={tag_a_key: tag_a_value, tag_b_key: tag_b_value}
                ),
            )
        )
        # 1.3 An expiration rule with a combination of prefix and tags filters
        expiration_rules.append(
            ExpirationRule(
                days=1,
                use_date=True,
                filter=LifecycleFilter(
                    prefix="to_expire_b/",
                    tags={tag_c_key: tag_c_value},
                ),
            )
        )
        lifecycle_policy_dict = LifecyclePolicy(expiration_rules).as_dict()
        logger.info(
            f"Setting lifecycle policy to bucket {bucket}: \n {json.dumps(lifecycle_policy_dict, indent=4)}"
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy_dict
        )

        # 2. Upload objects in the target prefix of the first rule
        logger.info("Uploading objects to match the prefix filter")
        write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{first_prefix_to_expire}",
            file_dir=f"{test_directory_setup.origin_dir}/{first_prefix_to_expire}",
            amount=object_count_per_case,
            pattern="prefixed-obj-",
            mcg_obj=mcg_obj,
        )

        # 3. Upload objects and tag them to match the tags filters
        tagged_objs_a = write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=f"{test_directory_setup.origin_dir}/tagged_objs_a",
            amount=object_count_per_case // 2,
            pattern="tag-a-obj-",
            mcg_obj=mcg_obj,
        )
        tagged_objs_b = write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=f"{test_directory_setup.origin_dir}/tagged_objs_b",
            amount=object_count_per_case // 2,
            pattern="tag-b-obj-",
            mcg_obj=mcg_obj,
        )
        tagged_objs_a_b = write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=f"{test_directory_setup.origin_dir}/tagged_objs_a_b",
            amount=object_count_per_case // 2,
            pattern="tag-ab-obj-",
            mcg_obj=mcg_obj,
        )
        tag_objects(
            awscli_pod_session, mcg_obj, bucket, tagged_objs_a, {tag_a_key: tag_a_value}
        )
        tag_objects(
            awscli_pod_session, mcg_obj, bucket, tagged_objs_b, {tag_b_key: tag_b_value}
        )
        tag_objects(
            awscli_pod_session,
            mcg_obj,
            bucket,
            tagged_objs_a_b,
            {tag_a_key: tag_a_value, tag_b_key: tag_b_value},
        )

        # 4. Upload objects that match a combination of the above filters
        mixed_criteria_objects = write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{second_prefix_to_expire}",
            file_dir=f"{test_directory_setup.origin_dir}/{second_prefix_to_expire}",
            amount=object_count_per_case,
            pattern="mixed-criteria-obj-",
            mcg_obj=mcg_obj,
        )
        tag_objects(
            awscli_pod_session,
            mcg_obj,
            bucket,
            mixed_criteria_objects,
            {tag_c_key: tag_c_value},
            prefix=second_prefix_to_expire,
        )

        # 5.1 Upload objects that don't match any of the above filters
        objs_expected_not_to_expire = write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/",
            file_dir=f"{test_directory_setup.origin_dir}/no_filter_rules_match",
            amount=object_count_per_case,
            pattern="no-filter-rules-match-obj-",
            mcg_obj=mcg_obj,
        )
        # 5.2 Upload objects to the prefix in the mixed criteria rule but don't match the tags
        prefixed_objs_expected_not_to_expire = write_random_test_objects_to_s3_path(
            io_pod=awscli_pod_session,
            s3_path=f"{bucket}/{second_prefix_to_expire}",
            file_dir=f"{test_directory_setup.origin_dir}/{second_prefix_to_expire}/no_tags_match",
            amount=object_count_per_case,
            pattern="mix-criteria-but-no-tags-obj-",
            mcg_obj=mcg_obj,
        )
        # Add the prefix to the name of the prefixed objects so they'll match the listing results later
        prefixed_objs_expected_not_to_expire = [
            second_prefix_to_expire + obj
            for obj in prefixed_objs_expected_not_to_expire
        ]
        objs_expected_not_to_expire += prefixed_objs_expected_not_to_expire

        # 6. Set the creation time of all of the objects to be older than the expiration time
        logger.info(f"Setting back the creation time of all the objects in {bucket}:")
        expire_objects_in_bucket(bucket)

        # 7. Verify that only the objects that should have been expired were deleted
        logger.info("Waiting for the expiration of the objects that should expire:")

        timeout = 600
        sleep = 30

        try:
            last_objs_seen_in_bucket = []
            list_objs_timeout_sampler_generator = TimeoutSampler(
                timeout,
                sleep,
                lambda: list_objects_from_bucket(
                    pod_obj=awscli_pod_session,
                    target=bucket,
                    s3_obj=mcg_obj,
                    recursive=True,
                ),
            )

            for objs_in_bucket in list_objs_timeout_sampler_generator:
                last_objs_seen_in_bucket = objs_in_bucket
                if objs_in_bucket == objs_expected_not_to_expire:
                    logger.info("Expiration complete as expected!")
                    break

        except TimeoutExpiredError as e:
            logger.error(
                (
                    "Mismatch between expected and actual objects in the bucket!\n",
                    f"Expected: {objs_expected_not_to_expire}\n",
                    f"Actual: {last_objs_seen_in_bucket}\n",
                )
            )
            raise e

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
        expire_objects_in_bucket(bucket, objs_to_expire)

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
