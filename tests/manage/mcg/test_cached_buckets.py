import pytest
import time
import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    copy_objects,
    copy_random_individual_objects,
    verify_s3_object_integrity,
    write_random_objects_in_pod,
    sync_object_directory,
)
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    polarion_id,
    skipif_disconnected_cluster,
    skipif_aws_creds_are_missing,
    tier2,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest

logger = logging.getLogger(__name__)


@mcg
@skipif_disconnected_cluster
@skipif_aws_creds_are_missing
class TestCachedBuckets(MCGTest):
    """
    Tests Noobaa cache bucket caching mechanism

    """

    @pytest.fixture()
    def setup(self, request, bucket_factory):
        def factory(ttl):
            """
            Setup a cached bucket

            Args:
                ttl (str): TTL in miliseconds
            Returns:
                cached_bucket: name of the cached bucket
                source_bucket_uls_name: name of ULS bucket
            """
            cache_bucketclass = {
                "interface": "OC",
                "namespace_policy_dict": {
                    "type": "Cache",
                    "ttl": ttl,
                    "namespacestore_dict": {
                        "aws": [(1, "eu-central-1")],
                    },
                },
                "placement_policy": {
                    "tiers": [
                        {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                    ]
                },
            }

            cached_bucket_obj = bucket_factory(bucketclass=cache_bucketclass)[0]
            cached_bucket = cached_bucket_obj.name
            source_bucket_uls_name = cached_bucket_obj.bucketclass.namespacestores[
                0
            ].uls_name

            return cached_bucket, source_bucket_uls_name

        return factory

    @tier2
    @polarion_id("OCS-4651")
    def test_cached_buckets_with_s3_cp(
        self, awscli_pod_session, test_directory_setup, mcg_obj, cld_mgr, setup
    ):
        """
        This test make sure caching mechanism works between hub bucket & cache bucket
        when we have TTL > 0 and we use `s3 cp` to download objects

        """
        ttl = 300000  # 300 seconds
        object_name = "fileobj0"

        cached_bucket, source_bucket_uls_name = setup(ttl=ttl)

        namespacestore_aws_s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.AWS_S3_ENDPOINT,
            "region": "eu-central-1",
        }

        first_dir = test_directory_setup.origin_dir
        second_dir = test_directory_setup.result_dir

        # write to cached buckets and make sure of copied object integrity
        copy_random_individual_objects(
            podobj=awscli_pod_session,
            file_dir=first_dir,
            target=f"s3://{cached_bucket}",
            pattern="fileobj",
            s3_obj=mcg_obj,
            amount=1,
        )
        copy_objects(
            podobj=awscli_pod_session,
            src_obj=f"s3://{cached_bucket}/{object_name}",
            target=second_dir,
            s3_obj=mcg_obj,
        )
        assert verify_s3_object_integrity(
            original_object_path=f"{first_dir}/{object_name}",
            result_object_path=f"{second_dir}/{object_name}",
            awscli_pod=awscli_pod_session,
        ), "Content of object dont match between cached bucket & local directory!!"
        logger.info(
            "Contents of object in both local directory and cached buckets match!"
        )

        # change the file content and then write directly to hub bucket
        time.sleep(5)
        write_random_objects_in_pod(
            io_pod=awscli_pod_session,
            file_dir=first_dir,
            amount=1,
            pattern="fileobj",
            bs="10M",
        )
        copy_objects(
            podobj=awscli_pod_session,
            src_obj=f"{first_dir}/{object_name}",
            target=f"s3://{source_bucket_uls_name}/",
            signed_request_creds=namespacestore_aws_s3_creds,
        )
        logger.info("Pushed the updated object with 10M to hub bucket!")

        # make sure content between cahced & hub buckets are different when TTL isn't expired
        time.sleep(5)
        copy_objects(
            podobj=awscli_pod_session,
            src_obj=f"s3://{cached_bucket}/{object_name}",
            target=second_dir,
            s3_obj=mcg_obj,
        )
        assert not verify_s3_object_integrity(
            original_object_path=f"{first_dir}/{object_name}",
            result_object_path=f"{second_dir}/{object_name}",
            awscli_pod=awscli_pod_session,
        ), "Cached bucket got updated too quickly!!"
        logger.info("Expected, Hub bucket & cache bucket's have different contents!")

        # make sure content of cached & hub buckets are same after TTL is expired
        time.sleep(ttl / 1000)
        logger.info(f"After TTL: {ttl} expired!")
        copy_objects(
            podobj=awscli_pod_session,
            src_obj=f"s3://{cached_bucket}/{object_name}",
            target=second_dir,
            s3_obj=mcg_obj,
        )
        assert verify_s3_object_integrity(
            original_object_path=f"{first_dir}/{object_name}",
            result_object_path=f"{second_dir}/{object_name}",
            awscli_pod=awscli_pod_session,
        ), "Cached bucket didnt get updated after TTL expired!!!"
        logger.info("[Success] Cached bucket got updated with latest object!")

    @tier2
    @bugzilla("2024107")
    @polarion_id("OCS-4652")
    def test_cached_buckets_with_s3_sync(
        self, test_directory_setup, setup, cld_mgr, mcg_obj, awscli_pod_session
    ):
        """
        This test make sure caching mechanism works between hub bucket & cache bucket
        when we have TTL > 0 and we use `s3 sync` to download objects

        """
        ttl = 300000  # 300 seconds
        object_name = "fileobj0"

        cached_bucket, source_bucket_uls_name = setup(ttl=ttl)

        namespacestore_aws_s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.AWS_S3_ENDPOINT,
            "region": "eu-central-1",
        }

        first_dir = test_directory_setup.origin_dir
        second_dir = test_directory_setup.result_dir

        # write to cached buckets and make sure of copied object integrity
        copy_random_individual_objects(
            podobj=awscli_pod_session,
            file_dir=first_dir,
            target=f"s3://{cached_bucket}",
            pattern="fileobj",
            s3_obj=mcg_obj,
            amount=1,
        )
        sync_object_directory(
            podobj=awscli_pod_session,
            src=f"s3://{cached_bucket}",
            target=second_dir,
            s3_obj=mcg_obj,
        )

        assert verify_s3_object_integrity(
            original_object_path=f"{first_dir}/{object_name}",
            result_object_path=f"{second_dir}/{object_name}",
            awscli_pod=awscli_pod_session,
        ), "Content of object dont match between cached bucket & local directory!!"
        logger.info(
            "Contents of object in both local directory and cached buckets match!"
        )

        # change the file content and then write directly to hub bucket
        time.sleep(5)
        write_random_objects_in_pod(
            io_pod=awscli_pod_session,
            file_dir=first_dir,
            amount=1,
            pattern="fileobj",
            bs="10M",
        )
        copy_objects(
            podobj=awscli_pod_session,
            src_obj=f"{first_dir}/{object_name}",
            target=f"s3://{source_bucket_uls_name}/",
            signed_request_creds=namespacestore_aws_s3_creds,
        )
        logger.info("Pushed the updated object with 10M to hub bucket!")

        # make sure content between cahced & hub buckets are different when TTL isn't expired
        time.sleep(5)
        try:
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{cached_bucket}",
                target=second_dir,
                s3_obj=mcg_obj,
            )
        except Exception as ex:
            if "InvalidRange" not in ex.args[0]:
                raise ex
            logger.info(
                "Expected, Sync fails which means cache bucket still doesnt have the update 10M object!"
            )
        else:
            logger.info(
                "[Not expected] Ideally sync should fail with Invalid Range exception!"
            )
            assert not verify_s3_object_integrity(
                original_object_path=f"{first_dir}/{object_name}",
                result_object_path=f"{second_dir}/{object_name}",
                awscli_pod=awscli_pod_session,
            ), "Cached bucket got updated too quickly!!"

        # make sure content of cached & hub buckets are same after TTL is expired
        time.sleep(ttl / 1000)
        logger.info(f"After {ttl} expired!")
        sync_object_directory(
            podobj=awscli_pod_session,
            src=f"s3://{cached_bucket}",
            target=second_dir,
            s3_obj=mcg_obj,
        )
        assert verify_s3_object_integrity(
            original_object_path=f"{first_dir}/{object_name}",
            result_object_path=f"{second_dir}/{object_name}",
            awscli_pod=awscli_pod_session,
        ), "Cached bucket didnt get updated after TTL expired!!!"
        logger.info("[Success] Hub bucket & cache bucket's have same contents!")
