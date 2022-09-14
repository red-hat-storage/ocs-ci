import time
import logging


from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    copy_objects,
    copy_random_individual_objects,
    verify_s3_object_integrity,
    write_random_objects_in_pod,
)


logger = logging.getLogger(__name__)


class TestCachedBuckets:
    def test_cached_buckets_with_cp(
        self, bucket_factory, awscli_pod_session, test_directory_setup, mcg_obj, cld_mgr
    ):
        TTL = 300000
        cache_bucketclass = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Cache",
                "ttl": TTL,
                "namespacestore_dict": {
                    "aws": [(1, "eu-central-1")],
                },
            },
            "placement_policy": {
                "tiers": [{"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}]
            },
        }

        cached_bucket_obj = bucket_factory(bucketclass=cache_bucketclass)[0]
        cached_bucket = cached_bucket_obj.name
        source_bucket_uls_name = cached_bucket_obj.bucketclass.namespacestores[
            0
        ].uls_name

        namespacestore_aws_s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.AWS_S3_ENDPOINT,
            "region": cache_bucketclass["namespace_policy_dict"]["namespacestore_dict"][
                "aws"
            ][0][1],
        }

        first_dir = test_directory_setup.origin_dir
        second_dir = test_directory_setup.result_dir

        # write to cached buckets
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
            src_obj=f"s3://{cached_bucket}/fileobj0",
            target=second_dir,
            s3_obj=mcg_obj,
        )
        assert verify_s3_object_integrity(
            original_object_path=f"{first_dir}/fileobj0",
            result_object_path=f"{second_dir}/fileobj0",
            awscli_pod=awscli_pod_session,
        ), "Contents dont match between cached bucket & local dir!!"
        logger.info(
            "Contents of objects in both local directory and cached buckets match!"
        )

        # change the file content and write to hub bucket
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
            src_obj=f"{first_dir}/fileobj0",
            target=f"s3://{source_bucket_uls_name}/",
            signed_request_creds=namespacestore_aws_s3_creds,
        )
        logger.info("Pushed the updated object with 10M to hub bucket!")

        # make sure content between cahced & hub buckets are different
        time.sleep(5)
        copy_objects(
            podobj=awscli_pod_session,
            src_obj=f"s3://{cached_bucket}/fileobj0",
            target=second_dir,
            s3_obj=mcg_obj,
        )
        assert not verify_s3_object_integrity(
            original_object_path=f"{first_dir}/fileobj0",
            result_object_path=f"{second_dir}/fileobj0",
            awscli_pod=awscli_pod_session,
        ), "Cached bucket got updated too quickly!!"
        logger.info("hub bucket & cache bucket's have different contents!")

        # make sure content of cached & hub buckets are same
        time.sleep(TTL / 1000)
        logger.info(f"After TTL: {TTL} expired!")
        copy_objects(
            podobj=awscli_pod_session,
            src_obj=f"s3://{cached_bucket}/fileobj0",
            target=second_dir,
            s3_obj=mcg_obj,
        )
        assert verify_s3_object_integrity(
            original_object_path=f"{first_dir}/fileobj0",
            result_object_path=f"{second_dir}/fileobj0",
            awscli_pod=awscli_pod_session,
        ), "Cached bucket didnt get updated after TTL expired!!!"
        logger.info("[Success] Cached bucket got updated with latest object!")
