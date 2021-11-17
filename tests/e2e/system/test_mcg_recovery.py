import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import tier4
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_directory,
    compare_object_checksums_between_bucket_and_local,
    patch_replication_policy_to_bucket,
    random_object_round_trip_verification,
    sync_object_directory,
    wait_for_cache,
    write_random_test_objects_to_bucket,
)

log = logging.getLogger(__name__)


class TestMCGRecovery(E2ETest):
    """
    Test MCG system recovery

    """

    @pytest.mark.parametrize(
        argnames=["bucket_amount", "object_amount"],
        argvalues=[
            pytest.param(
                5,
                10,
                marks=[tier4, pytest.mark.polarion_id("E2E TODO")],
            ),
        ],
    )
    def test_mcg_db_backup_recovery(
        self,
        awscli_pod_session,
        mcg_obj_session,
        bucket_factory,
        cld_mgr,
        test_directory_setup,
        bucket_amount,
        object_amount,
    ):
        # E2E TODO: Have a cluster with FIPS, KMS for RGW and Hugepages enabled
        # E2E TODO: Please add the necessary skips to verify that all prerequisites are met

        # Create standard MCG buckets
        test_buckets = bucket_factory(
            amount=bucket_amount,
            interface="CLI",
        )

        uploaded_objects_dir = test_directory_setup.origin_dir
        downloaded_obejcts_dir = test_directory_setup.result_dir

        # Perform a round-trip object verification -
        # 1. Generate random objects in uploaded_objects_dir
        # 2. Upload the objects to the bucket
        # 3. Download the objects from the bucket
        # 4. Compare the object checksums in downloaded_obejcts_dir
        # with the ones in uploaded_objects_dir
        for count, bucket in enumerate(test_buckets):
            assert random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=bucket.name,
                upload_dir=uploaded_objects_dir + f"Bucket{count}",
                download_dir=downloaded_obejcts_dir + f"Bucket{count}",
                amount=object_amount,
                mcg_obj=mcg_obj_session,
            ), "Some or all written objects were not found in the list of downloaded objects"

        # E2E TODO: Create RGW kafka notification & see the objects are notified to kafka

        # Create two MCG buckets with a bidirectional replication policy
        bucketclass = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, "eu-central-1")]},
        }
        first_bidi_bucket_name = bucket_factory(bucketclass=bucketclass)[0].name
        replication_policy = ("basic-replication-rule", first_bidi_bucket_name, None)
        second_bidi_bucket_name = bucket_factory(
            1, bucketclass=bucketclass, replication_policy=replication_policy
        )[0].name
        patch_replication_policy_to_bucket(
            first_bidi_bucket_name, "basic-replication-rule-2", second_bidi_bucket_name
        )

        bidi_uploaded_objs_dir_1 = uploaded_objects_dir + "/bidi_1"
        bidi_uploaded_objs_dir_2 = uploaded_objects_dir + "/bidi_2"
        bidi_downloaded_objs_dir_1 = downloaded_obejcts_dir + "/bidi_1"
        bidi_downloaded_objs_dir_2 = downloaded_obejcts_dir + "/bidi_2"

        # Verify replication is working as expected by performing a two-way round-trip object verification
        random_object_round_trip_verification(
            io_pod=awscli_pod_session,
            bucket_name=first_bidi_bucket_name,
            upload_dir=bidi_uploaded_objs_dir_1,
            download_dir=bidi_downloaded_objs_dir_1,
            amount=object_amount,
            pattern="FirstBidi-",
            wait_for_replication=True,
            second_bucket_name=second_bidi_bucket_name,
            mcg_obj=mcg_obj_session,
        )

        random_object_round_trip_verification(
            io_pod=awscli_pod_session,
            bucket_name=second_bidi_bucket_name,
            upload_dir=bidi_uploaded_objs_dir_2,
            download_dir=bidi_downloaded_objs_dir_2,
            amount=object_amount,
            pattern="SecondBidi-",
            wait_for_replication=True,
            second_bucket_name=first_bidi_bucket_name,
            mcg_obj=mcg_obj_session,
        )

        # Create a cache bucket
        cache_bucketclass = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Cache",
                "ttl": 3600000,
                "namespacestore_dict": {
                    "aws": [(1, "eu-central-1")],
                },
            },
            "placement_policy": {
                "tiers": [{"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}]
            },
        }
        cache_bucket = bucket_factory(bucketclass=cache_bucketclass)[0]

        cache_uploaded_objs_dir = uploaded_objects_dir + "/cache"
        cache_uploaded_objs_dir_2 = uploaded_objects_dir + "/cache_2"
        cache_downloaded_objs_dir = downloaded_obejcts_dir + "/cache"
        underlying_bucket_name = cache_bucket.bucketclass.namespacestores[0].uls_name

        # Upload a random object to the bucket
        objs_written_to_cache_bucket = write_random_test_objects_to_bucket(
            awscli_pod_session,
            cache_bucket.name,
            cache_uploaded_objs_dir,
            mcg_obj=mcg_obj_session,
        )
        wait_for_cache(mcg_obj_session, cache_bucket.name, objs_written_to_cache_bucket)
        # Write a random, larger object directly to the underlying storage of the bucket
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            underlying_bucket_name,
            cache_uploaded_objs_dir_2,
            s3_creds=cld_mgr.aws_client.nss_creds,
        )
        # Download the object from the cache bucket
        sync_object_directory(
            awscli_pod_session,
            f"s3://{cache_bucket.name}",
            cache_downloaded_objs_dir,
            mcg_obj_session,
        )
        # Make sure the cached object was returned, and not the one that was written to the underlying storage
        assert compare_directory(
            awscli_pod_session,
            cache_uploaded_objs_dir,
            cache_downloaded_objs_dir,
            amount=1,
        ), "The uploaded and downloaded cached objects have different checksums"
        assert (
            compare_directory(
                awscli_pod_session,
                cache_uploaded_objs_dir_2,
                cache_downloaded_objs_dir,
                amount=1,
            )
            is False
        ), "The cached object was replaced by the new one before the TTL has expired"

        # E2E TODO: Implement flows relating to PVC snapshots and clones, FIO pods

        # Verify the integrity of all objects in all buckets post-recovery
        for count, bucket in enumerate(test_buckets):
            compare_object_checksums_between_bucket_and_local(
                awscli_pod_session,
                mcg_obj_session,
                bucket.name,
                uploaded_objects_dir + f"Bucket{count}",
                amount=object_amount,
                pattern="RandomObject-",
            )

        compare_object_checksums_between_bucket_and_local(
            awscli_pod_session,
            mcg_obj_session,
            first_bidi_bucket_name,
            bidi_downloaded_objs_dir_2,
            amount=object_amount,
            pattern="FirstBidi-",
        )
        compare_object_checksums_between_bucket_and_local(
            awscli_pod_session,
            mcg_obj_session,
            second_bidi_bucket_name,
            bidi_downloaded_objs_dir_2,
            amount=object_amount,
            pattern="SecondBidi-",
        )

        compare_object_checksums_between_bucket_and_local(
            awscli_pod_session,
            mcg_obj_session,
            cache_bucket.name,
            cache_downloaded_objs_dir,
        )
