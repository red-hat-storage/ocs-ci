import pytest
import logging

from ocs_ci.ocs.bucket_utils import patch_replication_policy_to_bucket
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@pytest.fixture()
def setup_mcg_bucket_replication(request, bucket_factory):

    first_bucket_class_dict = {
        "interface": "cli",
        "backingstore_dict": {"rgw": [(1, None)]},
    }

    second_bucket_class_dict = {
        "interface": "OC",
        "backingstore_dict": {"aws": [(1, "eu-central-1")]},
    }
    first_bucket = bucket_factory(bucketclass=first_bucket_class_dict)[0].name
    replication_policy = ("basic-replication-rule", first_bucket, None)
    second_bucket = bucket_factory(
        1,
        bucketclass=second_bucket_class_dict,
        replication_policy=replication_policy,
    )[0].name
    patch_replication_policy_to_bucket(
        first_bucket, "basic-replication-rule-2", second_bucket
    )

    return first_bucket, second_bucket


@pytest.fixture
def setup_noobaa_caching(request, bucket_factory):
    ttl = 300000  # 300 seconds
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
            "tiers": [{"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}]
        },
    }

    cached_bucket_obj = bucket_factory(bucketclass=cache_bucketclass)[0]
    cached_bucket = cached_bucket_obj.name
    hub_bucket = cached_bucket_obj.bucketclass.namespacestores[0].uls_name

    return cached_bucket, hub_bucket


class TestFITonSC:
    def test_fit_on_sc(self, setup_mcg_bucket_replication, setup_noobaa_caching):

        # MCG bucket replication on RGW bucket and any other cloud provider. Both uni-directional & bi-directional
        first_bucket, second_bucket = setup_mcg_bucket_replication
        logger.info(f"First bucket: {first_bucket} Second bucket: {second_bucket}")

        # Noobaa caching
        cached_bucket, hub_bucket = setup_noobaa_caching
        logger.info(f"Cached bucket: {cached_bucket} Hub bucket: {hub_bucket}")

        # MCG NSFS

        # RGW kafka notification
