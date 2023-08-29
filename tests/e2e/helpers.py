import logging
import random
import copy

from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


def multi_obc_factory(bucket_factory, mcg_obj):
    def get_all_combinations_map(providers, bucket_types):
        all_combinations = dict()

        for provider, provider_config in providers.items():
            for bucket_type, type_config in bucket_types.items():
                if provider == "pv" and bucket_type != "data":
                    provider = random.choice(["aws", "azure"])
                    provider_config = providers[provider]
                bucketclass = copy.deepcopy(type_config)

                if "backingstore_dict" in bucketclass.keys():
                    bucketclass["backingstore_dict"][provider] = [provider_config]
                elif "namespace_policy_dict" in bucketclass.keys():
                    bucketclass["namespace_policy_dict"]["namespacestore_dict"][
                        provider
                    ] = [provider_config]
                all_combinations.update({f"{bucket_type}-{provider}": bucketclass})
        return all_combinations

    def create_obcs(num_obcs=50, type_of_bucket=None, expiration_rule=None):

        cloud_providers = {
            "aws": (1, "eu-central-1"),
            "azure": (1, None),
            "pv": (
                1,
                constants.MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                "ocs-storagecluster-ceph-rbd",
            ),
        }

        bucket_types = {
            "data": {
                "interface": "OC",
                "backingstore_dict": {},
            },
            "namespace": {
                "interface": "OC",
                "namespace_policy_dict": {
                    "type": "Single",
                    "namespacestore_dict": {},
                },
            },
            "cache": {
                "interface": "OC",
                "namespace_policy_dict": {
                    "type": "Cache",
                    "ttl": 300000,
                    "namespacestore_dict": {},
                },
                "placement_policy": {
                    "tiers": [
                        {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                    ]
                },
            },
        }
        to_remove = list()
        if isinstance(type_of_bucket, list):
            if set(type_of_bucket).issubset(set(list(bucket_types.keys()))):
                for type in bucket_types.keys():
                    if type not in type_of_bucket:
                        to_remove.append(type)
            else:
                logger.error(
                    "Invalid bucket types, only possible types are: data, cache, namespace"
                )
        elif type_of_bucket is not None:
            logger.error(
                "Invalid argument type for 'type_of_bucket': It should be list type"
            )

        for i in range(len(to_remove)):
            del bucket_types[to_remove[i]]

        all_combination_of_obcs = get_all_combinations_map(
            cloud_providers, bucket_types
        )
        buckets = list()
        buckets_created = dict()
        num_of_buckets_each = num_obcs // len(all_combination_of_obcs.keys())
        buckets_left = num_obcs % len(all_combination_of_obcs.keys())
        if num_of_buckets_each != 0:
            for combo, combo_config in all_combination_of_obcs.items():
                buckets.extend(
                    bucket_factory(
                        interface="OC",
                        amount=num_of_buckets_each,
                        bucketclass=combo_config,
                    )
                )
                buckets_created.update({combo: num_of_buckets_each})

        for i in range(0, buckets_left):
            buckets.extend(
                bucket_factory(
                    interface="OC",
                    amount=1,
                    bucketclass=all_combination_of_obcs[
                        list(all_combination_of_obcs.keys())[i]
                    ],
                )
            )
            buckets_created.update(
                {
                    list(all_combination_of_obcs.keys())[i]: (
                        buckets_created[list(all_combination_of_obcs.keys())[i]]
                        if len(buckets) >= len(all_combination_of_obcs.keys())
                        else 0
                    )
                    + 1
                }
            )

        for bucket in buckets:
            mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket.name, LifecycleConfiguration=expiration_rule
            )
        logger.info("These are the buckets created:" f"{buckets_created}")
        return buckets

    return create_obcs
