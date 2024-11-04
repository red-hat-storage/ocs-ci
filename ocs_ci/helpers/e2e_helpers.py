import logging

import random
import copy
import re
import time

from uuid import uuid4
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.bucket_utils import (
    random_object_round_trip_verification,
    write_random_test_objects_to_bucket,
    wait_for_cache,
    sync_object_directory,
    verify_s3_object_integrity,
    s3_put_object,
    expire_objects_in_bucket,
    sample_if_objects_expired,
)
from ocs_ci.ocs.resources.pod import get_rgw_pods, get_pod_logs
from ocs_ci.utility.utils import exec_cmd, run_cmd


logger = logging.getLogger(__name__)


def create_muliple_types_provider_obcs(
    num_of_buckets, bucket_types, cloud_providers, bucket_factory
):
    """
    This function creates valid OBCs of different cloud providers
    and bucket types

    Args:
        num_of_buckets (int): Number of buckets
        bucket_types (dict): Dict representing mapping between
            bucket type and relevant configuration
        cloud_providers (dict): Dict representing mapping between
            cloud providers and relevant configuration
        bucket_factory (fixture): bucket_factory fixture method

    Returns:
        List: list of created buckets

    """

    def get_all_combinations_map(providers, bucket_types):
        """
        Create valid combination of cloud-providers and bucket-types

        Args:
            providers (dict): dictionary representing cloud
                providers and the respective config
            bucket_types (dict): dictionary representing different
                types of bucket and the respective config
        Returns:
            List: containing all the possible combination of buckets

        """
        all_combinations = dict()

        for provider, provider_config in providers.items():
            for bucket_type, type_config in bucket_types.items():
                if provider == "pv" and bucket_type != "data":
                    available_providers = [
                        key for key in cloud_providers.keys() if key != "pv"
                    ]
                    if available_providers:
                        provider = random.choice(available_providers)
                    else:
                        # If 'pv' is the only available provider, choose between 'aws' and 'azure'
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

    all_combination_of_obcs = get_all_combinations_map(cloud_providers, bucket_types)
    buckets = list()
    num_of_buckets_each = num_of_buckets // len(all_combination_of_obcs.keys())
    buckets_left = num_of_buckets % len(all_combination_of_obcs.keys())
    if num_of_buckets_each != 0:
        for combo, combo_config in all_combination_of_obcs.items():
            buckets.extend(
                bucket_factory(
                    interface="OC",
                    amount=num_of_buckets_each,
                    bucketclass=combo_config,
                )
            )

    for index in range(0, buckets_left):
        buckets.extend(
            bucket_factory(
                interface="OC",
                amount=1,
                bucketclass=all_combination_of_obcs[
                    list(all_combination_of_obcs.keys())[index]
                ],
            )
        )

    return buckets


def validate_mcg_bucket_replicaton(
    awscli_pod_session,
    mcg_obj_session,
    source_target_map,
    uploaded_objects_dir,
    downloaded_obejcts_dir,
    event,
    run_in_bg=False,
    object_amount=5,
):
    """
    Validate MCG bucket replication feature

    Args:
        awscli_pod_session (Pod): Pod object representing aws-cli pod
        mcg_obj_session (MCG): MCG object
        source_target_map (Dict): Dictionary consisting of source - target buckets
        uploaded_objects_dir (str): directory where uploaded objects are kept
        downloaded_obejcts_dir (str): directory where downloaded objects are kept
        event (threading.Event()): Event() object
        run_in_bg (bool): If True, validation is run in background
        object_amount (int): Amounts of objects

    """
    bidi_uploaded_objs_dir_1 = uploaded_objects_dir + "/bidi_1"
    bidi_uploaded_objs_dir_2 = uploaded_objects_dir + "/bidi_2"
    bidi_downloaded_objs_dir_1 = downloaded_obejcts_dir + "/bidi_1"
    bidi_downloaded_objs_dir_2 = downloaded_obejcts_dir + "/bidi_2"

    # Verify replication is working as expected by performing a two-way round-trip object verification
    while True:
        for first_bucket, second_bucket in source_target_map.items():
            random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=first_bucket.name,
                upload_dir=bidi_uploaded_objs_dir_1,
                download_dir=bidi_downloaded_objs_dir_1,
                amount=object_amount,
                pattern=f"FirstBiDi-{uuid4().hex}",
                prefix="bidi_1",
                wait_for_replication=True,
                second_bucket_name=second_bucket.name,
                mcg_obj=mcg_obj_session,
                cleanup=True,
                timeout=1200,
            )

            random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=second_bucket.name,
                upload_dir=bidi_uploaded_objs_dir_2,
                download_dir=bidi_downloaded_objs_dir_2,
                amount=object_amount,
                pattern=f"SecondBiDi-{uuid4().hex}",
                prefix="bidi_2",
                wait_for_replication=True,
                second_bucket_name=first_bucket.name,
                mcg_obj=mcg_obj_session,
                cleanup=True,
                timeout=1200,
            )
            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.info("Verified bi-direction replication successfully")
            logger.warning("Stopping bi-direction replication verification")
            break
        time.sleep(30)


def validate_mcg_caching(
    awscli_pod_session,
    mcg_obj_session,
    cld_mgr,
    cache_buckets,
    uploaded_objects_dir,
    downloaded_obejcts_dir,
    event,
    run_in_bg=False,
):
    """
    Validate noobaa caching feature against the cache buckets

    Args:
        awscli_pod_session (Pod): Pod object representing aws-cli pod
        mcg_obj_session (MCG): MCG object
        cld_mgr (cld_mgr): cld_mgr object
        cache_buckets (List): List consisting of cache buckets
        uploaded_objects_dir (str): directory where uploaded objects are kept
        downloaded_obejcts_dir (str): directory where downloaded objects are kept
        event (threading.Event()): Event() object
        run_in_bg (bool): If True, validation is run in background

    """
    while True:
        for bucket in cache_buckets:
            cache_uploaded_objs_dir = uploaded_objects_dir + "/cache"
            cache_uploaded_objs_dir_2 = uploaded_objects_dir + "/cache_2"
            cache_downloaded_objs_dir = downloaded_obejcts_dir + "/cache"
            underlying_bucket_name = bucket.bucketclass.namespacestores[0].uls_name

            # Upload a random object to the bucket
            logger.info(f"Uploading to the cache bucket: {bucket.name}")
            obj_name = f"Cache-{uuid4().hex}"
            objs_written_to_cache_bucket = write_random_test_objects_to_bucket(
                awscli_pod_session,
                bucket.name,
                cache_uploaded_objs_dir,
                pattern=obj_name,
                mcg_obj=mcg_obj_session,
            )
            wait_for_cache(
                mcg_obj_session,
                bucket.name,
                objs_written_to_cache_bucket,
                timeout=300,
            )

            # Write a random, larger object directly to the underlying storage of the bucket
            logger.info(
                f"Uploading to the underlying bucket {underlying_bucket_name} directly"
            )
            write_random_test_objects_to_bucket(
                awscli_pod_session,
                underlying_bucket_name,
                cache_uploaded_objs_dir_2,
                pattern=obj_name,
                s3_creds=cld_mgr.aws_client.nss_creds,
                bs="2M",
            )

            # Download the object from the cache bucket
            awscli_pod_session.exec_cmd_on_pod(f"mkdir -p {cache_downloaded_objs_dir}")
            sync_object_directory(
                awscli_pod_session,
                f"s3://{bucket.name}",
                cache_downloaded_objs_dir,
                mcg_obj_session,
            )

            assert verify_s3_object_integrity(
                original_object_path=f"{cache_uploaded_objs_dir}/{obj_name}0",
                result_object_path=f"{cache_downloaded_objs_dir}/{obj_name}0",
                awscli_pod=awscli_pod_session,
            ), "The uploaded and downloaded cached objects have different checksums"

            assert (
                verify_s3_object_integrity(
                    original_object_path=f"{cache_uploaded_objs_dir_2}/{obj_name}0",
                    result_object_path=f"{cache_downloaded_objs_dir}/{obj_name}0",
                    awscli_pod=awscli_pod_session,
                )
                is False
            ), "The cached object was replaced by the new one before the TTL has expired"
            logger.info(f"Verified caching for bucket: {bucket.name}")

            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.warning("Stopping noobaa caching verification")
            break
        time.sleep(30)


def validate_rgw_kafka_notification(kafka_rgw_dict, event, run_in_bg=False):
    """
    Validate kafka notifications for RGW buckets

    Args:
        kafka_rgw_dict (Dict): Dict consisting of rgw bucket,
        kafka_topic, kafkadrop_host etc
        event (threading.Event()): Event() object
        run_in_bg (Bool): True if you want to run in the background

    """
    s3_client = kafka_rgw_dict["s3client"]
    bucketname = kafka_rgw_dict["kafka_rgw_bucket"]
    notify_cmd = kafka_rgw_dict["notify_cmd"]
    data = kafka_rgw_dict["data"]
    kafkadrop_host = kafka_rgw_dict["kafkadrop_host"]
    kafka_topic = kafka_rgw_dict["kafka_topic"]

    while True:
        data = data + f"{uuid4().hex}"

        def put_object_to_bucket(bucket_name, key, body):
            return s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

        assert put_object_to_bucket(
            bucketname, "key-1", data
        ), "Failed: Put object: key-1"
        exec_cmd(notify_cmd)

        # Validate rgw logs notification are sent
        # No errors are seen
        pattern = "ERROR: failed to create push endpoint"
        rgw_pod_obj = get_rgw_pods()
        rgw_log = get_pod_logs(pod_name=rgw_pod_obj[0].name, container="rgw")
        assert re.search(pattern=pattern, string=rgw_log) is None, (
            f"Error: {pattern} msg found in the rgw logs."
            f"Validate {pattern} found on rgw logs and also "
            f"rgw bucket notification is working correctly"
        )
        assert put_object_to_bucket(
            bucketname, "key-2", data
        ), "Failed: Put object: key-2"
        exec_cmd(notify_cmd)

        # Validate message are received Kafka side using curl command
        # A temporary way to check from Kafka side, need to check from UI
        @retry(Exception, tries=5, delay=5)
        def validate_kafa_for_message():
            curl_command = (
                f"curl -X GET {kafkadrop_host}/topic/{kafka_topic.name} "
                "-H 'content-type: application/vnd.kafka.json.v2+json'"
            )
            json_output = run_cmd(cmd=curl_command)
            # logger.info("Json output:" f"{json_output}")
            new_string = json_output.split()
            messages = new_string[new_string.index("messages</td>") + 1]
            logger.info("Messages:" + str(messages))
            if messages.find("1") == -1:
                raise Exception(
                    "Error: Messages are not recieved from Kafka side."
                    "RGW bucket notification is not working as expected."
                )

        validate_kafa_for_message()

        if event.is_set() or not run_in_bg:
            logger.warning("Stopping kafka rgw notification verification")
            break
        time.sleep(30)


def validate_mcg_object_expiration(
    mcg_obj,
    buckets,
    event,
    run_in_bg=False,
    object_amount=5,
):
    """
    Validates objects expiration for MCG buckets

    Args:
        mcg_obj (MCG): MCG object
        buckets (List): List of MCG buckets
        event (threading.Event()): Event() object
        run_in_bg (Bool): True if wants to run in background
        object_amount (Int): Amount of objects
        prefix (str): Any prefix used for objects

    """
    while True:
        for bucket in buckets:

            for i in range(object_amount):
                s3_put_object(
                    mcg_obj,
                    bucket.name,
                    f"obj-key-{uuid4().hex}",
                    "Some random data",
                )
            expire_objects_in_bucket(bucket.name)
            sample_if_objects_expired(mcg_obj, bucket.name)
            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.warning("Stopping MCG object expiration verification")
            break
        time.sleep(30)


def validate_mcg_nsfs_feature():
    logger.info("This is not implemented")
