import pytest
import shlex
import os
import time
import logging

from datetime import datetime, timedelta
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.bucket_utils import craft_s3_command, compare_bucket_object_list

# from ocs_ci.ocs import constants

from ocs_ci.ocs.bucket_utils import patch_replication_policy_to_bucket

# from ocs_ci.ocs import constants

# from concurrent.futures import ThreadPoolExecutor, wait
# from ocs_ci.helpers.helpers import default_storage_class
# from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
# from ocs_ci.ocs.resources.rgw import RGW

logger = logging.getLogger(__name__)


@pytest.fixture()
def start_mcg_bucket_replication(request, bucket_factory):

    """
    This fixture,
     * implements the bi-directional bucket replication between the two MCG buckets
     * Performs object upload alternatively each bucket for said duration of time
     * checks for the object replication between the buckets
    """

    base_dir = os.getcwd()
    origin_dir = "mcg_bi_replication/origin"
    result_dir = "mcg_bi_replication/result"

    def factory(first_bucket_class_dict, second_bucket_class_dict, duration=0, delay=5):

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

        if duration <= 0:
            condition = True
        else:
            condition = False

        end_time = datetime.now() + timedelta(minutes=duration)

        # create directories
        os.makedirs(origin_dir)
        os.makedirs(result_dir)
        os.chdir(origin_dir)
        index = 0

        while condition or (datetime.now() < end_time):
            obc_obj = OBC(first_bucket)

            # generate an object
            exec_cmd(cmd=f"dd if=/dev/urandom of=object_{index} bs=512 count=1")

            # upload the generated object to the bucket
            creds = {
                "access_key_id": obc_obj.access_key_id,
                "access_key": obc_obj.access_key,
                "endpoint": obc_obj.s3_external_endpoint,
                "ssl": False,
            }
            exec_cmd(
                cmd=craft_s3_command(
                    cmd=f"cp object_{index} s3://{first_bucket}",
                    signed_request_creds=creds,
                ),
            )
            time.sleep(delay)

            # check if both the buckets have the same objects
            compare_bucket_object_list(
                mcg_obj=MCG(),
                first_bucket_name=first_bucket,
                second_bucket_name=second_bucket,
            )

            # swap the buckets, so that next time other bucket gets object uploaded
            temp = first_bucket
            first_bucket = second_bucket
            second_bucket = temp

            index += 1

        return first_bucket, second_bucket

    def teardown():

        for file in os.listdir(f"{base_dir}/{origin_dir}"):
            file_path = os.path.join(f"{base_dir}/{origin_dir}", file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(f"{base_dir}/{origin_dir}")
        logger.info(f"Deleted {base_dir}/{origin_dir}")

        for file in os.listdir(f"{base_dir}/{result_dir}"):
            file_path = os.path.join(f"{base_dir}/{result_dir}", file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(f"{base_dir}/{result_dir}")
        logger.info(f"Deleted {base_dir}/{result_dir}")

        os.rmdir(base_dir)
        logger.info(f"Deleted {base_dir}")

    request.addfinalizer(teardown)

    return factory


@pytest.fixture()
def start_noobaa_cache_io(request, bucket_factory, cld_mgr):

    base_dir = os.getcwd()
    origin_dir = "noobaa_caching/origin"
    result_dir = "noobaa_caching/result"

    client_map = {
        "rgw": cld_mgr.rgw_client,
        "aws": cld_mgr.aws_client,
    }

    def factory(cache_bucketclass, duration=0, delay=10):

        client = client_map[
            list(
                cache_bucketclass["namespace_policy_dict"]["namespacestore_dict"].keys()
            )[0]
        ]

        cached_bucket_obj = bucket_factory(bucketclass=cache_bucketclass)[0]
        cached_bucket = cached_bucket_obj.name
        hub_bucket = cached_bucket_obj.bucketclass.namespacestores[0].uls_name
        logger.info(
            f"Created cache bucket: {cached_bucket} and hub bucket : {hub_bucket}"
        )

        namespacestore__s3_creds = {
            "access_key_id": client.access_key,
            "access_key": client.secret_key,
            "endpoint": client.endpoint,
            "ssl": False,
        }

        obc_obj = OBC(cached_bucket)
        creds = {
            "access_key_id": obc_obj.access_key_id,
            "access_key": obc_obj.access_key,
            "endpoint": obc_obj.s3_external_endpoint,
            "ssl": False,
        }

        if duration <= 0:
            condition = True
        else:
            condition = False

        end_time = datetime.now() + timedelta(minutes=duration)

        # create directories
        os.makedirs(origin_dir)
        logger.info(f"Created: {origin_dir}")
        os.makedirs(result_dir)
        logger.info(f"Created: {result_dir}")
        os.chdir(origin_dir)
        index = 0

        while condition or (datetime.now() < end_time):

            # generate object
            exec_cmd(cmd=f"dd if=/dev/urandom of=object_{index} bs=512 count=1")

            # upload to cache bucket
            exec_cmd(
                cmd=craft_s3_command(
                    cmd=f"cp {base_dir}/{origin_dir}/object_{index} s3://{cached_bucket}",
                    signed_request_creds=creds,
                ),
            )

            # download the object from cached bucket
            exec_cmd(
                cmd=craft_s3_command(
                    cmd=f"sync s3://{cached_bucket}/ {base_dir}/{result_dir}/",
                    signed_request_creds=creds,
                ),
            )
            md5sum = shlex.split(
                exec_cmd(
                    cmd=f"md5sum {base_dir}/{origin_dir}/object_{index} {base_dir}/{result_dir}/object_{index}"
                )
            )
            assert md5sum[0] == md5sum[1], "Object integrity didnt match!"
            logger.info("Object integrity matched!")

            # increase the object size to 1M
            exec_cmd(cmd=f"dd if=/dev/urandom of=object_{index} bs=1M count=1")

            # upload to hub bucket
            exec_cmd(
                cmd=craft_s3_command(
                    cmd=f"cp {base_dir}/{origin_dir}/object_{index} s3://{hub_bucket}",
                    signed_request_creds=namespacestore__s3_creds,
                ),
            )

            # download from cache bucket
            exec_cmd(
                cmd=craft_s3_command(
                    cmd=f"sync s3://{cached_bucket}/object_{index} {base_dir}/{result_dir}",
                    signed_request_creds=creds,
                ),
            )
            exec_cmd(
                cmd=craft_s3_command(
                    cmd=f"sync s3://{cached_bucket}/ {base_dir}/{result_dir}/",
                    signed_request_creds=creds,
                ),
            )
            md5sum = shlex.split(
                exec_cmd(
                    cmd=f"md5sum {base_dir}/{origin_dir}/object_{index} {base_dir}/{result_dir}/object_{index}"
                )
            )
            assert md5sum[0] != md5sum[1], "Object integrity matched!"
            logger.info("[expected] Object integrity didnt match!")

            time.sleep(delay)

        return cached_bucket, hub_bucket

    return factory
