import os
import time
import logging

from datetime import datetime, timedelta
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.bucket_utils import craft_s3_command, compare_bucket_object_list

# from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


def start_mcg_bi_replication(first_bucket, second_bucket, duration=0, delay=5):

    """
    Run MCG bucket replication
    """

    if duration <= 0:
        condition = True
    else:
        condition = False

    end_time = datetime.now() + timedelta(minutes=duration)

    # create directories
    dir_name = "mcg_bi_replication"
    os.makedirs(f"{dir_name}/origin")
    os.makedirs(f"{dir_name}/result")
    os.chdir(f"{dir_name}/origin")
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
                cmd=f"cp object_{index} s3://{first_bucket}", signed_request_creds=creds
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


def start_noobaa_cache_bucket_ios(cache_bucket, hub_bucket, duration=0, delay=5):
    pass
