import os
import time

from datetime import datetime, timedelta
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.bucket_utils import craft_s3_command


def start_mcg_bi_replication(first_bucket, second_bucket, duration=0, delay=5):

    """
    Run MCG bucket replication
    """

    if duration <= 0:
        condition = True
    else:
        condition = False

    end_time = datetime.now() + timedelta(minutes=duration)

    # create directory
    dir_name = "mcg_bi_replication"
    os.mkdir(dir_name)
    os.mkdir(f"{dir_name}/origin")
    os.mkdir(f"{dir_name}/result")
    os.chdir(f"{dir_name}/origin")
    index = 0
    obc_obj = OBC(first_bucket)
    while condition or (datetime.now() < end_time):

        # generate an object
        exec_cmd(cmd=f"dd if=/dev/urandom of=object_{index} bs=512 count=1")

        exec_cmd(
            cmd=craft_s3_command(
                cmd=f"cp object_{index} s3://{first_bucket}", mcg_obj=obc_obj
            ),
            secrets=[
                obc_obj.access_key_id,
                obc_obj.access_key,
                obc_obj.s3_external_endpoint,
            ],
        )

        time.sleep(delay)

        temp = first_bucket
        first_bucket = second_bucket
        second_bucket = temp
