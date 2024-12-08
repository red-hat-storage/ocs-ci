import argparse
import logging
import re
from datetime import datetime, timedelta

from ocs_ci.framework import config
from ocs_ci.deployment.ibmcloud import IBMCloudIPI
from ocs_ci.cleanup.ibm import defaults


logger = logging.getLogger(__name__)


def ibm_cleanup():
    parser = argparse.ArgumentParser(
        description="ibmcloud cleanup",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    bucket_group = parser.add_argument_group("S3 Bucket Sweeping Options")
    bucket_group.add_argument(
        "--sweep-buckets", action="store_true", help="Deleting S3 buckets."
    )
    parser.add_argument(
        "--hours-buckets",
        action="store",
        required=False,
        help="""
            Running time for the buckets in hours.
            Buckets older than to this will be deleted.
        """,
    )
    args = parser.parse_args()
    if args.sweep_buckets:
        bucket_hours = (
            args.hours_buckets
            if args.hours_buckets is not None
            else defaults.DEFAULT_TIME_BUCKETS
        )
        delete_buckets(bucket_hours)


def delete_buckets(hours):
    """ """
    status = []
    config.ENV_DATA["cluster_path"] = "/"
    config.ENV_DATA["cluster_name"] = "cluster"
    ibm_cloud_ipi_obj = IBMCloudIPI()
    buckets = ibm_cloud_ipi_obj.get_bucket_list()
    buckets_delete = buckets_to_delete(buckets, hours)
    for bucket_delete in buckets_delete:
        try:
            ibm_cloud_ipi_obj.delete_bucket(bucket_delete)
        except Exception as e:
            log = f"Failed to delete {bucket_delete}\nerror: {e}"
            logger.info(log)
            status.append(log)
    if len(status) > 0:
        raise Exception(status)


def buckets_to_delete(buckets, hours):
    """
    Buckets to Delete

    Args:

    """
    buckets_delete = []
    current_time = datetime.utcnow()
    for bucket in buckets:
        bucket_name = bucket["Name"]
        creation_date = datetime.strptime(
            bucket["CreationDate"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        # Check if the bucket matches any prefix rule
        hours_bucket = hours
        for prefix, max_age_hours in defaults.BUCKET_PREFIXES_SPECIAL_RULES.items():
            if re.match(prefix, bucket_name):
                hours_bucket = max_age_hours
        if hours_bucket == "never":
            continue
        if current_time - creation_date > timedelta(hours=int(hours_bucket)):
            buckets_delete.append(bucket_name)
    return buckets_delete[:10]
