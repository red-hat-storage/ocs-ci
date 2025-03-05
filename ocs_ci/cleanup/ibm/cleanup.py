import argparse
import logging
import re
import yaml
from datetime import datetime, timezone

from ocs_ci.framework import config
from ocs_ci.utility.ibmcloud import IBMCloudObjectStorage, get_bucket_regions_map
from ocs_ci.ocs import constants
from ocs_ci import framework
from ocs_ci.cleanup.ibm import defaults


logger = logging.getLogger(__name__)


def ibm_cleanup():
    parser = argparse.ArgumentParser(
        description="ibmcloud cleanup",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    bucket_group = parser.add_argument_group("S3 Bucket Sweeping Options")
    bucket_group.add_argument(
        "--sweep-buckets", action="store_true", help="Deleting IBM buckets."
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
    parser.add_argument(
        "--ocsci-conf",
        action="store",
        required=True,
        type=argparse.FileType("r", encoding="UTF-8"),
        help="""vSphere configuration file in yaml format.
            Example file:
                ---
                AUTH:
                  ibmcloud:
                    api_key: '<api-key>'
                    cos_instance_crn: '<cos_instance_crn>'
            """,
    )
    args = parser.parse_args()
    ibmcloud_conf = args.ocsci_conf

    # load vsphere_conf data to config
    ibmcloud_config_data = yaml.safe_load(ibmcloud_conf)
    framework.config.update(ibmcloud_config_data)
    ibmcloud_conf.close()

    if args.sweep_buckets:
        bucket_hours = int(
            args.hours_buckets
            if args.hours_buckets is not None
            else defaults.DEFAULT_TIME_BUCKETS
        )
        delete_buckets(bucket_hours)


def delete_buckets(hours):
    """ """
    status = []
    api_key = config.AUTH["ibmcloud"]["api_key"]
    service_instance_id = config.AUTH["ibmcloud"]["cos_instance_crn"]
    endpoint_url = constants.IBM_COS_GEO_ENDPOINT_TEMPLATE.format("us")
    cos = IBMCloudObjectStorage(
        api_key=api_key,
        service_instance_id=service_instance_id,
        endpoint_url=endpoint_url,
    )
    buckets_time = cos.get_buckets_data()
    buckets_region = get_bucket_regions_map()
    buckets_combine = {}
    for bucket_name, bucket_region in buckets_region.items():
        for bucket_time in buckets_time:
            if bucket_time["Name"] == bucket_name:
                buckets_combine[bucket_name] = [
                    bucket_region,
                    buckets_time[0]["CreationDate"],
                ]
    bucket_delete_names = buckets_to_delete(buckets_combine, hours)
    for bucket_delete_name in bucket_delete_names:
        res = cos.delete_bucket(bucket_delete_name)
        if res is False:
            status.append(bucket_delete_name)
    if len(status) > 0:
        raise Exception(f"Failed to delelte buckets {status}")


def buckets_to_delete(buckets, hours):
    """
    Buckets to Delete

    Args:

    """
    buckets_delete = []
    current_date = datetime.now(timezone.utc)
    for bucket_name, bucket_data in buckets.items():
        age_in_hours = (current_date - bucket_data[1]).total_seconds() / 3600
        hours_bucket = hours
        for prefix, max_age_hours in defaults.BUCKET_PREFIXES_SPECIAL_RULES.items():
            if re.match(prefix, bucket_name):
                hours_bucket = max_age_hours
        if hours_bucket == "never":
            continue
        if hours_bucket < age_in_hours:
            buckets_delete.append(bucket_name)
    return buckets_delete[:10]
