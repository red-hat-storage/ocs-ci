import logging
import uuid
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import bugzilla, system_test
from ocs_ci.framework.testlib import version
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.ocs.bucket_utils import s3_put_object, s3_get_object

logger = logging.getLogger(__name__)


@system_test
@bugzilla("2039309")
@skipif_ocs_version("<4.11")
@pytest.mark.polarion_id("OCS-4852")
def test_object_expiration(mcg_obj, bucket_factory):
    """
    Test object expiration, see if the object is deleted within the expiration + 8 hours buffer time

    """
    # Creating S3 bucket
    bucket = bucket_factory()[0].name
    object_key = "ObjKey-" + str(uuid.uuid4().hex)
    obj_data = "Random data" + str(uuid.uuid4().hex)
    expiration_days = 1
    buffer_time_in_hours = 8

    expire_rule_4_10 = {
        "Rules": [
            {
                "Expiration": {
                    "Days": expiration_days,
                    "ExpiredObjectDeleteMarker": False,
                },
                "ID": "data-expire",
                "Prefix": "",
                "Status": "Enabled",
            }
        ]
    }
    expire_rule = {
        "Rules": [
            {
                "Expiration": {
                    "Days": expiration_days,
                    "ExpiredObjectDeleteMarker": False,
                },
                "Filter": {"Prefix": ""},
                "ID": "data-expire",
                "Status": "Enabled",
            }
        ]
    }

    logger.info(f"Setting object expiration on bucket: {bucket}")
    if version.get_semantic_ocs_version_from_config() < version.VERSION_4_11:
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=expire_rule_4_10
        )
    else:
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=expire_rule
        )

    logger.info(f"Getting object expiration configuration from bucket: {bucket}")
    logger.info(
        f"Got configuration: {mcg_obj.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)}"
    )

    logger.info(f"Writing {object_key} to bucket: {bucket}")
    assert s3_put_object(
        s3_obj=mcg_obj, bucketname=bucket, object_key=object_key, data=obj_data
    ), "Failed: Put Object"

    logger.info("Waiting for 1 day + 8 hours buffer time")
    sleep(((expiration_days * 24) + buffer_time_in_hours) * 60 * 60)

    logger.info(f"Getting {object_key} from bucket: {bucket} after 1 day + 8 hours")
    try:
        s3_get_object(s3_obj=mcg_obj, bucketname=bucket, object_key=object_key)
    except Exception:
        logger.info(
            f"Test passed, object {object_key} got deleted after expiration + buffer time"
        )
    else:
        assert (
            False
        ), f"Test failed, object {object_key} didn't get deleted after expiration + buffer time"
