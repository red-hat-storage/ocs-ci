import logging
import uuid
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import tier1, bugzilla, red_squad, mcg
from ocs_ci.framework.testlib import MCGTest, version
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.ocs.bucket_utils import s3_put_object, s3_get_object

logger = logging.getLogger(__name__)


@mcg
@red_squad
class TestObjectExpiration(MCGTest):
    """
    Tests suite for object expiration

    """

    @skipif_ocs_version("<4.10")
    @bugzilla("2034661")
    @bugzilla("2029298")
    @pytest.mark.polarion_id("OCS-3929")
    @tier1
    def test_object_expiration(self, mcg_obj, bucket_factory):
        """
        Test object is not deleted in minutes when object is set to expire in a day

        """
        # Creating S3 bucket
        bucket = bucket_factory()[0].name
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        obj_data = "Random data" + str(uuid.uuid4().hex)
        # 4.10 dict to be removed once BZ 2091509 is fixed
        expire_rule_4_10 = {
            "Rules": [
                {
                    "Expiration": {"Days": 1, "ExpiredObjectDeleteMarker": False},
                    "ID": "data-expire",
                    "Prefix": "",
                    "Status": "Enabled",
                }
            ]
        }
        expire_rule = {
            "Rules": [
                {
                    "Expiration": {"Days": 1, "ExpiredObjectDeleteMarker": False},
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

        logger.info("Sleeping for 90 seconds")
        sleep(90)

        logger.info(f"Getting {object_key} from bucket: {bucket} after 90 seconds")
        assert s3_get_object(
            s3_obj=mcg_obj, bucketname=bucket, object_key=object_key
        ), "Failed: Get Object"
