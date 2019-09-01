import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1

logger = logging.getLogger(__name__)


@pytest.mark.skipif(condition=True, reason="NooBaa is not deployed")
@pytest.mark.filterwarnings(
    'ignore::urllib3.exceptions.InsecureRequestWarning'
)
@tier1
class TestBucketDeletion:
    """
    Test bucket Creation Deletion of buckets
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    @pytest.mark.polarion_id("OCS-1299")
    def test_s3_bucket_delete(self, noobaa_obj, bucket_factory):
        """
        Test deletion of bucket using the S3 SDK
        """

        for bucket in bucket_factory(3):
            logger.info(f"Deleting bucket: {bucket.name}")
            noobaa_obj.s3_delete_bucket(bucket)
            assert noobaa_obj.s3_verify_bucket_exists(bucket) is False
