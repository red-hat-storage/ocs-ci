import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1, ocs_openshift_ci

logger = logging.getLogger(__name__)


@pytest.mark.skipif(condition=True, reason="MCG is not deployed")
@pytest.mark.filterwarnings(
    'ignore::urllib3.exceptions.InsecureRequestWarning'
)
@tier1
@ocs_openshift_ci
class TestBucketDeletion:
    """
    Test bucket Creation Deletion of buckets
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    @pytest.mark.polarion_id("OCS-1299")
    def test_s3_bucket_delete(self, mcg_obj, bucket_factory):
        """
        Test deletion of bucket using the S3 SDK
        """

        for bucket in bucket_factory(3):
            logger.info(f"Deleting bucket: {bucket.name}")
            mcg_obj.s3_delete_bucket(bucket)
            assert mcg_obj.s3_verify_bucket_exists(bucket) is False
