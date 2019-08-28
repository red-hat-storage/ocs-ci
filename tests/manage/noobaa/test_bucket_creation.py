import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1
from tests.helpers import create_unique_resource_name

logger = logging.getLogger(__name__)


@pytest.mark.skipif(condition=True, reason="NooBaa is not deployed")
@pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
@tier1
class TestBucketCreation:
    """
    Test creation of a bucket
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    @pytest.mark.polarion_id("OCS-1298")
    def test_s3_bucket_creation(self, noobaa_obj, created_buckets):
        """
        Test bucket creation using the S3 SDK
        """

        bucketname = create_unique_resource_name(self.__class__.__name__.lower(), 's3-bucket')
        logger.info(f'Creating new bucket - {bucketname}')
        created_buckets.append(noobaa_obj.s3_create_bucket(bucketname=bucketname))
