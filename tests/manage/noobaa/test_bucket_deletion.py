import logging
import pytest

from tests.helpers import create_unique_resource_name
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1

logger = logging.getLogger(__name__)


@pytest.fixture()
def create_buckets(request, noobaa_obj):
    """
    Creates multiple buckets
    """
    created_buckets = []

    bucket_name = create_unique_resource_name(
        resource_description='bucket', resource_type='s3'
    )
    logger.info(f'Creating bucket: {bucket_name}')
    created_buckets.append(
        noobaa_obj.s3_create_bucket(bucketname=bucket_name)
    )

    def verify_bucket():
        """
        Verifies whether buckets exists after deletion

        """
        for bucket in created_buckets:
            logger.info(f"Verifying whether bucket: {bucket.name} exists"
                        f" after deletion")
            noobaa_obj.s3_verify_bucket_exists(bucket)

    request.addfinalizer(verify_bucket)

    return created_buckets


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
    def test_s3_bucket_delete(self, noobaa_obj, create_buckets):
        """
        Test deletion of bucket using the S3 SDK
        """
        for bucket in create_buckets:
            logger.info(f"Deleting bucket: {bucket.name}")
            noobaa_obj.s3_delete_bucket(bucket)
