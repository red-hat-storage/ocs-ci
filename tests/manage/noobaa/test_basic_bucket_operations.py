import logging
import pytest

from tests.helpers import create_unique_resource_name
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.ocs.resources import noobaa


logger = logging.getLogger(__name__)


@pytest.fixture()
def noobaa_obj():
    """
    Returns a NooBaa resource that's connected to the S3 endpoint
    Returns:
        s3_res: A NooBaa resource

    """
    s3_res = noobaa.NooBaa()
    return s3_res


@pytest.fixture()
def create_delete_buckets(request, noobaa_obj):
    """
    Creates and Deletes multiple buckets that were created as part of
    the test
    """
    created_buckets = []
    number_of_buckets = 5
    for i in range(number_of_buckets):
        bucket_name = create_unique_resource_name(
            resource_description='bucket', resource_type='s3'
        )
        logger.info(f'Creating bucket: {bucket_name}')
        created_buckets.append(
            noobaa_obj.s3_create_bucket(bucketname=bucket_name)
        )

    def bucket_deletion():
        for bucket in created_buckets:
            logger.info(f"Deleting bucket: {bucket.name}")
            noobaa_obj.s3_delete_bucket(bucket)

    request.addfinalizer(bucket_deletion)


@pytest.mark.skipif(condition=True, reason="NooBaa is not deployed")
@pytest.mark.filterwarnings(
    'ignore::urllib3.exceptions.InsecureRequestWarning'
)
@tier1
@pytest.mark.usefixtures(
    create_delete_buckets.__name__
)
class TestBucketOperation:
    """
    Test multiple bucket Creation, Listing and Deletion
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    def test_s3_bucket_list(self, noobaa_obj):
        """
        Test Listing of buckets using the S3 SDK
        """
        logger.info(f"Listing all Buckets: "
                    f"{noobaa_obj.s3_list_all_bucket_names()}"
                    )
