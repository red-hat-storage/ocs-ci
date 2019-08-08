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
def buckets(request, noobaa_obj):
    """
    Deletes all buckets that were created as part of the test

    Returns:
        Empty list of buckets

    """
    created_buckets = []

    def bucket_cleanup():
        for bucket in created_buckets:
            noobaa_obj.s3_delete_bucket(bucket)

    request.addfinalizer(bucket_cleanup)

    return created_buckets


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
    def test_s3_bucket_creation(self, noobaa_obj, buckets):
        """
        Test bucket creation using the S3 SDK
        """

        bucketname = create_unique_resource_name(self.__class__.__name__.lower(), 's3-bucket')
        logger.info(f'Creating new bucket - {bucketname}')
        buckets.append(noobaa_obj.s3_create_bucket(bucketname=bucketname))
