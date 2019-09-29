import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import tier1, noobaa_cli_required, aws_platform_required

logger = logging.getLogger(__name__)


@pytest.mark.filterwarnings(
    'ignore::urllib3.exceptions.InsecureRequestWarning'
)
@aws_platform_required
@tier1
class TestBucketDeletion:
    """
    Test bucket Creation Deletion of buckets
    """
    @pytest.mark.polarion_id("OCS-1299")
    def test_s3_bucket_delete(self, mcg_obj, bucket_factory):
        """
        Test deletion of bucket using the S3 SDK
        """
        for bucket in bucket_factory(3, 'S3'):
            logger.info(f"Deleting bucket: {bucket.name}")
            bucket.delete()
            assert not mcg_obj.s3_verify_bucket_exists(bucket.name), (
                f"Found {bucket.name} that should've been removed"
            )

    @noobaa_cli_required
    def test_cli_bucket_delete(self, mcg_obj, bucket_factory, mcg_storageclass):
        """
        Test deletion of buckets using the MCG CLI
        """
        sc_obj = mcg_storageclass()
        for bucket in bucket_factory(3, 'CLI', sc_obj):
            bucket.delete()
            assert not mcg_obj.cli_verify_bucket_exists(bucket.name), (
                f"Found {bucket.name} that should've been removed"
            )

    def test_oc_bucket_delete(self, mcg_obj, bucket_factory, mcg_storageclass):
        """
        Test deletion of buckets using OC commands
        """
        sc_obj = mcg_storageclass()
        for bucket in bucket_factory(3, 'OC', sc_obj):
            logger.info(f"Deleting bucket: {bucket.name}")
            bucket.delete()
            assert not mcg_obj.oc_verify_bucket_exists(bucket.name)
