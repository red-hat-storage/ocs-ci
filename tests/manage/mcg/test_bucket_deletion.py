import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, noobaa_cli_required, acceptance,
    filter_insecure_request_warning
)

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
@acceptance
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
    def test_cli_bucket_delete(self, mcg_obj, bucket_factory):
        """
        Test deletion of buckets using the MCG CLI
        """
        for bucket in bucket_factory(3, 'CLI'):
            bucket.delete()
            assert not mcg_obj.cli_verify_bucket_exists(bucket.name), (
                f"Found {bucket.name} that should've been removed"
            )

    def test_oc_bucket_delete(self, mcg_obj, bucket_factory):
        """
        Test deletion of buckets using OC commands
        """
        for bucket in bucket_factory(3, 'OC'):
            logger.info(f"Deleting bucket: {bucket.name}")
            bucket.delete()
            assert not mcg_obj.oc_verify_bucket_exists(bucket.name)
