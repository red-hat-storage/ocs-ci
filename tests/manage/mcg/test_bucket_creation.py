import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, noobaa_cli_required, aws_platform_required,
    filter_insecure_request_warning, acceptance
)

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
@aws_platform_required
@acceptance
@tier1
class TestBucketCreation:
    """
    Test creation of a bucket
    """
    @pytest.mark.polarion_id("OCS-1298")
    def test_s3_bucket_creation(self, mcg_obj, bucket_factory):
        """
        Test bucket creation using the S3 SDK
        """
        assert set(
            bucket.name for bucket in bucket_factory(3, 'S3')
        ).issubset(
            mcg_obj.s3_list_all_bucket_names()
        )

    @noobaa_cli_required
    def test_cli_bucket_creation(self, mcg_obj, bucket_factory):
        """
        Test bucket creation using the MCG CLI
        """
        assert set(
            bucket.name for bucket in bucket_factory(3, 'CLI')
        ).issubset(
            mcg_obj.cli_list_all_bucket_names()
        )

    def test_oc_bucket_creation(self, mcg_obj, bucket_factory):
        """
        Test bucket creation using OC commands
        """
        assert set(
            bucket.name for bucket in bucket_factory(3, 'OC')
        ).issubset(
            mcg_obj.oc_list_all_bucket_names()
        )
