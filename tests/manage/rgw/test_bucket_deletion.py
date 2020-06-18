import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, acceptance
)

logger = logging.getLogger(__name__)


class TestBucketDeletion:
    """
    Test bucket Creation Deletion of buckets
    """
    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, 'RGW-OC'],
                marks=[pytest.mark.polarion_id("OCS-1939"), tier1, acceptance]
            ),
        ]
    )
    def test_bucket_delete(self, rgw_bucket_factory, amount, interface):
        """
        Test deletion of bucket using the S3 SDK, MCG CLI and OC
        """
        for bucket in rgw_bucket_factory(amount, interface):
            logger.info(f"Deleting bucket: {bucket.name}")
            assert bucket.delete()
