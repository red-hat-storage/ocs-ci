import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, noobaa_cli_required, aws_platform_required,
    filter_insecure_request_warning, acceptance
)

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
@aws_platform_required
@pytest.mark.parametrize(
    argnames="amount,interface",
    argvalues=[
        pytest.param(
            *[3, 'S3'],
            marks=[pytest.mark.polarion_id("OCS-1298"), tier1, acceptance]
        ),
        pytest.param(
            *[3, 'CLI'], marks=[tier1, acceptance, noobaa_cli_required]
        ),
        pytest.param(
            *[3, 'OC'], marks=[tier1, acceptance]
        ),
        pytest.param(
            *[10, 'S3'],
        ),
        pytest.param(
            *[10, 'CLI'], marks=noobaa_cli_required
        ),
        pytest.param(
            *[10, 'OC'],
        ),
    ]
)
class TestBucketCreation:
    """
    Test creation of a bucket
    """
    def test_bucket_creation(self, mcg_obj, bucket_factory, amount, interface):
        """
        Test bucket creation using the S3 SDK, OC command or MCG CLI
        """
        bucket_set = set(
            bucket.name for bucket in bucket_factory(amount, interface)
        )
        assert bucket_set.issubset(
            getattr(mcg_obj, f'{interface.lower()}_list_all_bucket_names')()
        )
