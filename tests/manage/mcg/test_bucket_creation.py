import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, tier2, noobaa_cli_required, aws_platform_required,
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
            *[3, 'CLI'],
            marks=[tier1, acceptance, noobaa_cli_required,
                   pytest.mark.polarion_id("OCS-1298")]
        ),
        pytest.param(
            *[3, 'OC'],
            marks=[tier1, acceptance, pytest.mark.polarion_id("OCS-1298")]
        ),
        pytest.param(
            *[100, 'S3'], marks=[tier2, pytest.mark.polarion_id("OCS-1823")]
        ),
        pytest.param(
            *[1000, 'S3'], marks=[tier2, pytest.mark.polarion_id("OCS-1824")]
        ),
        pytest.param(
            *[100, 'CLI'],
            marks=[tier2, noobaa_cli_required,
                   pytest.mark.polarion_id("OCS-1825")]
        ),
        pytest.param(
            *[1000, 'CLI'],
            marks=[tier2, noobaa_cli_required,
                   pytest.mark.polarion_id("OCS-1828")]
        ),
        pytest.param(
            *[100, 'OC'], marks=[tier2, pytest.mark.polarion_id("OCS-1826")]
        ),
        pytest.param(
            *[1000, 'OC'], marks=[tier2, pytest.mark.polarion_id("OCS-1827")]
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
