import logging
import re

import botocore
import pytest
from pytest import skip

from ocs_ci.framework.pytest_customization.marks import (
    tier1, tier3, noobaa_cli_required, filter_insecure_request_warning,
    acceptance, performance
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.mcg_bucket import S3Bucket, OCBucket, CLIBucket

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
class TestBucketCreation:
    """
    Test creation of a bucket
    """
    ERRATIC_TIMEOUTS_SKIP_REASON = 'Skipped because of erratic timeouts'
    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, 'S3'],
                marks=[pytest.mark.polarion_id("OCS-1298"), tier1, acceptance]
            ),
            pytest.param(
                *[100, 'S3'],
                marks=[
                    skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance, pytest.mark.polarion_id("OCS-1823")
                ]
            ),
            pytest.param(
                *[1000, 'S3'],
                marks=[
                    skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance, pytest.mark.polarion_id("OCS-1824")
                ]
            ),
            pytest.param(
                *[3, 'OC'],
                marks=[tier1, acceptance, pytest.mark.polarion_id("OCS-1298")]
            ),
            pytest.param(
                *[100, 'OC'],
                marks=[
                    skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance, pytest.mark.polarion_id("OCS-1826")
                ]
            ),
            pytest.param(
                *[1000, 'OC'],
                marks=[
                    skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance, pytest.mark.polarion_id("OCS-1827")
                ]
            ),
            pytest.param(
                *[3, 'CLI'],
                marks=[
                    tier1, acceptance, noobaa_cli_required,
                    pytest.mark.polarion_id("OCS-1298")
                ]
            ),
            pytest.param(
                *[100, 'CLI'],
                marks=[
                    skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance, noobaa_cli_required, pytest.mark.polarion_id("OCS-1825")
                ]
            ),
            pytest.param(
                *[1000, 'CLI'],
                marks=[
                    skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance, noobaa_cli_required, pytest.mark.polarion_id("OCS-1828")
                ]
            ),
        ]
    )
    def test_bucket_creation(self, mcg_obj, bucket_factory, amount, interface):
        """
        Test bucket creation using the S3 SDK, OC command or MCG CLI
        """
        bucket_set = set(
            bucket.name for bucket in bucket_factory(amount, interface)
        )
        assert bucket_set.issubset(
            getattr(mcg_obj, f'{interface.lower()}_get_all_bucket_names')()
        )

    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, 'S3'],
                marks=[pytest.mark.polarion_id("OCS-1863"), tier3]
            ),
            pytest.param(
                *[3, 'CLI'],
                marks=[tier3, noobaa_cli_required,
                       pytest.mark.polarion_id("OCS-1863")]
            ),
            pytest.param(
                *[3, 'OC'],
                marks=[tier3, pytest.mark.polarion_id("OCS-1863")]
            ),
        ]
    )
    def test_duplicate_bucket_creation(self, mcg_obj, bucket_factory,
                                       amount, interface):
        """
        Negative test with duplicate bucket creation using the S3 SDK, OC
        command or MCG CLI
        """
        expected_err = "Already ?Exists"
        bucket_map = {
            's3': S3Bucket,
            'oc': OCBucket,
            'cli': CLIBucket
        }
        bucket_set = set(
            bucket.name for bucket in bucket_factory(amount, interface)
        )
        for bucket_name in bucket_set:
            try:
                bucket = bucket_map[interface.lower()](mcg_obj, bucket_name)
                assert not bucket, (
                    "Unexpected: Duplicate creation hasn't failed."
                )
            except (CommandFailed, botocore.exceptions.ClientError) as err:
                assert re.search(expected_err, str(err)), (
                    "Couldn't verify OBC creation. Unexpected error "
                    f"{str(err)}"
                )
                logger.info(f"Create duplicate bucket {bucket_name} failed as"
                            " expected")
