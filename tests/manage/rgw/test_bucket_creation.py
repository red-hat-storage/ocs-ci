import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    acceptance,
    skipif_mcg_only,
    red_squad,
    rgw,
    tier1,
    tier3,
)
from ocs_ci.ocs.resources.objectbucket import BUCKET_MAP
from ocs_ci.ocs.exceptions import CommandFailed
import botocore
import re

logger = logging.getLogger(__name__)


@rgw
@red_squad
@skipif_mcg_only
class TestRGWBucketCreation:
    """
    Test creation of a bucket
    """

    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, "RGW-OC"],
                marks=[tier1, acceptance, pytest.mark.polarion_id("OCS-2242")],
            ),
        ],
    )
    def test_bucket_creation(self, rgw_bucket_factory, amount, interface):
        """
        Test RGW OBC creation using the OC command.
        The factory checks the bucket's health by default.
        """
        rgw_bucket_factory(amount, interface)

    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, "RGW-OC"], marks=[tier3, pytest.mark.polarion_id("OCS-2247")]
            ),
        ],
    )
    def test_duplicate_bucket_creation(
        self, rgw_obj, rgw_bucket_factory, amount, interface
    ):
        """
        Negative test with duplicate bucket creation using OC commands
        """
        expected_err = "BucketAlready|Already ?Exists"
        bucket_set = set(
            bucket.name
            for bucket in rgw_bucket_factory(amount, interface, verify_health=False)
        )
        for bucket_name in bucket_set:
            try:
                bucket = BUCKET_MAP[interface.lower()](bucket_name, rgw=rgw_obj)
                assert not bucket, "Unexpected: Duplicate creation hasn't failed."
            except (CommandFailed, botocore.exceptions.ClientError) as err:
                assert re.search(expected_err, str(err)), (
                    "Couldn't verify OBC creation. Unexpected error " f"{str(err)}"
                )
                logger.info(
                    f"Creation of duplicate bucket {bucket_name} failed as expected"
                )
