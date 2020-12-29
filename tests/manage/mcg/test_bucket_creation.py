import logging
import re

import botocore
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier3,
    acceptance,
    performance,
)
from ocs_ci.ocs.constants import DEFAULT_STORAGECLASS_RBD
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.objectbucket import BUCKET_MAP
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated

logger = logging.getLogger(__name__)


@skipif_openshift_dedicated
class TestBucketCreation(MCGTest):
    """
    Test creation of a bucket
    """

    ERRATIC_TIMEOUTS_SKIP_REASON = "Skipped because of erratic timeouts"

    @pytest.mark.parametrize(
        argnames="amount,interface,bucketclass_dict",
        argvalues=[
            pytest.param(
                *[3, "S3", None],
                marks=[pytest.mark.polarion_id("OCS-1298"), tier1, acceptance],
            ),
            pytest.param(
                *[100, "S3", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1823"),
                ],
            ),
            pytest.param(
                *[1000, "S3", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1824"),
                ],
            ),
            pytest.param(
                *[3, "OC", None],
                marks=[tier1, acceptance, pytest.mark.polarion_id("OCS-1298")],
            ),
            pytest.param(
                *[100, "OC", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1826"),
                ],
            ),
            pytest.param(
                *[1000, "OC", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1827"),
                ],
            ),
            pytest.param(
                *[3, "CLI", None],
                marks=[tier1, acceptance, pytest.mark.polarion_id("OCS-1298")],
            ),
            pytest.param(
                *[100, "CLI", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1825"),
                ],
            ),
            pytest.param(
                *[1000, "CLI", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1828"),
                ],
            ),
            pytest.param(
                *[
                    1,
                    "OC",
                    {
                        "interface": "OC",
                        "backingstore_dict": {
                            "pv": [(1, 50, DEFAULT_STORAGECLASS_RBD)]
                        },
                    },
                ],
                marks=[tier1, pytest.mark.polarion_id("OCS-2331")],
            ),
            pytest.param(
                *[
                    1,
                    "CLI",
                    {
                        "interface": "CLI",
                        "backingstore_dict": {
                            "pv": [(1, 50, DEFAULT_STORAGECLASS_RBD)]
                        },
                    },
                ],
                marks=[tier1, pytest.mark.polarion_id("OCS-2331")],
            ),
        ],
    )
    def test_bucket_creation(
        self, bucket_class_factory, bucket_factory, amount, interface, bucketclass_dict
    ):
        """
        Test bucket creation using the S3 SDK, OC command or MCG CLI.
        The factory checks the bucket's health by default.
        """
        if bucketclass_dict:
            bucketclass = bucket_class_factory(bucketclass_dict)
            bucket_factory(amount, interface, bucketclass=bucketclass.name)
        else:
            bucket_factory(amount, interface)

    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, "S3"], marks=[pytest.mark.polarion_id("OCS-1863"), tier3]
            ),
            pytest.param(
                *[3, "CLI"], marks=[tier3, pytest.mark.polarion_id("OCS-1863")]
            ),
            pytest.param(
                *[3, "OC"], marks=[tier3, pytest.mark.polarion_id("OCS-1863")]
            ),
        ],
    )
    def test_duplicate_bucket_creation(
        self, mcg_obj, bucket_factory, amount, interface
    ):
        """
        Negative test with duplicate bucket creation using the S3 SDK, OC
        command or MCG CLI
        """
        expected_err = "BucketAlready|Already ?Exists"
        bucket_set = set(
            bucket.name
            for bucket in bucket_factory(amount, interface, verify_health=False)
        )
        for bucket_name in bucket_set:
            try:
                bucket = BUCKET_MAP[interface.lower()](bucket_name, mcg=mcg_obj)
                assert not bucket, "Unexpected: Duplicate creation hasn't failed."
            except (CommandFailed, botocore.exceptions.ClientError) as err:
                assert re.search(expected_err, str(err)), (
                    "Couldn't verify OBC creation. Unexpected error " f"{str(err)}"
                )
                logger.info(
                    f"Create duplicate bucket {bucket_name} failed as" " expected"
                )
