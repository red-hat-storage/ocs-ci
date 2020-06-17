import pytest

from ocs_ci.framework.pytest_customization.marks import (
    acceptance, tier1
)
from ocs_ci.ocs.resources.objectbucket import OBC


class TestRGWBucketCreation:
    """
    Test creation of a bucket
    """
    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, 'rgw-oc'],
                marks=[tier1, acceptance]
            ),
        ]
    )
    def test_bucket_creation(self, rgw_bucket_factory, amount, interface):
        """
        Test bucket creation using the S3 SDK, OC command or MCG CLI.
        The factory checks the bucket's health by default.
        """
        obc = rgw_bucket_factory(amount, interface)[0]
        OBC(obc.name)
