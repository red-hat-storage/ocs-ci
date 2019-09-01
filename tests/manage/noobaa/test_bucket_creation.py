import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1

logger = logging.getLogger(__name__)


@pytest.mark.skipif(condition=True, reason="NooBaa is not deployed")
@pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
@tier1
class TestBucketCreation:
    """
    Test creation of a bucket
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    @pytest.mark.polarion_id("OCS-1298")
    def test_s3_bucket_creation(self, noobaa_obj, bucket_factory):
        """
        Test bucket creation using the S3 SDK
        """

        assert set(
            bucket.name for bucket in bucket_factory(3)
        ).issubset(
            noobaa_obj.s3_list_all_bucket_names()
        )
