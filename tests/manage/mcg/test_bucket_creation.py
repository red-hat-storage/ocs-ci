import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.utility.utils import check_if_executable_in_path

logger = logging.getLogger(__name__)


@pytest.mark.filterwarnings(
    'ignore::urllib3.exceptions.InsecureRequestWarning'
)
@pytest.mark.skipif(
    condition=config.ENV_DATA['platform'] != 'AWS',
    reason="Tests are not running on AWS deployed cluster"
)
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

    @pytest.mark.skipif(
        condition=check_if_executable_in_path('noobaa') is False,
        reason='MCG CLI was not found'
    )
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
