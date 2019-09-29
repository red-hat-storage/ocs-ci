import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import tier1, noobaa_cli_required, aws_platform_required

logger = logging.getLogger(__name__)


@pytest.mark.filterwarnings(
    'ignore::urllib3.exceptions.InsecureRequestWarning'
)
@aws_platform_required
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
    def test_cli_bucket_creation(self, mcg_obj, bucket_factory, mcg_storageclass):
        """
        Test bucket creation using the MCG CLI
        """
        sc_obj = mcg_storageclass()
        assert set(
            bucket.name for bucket in bucket_factory(3, 'CLI', sc_obj)
        ).issubset(
            mcg_obj.cli_list_all_bucket_names()
        )

    def test_oc_bucket_creation(self, mcg_obj, bucket_factory, mcg_storageclass):
        """
        Test bucket creation using OC commands
        """
        sc_obj = mcg_storageclass()
        assert set(
            bucket.name for bucket in bucket_factory(3, 'OC', sc_obj)
        ).issubset(
            mcg_obj.oc_list_all_bucket_names()
        )
