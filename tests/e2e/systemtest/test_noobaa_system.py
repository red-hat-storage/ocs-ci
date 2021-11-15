import logging

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.bucket_utils import (
    write_data_to_buckets_and_check_integrity,
    verify_s3_buckets_integrity,
)

log = logging.getLogger(__name__)


class TestNoobaaSystem(E2ETest):
    """
    Test Noobaa System

    """

    def test_noobaa_system(
        self,
        bucket_factory,
        mcg_obj,
        awscli_pod,
    ):
        """
        Test Procedure:
        1.Write data on several buckets and check data integrity with md5sum
        2.Check the data integrity of buckets

        """
        log.info("Write data on several buckets and check data integrity with md5sum")
        buckets_data_dic = write_data_to_buckets_and_check_integrity(
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod,
            bucket_factory=bucket_factory,
            bucketclass_dict=None,
            number_of_buckets=5,
        )
        log.info("Check the data integrity of buckets")
        verify_s3_buckets_integrity(buckets_data_dic)
