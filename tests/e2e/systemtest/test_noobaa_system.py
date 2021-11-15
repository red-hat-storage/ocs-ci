import logging

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import noobaa_db_backup_and_recovery
from ocs_ci.ocs.bucket_utils import (
    write_data_to_buckets_and_check_integrity,
    verify_s3_buckets_integrity,
    write_object_to_cache,
)


log = logging.getLogger(__name__)


class TestNoobaaBackupRecoverySystemTest(E2ETest):
    """
    Test Noobaa System

    """

    def test_noobaa_backup_recovery(
        self,
        bucket_factory,
        mcg_obj,
        awscli_pod,
        cld_mgr,
        awscli_pod_session,
        test_directory_setup,
        snapshot_factory,
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

        log.info("Create the cached namespace bucket on top of the namespace resource")
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Cache",
                "ttl": 10000,
                "namespacestore_dict": {
                    "aws": [(1, "eu-central-1")],
                },
            },
            "placement_policy": {
                "tiers": [{"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}]
            },
        }
        write_object_to_cache(
            bucket_factory=bucket_factory,
            mcg_obj=mcg_obj,
            cld_mgr=cld_mgr,
            awscli_pod_session=awscli_pod_session,
            test_directory_setup=test_directory_setup,
            bucketclass_dict=bucketclass_dict,
        )

        log.info("")
        noobaa_db_backup_and_recovery(snapshot_factory)

        log.info("Check the data integrity of buckets")
        verify_s3_buckets_integrity(buckets_data_dic)
