import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    ignore_leftovers,
    polarion_id,
    skipif_ocs_version,
    magenta_squad,
)
from ocs_ci.framework.testlib import E2ETest

logger = logging.getLogger(__name__)


@magenta_squad
@system_test
@ignore_leftovers
@polarion_id("OCS-2716")
@skipif_ocs_version("<4.9")
class TestMCGRecovery(E2ETest):
    """
    Test MCG system recovery

    """

    @pytest.mark.parametrize(
        argnames=["bucket_amount", "object_amount"],
        argvalues=[pytest.param(5, 5)],
    )
    def test_mcg_db_backup_recovery(
        self,
        setup_mcg_bg_features,
        bucket_amount,
        object_amount,
        noobaa_db_backup_and_recovery_locally,
        validate_mcg_bg_features,
    ):

        logger.test_step(
            f"Setup MCG background features with {bucket_amount} buckets "
            f"and {object_amount} objects per bucket"
        )
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=bucket_amount,
            object_amount=object_amount,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )
        logger.info("MCG background features configured successfully")

        logger.test_step("Perform NooBaa database backup and recovery")
        noobaa_db_backup_and_recovery_locally()
        logger.info("NooBaa database backup and recovery completed")

        logger.test_step("Wait for system stabilization after recovery")
        logger.info("Waiting 60 seconds for complete stabilization")
        time.sleep(60)

        logger.test_step("Validate MCG background features after recovery")
        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=object_amount,
        )
        logger.info("MCG background feature validation completed successfully")
