import logging
import pytest
import time

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.retry import retry
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    ignore_leftovers,
    polarion_id,
    skipif_ocs_version,
    magenta_squad,
)
from ocs_ci.framework.testlib import E2ETest

log = logging.getLogger(__name__)


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
        snapshot_factory,
        noobaa_db_backup_and_recovery,
        validate_mcg_bg_features,
    ):

        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=bucket_amount,
            object_amount=object_amount,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )

        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)

        # wait 1 min for complete stabilization
        time.sleep(60)

        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=object_amount,
        )
        log.info("No issues seen with the MCG bg feature validation")

