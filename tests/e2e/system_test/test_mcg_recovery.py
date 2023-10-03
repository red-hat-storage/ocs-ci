import logging
import pytest

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
        argvalues=[pytest.param(2, 15)],
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
            skip_any_features=["caching", "nsfs", "rgw kafka"],
        )

        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)
        import time

        time.sleep(60)
        event, threads = validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["caching", "nsfs", "rgw kafka"],
            object_amount=object_amount,
        )

        event.set()
        for th in threads:
            th.result()
        log.info("No issues seen with the MCG bg feature validation")
