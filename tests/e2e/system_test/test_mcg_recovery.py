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
        setup_mcg_system,
        bucket_amount,
        object_amount,
        verify_mcg_system_recovery,
        snapshot_factory,
        noobaa_db_backup_and_recovery,
    ):
        mcg_sys_dict = setup_mcg_system(bucket_amount, object_amount)

        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)

        verify_mcg_system_recovery(mcg_sys_dict)
