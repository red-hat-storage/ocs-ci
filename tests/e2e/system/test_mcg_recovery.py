import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import tier4a, ignore_leftovers
from ocs_ci.framework.testlib import E2ETest

log = logging.getLogger(__name__)


@ignore_leftovers
class TestMCGRecovery(E2ETest):
    """
    Test MCG system recovery

    """

    @pytest.mark.parametrize(
        argnames=["bucket_amount", "object_amount"],
        argvalues=[
            pytest.param(
                2,
                15,
                marks=[tier4a, pytest.mark.polarion_id("OCS-2716")],
            ),
        ],
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
