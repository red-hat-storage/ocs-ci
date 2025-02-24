import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    polarion_id,
    skipif_ocs_version,
)
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.15")
class TestRwopPvcReclaimSpace(ManageTest):
    """
    Tests Reclaim Space on ReadWriteOncePod RBD PVC
    """

    @polarion_id("OCS-5923")
    def test_rwop_pvc_reclaim_space(self, pvc_factory):
        """
        Test to verify creation of reclaim space job and reclaim space cron job on RWOP pvc
        """

        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_mode=constants.ACCESS_MODE_RWOP,
            size=10,
        )

        schedule = "weekly"
        reclaim_space_job = pvc_obj.create_reclaim_space_cronjob(schedule)
        helpers.wait_for_reclaim_space_cronjob(reclaim_space_job, schedule)

        reclaim_space_job = pvc_obj.create_reclaim_space_job()

        helpers.wait_for_reclaim_space_job(reclaim_space_job)
