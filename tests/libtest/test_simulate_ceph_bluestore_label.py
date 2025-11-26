import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
    brown_squad,
    skipif_no_lso,
)

from ocs_ci.framework import config
from ocs_ci.deployment.helpers.ceph_cluster import (
    simulate_full_ceph_bluestore_process_on_wnodes,
)

log = logging.getLogger(__name__)


@brown_squad
@libtest
@ignore_leftovers
@skipif_no_lso
class TestSimulateCephBlueStoreLabel(ManageTest):
    """
    Test that simulate_bluestore_label correctly stamps and verifies a BlueStore label on a test disk.
    """

    def test_simulate_bluestore_label_on_worker_nodes(self):
        """
        Test simulates a Ceph BlueStore label on the worker node disks.

        """
        simulate_bluestore_label = config.ENV_DATA.get(
            "simulate_bluestore_label", False
        )
        if simulate_bluestore_label:
            simulate_full_ceph_bluestore_process_on_wnodes()
            log.info("BlueStore label simulation succeeded on all worker nodes disks")
