import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
    brown_squad,
    skipif_no_lso,
)
from ocs_ci.deployment.baremetal import simulate_ceph_bluestore_on_node_disk
from ocs_ci.ocs.node import get_nodes

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
        results = []
        wnodes = get_nodes()
        for wnode in wnodes:
            result = simulate_ceph_bluestore_on_node_disk(wnode)
            results.append(result)

        assert all(results), "BlueStore label simulation failed"
        log.info("BlueStore label simulation succeeded ")
