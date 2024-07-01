import logging

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, ignore_leftovers, skipif_external_mode
from ocs_ci.ocs.longevity import Longevity


log = logging.getLogger(__name__)


@magenta_squad
@skipif_external_mode
@ignore_leftovers
class TestLongevity(E2ETest):
    """
    Test class for Longevity: Stage-3
    """

    def test_stage_3(self, project_factory):
        """
        This test starts Longevity Stage3
        In Longevity Stage3 below operations are run concurrently for the specified run time
        Concurrent bulk operations of following
            1) PVC creation -> all supported types  (RBD, CephFS, RBD-block)
            2) PVC deletion -> all supported types  (RBD, CephFS, RBD-block)
            3) OBC creation
            4) OBC deletion
            5) APP pod creation -> all supported types  (RBD, CephFS, RBD-block)
            6) APP pod deletion -> all supported types  (RBD, CephFS, RBD-block)

        """
        # Start Longevity Stage3
        long = Longevity()
        log.info("Starting Longevity Stage3 execution")
        long.stage_3(
            project_factory,
            num_of_pvc=150,
            num_of_obc=150,
            collect_cluster_sanity_checks=True,
            run_time=180,
            delay=60,
        )
