import logging

from ocs_ci.framework.testlib import E2ETest, ignore_leftovers, skipif_external_mode
from ocs_ci.ocs.longevity import Longevity


log = logging.getLogger(__name__)


@skipif_external_mode
@ignore_leftovers
class TestLongevity(E2ETest):
    """
    Test class for Longevity: Stage-0
    """

    def test_stage_0(self, project_factory):
        """
        This test creates all the initial soft configuration that is required for
        starting longevity testing. These resources will be created and run forever
        on the cluster
        """
        long = Longevity()
        long.stage_0(
            project_factory,
            num_of_pvc=30,
            num_of_obc=30,
            pvc_size="10Gi",
            ignore_teardown=True,
        )
