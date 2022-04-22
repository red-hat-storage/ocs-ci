import logging

from ocs_ci.framework.testlib import E2ETest, ignore_leftovers, skipif_external_mode
from ocs_ci.ocs.longevity import Longevity
from ocs_ci.ocs import constants


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
        project_factory(project_name=constants.STAGE_0_NAMESPACE)
        long = Longevity()
        long.stage_0(
            num_of_pvc=30,
            num_of_obc=30,
            namespace=constants.STAGE_0_NAMESPACE,
            pvc_size="10Gi",
            ignore_teardown=True,
        )
