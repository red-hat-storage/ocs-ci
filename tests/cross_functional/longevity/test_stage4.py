import logging

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.longevity import Longevity

logger = logging.getLogger(__name__)


@magenta_squad
class TestLongevityStage4(E2ETest):
    """
    Tests Longevity Testing - Stage 4
    """

    def test_longevity_stage4(
        self,
        project_factory,
        multi_pvc_pod_lifecycle_factory,
        pod_factory,
        multi_pvc_clone_factory,
        multi_snapshot_factory,
        snapshot_restore_factory,
        teardown_factory,
    ):
        """
        Tests Longevity Testing - Stage 4

        1. PVC, POD Creation + fill data upto 25% of mount point space
        2. Clone - Creation, Deletion
        3. Snapshot - Creation, Restoration, Deletion
        4. Expansion of original PVCs
        5. PVC, POD deletion

        """
        long = Longevity()
        long.stage_4(
            project_factory,
            multi_pvc_pod_lifecycle_factory,
            pod_factory,
            multi_pvc_clone_factory,
            multi_snapshot_factory,
            snapshot_restore_factory,
            teardown_factory,
            run_time=180,
            delay=60,
            collect_cluster_sanity_checks=True,
        )
