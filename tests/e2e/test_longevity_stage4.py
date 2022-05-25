import logging

from ocs_ci.framework.testlib import E2ETest

from ocs_ci.ocs.longevity_stage4 import stage4

logger = logging.getLogger(__name__)


class TestLongevityStage4(E2ETest):
    """
    Tests Longevity Testing - Stage 4
    """

    def test_longevity_stage4(
        self,
        project_factory,
        multi_pvc_factory,
        pod_factory,
        multi_pvc_clone_factory,
        multi_snapshot_factory,
        multi_snapshot_restore_factory,
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
        stage4(
            project_factory,
            multi_pvc_factory,
            pod_factory,
            multi_pvc_clone_factory,
            multi_snapshot_factory,
            multi_snapshot_restore_factory,
            teardown_factory,
        )
