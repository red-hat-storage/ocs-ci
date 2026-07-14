import logging

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.longevity import Longevity

logger = logging.getLogger(__name__)


@magenta_squad
class TestLongevityStage2(E2ETest):
    """
    Tests Longevity Testing - Stage 2
    """

    def test_longevity_stage2(
        self,
        project_factory,
        multi_pvc_pod_lifecycle_factory,
        setup_mcg_bg_features,
    ):
        """
        Tests Longevity Testing - Stage 2

        1. Sequential operations of following - all supported types (RBD, CephFS, RBD-block):
           PVC creation
           APP pod creation +  fill data upto 25% of mount point space
           APP pod deletion
           PVC deletion
           OBC creation (normal bs obc, ns obc, ns cache obc, bucket replication obcs) + fill some data
           OBC deletion

        2. Bulk create/delete operations without waiting for the individual PVC/POD/OBC to reach its desired state.

        """

        # Num of OBCs is set as 0 owing to the BZ https://bugzilla.redhat.com/show_bug.cgi?id=2090968. As soon as the
        # issue gets resolved we will set it back to default.
        long = Longevity()
        long.stage_2(
            project_factory,
            multi_pvc_pod_lifecycle_factory,
            setup_mcg_bg_features,
            run_pvc_pod_only=True,
            run_mcg_only=True,
            collect_cluster_sanity_checks=True,
            run_time=180,
        )
