import logging

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.longevity_stage2 import stage2

logger = logging.getLogger(__name__)


class TestLongevityStage2(E2ETest):
    """
    Tests Longevity Testing - Stage 2
    """

    def test_longevity_stage2(
       self, multi_pvc_pod_lifecycle_factory, multi_obc_lifecycle_factory
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
        stage2(multi_pvc_pod_lifecycle_factory, multi_obc_lifecycle_factory)
