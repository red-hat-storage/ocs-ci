import logging
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, skipif_external_mode
from ocs_ci.ocs.longevity import Longevity


log = logging.getLogger(__name__)


@magenta_squad
@skipif_external_mode
class TestLongevity(E2ETest):
    """
    Test class for Longevity: Stage-1
    """

    def test_all_stages(
        self,
        project_factory,
        start_apps_workload,
        multi_pvc_pod_lifecycle_factory,
        multi_obc_lifecycle_factory,
        pod_factory,
        multi_pvc_clone_factory,
        multi_snapshot_factory,
        snapshot_restore_factory,
        teardown_factory,
    ):
        """
        In Longevity testing we make sure that the ODF can continue
        responding to the user and admin operations reliably and consistently under
        significant load for an extended period of time.

        To achieve this the tests are categorized into below stages.

        Stage-0: Creates the initial softconfiguration of all supported types
        on the Longevity cluster and keep the created resources stay forever on the cluster
        Stage-1: Configure and run OCP and Application workloads. These workloads
        will be run continuously for a specified period of time
        Stage-2: Sequential PVC, OBC, APP pod lifecycle operations for a specified period of time
        Stage-3: Concurrent PVC, OBC, APP pod lifecycle operations for a specified period of time
        Stage-4: Clone, Snapshot, Expand operations for a specified period of time

        This test starts all stages of Longevity testing

        """
        long = Longevity()
        long.longevity_all_stages(
            project_factory,
            start_apps_workload,
            multi_pvc_pod_lifecycle_factory,
            multi_obc_lifecycle_factory,
            pod_factory,
            multi_pvc_clone_factory,
            multi_snapshot_factory,
            snapshot_restore_factory,
            teardown_factory,
            apps_run_time=2160,
            stage_run_time=720,
            concurrent=False,
        )
