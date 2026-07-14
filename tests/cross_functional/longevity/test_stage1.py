import logging
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, skipif_external_mode

log = logging.getLogger(__name__)


@magenta_squad
@skipif_external_mode
class TestLongevity(E2ETest):
    """
    Test class for Longevity: Stage-1
    """

    def test_stage1(self, start_apps_workload):
        """
        This test starts Longevity Stage1
        In Stage 1, we configure and run both OCP and APP workloads
        Detailed steps:
        OCP workloads
        1) Configure openshift-monitoring backed by OCS RBD PVCs
        2) Configure openshift-logging backed by OCS RBD PVCs
        3) Configure openshift-registry backed by OCS CephFs PVC
        APP workloads
        1) Configure and run APP workloads (Pgsql, Couchbase, Cosbench, Jenkins, etc)
        2) Repeat Step-1 and run the workloads continuously for a specified period
        of time

        """
        # Start stage-1
        log.info("Starting Longevity Stage-1")
        # Commenting the OCP workloads code for dry test runs, will be uncomment this code in another PR
        # log.info("Start configuring OCP workloads")
        # start_ocp_workload(workloads_list=['logging','registry', 'monitoring'], run_in_bg=True)
        # Start application workloads and continuously run the workloads for a specified period of time
        log.info("Start running application workloads")
        start_apps_workload(
            workloads_list=["pgsql", "couchbase", "cosbench"],
            run_time=180,
            run_in_bg=True,
        )
