"""
Test to verify cluster health/stability when it's full (85%)
"""

import logging
import pytest
import time
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.constants import MGR_APP_LABEL, MON_APP_LABEL, OSD_APP_LABEL
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.disruptive_operations import osd_node_reboot
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    polarion_id,
    magenta_squad,
    jira,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.cluster import (
    change_ceph_full_ratio,
)

logger = logging.getLogger(__name__)

from ocs_ci.ocs.benchmark_operator_fio import get_file_size, BenchmarkOperatorFIO
from ocs_ci.helpers.managed_services import (
    verify_osd_used_capacity_greater_than_expected,
)


@magenta_squad
class TestFullClusterHealth(PASTest):
    """
    Test Cluster health when storage is ~85%
    """

    TIMEOUT_CEPH_MGR = 600
    TIMEOUT_CEPH_MON = 1500
    TIMEOUT_CEPH_OSD = 600
    TIMEOUT_POD_RUNNING = 600
    TIMEOUT_BENCHMARK_SETUP = 2500

    @pytest.fixture(autouse=True)
    def setup(self, request, nodes):
        """
        Setting up test parameters
        """

        logger.info("Setup test environment with cluster at 85% capacity")
        logger.info("Starting full cluster health test setup")

        logger.info("Calculating workload size to fill cluster to 85%")
        size = get_file_size(100)

        logger.info(f"Initializing benchmark operator with total_size={size}")
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=False)
        self.benchmark_operator_teardown = True

        logger.info(
            f"Waiting for cluster capacity to reach 85% (timeout: {self.TIMEOUT_BENCHMARK_SETUP}s)"
        )
        sample = TimeoutSampler(
            timeout=self.TIMEOUT_BENCHMARK_SETUP,
            sleep=40,
            func=verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )

        if not sample.wait_for_func_status(result=True):
            logger.error(
                f"Cluster capacity did not reach 85% after {self.TIMEOUT_BENCHMARK_SETUP}s timeout"
            )
            raise TimeoutExpiredError

        def teardown():
            if self.benchmark_obj:
                logger.info("Teardown: Reset Ceph configuration and cleanup resources")
                logger.info("Changing Ceph full_ratio from 85% to 95%")
                change_ceph_full_ratio(95)

                logger.info("Deleting benchmark-operator PVCs")
                self.benchmark_obj.cleanup()
                self.benchmark_operator_teardown = False

            logger.info("Running environment cleanup")
            nodes.restart_nodes_by_stop_and_start_teardown()

            logger.info("Resetting Ceph full_ratio to 85%")
            change_ceph_full_ratio(85)

        logger.info("Benchmark setup completed successfully - cluster at ~85% capacity")

        request.addfinalizer(teardown)

        self.ceph_cluster = CephCluster()
        self.nodes = None

        # Save benchmark_obj before parent setup(); PASTest.setup() sets self.benchmark_obj = None
        benchmark_obj = self.benchmark_obj
        super(TestFullClusterHealth, self).setup()
        self.benchmark_obj = benchmark_obj

    def delete_pods(self):
        """
        Try to delete pods:
            - Rook operator
            - OSD
            - MGR
            - MON
        """
        logger.info("Collecting Rook operator, OSD, MGR, and MON pods for deletion")
        pod_list = []

        rook_operator_pod = pod.get_ocs_operator_pod(
            ocs_label=constants.OPERATOR_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        pod_list.append(rook_operator_pod)
        logger.debug(f"Found Rook operator pod: {rook_operator_pod.name}")

        osd_pods = pod.get_osd_pods()
        pod_list.extend(osd_pods)
        logger.debug(f"Found {len(osd_pods)} OSD pods")

        mgr_pods = pod.get_mgr_pods()
        pod_list.extend(mgr_pods)
        logger.debug(f"Found {len(mgr_pods)} MGR pods")

        mon_pods = pod.get_mon_pods()
        pod_list.extend(mon_pods)
        logger.debug(f"Found {len(mon_pods)} MON pods")

        logger.info(f"Deleting {len(pod_list)} pods: {[p.name for p in pod_list]}")
        pod.delete_pods(pod_objs=pod_list)

    def mgr_pod_node_restart(self):
        """
        Restart node that runs mgr pod
        """
        logger.info("Identifying MGR pod and its node")
        mgr_pod_obj = pod.get_mgr_pods()
        mgr_node_obj = pod.get_pod_node(mgr_pod_obj[0])
        logger.info(
            f"MGR pod '{mgr_pod_obj[0].name}' running on node '{mgr_node_obj.name}'"
        )

        logger.info(f"Restarting node: {mgr_node_obj.name}")
        self.nodes.restart_nodes([mgr_node_obj])

        logger.info("Waiting for all nodes to reach Ready status")
        wait_for_nodes_status()

        # Check for Ceph pods
        logger.info("Verifying Ceph pods are running after node restart")
        pod_obj = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )

        logger.info(
            f"MGR pod check: waiting for Running state (timeout: {self.TIMEOUT_CEPH_MGR}s)"
        )
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector=MGR_APP_LABEL,
            timeout=self.TIMEOUT_CEPH_MGR,
        ), f"MGR pod did not reach Running state within {self.TIMEOUT_CEPH_MGR}s"

        logger.info(
            f"MON pods check: waiting for 3 pods in Running state (timeout: {self.TIMEOUT_CEPH_MON}s)"
        )
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector=MON_APP_LABEL,
            resource_count=3,
            timeout=self.TIMEOUT_CEPH_MON,
        ), f"MON pods did not reach Running state within {self.TIMEOUT_CEPH_MON}s"

        logger.info(
            f"OSD pods check: waiting for 3 pods in Running state (timeout: {self.TIMEOUT_CEPH_OSD}s)"
        )
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector=OSD_APP_LABEL,
            resource_count=3,
            timeout=self.TIMEOUT_CEPH_OSD,
        ), f"OSD pods did not reach Running state within {self.TIMEOUT_CEPH_OSD}s"

        logger.info("All Ceph pods verified running after MGR node restart")

    def restart_ocs_operator_node(self):
        """
        Restart node that runs OCS operator pod
        """
        logger.info("Identifying OCS operator pod and its node")
        pod_obj = pod.get_ocs_operator_pod()
        node_obj = pod.get_pod_node(pod_obj)
        logger.info(
            f"OCS operator pod '{pod_obj.name}' running on node '{node_obj.name}'"
        )

        logger.info(f"Restarting OCS operator node: {node_obj.name}")
        self.nodes.restart_nodes([node_obj])

        logger.info("Waiting for all nodes to reach Ready status")
        wait_for_nodes_status()

        logger.info("Waiting 180s for cluster stabilization after node restart")
        time.sleep(180)

        logger.info(
            f"Verifying OCS operator pod '{pod_obj.name}' is running (timeout: 300s)"
        )
        pod.wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            pod_names=[pod_obj.name],
            timeout=300,
        )
        logger.info("OCS operator pod verified running after node restart")

    def is_cluster_healthy(self, ceph_recovery_timeout=600):
        """
        Wrapper function for cluster health check

        Args:
            ceph_recovery_timeout (int): Time to wait for Ceph to recover (default: 600s/10min)

        Returns:
            bool: True if ALL checks passed (Ceph healthy AND all pods running), False otherwise
        """
        logger.debug(
            f"Starting cluster health check with ceph_recovery_timeout={ceph_recovery_timeout}s"
        )
        start_time = time.time()

        logger.info("Verifying all pods are running (timeout: 1200s)")
        pods_running = pod.wait_for_pods_to_be_running(timeout=1200)

        execution_time = time.time() - start_time

        logger.info(
            f"Cluster health check completed in {execution_time:.2f}s: "
            f" pods_running={pods_running}"
        )

        return pods_running

    @system_test
    @polarion_id("OCS-2749")
    @jira("DFBUGS-5769")
    def test_full_cluster_health(
        self,
        nodes,
    ):
        """
        Verify that the cluster health is ok when the storage is ~85% full

        Steps:
          1. Deploy benchmark operator and run fio workload
          2. Check Ceph health before/after each operation:
            2.1 Osd node reboot
            2.2 Mgr node reboot
            2.3 OCS operator node reboot
            2.4 Delete Rook, OSD, MGR & MON pods
            2.5 Creation and deletion of resources

        """
        self.nodes = nodes

        logger.info(
            "Pre-test cluster health: checking if cluster is healthy before starting tests"
        )
        assert self.is_cluster_healthy(
            ceph_recovery_timeout=0
        ), "Cluster is not healthy before starting disruptive operations"

        logger.info("Executing OSD node reboot")
        osd_node_reboot()
        logger.info("Post OSD node reboot: verifying cluster health")
        assert self.is_cluster_healthy(), "Cluster is not healthy after OSD node reboot"

        logger.info("Executing MGR pod node restart (worker node shutdown)")
        self.mgr_pod_node_restart()
        logger.info("Post MGR node restart: verifying cluster health")
        assert (
            self.is_cluster_healthy()
        ), "Cluster is not healthy after MGR pod node restart (worker node shutdown)"

        logger.info("Executing OCS operator node restart")
        self.restart_ocs_operator_node()
        logger.info("Post OCS operator node restart: verifying cluster health")
        assert (
            self.is_cluster_healthy()
        ), "Cluster is not healthy after OCS operator node restart"

        logger.info("Executing Rook, OSD, MGR & MON pods deletion")
        self.delete_pods()
        logger.info("Post pod deletion: verifying cluster health and pod recovery")
        assert (
            self.is_cluster_healthy()
        ), "Cluster is not healthy after Rook, OSD, MGR & MON pods deletion"

        logger.info("All cluster resilience tests completed successfully")
