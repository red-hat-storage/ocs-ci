"""
Test to verify cluster health/stability when it's full (82%)
"""

import logging
import pytest
import time
from ocs_ci.framework import config
from ocs_ci.utility.utils import TimeoutSampler
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
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.benchmark_operator_fio import get_file_size, BenchmarkOperatorFIO
from ocs_ci.helpers.managed_services import (
    verify_osd_used_capacity_greater_than_expected,
)

logger = logging.getLogger(__name__)


@magenta_squad
class TestFullClusterHealth(PASTest):
    """
    Test Cluster health when storage is ~82%
    """

    TIMEOUT_CEPH_MGR = 600
    TIMEOUT_CEPH_MON = 600
    TIMEOUT_CEPH_OSD = 900

    @pytest.fixture(autouse=True)
    def setup(self, request, nodes):
        """
        Setting up test parameters
        """

        logger.test_step("Fill cluster to 82% capacity using benchmark operator")

        logger.info("Calculating workload size to fill cluster to 82%")
        size = get_file_size(82)

        logger.info(f"Initializing benchmark operator with total_size={size}")
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(
            total_size=size,
        )
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=True)
        self.benchmark_operator_teardown = True  # guard: cleanup needed in teardown

        logger.test_step("Verify cluster capacity reached 82%")
        logger.info("Polling OSD utilization until at least one OSD exceeds 82%")
        sampler = TimeoutSampler(
            timeout=600,
            sleep=30,
            func=verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=82.0,
        )
        if not sampler.wait_for_func_status(result=True):
            raise TimeoutExpiredError(
                "Cluster capacity did not reach 82% within 600s after IO completion"
            )

        def teardown():
            if getattr(self, "benchmark_operator_teardown", False):
                logger.info("Teardown: Cleanup benchmark-operator resources")
                logger.info("Deleting benchmark-operator PVCs")
                self.benchmark_obj.cleanup()

            logger.info("Running environment cleanup")
            nodes.restart_nodes_by_stop_and_start_teardown()

        logger.info("Benchmark setup completed successfully - cluster at ~82% capacity")

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
            f"Waiting for 2 MGR pods to reach Running state (timeout: {self.TIMEOUT_CEPH_MGR}s)"
        )
        mgr_running = pod_obj.wait_for_resource(
            condition="Running",
            selector=MGR_APP_LABEL,
            resource_count=2,
            timeout=self.TIMEOUT_CEPH_MGR,
        )
        logger.assertion(f"MGR pods (2) Running state: {mgr_running}")
        assert (
            mgr_running
        ), f"MGR pods did not reach Running state within {self.TIMEOUT_CEPH_MGR}s"

        logger.info(
            f"Waiting for 3 MON pods to reach Running state (timeout: {self.TIMEOUT_CEPH_MON}s)"
        )
        mon_running = pod_obj.wait_for_resource(
            condition="Running",
            selector=MON_APP_LABEL,
            resource_count=3,
            timeout=self.TIMEOUT_CEPH_MON,
        )
        logger.assertion(f"MON pods (3) Running state: {mon_running}")
        assert (
            mon_running
        ), f"MON pods did not reach Running state within {self.TIMEOUT_CEPH_MON}s"

        logger.info(
            f"Waiting for 3 OSD pods to reach Running state (timeout: {self.TIMEOUT_CEPH_OSD}s)"
        )
        osd_running = pod_obj.wait_for_resource(
            condition="Running",
            selector=OSD_APP_LABEL,
            resource_count=3,
            timeout=self.TIMEOUT_CEPH_OSD,
        )
        logger.assertion(f"OSD pods (3) Running state: {osd_running}")
        assert (
            osd_running
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

    def wait_for_ceph_health_ok_or_warn(self, ceph_recovery_timeout=600):
        """
        Wait until Ceph health is HEALTH_OK or HEALTH_WARN within the given timeout.

        HEALTH_WARN is the expected steady state when the cluster is near-full
        (82%) — nearfull OSD warnings are normal and accepted.
        Only HEALTH_ERR is treated as a failure; we keep retrying until the
        cluster recovers or the timeout expires.

        Args:
            ceph_recovery_timeout (int): Total seconds to retry while Ceph is
                in HEALTH_ERR. Uses a fixed 30s sleep between retries.
                Pass 0 for a single immediate check with no retries.

        Returns:
            bool: True if Ceph reaches HEALTH_OK or HEALTH_WARN within the
                timeout, False if HEALTH_ERR persists until timeout expires.
        """
        sleep = 30

        # TimeoutSampler requires timeout > sleep; when the caller passes 0
        # (single immediate check, no retries) fall back to a direct call.
        if ceph_recovery_timeout == 0:
            status = self.ceph_cluster.get_ceph_health()
            logger.info(f"Ceph health (single check): {status}")
            return "HEALTH_OK" in status or "HEALTH_WARN" in status

        def _check():
            status = self.ceph_cluster.get_ceph_health()
            logger.info(f"Ceph health: {status}")
            if "HEALTH_OK" in status or "HEALTH_WARN" in status:
                return True
            logger.warning(f"Ceph is in {status!r}, retrying in {sleep}s")
            return False

        sampler = TimeoutSampler(
            timeout=ceph_recovery_timeout,
            sleep=sleep,
            func=_check,
        )
        if not sampler.wait_for_func_status(result=True):
            logger.warning(
                f"Ceph remained in HEALTH_ERR after {ceph_recovery_timeout}s"
            )
            return False
        return True

    def is_cluster_healthy(self, ceph_recovery_timeout=600):
        """
        Wrapper function for cluster health check.

        Waits for Ceph to reach HEALTH_OK or HEALTH_WARN (HEALTH_ERR triggers
        retries up to ceph_recovery_timeout). HEALTH_WARN is the expected
        steady state for a near-full cluster and is treated as healthy.

        Args:
            ceph_recovery_timeout (int): Total seconds to wait for Ceph to exit
                HEALTH_ERR. Pass 0 for a single immediate Ceph check.

        Returns:
            bool: True if Ceph is HEALTH_OK/HEALTH_WARN AND all pods are running.
        """
        logger.debug(
            f"Starting cluster health check with ceph_recovery_timeout={ceph_recovery_timeout}s"
        )

        ceph_healthy = self.wait_for_ceph_health_ok_or_warn(
            ceph_recovery_timeout=ceph_recovery_timeout
        )

        logger.info("Verifying all pods are running (timeout: 1200s)")
        pods_running = pod.wait_for_pods_to_be_running(timeout=1200)

        logger.info(
            f"Cluster health check result: ceph_healthy={ceph_healthy}, "
            f"pods_running={pods_running}"
        )

        return ceph_healthy and pods_running

    @system_test
    @polarion_id("OCS-2749")
    def test_full_cluster_health(
        self,
        nodes,
    ):
        """
        Verify that the cluster health is ok when the storage is ~82% full

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
        logger.test_step("Verify cluster health before disruptive operations")
        pre_test_healthy = self.is_cluster_healthy(ceph_recovery_timeout=0)
        logger.assertion(f"Pre-test cluster health: healthy={pre_test_healthy}")
        assert (
            pre_test_healthy
        ), "Cluster is not healthy before starting disruptive operations"

        logger.test_step("Reboot OSD node and verify cluster recovery")
        osd_node_reboot()
        logger.info("Waiting for all nodes to reach Ready status after OSD node reboot")
        wait_for_nodes_status(timeout=900)
        post_osd_healthy = self.is_cluster_healthy()
        logger.assertion(
            f"Post OSD node reboot cluster health: healthy={post_osd_healthy}"
        )
        assert post_osd_healthy, "Cluster is not healthy after OSD node reboot"

        logger.test_step("Restart MGR pod node and verify cluster recovery")
        self.mgr_pod_node_restart()
        post_mgr_healthy = self.is_cluster_healthy()
        logger.assertion(
            f"Post MGR node restart cluster health: healthy={post_mgr_healthy}"
        )
        assert (
            post_mgr_healthy
        ), "Cluster is not healthy after MGR pod node restart (worker node shutdown)"

        logger.test_step("Restart OCS operator node and verify cluster recovery")
        self.restart_ocs_operator_node()
        post_ocs_healthy = self.is_cluster_healthy()
        logger.assertion(
            f"Post OCS operator node restart cluster health: healthy={post_ocs_healthy}"
        )
        assert (
            post_ocs_healthy
        ), "Cluster is not healthy after OCS operator node restart"

        logger.test_step("Delete Rook, OSD, MGR & MON pods and verify cluster recovery")
        self.delete_pods()
        post_delete_healthy = self.is_cluster_healthy()
        logger.assertion(
            f"Post pod deletion cluster health: healthy={post_delete_healthy}"
        )
        assert (
            post_delete_healthy
        ), "Cluster is not healthy after Rook, OSD, MGR & MON pods deletion"

        logger.info("All cluster resilience tests completed successfully")
