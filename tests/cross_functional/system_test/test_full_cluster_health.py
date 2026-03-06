"""
Test to verify cluster health/stability when it's full (85%)
"""

import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    polarion_id,
    magenta_squad,
    ignore_leftovers,
)
from ocs_ci.helpers.helpers import wait_for_ct_pod_recovery
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError

logger = logging.getLogger(__name__)

from ocs_ci.ocs.benchmark_operator_fio import get_file_size
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.helpers.managed_services import (
    verify_osd_used_capacity_greater_than_expected,
)

from ocs_ci.ocs.cluster import (
    change_ceph_full_ratio,
)


# Expected leftovers (ignore_leftovers) after disruptive steps in this test:
#   - MON pod identity change (e.g. mon-c -> mon-d) after node restarts
#   - Benchmark-operator PVs after pod deletions
@ignore_leftovers
@magenta_squad
class TestFullClusterHealth(PASTest):
    """
    Test Cluster health when storage is ~85%
    """

    TIMEOUT_CEPH_MGR = 900
    TIMEOUT_CEPH_MON = 900
    TIMEOUT_CEPH_OSD = 1600
    TIMEOUT_POD_RUNNING = 1500
    TIMEOUT_BENCHMARK_SETUP = 2500

    @pytest.fixture(autouse=True)
    def setup(self, request, nodes):
        """
        Setting up test parameters
        """

        logger.info("Starting the test setup")
        logger.info(
            "Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator"
        )
        size = get_file_size(100)
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=False)
        self.benchmark_operator_teardown = True

        logger.info("Verify used capacity bigger than 85%")
        sample = TimeoutSampler(
            timeout=self.TIMEOUT_BENCHMARK_SETUP,
            sleep=40,
            func=verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )

        if not sample.wait_for_func_status(result=True):
            logger.error(
                "After %s seconds the used capacity was still below 85%%",
                self.TIMEOUT_BENCHMARK_SETUP,
            )
            raise TimeoutExpiredError

        def teardown():
            if self.benchmark_obj:
                logger.info("Change Ceph full_ratio from 85% to 95%")
                logger.info(
                    "Based on doc we need to change the ceph_full_ratio to 88%, but we run "
                    "many fio pods therefore, it may not be enough to increase by only 3%"
                )
                change_ceph_full_ratio(95)

                logger.info("Delete  benchmark-operator PVCs")
                self.benchmark_obj.cleanup()
                self.benchmark_operator_teardown = False

            logger.info("cleanup the environment")
            nodes.restart_nodes_by_stop_and_start_teardown()

            change_ceph_full_ratio(85)

        logger.info("Benchmark setup completed. Cluster at ~85% capacity")

        request.addfinalizer(teardown)

        self.ceph_cluster = CephCluster()
        self.nodes = None

        # Save benchmark_obj before parent setup(); PASTest.setup() sets self.benchmark_obj = None
        benchmark_obj = self.benchmark_obj
        super(TestFullClusterHealth, self).setup()
        self.benchmark_obj = benchmark_obj
        assert self.is_cluster_healthy(), "Cluster is not healthy"

    def delete_pods(self):
        """
        Try to delete pods:
            - Rook operator
            - OSD
            - MGR
            - MON
        """
        pod_list = []
        rook_operator_pod = pod.get_ocs_operator_pod(
            ocs_label=constants.OPERATOR_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        pod_list.append(rook_operator_pod)

        osd_pods = pod.get_osd_pods()
        pod_list.extend(osd_pods)

        mgr_pods = pod.get_mgr_pods()
        pod_list.extend(mgr_pods)

        mon_pods = pod.get_mon_pods()
        pod_list.extend(mon_pods)

        logger.info(f"Deleting pods: {[p.name for p in pod_list]}")
        pod.delete_pods(pod_objs=pod_list)

    def ceph_not_health_error(self):
        """
        Check if Ceph is NOT in "HEALTH_ERR" state
        Warning state is ok since the cluster is low in storage space

        Returns:
            bool: True if Ceph state is NOT "HEALTH_ERR"
        """
        ceph_status = self.ceph_cluster.get_ceph_health()
        logger.info(f"Ceph status is: {ceph_status}")
        return ceph_status != "HEALTH_ERR"

    def mgr_pod_node_restart(self):
        """
        Restart node that runs mgr pod
        """
        mgr_pod_obj = pod.get_mgr_pods()
        mgr_node_obj = pod.get_pod_node(mgr_pod_obj[0])

        self.nodes.restart_nodes([mgr_node_obj])

        wait_for_nodes_status()

        # Check for Ceph pods
        pod_obj = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-mgr",
            timeout=self.TIMEOUT_CEPH_MGR,
        )
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-mon",
            resource_count=3,
            timeout=self.TIMEOUT_CEPH_MON,
        )
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-osd",
            resource_count=3,
            timeout=self.TIMEOUT_CEPH_OSD,
        )

    def restart_ocs_operator_node(self):
        """
        Restart node that runs OCS operator pod
        """

        pod_obj = pod.get_ocs_operator_pod()
        node_obj = pod.get_pod_node(pod_obj)

        self.nodes.restart_nodes([node_obj])

        wait_for_nodes_status()
        time.sleep(180)
        pod.wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"], pod_names=[pod_obj.name]
        )

    def is_cluster_healthy(self):
        """
        Wrapper function for cluster health check

        Returns:
            bool: True if ALL checks passed, False otherwise
        """
        return self.ceph_not_health_error() and pod.wait_for_pods_to_be_running(
            timeout=self.TIMEOUT_POD_RUNNING
        )

    def reload_ceph_cluster(self):
        """
        Refresh the Ceph cluster object state from the API.

        This method should be called after disruptive operations (node reboot,
        pod deletion) to ensure health checks use current pod references instead
        of stale ones. It waits for toolbox recovery before scanning.

        Raises:
            TimeoutExpiredError: If toolbox recovery times out
        """
        assert (
            wait_for_ct_pod_recovery()
        ), "Ceph tools pod failed to come up on another node"
        self.ceph_cluster.scan_cluster()
        logger.debug("Ceph cluster object refreshed (toolbox and pod refs updated)")

    @system_test
    @polarion_id("OCS-2749")
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

        # Commented below cod due to Bug: DFBUGS-5633
        # logger.info("Checking health before disruptive operations")
        # assert self.is_cluster_healthy(), "Cluster is not healthy"
        # osd_node_reboot()
        # logger.info("Checking health after OSD node reboot")
        # time.sleep(180)
        # self.reload_ceph_cluster()
        # assert self.is_cluster_healthy(), "Cluster is not healthy"

        logger.info("Starting MGR pod node restart (worker node shutdown)")
        self.mgr_pod_node_restart()
        logger.info("Checking health after worker node shutdown")
        time.sleep(300)
        assert self.is_cluster_healthy(), "Cluster is not healthy"

        logger.info("Starting OCS operator node restart")
        self.restart_ocs_operator_node()
        logger.info("Checking health after OCS operator node restart")
        time.sleep(300)
        self.reload_ceph_cluster()
        assert self.is_cluster_healthy(), "Cluster is not healthy"

        logger.info("Starting Rook, OSD, MGR & MON pods deletion")
        self.delete_pods()
        logger.info("Checking health after Rook, OSD, MGR & MON pods deletion")
        assert self.is_cluster_healthy(), "Cluster is not healthy"
