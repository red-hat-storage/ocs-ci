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
from ocs_ci.ocs.disruptive_operations import osd_node_reboot
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    polarion_id,
    magenta_squad,
)
from ocs_ci.helpers import sanity_helpers
from ocs_ci.helpers.helpers import wait_for_ct_pod_recovery
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError

logger = logging.getLogger(__name__)

log = logging.getLogger(__name__)
from ocs_ci.ocs.benchmark_operator_fio import get_file_size
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.helpers.managed_services import (
    verify_osd_used_capacity_greater_than_expected,
)

from ocs_ci.ocs.cluster import (
    change_ceph_full_ratio,
)


@magenta_squad
class TestFullClusterHealth(PASTest):
    """
    Test Cluster health when storage is ~85%
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, nodes):
        """
        Setting up test parameters
        """

        def teardown():

            if self.benchmark_obj:
                log.info("Change Ceph full_ratio from from 85% to 95%")
                log.info(
                    "Based on doc we need to change the ceph_full_ratio to 88%, but we run "
                    "many fio pods therefore, it may not be enough to increase by only 3%"
                )
                change_ceph_full_ratio(95)

                log.info("Delete  benchmark-operator PVCs")
                self.benchmark_obj.cleanup()
                self.benchmark_operator_teardown = False

            logger.info("cleanup the environment")
            nodes.restart_nodes_by_stop_and_start_teardown()

            change_ceph_full_ratio(85)

        request.addfinalizer(teardown)

        # logger.info("Starting the test setup")
        self.percent_to_fill = 25.0
        self.ceph_cluster = CephCluster()
        self.nodes = None
        self.benchmark_name = "FIO"
        self.client_pod_name = "fio-client"
        #
        self.sanity_helpers = sanity_helpers.Sanity()
        #
        super(TestFullClusterHealth, self).setup()
        # # deploy the benchmark-operator
        # self.deploy_benchmark_operator()

    # def run(self):
    #     """
    #
    #     Run the test, and wait until it finished
    #     """
    #
    #     self.deploy_and_wait_for_wl_to_start(timeout=900)
    #     self.wait_for_wl_to_finish(sleep=300)
    #
    #     try:
    #         if "Fio failed to execute" not in self.test_logs:
    #             logger.info("FIO has completed successfully")
    #     except IOError:
    #         logger.warning("FIO failed to complete")
    #
    # def calculate_crd_data(self):
    #     """
    #     Getting the storage capacity and calculate pod count and pvc size
    #
    #     """
    #
    #     ceph_used_capacity_percent = get_percent_used_capacity()
    #     logger.info(f"Ceph used capacity percent is {ceph_used_capacity_percent}%")
    #
    #     ceph_capacity = self.ceph_cluster.get_ceph_capacity()
    #     logger.info(f"Total storage capacity is {ceph_capacity} GiB")
    #
    #     self.percent_to_fill = self.percent_to_fill - ceph_used_capacity_percent
    #     logger.info(f"Percentage to fill is {self.percent_to_fill}%")
    #
    #     self.total_data_set = int(ceph_capacity * (int(self.percent_to_fill) / 100))
    #     self.filesize = int(
    #         self.crd_data["spec"]["workload"]["args"]["filesize"].replace("GiB", "")
    #     )
    #
    #     # Make sure that filesize>=10 and servers<=60
    #     self.servers = 60
    #     self.filesize = int(self.total_data_set / self.servers)
    #     if self.filesize < 10:
    #         self.filesize = 10
    #         self.servers = int(self.total_data_set / self.filesize)
    #
    #     self.crd_data["spec"]["workload"]["args"]["filesize"] = f"{self.filesize}GiB"
    #     self.crd_data["spec"]["workload"]["args"][
    #         "storagesize"
    #     ] = f"{int(self.total_data_set)}Gi"
    #     self.crd_data["spec"]["workload"]["args"]["servers"] = self.servers
    #     self.crd_data["spec"]["workload"]["args"]["bs"] = "1024KiB"
    #     self.crd_data["spec"]["workload"]["args"]["jobs"] = ["write", "read"]
    #     self.crd_data["spec"]["workload"]["args"]["iodepth"] = 1

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
            condition="Running", selector="app=rook-ceph-mgr", timeout=900
        )
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-mon",
            resource_count=3,
            timeout=900,
        )
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-osd",
            resource_count=3,
            timeout=900,
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
            timeout=1200
        )

    def reload_ceph_cluster(self):
        """
        Refresh the Ceph cluster object state from the API (toolbox and other pods).

        Use after disruptive operations (node reboot, pod deletion) so
        is_cluster_healthy() and ceph_not_health_error() use the current
        ceph-rook-tools pod instead of a stale reference. Waits for toolbox
        recovery first so the refreshed object gets the new running pod.
        """
        wait_for_ct_pod_recovery()
        self.ceph_cluster.scan_cluster()
        logger.debug("Ceph cluster object refreshed (toolbox and pod refs updated)")

    @system_test
    @polarion_id("OCS-2749")
    def test_full_cluster_health(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
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
        #
        # self.full_log_path = get_full_test_logs_path(cname=self)
        # logger.info(f"Logs file path name is : {self.full_log_path}")

        # logger.info("Create resource file for fio workload")
        # self.crd_data = templating.load_yaml(constants.FIO_CR_YAML)
        # self.calculate_crd_data()

        # self.set_storageclass(interface=constants.CEPHBLOCKPOOL)

        # self.run()

        log.info(
            "Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator"
        )
        size = get_file_size(100)
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=False)
        self.benchmark_operator_teardown = True

        log.info("Verify used capacity bigger than 85%")
        sample = TimeoutSampler(
            timeout=2500,
            sleep=40,
            func=verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )

        if not sample.wait_for_func_status(result=True):
            log.error("The after 1800 seconds the used capacity smaller than 85%")
            raise TimeoutExpiredError

        logger.info("Checking health before disruptive operations")
        assert self.is_cluster_healthy(), "Cluster is not healthy"
        osd_node_reboot()
        logger.info("Checking health after OSD node reboot")
        time.sleep(180)
        self.reload_ceph_cluster()
        assert self.is_cluster_healthy(), "Cluster is not healthy"

        self.mgr_pod_node_restart()
        logger.info("Checking health after worker node shutdown")
        time.sleep(300)
        self.reload_ceph_cluster()
        assert self.is_cluster_healthy(), "Cluster is not healthy"

        self.restart_ocs_operator_node()
        logger.info("Checking health after OCS operator node restart")
        time.sleep(300)
        self.reload_ceph_cluster()
        assert self.is_cluster_healthy(), "Cluster is not healthy"

        self.delete_pods()
        logger.info("Checking health after Rook, OSD, MGR & MON pods deletion")
        self.reload_ceph_cluster()
        assert self.is_cluster_healthy(), "Cluster is not healthy"
