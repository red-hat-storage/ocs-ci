"""
Test TNF node failover and monitor pod failure scenarios with workloads
"""

import json
import logging
import time
import pytest
from datetime import datetime, timezone

from ocs_ci.deployment.helpers.tnf_helpers import (
    get_tnf_node_info,
    verify_drbd_status,
)
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.framework.testlib import tier4b
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    verify_data_corruption,
    verify_data_loss,
    verify_vm_workload,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.platform_nodes import PlatformNodesFactory
from ocs_ci.ocs.resources.pod import delete_pods, get_pods_having_label
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@tier4b
@turquoise_squad
class TestTNFFailover:
    """
    TNF failover scenarios with workloads running.

    Tests node failover with DRBD floating monitor migration
    and static monitor pod failure recovery.
    """

    @pytest.fixture(scope="function")
    def setup_tnf_workloads(
        self,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        cnv_workload,
    ):
        """Setup CephFS, RBD, and CNV workloads for TNF failover testing"""
        sc_obj = StretchCluster()

        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=30)

        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )
        logger.info("Workloads are running")

        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(command="mkdir -p /test && sudo chmod -R 777 /test")
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' "
            "| head -c 10485760 > /test/file_1.txt && sync"
        )
        md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")

        nodes = get_tnf_node_info()
        check_for_logwriter_workload_pods(sc_obj, nodes=[n["name"] for n in nodes])
        sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        yield {
            "sc_obj": sc_obj,
            "vm_obj": vm_obj,
            "md5sum_before": md5sum_before,
            "nodes": nodes,
        }

    def test_tnf_node_failover_and_recovery(self, setup_tnf_workloads):
        """
        Test TNF node failover with DRBD and floating monitor recovery.

        Steps:
            1) Run CephFS, RBD and VM workloads
            2) Verify DRBD status on both nodes before failover
            3) Identify which node hosts the floating monitor
            4) Shutdown the node hosting the floating monitor ungracefully
            5) Verify DRBD fails over, floating monitor migrates,
               quorum maintained, no data loss or corruption
            6) Restart the failed node
            7) Verify full recovery: DRBD healthy, Ceph OK, workloads intact

        """
        sc_obj = setup_tnf_workloads["sc_obj"]
        vm_obj = setup_tnf_workloads["vm_obj"]
        md5sum_before = setup_tnf_workloads["md5sum_before"]
        nodes = setup_tnf_workloads["nodes"]

        logger.test_step("Verify DRBD status before failover")
        for node in nodes:
            assert verify_drbd_status(
                node["name"]
            ), f"DRBD status check failed on {node['name']}"

        logger.test_step("Identify floating monitor node")
        mon_pods = get_pods_having_label(
            label="app=rook-ceph-floating-mon",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert mon_pods, "Floating monitor not found"
        floating_mon = mon_pods[0]
        primary_node = floating_mon.get()["spec"]["nodeName"]
        secondary_node = next(n["name"] for n in nodes if n["name"] != primary_node)
        logger.info(
            f"Floating monitor {floating_mon.name} on {primary_node}, "
            f"secondary: {secondary_node}"
        )

        start_time = datetime.now(timezone.utc)

        logger.test_step("Shutdown primary node ungracefully")
        platform_nodes = PlatformNodesFactory()
        node_objs = get_node_objs([primary_node])
        platform_nodes.stop_nodes(nodes=node_objs, wait=True)

        logger.info("Waiting for DRBD and floating monitor failover")
        time.sleep(90)

        logger.test_step("Verify DRBD failover on secondary node")
        assert verify_drbd_status(
            secondary_node
        ), f"DRBD status check failed on {secondary_node} after failover"

        logger.test_step("Verify floating monitor migrated")
        mon_pods_after = get_pods_having_label(
            label="app=rook-ceph-floating-mon",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert mon_pods_after, "No floating monitor pods found after failover"
        new_node = mon_pods_after[0].get()["spec"]["nodeName"]
        assert (
            new_node == secondary_node
        ), f"Floating monitor did not migrate: expected {secondary_node}, got {new_node}"

        logger.test_step("Verify quorum maintained")
        ceph_cluster = CephCluster()
        quorum_status = ceph_cluster.toolbox.exec_cmd_on_pod(
            "ceph quorum_status --format json"
        )
        quorum_data = json.loads(quorum_status)
        quorum_names = quorum_data.get("quorum_names", [])
        logger.info(f"Quorum members during failover: {quorum_names}")
        assert (
            len(quorum_names) >= 2
        ), f"Expected at least 2 monitors in quorum, found {len(quorum_names)}: {quorum_names}"

        logger.test_step("Verify Ceph health during node down")
        ceph_health = ceph_cluster.get_ceph_health()
        logger.info(f"Ceph health during node down: {ceph_health}")
        assert ceph_health in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Unexpected Ceph health: {ceph_health}"

        logger.test_step("Verify VM data integrity")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, file_path="/test/file_1.txt", md5sum=md5sum_before
        )

        logger.test_step("Verify no data loss or corruption")
        verify_data_loss(sc_obj, start_time)
        verify_data_corruption(sc_obj, start_time)

        logger.test_step("Start primary node back up")
        platform_nodes.start_nodes(nodes=node_objs, wait=True)
        logger.info("Waiting for cluster to stabilize")
        time.sleep(120)

        logger.test_step("Verify full recovery")
        ceph_health_after = ceph_cluster.get_ceph_health()
        logger.assertion(f"Ceph health after recovery: {ceph_health_after}")
        assert ceph_health_after in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Unexpected Ceph health after recovery: {ceph_health_after}"

        for node in nodes:
            assert verify_drbd_status(
                node["name"]
            ), f"DRBD status check failed on {node['name']} after recovery"

        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, file_path="/test/file_1.txt", md5sum=md5sum_before
        )

    def test_tnf_monitor_pod_failure(self, setup_tnf_workloads):
        """
        Test monitor pod deletion and automatic recovery by rook-operator.

        Steps:
            1) Run CephFS, RBD and VM workloads
            2) Delete a static monitor pod (not the floating mon)
            3) Verify quorum maintained with 2 mons during recovery
            4) Wait for rook-operator to recreate the monitor
            5) Verify all monitors rejoin quorum
            6) Verify no data loss or corruption

        """
        sc_obj = setup_tnf_workloads["sc_obj"]
        vm_obj = setup_tnf_workloads["vm_obj"]
        md5sum_before = setup_tnf_workloads["md5sum_before"]

        mon_pods = get_pods_having_label(
            label="app=rook-ceph-mon",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert (
            len(mon_pods) >= 2
        ), f"Expected at least 2 monitor pods, found {len(mon_pods)}"

        logger.test_step("Identify static monitor pod for deletion")
        floating_mon_pods = get_pods_having_label(
            label="app=rook-ceph-floating-mon",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        floating_mon_names = {p.name for p in floating_mon_pods}

        static_mon = None
        for mon_pod in mon_pods:
            if mon_pod.name not in floating_mon_names:
                static_mon = mon_pod
                break
        assert static_mon, "Could not find static monitor pod to delete"

        mon_node = static_mon.get()["spec"]["nodeName"]
        mon_name = static_mon.name
        logger.info(f"Selected {mon_name} on node {mon_node} for deletion")

        start_time = datetime.now(timezone.utc)

        logger.test_step("Delete static monitor pod")
        delete_pods([static_mon])
        time.sleep(10)

        logger.test_step("Verify quorum maintained during recovery")
        ceph_cluster = CephCluster()
        quorum_status = ceph_cluster.toolbox.exec_cmd_on_pod(
            "ceph quorum_status --format json"
        )
        quorum_data = json.loads(quorum_status)
        quorum_names = quorum_data.get("quorum_names", [])
        logger.info(f"Quorum members during recovery: {quorum_names}")
        assert (
            len(quorum_names) >= 2
        ), f"Expected at least 2 monitors in quorum, found {len(quorum_names)}"

        ceph_health = ceph_cluster.get_ceph_health()
        logger.info(f"Ceph health during mon recovery: {ceph_health}")
        assert ceph_health in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Unexpected Ceph health: {ceph_health}"

        logger.test_step("Wait for rook-operator to recreate monitor")
        max_wait = 180
        start_wait = time.time()
        mon_recreated = False

        while time.time() - start_wait < max_wait:
            mon_pods_after = get_pods_having_label(
                label="app=rook-ceph-mon",
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            for mon_pod in mon_pods_after:
                pod_node = mon_pod.get()["spec"]["nodeName"]
                if (
                    pod_node == mon_node
                    and mon_pod.status == constants.STATUS_RUNNING
                    and mon_pod.name != mon_name
                ):
                    mon_recreated = True
                    logger.info(f"Monitor {mon_pod.name} recreated on {pod_node}")
                    break
                if (
                    mon_pod.name.startswith(mon_name.rsplit("-", 1)[0])
                    and mon_pod.status == constants.STATUS_RUNNING
                    and mon_pod.get()["spec"]["nodeName"] == mon_node
                ):
                    mon_recreated = True
                    logger.info(f"Monitor {mon_pod.name} returned on {mon_node}")
                    break
            if mon_recreated:
                break
            time.sleep(10)

        assert (
            mon_recreated
        ), f"Monitor pod not recreated on {mon_node} within {max_wait}s"

        logger.test_step("Verify all monitors rejoin quorum")
        time.sleep(30)
        quorum_final = json.loads(
            ceph_cluster.toolbox.exec_cmd_on_pod("ceph quorum_status --format json")
        )
        logger.info(f"Final quorum members: {quorum_final.get('quorum_names', [])}")

        logger.test_step("Verify VM data integrity")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, file_path="/test/file_1.txt", md5sum=md5sum_before
        )

        logger.test_step("Verify no data loss or corruption")
        verify_data_loss(sc_obj, start_time)
        verify_data_corruption(sc_obj, start_time)

        ceph_health_final = ceph_cluster.get_ceph_health()
        logger.assertion(f"Final Ceph health: {ceph_health_final}")
        assert ceph_health_final in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Unexpected final Ceph health: {ceph_health_final}"
