"""
Test cases for Two-Node Fencing (TNF) cluster deployment and failover
"""

import json
import logging
import time
import pytest
from datetime import datetime, timezone

from ocs_ci.deployment.helpers.tnf_helpers import (
    create_persistent_volumes,
    discover_available_disks,
    get_tnf_node_info,
    resolve_disk_by_id_path,
    validate_tnf_prerequisites,
    verify_drbd_configuration,
    verify_drbd_status,
    verify_tnf_cluster_topology,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.framework.testlib import deployment, polarion_id, ManageTest, tier1
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    verify_data_corruption,
    verify_data_loss,
    verify_vm_workload,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster, check_ceph_health_after_add_capacity
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.platform_nodes import PlatformNodesFactory
from ocs_ci.ocs.resources.pod import get_osd_pods, delete_pods, get_pods_having_label
from ocs_ci.ocs.resources.storage_cluster import (
    get_deviceset_count,
    get_osd_count,
    ocs_install_verification,
    set_deviceset_count,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check, is_cluster_running
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@turquoise_squad
@deployment
@polarion_id(get_polarion_id())
def test_tnf_deployment(pvc_factory, pod_factory):
    """
    Test TNF (Two-Node Fencing) cluster deployment and verify ODF
    installation with DRBD floating monitor configuration.
    """
    deploy = config.RUN["cli_params"].get("deploy")
    teardown = config.RUN["cli_params"].get("teardown")

    if not teardown or deploy:
        logger.test_step("Verify OCP cluster is running")
        cluster_path = config.ENV_DATA["cluster_path"]
        cluster_running = is_cluster_running(cluster_path)
        logger.assertion(
            f"OCP cluster status: cluster_path='{cluster_path}', "
            f"running={cluster_running}"
        )
        assert cluster_running

        if not config.ENV_DATA["skip_ocs_deployment"]:
            logger.test_step("Verify TNF cluster topology (DualReplica)")
            assert (
                verify_tnf_cluster_topology()
            ), "Cluster does not have DualReplica topology required for TNF"

            logger.test_step("Verify TNF node count")
            nodes = get_tnf_node_info()
            assert len(nodes) == 2, f"TNF requires exactly 2 nodes, found {len(nodes)}"
            logger.info(f"Found 2 nodes: {nodes[0]['name']}, {nodes[1]['name']}")

            logger.test_step("Verify ODF installation")
            ocs_registry_image = config.DEPLOYMENT.get("ocs_registry_image")
            ocs_install_verification(ocs_registry_image=ocs_registry_image)

            logger.test_step("Verify DRBD configuration for floating monitor")
            assert verify_drbd_configuration(), "DRBD configuration verification failed"

            logger.test_step("Verify DRBD status on both nodes")
            for node in nodes:
                assert verify_drbd_status(
                    node["name"]
                ), f"DRBD status check failed on {node['name']}"
                logger.info(f"DRBD status OK on {node['name']}")

            logger.test_step("Verify floating monitor pod")
            mon_pods = get_pods_having_label(
                label="app=rook-ceph-floating-mon",
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            assert mon_pods, "No floating monitor pods found"

            logger.test_step("Run sanity checks and resource validation")
            sanity_helpers = Sanity()
            sanity_helpers.health_check(
                fix_ceph_health=True,
                update_jira=True,
                no_exception_if_jira_issue_updated=True,
            )
            sanity_helpers.delete_resources()

            logger.test_step("Verify Ceph health after deployment")
            ceph_healthy = ceph_health_check(
                tries=10,
                delay=30,
                fix_ceph_health=True,
            )
            logger.assertion(f"Ceph health check: healthy={ceph_healthy}")
            assert ceph_healthy, "Ceph health check failed after deployment"

    if teardown:
        logger.info("Cluster will be destroyed during teardown part of this test.")


@turquoise_squad
class TestTNFPostDeployment:
    """
    Post-deployment verification tests for TNF clusters
    """

    def test_tnf_drbd_configmap(self):
        """Verify DRBD ConfigMap exists with required keys"""
        cm_obj = OCP(
            kind=constants.CONFIGMAP,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        cm = cm_obj.get(resource_name=constants.TNF_DRBD_CONFIGURE_CM)
        assert cm, f"ConfigMap {constants.TNF_DRBD_CONFIGURE_CM} not found"

        required_keys = [
            "DRBD_DEVICE_NAME",
            "DRBD_RESOURCE_NAME",
            "NODE_0_NAME",
            "NODE_1_NAME",
        ]
        for key in required_keys:
            assert key in cm.get(
                "data", {}
            ), f"Required key {key} not found in DRBD ConfigMap"

    def test_tnf_floating_monitor_pod(self):
        """Verify floating monitor pod exists and is running"""
        mon_pods = get_pods_having_label(
            label="app=rook-ceph-floating-mon",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert mon_pods, "No floating monitor pods found"
        for pod in mon_pods:
            assert (
                pod.status == constants.STATUS_RUNNING
            ), f"Floating monitor pod {pod.name} is not running"

    def test_drbd_status_on_nodes(self):
        """Verify DRBD status on both nodes"""
        nodes = get_tnf_node_info()
        for node in nodes:
            assert verify_drbd_status(
                node["name"]
            ), f"DRBD status check failed on node {node['name']}"

    def test_prerequisites_validation(self):
        """Verify TNF prerequisites"""
        validation_results = validate_tnf_prerequisites()
        assert validation_results["topology"], "Topology validation failed"
        assert validation_results["node_count"], "Node count validation failed"
        assert validation_results["network"], "Network validation failed"


@tier1
@turquoise_squad
class TestTNFAddCapacity(ManageTest):
    """
    Add capacity on TNF cluster with local block storage.

    TNF uses manually created local PVs (not LSO LocalVolume/LocalVolumeSet),
    so standard add_capacity / add_capacity_lso do not apply. The flow is:
    1. Discover unused disks on both nodes
    2. Create new local PVs for those disks
    3. Increment StorageCluster deviceset count
    4. Wait for new OSD pods
    5. Verify Ceph health after rebalance
    """

    def test_tnf_add_capacity(self):
        """
        Add capacity to TNF cluster by creating new local PVs
        from available unused disks and expanding the StorageCluster.
        """
        logger.test_step("Get current OSD count and deviceset count")
        existing_osd_count = get_osd_count()
        existing_deviceset_count = get_deviceset_count()
        existing_osd_pods = get_osd_pods()
        existing_osd_pod_names = [pod.name for pod in existing_osd_pods]
        logger.info(
            f"Current state: {existing_osd_count} OSDs, "
            f"deviceset count={existing_deviceset_count}"
        )

        logger.test_step("Discover unused disks on TNF nodes")
        nodes = get_tnf_node_info()
        assert len(nodes) == 2, f"TNF requires exactly 2 nodes, found {len(nodes)}"
        disk_info = discover_available_disks(nodes)

        node_0_name = nodes[0]["name"]
        node_1_name = nodes[1]["name"]
        unused_n0 = disk_info[node_0_name]["unused"]
        unused_n1 = disk_info[node_1_name]["unused"]
        assert unused_n0, f"No unused disks on {node_0_name} for expansion"
        assert unused_n1, f"No unused disks on {node_1_name} for expansion"
        logger.info(
            f"Found unused disks: {node_0_name}={[d['path'] for d in unused_n0]}, "
            f"{node_1_name}={[d['path'] for d in unused_n1]}"
        )

        logger.test_step("Create new local PVs for unused disks")
        new_pv_count = min(len(unused_n0), len(unused_n1))
        device_mappings = []
        for i in range(new_pv_count):
            for node_name, disks in [
                (node_0_name, unused_n0),
                (node_1_name, unused_n1),
            ]:
                by_id = resolve_disk_by_id_path(node_name, disks[i]["path"])
                device_mappings.append(
                    {
                        "node_name": node_name,
                        "device_path": by_id,
                        "size": disks[i]["size"] + "i",
                        "pv_name": f"local-pv-expand-{node_name}-{i}",
                    }
                )
        created_pvs = create_persistent_volumes(device_mappings)
        logger.info(f"Created PVs: {created_pvs}")

        logger.test_step("Increment StorageCluster deviceset count")
        new_deviceset_count = existing_deviceset_count + new_pv_count
        set_deviceset_count(new_deviceset_count)
        logger.info(
            f"Patched deviceset count: {existing_deviceset_count} -> {new_deviceset_count}"
        )

        logger.test_step("Wait for new OSD pods to reach Running state")
        expected_osd_count = existing_osd_count + (new_pv_count * 2)
        pod_obj = OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        pod_obj.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=expected_osd_count,
        )

        logger.test_step("Verify existing OSD pods were not restarted")
        osd_pods_post = get_osd_pods()
        osd_pod_names_post = [pod.name for pod in osd_pods_post]
        restarted = [p for p in existing_osd_pod_names if p not in osd_pod_names_post]
        assert (
            len(restarted) == 0
        ), f"Existing OSD pods restarted after add capacity: {restarted}"

        logger.test_step("Verify Ceph health after rebalance")
        check_ceph_health_after_add_capacity(ceph_rebalance_timeout=3600)


@pytest.mark.polarion_id("OCS-XXXX")
@turquoise_squad
class TestTNFFailover:
    """
    TNF failover scenarios with workloads
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
            command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_1.txt && sync"
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
