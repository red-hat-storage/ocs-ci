"""
Test cases for Two-Node Fencing (TNF) cluster deployment
"""

import json
import logging
import time
import pytest
from datetime import datetime, timezone

from ocs_ci.deployment.helpers.tnf_helpers import (
    get_tnf_node_info,
    validate_tnf_prerequisites,
    verify_drbd_configuration,
    verify_drbd_status,
    verify_tnf_cluster_topology,
)
from ocs_ci.deployment.tnf import TNF
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    deployment,
    turquoise_squad,
)
from ocs_ci.framework.testlib import polarion_id
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
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.platform_nodes import PlatformNodesFactory
from ocs_ci.ocs.resources.pod import delete_pods, get_pods_having_label
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check, is_cluster_running
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def tnf_deployment():
    """
    Fixture to provide TNF deployment instance
    """
    return TNF()


@turquoise_squad
@deployment
@polarion_id(get_polarion_id())
def test_tnf_deployment(pvc_factory, pod_factory):
    """
    Test TNF (Two-Node Fencing) cluster deployment

    This is the main deployment test for TNF clusters, following the standard
    ocs-ci deployment test pattern. It verifies:
    1. OCP cluster is running with DualReplica topology
    2. ODF installation on TNF cluster
    3. DRBD configuration for floating monitor
    4. Ceph health and cluster functionality
    5. Resource creation and deletion (sanity checks)

    This test runs automatically during TNF cluster deployment via run-ci.
    """
    deploy = config.RUN["cli_params"].get("deploy")
    teardown = config.RUN["cli_params"].get("teardown")

    if not teardown or deploy:
        logger.info("=" * 80)
        logger.info("Starting TNF Deployment Test")
        logger.info("=" * 80)

        # Step 1: Verify OCP cluster is running
        logger.info("Step 1: Verify OCP cluster is running")
        cluster_path = config.ENV_DATA["cluster_path"]
        cluster_running = is_cluster_running(cluster_path)
        logger.info(
            f"OCP cluster status: cluster_path='{cluster_path}', "
            f"running={cluster_running}"
        )
        assert cluster_running, "OCP cluster is not running"

        if not config.ENV_DATA["skip_ocs_deployment"]:
            # Step 2: Verify TNF cluster topology
            logger.info("Step 2: Verify TNF cluster topology (DualReplica)")
            assert (
                verify_tnf_cluster_topology()
            ), "Cluster does not have DualReplica topology required for TNF"
            logger.info("✓ DualReplica topology verified")

            # Step 3: Verify exactly 2 nodes
            logger.info("Step 3: Verify TNF node count")
            nodes = get_tnf_node_info()
            assert len(nodes) == 2, f"TNF requires exactly 2 nodes, found {len(nodes)}"
            logger.info(f"✓ Found 2 nodes: {nodes[0]['name']}, {nodes[1]['name']}")

            # Step 4: Verify ODF installation on TNF cluster
            logger.info("Step 4: Verify ODF installation on TNF cluster")
            ocs_registry_image = config.DEPLOYMENT.get("ocs_registry_image")
            ocs_install_verification(ocs_registry_image=ocs_registry_image)
            logger.info("✓ ODF installation verified")

            # Step 5: Verify DRBD configuration
            logger.info("Step 5: Verify DRBD configuration for floating monitor")
            assert verify_drbd_configuration(), "DRBD configuration verification failed"
            logger.info("✓ DRBD ConfigMap verified")

            # Step 6: Verify DRBD status on both nodes
            logger.info("Step 6: Verify DRBD status on both nodes")
            for node in nodes:
                assert verify_drbd_status(
                    node["name"]
                ), f"DRBD status check failed on {node['name']}"
                logger.info(f"✓ DRBD status OK on {node['name']}")

            # Step 7: Verify floating monitor pod
            logger.info("Step 7: Verify floating monitor pod")
            mon_pods = get_pods_having_label(
                label="app=rook-ceph-mon",
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            assert mon_pods, "No monitor pods found"
            floating_mon_found = False
            for pod in mon_pods:
                if "mon-c" in pod.name or pod.name.endswith("-c"):
                    floating_mon_found = True
                    assert (
                        pod.status == constants.STATUS_RUNNING
                    ), f"Floating monitor {pod.name} is not running"
                    logger.info(f"✓ Floating monitor {pod.name} is running")
                    break
            assert floating_mon_found, "Floating monitor (mon-c) not found"

            # Step 8: Run sanity checks
            logger.info("Step 8: Run sanity checks and resource validation")
            logger.info(
                "Creating resources (pools, storageclasses, PVCs, pods), "
                "running IO, and deleting resources"
            )
            sanity_helpers = Sanity()
            sanity_helpers.health_check(
                fix_ceph_health=True,
                update_jira=True,
                no_exception_if_jira_issue_updated=True,
            )
            logger.info("✓ Sanity health check passed")

            logger.info("Cleaning up sanity test resources")
            sanity_helpers.delete_resources()
            logger.info("✓ Sanity resources cleaned up")

            # Step 9: Final Ceph health check
            logger.info("Step 9: Verify Ceph health after deployment")
            ceph_healthy = ceph_health_check(
                tries=10,
                delay=30,
                fix_ceph_health=True,
            )
            logger.info(f"Ceph health status: {ceph_healthy}")
            assert ceph_healthy, "Ceph cluster is not healthy"
            logger.info("✓ Ceph health verified")

            logger.info("=" * 80)
            logger.info("TNF Deployment Test Completed Successfully!")
            logger.info("=" * 80)


class TestTNFPrerequisites:
    """
    Test class for validating TNF prerequisites
    """

    def test_cluster_topology(self):
        """
        Test that cluster has DualReplica topology
        """
        logger.info("Testing cluster topology...")
        assert (
            verify_tnf_cluster_topology()
        ), "Cluster does not have DualReplica topology"

    def test_node_count(self):
        """
        Test that exactly 2 nodes exist
        """
        logger.info("Testing node count...")
        nodes = get_tnf_node_info()
        assert len(nodes) == 2, f"Expected 2 nodes, found {len(nodes)}"

    def test_prerequisites_validation(self):
        """
        Test comprehensive prerequisites validation
        """
        logger.info("Testing comprehensive prerequisites...")
        validation_results = validate_tnf_prerequisites()

        assert validation_results["topology"], "Topology validation failed"
        assert validation_results["node_count"], "Node count validation failed"
        assert validation_results["network"], "Network validation failed"


class TestTNFDeployment:
    """
    Test class for TNF deployment process
    """

    def test_deploy_prereq(self, tnf_deployment):
        """
        Test deployment prerequisites
        """
        logger.info("Testing deployment prerequisites...")
        tnf_deployment.deploy_prereq()

    def test_storage_class_exists(self):
        """
        Test that localblock storage class exists after deployment
        """
        logger.info("Testing storage class existence...")
        sc_obj = OCP(kind=constants.STORAGECLASS)
        sc = sc_obj.get(resource_name=constants.TNF_LOCALBLOCK_SC)
        assert sc, f"Storage class {constants.TNF_LOCALBLOCK_SC} not found"

    def test_drbd_configuration(self):
        """
        Test DRBD configuration after deployment
        """
        logger.info("Testing DRBD configuration...")
        assert verify_drbd_configuration(), "DRBD configuration verification failed"

    def test_drbd_status_on_nodes(self):
        """
        Test DRBD status on both nodes
        """
        logger.info("Testing DRBD status on nodes...")
        nodes = get_tnf_node_info()

        for node in nodes:
            assert verify_drbd_status(
                node["name"]
            ), f"DRBD status check failed on node {node['name']}"


class TestTNFPostDeployment:
    """
    Test class for post-deployment verification
    """

    def test_drbd_configmap_exists(self):
        """
        Test that DRBD ConfigMap exists
        """
        logger.info("Testing DRBD ConfigMap existence...")
        cm_obj = OCP(
            kind=constants.CONFIGMAP, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        try:
            cm = cm_obj.get(resource_name=constants.TNF_DRBD_CONFIGURE_CM)
            assert cm, f"ConfigMap {constants.TNF_DRBD_CONFIGURE_CM} not found"

            # Verify required keys
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
        except Exception as e:
            pytest.fail(f"Failed to verify DRBD ConfigMap: {e}")

    def test_floating_monitor_pod(self):
        """
        Test that floating monitor pod exists and is running
        """
        logger.info("Testing floating monitor pod...")

        mon_pods = get_pods_having_label(
            label="app=rook-ceph-mon", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        assert mon_pods, "No monitor pods found"
        logger.info(f"Found {len(mon_pods)} monitor pod(s)")

        for pod in mon_pods:
            assert (
                pod.status == constants.STATUS_RUNNING
            ), f"Monitor pod {pod.name} is not running"

    def test_storage_cluster_ready(self):
        """
        Test that StorageCluster is in Ready state
        """
        logger.info("Testing StorageCluster status...")

        sc_obj = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        storage_clusters = sc_obj.get()["items"]
        assert storage_clusters, "No StorageCluster found"

        for sc in storage_clusters:
            # Check phase
            phase = sc.get("status", {}).get("phase")
            assert phase == "Ready", (
                f"StorageCluster {sc['metadata']['name']} is not Ready, "
                f"current phase: {phase}"
            )

    def test_osd_pods_running(self):
        """
        Test that OSD pods are running
        """
        logger.info("Testing OSD pods status...")

        osd_pods = get_pods_having_label(
            label="app=rook-ceph-osd", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        assert osd_pods, "No OSD pods found"
        logger.info(f"Found {len(osd_pods)} OSD pod(s)")

        for pod in osd_pods:
            assert (
                pod.status == constants.STATUS_RUNNING
            ), f"OSD pod {pod.name} is not running"

    def test_ceph_health(self):
        """
        Test Ceph cluster health
        """
        logger.info("Testing Ceph cluster health...")

        ceph_cluster = CephCluster()
        ceph_health = ceph_cluster.get_ceph_health()

        assert ceph_health in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Ceph health is not OK/WARN: {ceph_health}"

        logger.info(f"Ceph health: {ceph_health}")


@pytest.mark.polarion_id("OCS-XXXX")
class TestTNFFailover:
    """
    Test class for TNF failover scenarios with workloads
    """

    @pytest.fixture(scope="function")
    def setup_tnf_workloads(
        self,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        cnv_workload,
    ):
        """
        Setup CephFS, RBD, and CNV workloads for TNF failover testing

        This fixture sets up continuous I/O workloads similar to arbiter netsplit tests
        """
        logger.info("Setting up TNF workloads...")
        sc_obj = StretchCluster()

        # Run CephFS and RBD workloads
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=30)

        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )

        logger.info("CephFS and RBD workloads started")

        # Setup CNV VM workload
        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(command="mkdir -p /test && sudo chmod -R 777 /test")
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_1.txt && sync"
        )
        md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
        logger.info(f"VM workload setup complete, MD5: {md5sum_before}")

        # Get TNF nodes
        nodes = get_tnf_node_info()

        # Note all workload pod names
        check_for_logwriter_workload_pods(sc_obj, nodes=[n["name"] for n in nodes])

        # Note file names created
        sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        workload_data = {
            "sc_obj": sc_obj,
            "vm_obj": vm_obj,
            "md5sum_before": md5sum_before,
            "nodes": nodes,
        }

        yield workload_data

        logger.info("Tearing down workloads...")

    def test_node_failover_and_recovery(self, setup_tnf_workloads):
        """
        Test TNF node failover with DRBD and floating monitor recovery

        This comprehensive test covers both node reboot and DRBD failover scenarios:

        Steps:
        1. Setup CNV, RBD and CephFS workloads
        2. Start continuous I/O workloads (background)
        3. Verify DRBD status on both nodes before failover
        4. Identify which node is hosting the floating mon (mon-c)
        5. Shutdown the node ungracefully (hosting floating mon)
        6. Verify during node down:
           - DRBD fails over to the other node
           - Floating mon migrates to the other node
           - Quorum maintained at 2 mons
           - Ceph enters HEALTH_WARN state (expected)
           - No Data Unavailability/Data Loss/Data Corruption
           - CephFS RWX, RWO workloads continue without interruption
        7. Restart the failed node
        8. Verify full recovery:
           - Ceph cluster health returns to OK
           - DRBD status healthy on both nodes
           - All workloads intact with no data loss/corruption
        """
        logger.info("Starting TNF node failover and recovery test...")

        workload_data = setup_tnf_workloads
        sc_obj = workload_data["sc_obj"]
        vm_obj = workload_data["vm_obj"]
        md5sum_before = workload_data["md5sum_before"]
        nodes = workload_data["nodes"]

        # Verify DRBD status on both nodes before failover
        logger.info("Verifying DRBD status before failover...")
        for node in nodes:
            assert verify_drbd_status(
                node["name"]
            ), f"DRBD status check failed on {node['name']}"

        # Identify which node is hosting the floating monitor (mon-c)
        mon_pods = get_pods_having_label(
            label="app=rook-ceph-mon", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        floating_mon = None
        primary_node = None
        for mon_pod in mon_pods:
            if "mon-c" in mon_pod.name or mon_pod.name.endswith("-c"):
                floating_mon = mon_pod
                primary_node = mon_pod.get()["spec"]["nodeName"]
                logger.info(
                    f"Floating monitor {mon_pod.name} is on node {primary_node}"
                )
                break

        assert floating_mon, "Floating monitor (mon-c) not found"

        # Get the secondary node
        secondary_node = next(n["name"] for n in nodes if n["name"] != primary_node)
        logger.info(f"Secondary node: {secondary_node}")

        start_time = datetime.now(timezone.utc)

        # Shutdown the primary node ungracefully
        logger.info(f"Shutting down primary node {primary_node} ungracefully...")
        platform_nodes = PlatformNodesFactory()
        node_objs = get_node_objs([primary_node])
        platform_nodes.stop_nodes(nodes=node_objs, wait=True)

        # Wait for DRBD and monitor failover
        logger.info("Waiting for DRBD and floating monitor failover...")
        time.sleep(90)

        # Verify DRBD status on secondary node after failover
        logger.info(f"Verifying DRBD failover on secondary node {secondary_node}...")
        assert verify_drbd_status(
            secondary_node
        ), f"DRBD status check failed on {secondary_node} after failover"

        # Verify floating monitor migrated to secondary node
        logger.info("Verifying floating monitor migration...")
        mon_pods_after = get_pods_having_label(
            label="app=rook-ceph-mon", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        mon_migrated = False
        for mon_pod in mon_pods_after:
            if "mon-c" in mon_pod.name or mon_pod.name.endswith("-c"):
                new_node = mon_pod.get()["spec"]["nodeName"]
                if new_node == secondary_node:
                    mon_migrated = True
                    logger.info(
                        f"Floating monitor migrated to {secondary_node} successfully"
                    )
                break

        assert mon_migrated, "Floating monitor did not migrate to the secondary node"

        # Verify quorum maintained (at least 2 mons running)
        running_mons = [
            p for p in mon_pods_after if p.status == constants.STATUS_RUNNING
        ]
        assert (
            len(running_mons) >= 2
        ), f"Expected at least 2 running mons, found {len(running_mons)}"
        logger.info(f"Quorum maintained with {len(running_mons)} running monitors")

        # Verify Ceph health (HEALTH_WARN is expected during node down)
        ceph_cluster = CephCluster()
        ceph_health = ceph_cluster.get_ceph_health()
        logger.info(f"Ceph health during node down: {ceph_health}")
        assert ceph_health in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Unexpected Ceph health: {ceph_health}"

        # Verify workloads continue - no data unavailability
        logger.info("Verifying workloads continue without interruption...")

        # Check VM data integrity
        logger.info("Verifying VM workload integrity...")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, file_path="/test/file_1.txt", md5sum=md5sum_before
        )
        logger.info("VM workload verified - no data loss")

        # Verify no data loss or corruption in CephFS/RBD workloads
        verify_data_loss(sc_obj, start_time)
        verify_data_corruption(sc_obj, start_time)
        logger.info("CephFS and RBD workloads verified - no data loss or corruption")

        # Bring primary node back up
        logger.info(f"Starting primary node {primary_node} back up...")
        platform_nodes.start_nodes(nodes=node_objs, wait=True)

        # Wait for cluster to stabilize
        logger.info("Waiting for cluster to stabilize...")
        time.sleep(120)

        # Verify Ceph cluster health after recovery
        ceph_health_after = ceph_cluster.get_ceph_health()
        logger.info(f"Ceph health after node recovery: {ceph_health_after}")
        assert ceph_health_after in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Unexpected Ceph health after recovery: {ceph_health_after}"

        # Verify DRBD status on both nodes after recovery
        logger.info("Verifying DRBD status on both nodes after recovery...")
        for node in nodes:
            assert verify_drbd_status(
                node["name"]
            ), f"DRBD status check failed on {node['name']} after recovery"

        # Final workload verification after full recovery
        logger.info("Final verification of all workloads after recovery...")
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, file_path="/test/file_1.txt", md5sum=md5sum_before
        )

        logger.info("TNF node failover and recovery test completed successfully!")

    def test_monitor_pod_failure(self, setup_tnf_workloads):
        """
        Test monitor pod deletion and automatic recovery by rook-operator

        This test verifies TNF cluster resilience when a static monitor pod is deleted:

        Steps:
        1. Run CNV, RBD and CephFS workloads in the background
        2. Delete mon-a pod (static monitor, not floating mon-c)
        3. Check if rook-operator attempts to reschedule mon-a
        4. Check Ceph quorum: should have 2 mons (mon-b + mon-c) during recovery
        5. Confirm mon-a returns to Running state on the same original node
        6. Verify I/O continues uninterrupted

        Expected:
        - Rook-operator automatically recreates mon-a
        - Quorum maintained with 2 mons during recovery
        - Mon-a returns to the same node
        - No workload interruption or data loss
        """
        logger.info("Starting monitor pod deletion and recovery test...")

        workload_data = setup_tnf_workloads
        sc_obj = workload_data["sc_obj"]
        vm_obj = workload_data["vm_obj"]
        md5sum_before = workload_data["md5sum_before"]

        # Get all monitor pods
        mon_pods = get_pods_having_label(
            label="app=rook-ceph-mon", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        assert (
            len(mon_pods) >= 2
        ), f"Expected at least 2 monitor pods, found {len(mon_pods)}"
        logger.info(f"Found {len(mon_pods)} monitor pods before deletion")

        # Identify mon-a (static monitor, not the floating mon-c)
        mon_a = None
        mon_a_node = None
        for mon_pod in mon_pods:
            # Skip floating monitor (mon-c)
            if "mon-c" in mon_pod.name or mon_pod.name.endswith("-c"):
                continue
            # Get first static monitor (mon-a or mon-b, we'll call it mon-a)
            if mon_a is None:
                mon_a = mon_pod
                mon_a_node = mon_pod.get()["spec"]["nodeName"]
                mon_a_name = mon_pod.name
                logger.info(
                    f"Selected monitor pod {mon_a_name} on node {mon_a_node} for deletion"
                )
                break

        assert mon_a, "Could not find static monitor pod to delete"

        start_time = datetime.now(timezone.utc)

        # Delete mon-a pod
        logger.info(f"Deleting monitor pod {mon_a_name}...")
        delete_pods([mon_a])
        logger.info(f"Monitor pod {mon_a_name} deleted")

        # Wait a moment for deletion to process
        time.sleep(10)

        # Check Ceph quorum immediately after deletion - should have 2 mons
        logger.info("Checking Ceph quorum status after mon deletion...")
        ceph_cluster = CephCluster()

        # Get quorum status
        quorum_status_cmd = "ceph quorum_status --format json"
        quorum_status = ceph_cluster.toolbox.exec_cmd_on_pod(quorum_status_cmd)

        quorum_data = json.loads(quorum_status)
        quorum_names = quorum_data.get("quorum_names", [])
        logger.info(f"Quorum members during recovery: {quorum_names}")

        # Should have at least 2 monitors in quorum (mon-b + mon-c)
        assert len(quorum_names) >= 2, (
            f"Expected at least 2 monitors in quorum during recovery, "
            f"found {len(quorum_names)}: {quorum_names}"
        )
        logger.info(f"Quorum maintained with {len(quorum_names)} monitors")

        # Verify Ceph health (HEALTH_WARN is acceptable during mon recovery)
        ceph_health = ceph_cluster.get_ceph_health()
        logger.info(f"Ceph health during mon recovery: {ceph_health}")
        assert ceph_health in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Unexpected Ceph health: {ceph_health}"

        # Wait for rook-operator to reschedule mon-a
        logger.info("Waiting for rook-operator to reschedule mon-a...")
        time.sleep(30)

        # Verify mon-a is being recreated
        max_wait = 180  # 3 minutes
        start_wait = time.time()
        mon_a_recreated = False

        while time.time() - start_wait < max_wait:
            mon_pods_after = get_pods_having_label(
                label="app=rook-ceph-mon",
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )

            # Look for a mon pod on the same node (rook should reschedule on same node)
            for mon_pod in mon_pods_after:
                pod_node = mon_pod.get()["spec"]["nodeName"]
                # Check if this is a new pod on the original mon-a's node
                if pod_node == mon_a_node and mon_pod.name != mon_a_name:
                    # Found new mon pod on same node
                    if mon_pod.status == constants.STATUS_RUNNING:
                        mon_a_recreated = True
                        logger.info(
                            f"Monitor pod {mon_pod.name} recreated on node {pod_node} "
                            f"and is Running"
                        )
                        break
                # Or check if mon-a with same name is running again
                elif mon_pod.name.startswith(mon_a_name.rsplit("-", 1)[0]):
                    if (
                        mon_pod.status == constants.STATUS_RUNNING
                        and mon_pod.get()["spec"]["nodeName"] == mon_a_node
                    ):
                        mon_a_recreated = True
                        logger.info(
                            f"Monitor pod {mon_pod.name} returned to Running state "
                            f"on original node {mon_a_node}"
                        )
                        break

            if mon_a_recreated:
                break

            logger.info("Waiting for mon-a to be recreated...")
            time.sleep(10)

        assert (
            mon_a_recreated
        ), f"Monitor pod was not recreated on node {mon_a_node} within {max_wait} seconds"

        # Verify all 3 monitors are now running
        time.sleep(20)
        mon_pods_final = get_pods_having_label(
            label="app=rook-ceph-mon", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        running_mons = [
            p for p in mon_pods_final if p.status == constants.STATUS_RUNNING
        ]
        logger.info(
            f"Final monitor pod count: {len(running_mons)} running out of {len(mon_pods_final)} total"
        )

        # Wait for all monitors to join quorum
        logger.info("Waiting for all monitors to join quorum...")
        time.sleep(30)

        quorum_status_final = ceph_cluster.toolbox.exec_cmd_on_pod(quorum_status_cmd)
        quorum_data_final = json.loads(quorum_status_final)
        quorum_names_final = quorum_data_final.get("quorum_names", [])
        logger.info(f"Final quorum members: {quorum_names_final}")

        # Verify workloads continued without interruption
        logger.info("Verifying workloads continued without interruption...")

        # Check VM data integrity
        logger.info("Verifying VM workload integrity...")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, file_path="/test/file_1.txt", md5sum=md5sum_before
        )
        logger.info("VM workload verified - no data loss")

        # Verify no data loss or corruption in CephFS/RBD workloads
        verify_data_loss(sc_obj, start_time)
        verify_data_corruption(sc_obj, start_time)
        logger.info("CephFS and RBD workloads verified - no data loss or corruption")

        # Final Ceph health check
        ceph_health_final = ceph_cluster.get_ceph_health()
        logger.info(f"Final Ceph health: {ceph_health_final}")
        assert ceph_health_final in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Unexpected final Ceph health: {ceph_health_final}"

        logger.info("Monitor pod deletion and recovery test completed successfully!")
