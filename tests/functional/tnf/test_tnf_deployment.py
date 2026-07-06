"""
Test cases for Two-Node Failover (TNF) cluster deployment
"""

import logging
import pytest

from ocs_ci.deployment.helpers.tnf_validation import (
    validate_tnf_prerequisites,
    validate_tnf_features,
)
from ocs_ci.deployment.helpers.tnf_helpers import (
    verify_tnf_cluster_topology,
    get_tnf_node_info,
    verify_drbd_configuration,
    verify_drbd_status,
)
from ocs_ci.deployment.tnf import TNFDeployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def tnf_deployment():
    """
    Fixture to provide TNF deployment instance
    """
    return TNFDeployment()


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

    def test_storage_preparation(self, tnf_deployment):
        """
        Test storage preparation (storage class, PVs)
        """
        logger.info("Testing storage preparation...")
        tnf_deployment.prepare_storage()

        # Verify storage class exists
        sc_obj = OCP(kind=constants.STORAGECLASS)
        sc = sc_obj.get(resource_name=constants.TNF_LOCALBLOCK_SC)
        assert sc, f"Storage class {constants.TNF_LOCALBLOCK_SC} not found"

    def test_drbd_configuration(self, tnf_deployment):
        """
        Test DRBD configuration for floating monitor
        """
        logger.info("Testing DRBD configuration...")
        tnf_deployment.configure_drbd_for_floating_monitor()

        # Verify DRBD configuration
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

        from ocs_ci.ocs.cluster import CephCluster

        ceph_cluster = CephCluster()
        ceph_health = ceph_cluster.get_ceph_health()

        assert ceph_health in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Ceph health is not OK/WARN: {ceph_health}"

        logger.info(f"Ceph health: {ceph_health}")


class TestTNFFeatureRestrictions:
    """
    Test class for validating TNF feature restrictions
    """

    def test_unsupported_features_not_enabled(self):
        """
        Test that unsupported features are not enabled
        """
        logger.info("Testing unsupported features...")
        validate_tnf_features()

    def test_noobaa_not_deployed(self):
        """
        Test that NooBaa/MCG is not deployed
        """
        logger.info("Testing NooBaa deployment status...")

        noobaa_pods = get_pods_having_label(
            label="app=noobaa", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        # NooBaa should not be deployed in TNF
        if config.ENV_DATA.get("deployment_type") == "tnf":
            assert (
                not noobaa_pods
            ), "NooBaa pods found but NooBaa is not supported in TNF deployments"

    def test_rgw_not_deployed(self):
        """
        Test that RGW is not deployed
        """
        logger.info("Testing RGW deployment status...")

        rgw_pods = get_pods_having_label(
            label="app=rook-ceph-rgw", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        # RGW should not be deployed in TNF
        if config.ENV_DATA.get("deployment_type") == "tnf":
            assert (
                not rgw_pods
            ), "RGW pods found but RGW is not supported in TNF deployments"


@pytest.mark.polarion_id("OCS-XXXX")
class TestTNFFailover:
    """
    Test class for TNF failover scenarios
    """

    def test_node_reboot_recovery(self):
        """
        Test node reboot and recovery

        This test reboots one node and verifies:
        1. Ceph cluster recovers
        2. Floating monitor relocates if needed
        3. OSD pods restart correctly
        """
        pytest.skip("Implement node reboot test")

    def test_drbd_failover(self):
        """
        Test DRBD failover functionality

        This test verifies DRBD failover when primary node fails
        """
        pytest.skip("Implement DRBD failover test")
