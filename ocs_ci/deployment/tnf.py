"""
Two-Node Failover (TNF) cluster deployment module

This module handles deployment of OpenShift Data Foundation on
two-node clusters with DRBD for high availability.
"""

import logging

from ocs_ci.deployment.baremetal import BMBaseOCPDeployment
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.deployment.helpers.tnf_helpers import (
    verify_tnf_cluster_topology,
    get_tnf_node_info,
    create_local_storage_class,
    create_persistent_volumes,
    configure_drbd,
    verify_drbd_configuration,
    verify_drbd_status,
    verify_port_connectivity,
    get_block_devices_on_node,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedDeploymentConfiguration
from ocs_ci.ocs.resources.pod import get_pods_having_label

logger = logging.getLogger(__name__)


class TNFBASE(Deployment):
    """
    Base class for Two-Node Failover deployments
    """

    def __init__(self):
        super().__init__()
        self.tnf_config = config.ENV_DATA.get("tnf", {})
        logger.info("Initializing TNF Base Deployment")


class TNF(TNFBASE):
    """
    Two-Node Failover deployment class

    Handles complete OCP + ODF deployment on two-node clusters
    following Red Hat ODF 4.22 documentation.
    """

    def __init__(self):
        logger.info("TNF Deployment")
        super().__init__()

    class OCPDeployment(BMBaseOCPDeployment):
        """
        TNF-specific OCP deployment class

        Handles OCP deployment for two-node clusters with DualReplica topology
        """

        def __init__(self):
            super().__init__()
            self.tnf_config = config.ENV_DATA.get("tnf", {})

        def deploy_prereq(self):
            """
            Pre-requisites for TNF OCP deployment

            Verifies two-node cluster requirements
            """
            super(TNF.OCPDeployment, self).deploy_prereq()
            logger.info("Verifying TNF cluster prerequisites...")

            # Get and verify node information
            self.node_info = get_tnf_node_info()
            if len(self.node_info) != 2:
                raise UnexpectedDeploymentConfiguration(
                    f"TNF requires exactly 2 nodes, found {len(self.node_info)}"
                )

            logger.info(
                f"Node 0: {self.node_info[0]['name']} - {self.node_info[0]['ip']}"
            )
            logger.info(
                f"Node 1: {self.node_info[1]['name']} - {self.node_info[1]['ip']}"
            )

        def deploy(self, log_level=""):
            """
            Deploy OCP for TNF cluster

            Args:
                log_level (str): log level for installer
            """
            logger.info("Deploying OCP on TNF cluster...")
            super().deploy(log_level=log_level)

            # Verify DualReplica topology after deployment
            if not verify_tnf_cluster_topology():
                raise UnexpectedDeploymentConfiguration(
                    "Cluster does not have DualReplica topology"
                )
            logger.info("OCP TNF cluster deployed and verified successfully")

    def deploy_prereq(self):
        """
        Pre-deployment checks for TNF ODF deployment
        """
        super().deploy_prereq()
        logger.info("Verifying TNF ODF deployment prerequisites...")

        # Verify cluster topology
        if not verify_tnf_cluster_topology():
            raise UnexpectedDeploymentConfiguration(
                "Cluster must have DualReplica topology for TNF deployment"
            )

        # Get node information
        self.node_info = get_tnf_node_info()
        if len(self.node_info) != 2:
            raise UnexpectedDeploymentConfiguration(
                f"TNF deployment requires exactly 2 nodes, found {len(self.node_info)}"
            )

        # Verify DRBD port connectivity
        for i, node in enumerate(self.node_info):
            peer_node = self.node_info[1 - i]
            verify_port_connectivity(
                node["name"], peer_node["ip"], constants.TNF_DRBD_PORT
            )

        logger.info("TNF ODF prerequisites verified")

    def deploy_ocs_prereq(self):
        """
        Deploy prerequisites for ODF on TNF cluster

        This includes:
        1. Creating local storage class
        2. Creating persistent volumes for OSD disks
        3. Configuring DRBD for floating monitor
        """
        logger.info("Preparing storage for TNF ODF deployment...")

        # Create local storage class
        create_local_storage_class()

        # Get device mappings from config
        device_mappings = self.tnf_config.get("osd_device_mappings", [])
        if device_mappings:
            # Create persistent volumes for OSD disks
            create_persistent_volumes(device_mappings)
        else:
            logger.warning(
                "No OSD device mappings found in config. "
                "PVs must be created manually before StorageCluster creation."
            )

        # Configure DRBD for floating monitor
        logger.info("Configuring DRBD for floating monitor...")

        monitor_disk_node_0 = self.tnf_config.get("monitor_disk_node_0")
        monitor_disk_node_1 = self.tnf_config.get("monitor_disk_node_1")

        if not monitor_disk_node_0 or not monitor_disk_node_1:
            raise UnexpectedDeploymentConfiguration(
                "Monitor disk paths must be specified in config for both nodes. "
                "Set 'tnf.monitor_disk_node_0' and 'tnf.monitor_disk_node_1'"
            )

        # Configure DRBD using ODF operator's script
        configure_drbd(
            self.node_info[0],
            self.node_info[1],
            monitor_disk_node_0,
            monitor_disk_node_1,
        )

        # Verify DRBD configuration
        if not verify_drbd_configuration():
            raise UnexpectedDeploymentConfiguration(
                "DRBD configuration verification failed"
            )

        # Verify DRBD status on both nodes
        for node in self.node_info:
            verify_drbd_status(node["name"])

        logger.info("TNF storage prerequisites completed successfully")

    def deploy_ocs(self):
        """
        Deploy ODF on TNF cluster

        Follows the standard deployment workflow with TNF-specific prerequisites
        """
        logger.info("Deploying ODF on TNF cluster...")

        # Deploy TNF-specific prerequisites (storage, DRBD)
        self.deploy_ocs_prereq()

        # Deploy ODF using parent class method
        # This installs operator and creates StorageCluster
        super().deploy_ocs()

        logger.info("ODF deployment on TNF cluster completed")

    def verify_deployment(self):
        """
        Verify TNF ODF deployment

        Returns:
            bool: True if deployment is verified successfully
        """
        logger.info("Verifying TNF ODF deployment...")

        try:
            # Verify DRBD configuration
            if not verify_drbd_configuration():
                logger.error("DRBD configuration verification failed")
                return False

            # Verify DRBD status on both nodes
            for node in self.node_info:
                if not verify_drbd_status(node["name"]):
                    logger.error(f"DRBD status check failed on node {node['name']}")
                    return False

            # Verify floating monitor pod
            mon_pods = get_pods_having_label(
                label="app=rook-ceph-mon",
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )

            if not mon_pods:
                logger.error("No monitor pods found")
                return False

            logger.info(f"Found {len(mon_pods)} monitor pod(s)")
            logger.info("TNF ODF deployment verified successfully")
            return True

        except Exception as e:
            logger.error(f"TNF deployment verification failed: {e}")
            return False


def list_available_disks():
    """
    Helper function to list available disks on both nodes

    This can be used during deployment planning to identify
    suitable disks for OSD and monitor.

    Returns:
        dict: Dictionary with node names as keys and device lists as values
    """
    logger.info("Listing available disks on TNF nodes...")

    node_info = get_tnf_node_info()
    available_disks = {}

    for node in node_info:
        try:
            devices = get_block_devices_on_node(node["name"])
            available_disks[node["name"]] = devices

            logger.info(f"\nDisks on {node['name']}:")
            for device in devices:
                logger.info(
                    f"  {device['name']}: {device['path']} - "
                    f"{device['size']} (ROTA: {device['rota']}, "
                    f"TYPE: {device['type']})"
                )

        except Exception as e:
            logger.error(f"Failed to list disks on {node['name']}: {e}")

    return available_disks
