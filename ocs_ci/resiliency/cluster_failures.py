import logging
from ocs_ci.ocs.node import get_node_ips
from abc import ABC, abstractmethod
from ocs_ci.framework import config
from ocs_ci.utility.vsphere import VSPHERE
from ocs_ci.ocs import constants
import random
import time

log = logging.getLogger(__name__)


class ClusterFailures(ABC):
    def __init__(self, cluster_name):
        self.cluster_name = cluster_name

    def random_node_ip(self, node_type="worker"):
        """Return a random node IP of a given node type."""
        ips = get_node_ips(node_type=node_type)
        return random.choice(ips)

    @abstractmethod
    def shutdown_node(self, node_ip=None, node_type="worker"):
        pass

    @abstractmethod
    def change_node_network_interface_state(
        self, node_ip=None, node_type="worker", interface_name=None, connect=False
    ):
        pass

    @abstractmethod
    def network_split(self, nodes):
        pass


class VsphereClusterFailures(ClusterFailures):
    def __init__(self):
        super().__init__(cluster_name="vSphere")
        self.vsphere_host = config.ENV_DATA["vsphere_server"]
        self.vsphere_password = config.ENV_DATA["vsphere_password"]
        self.vsphere_username = config.ENV_DATA["vsphere_user"]
        self.dc = config.ENV_DATA["vsphere_datacenter"]
        self.vsobj = VSPHERE(
            self.vsphere_host, self.vsphere_username, self.vsphere_password
        )

    def shutdown_node(self, node_ip=None, node_type="worker"):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        log.info(f"Shutting down node {node_ip} on vSphere cluster {self.cluster_name}")
        vm = self.vsobj.get_vm_by_ip(node_ip, self.dc)
        self.vsobj.stop_vms([vm])
        log.info(f"Node {node_ip} VM instance stopped.")

    def reboot_node(self, node_ip=None, node_type="worker"):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        vm = self.vsobj.get_vm_by_ip(node_ip, self.dc)
        vm_name = vm.name
        self.vsobj.stop_vms([vm])
        log.info(f"VM instance {vm_name} is stopped.")
        time.sleep(20)
        self.vsobj.start_vms([vm])
        log.info(f"VM instance {vm_name} is started.")

    def change_node_network_interface_state(
        self, node_ip=None, node_type="worker", interface_name=None, connect=False
    ):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        log.info(
            f"{'Connecting' if connect else 'Disconnecting'} network interface"
            f"of node {node_ip} on vSphere cluster {self.cluster_name}"
        )
        self.vsobj.change_vm_network_state(node_ip, self.dc, connect=connect)

    def network_split(self, nodes):
        log.warning("Function 'network_split' is not implemented.")
        raise NotImplementedError("Function 'network_split' is not implemented.")


class IbmCloudClusterFailures(ClusterFailures):
    def __init__(self):
        super().__init__(cluster_name="IBM Cloud")

    def shutdown_node(self, node_ip=None, node_type="worker"):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        log.info(
            f"Shutting down node {node_ip} on IBM Cloud cluster {self.cluster_name}"
        )
        raise NotImplementedError("IBM Cloud shutdown logic is not implemented.")

    def change_node_network_interface_state(
        self, node_ip=None, node_type="worker", interface_name=None, connect=False
    ):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        log.info(
            f"{'Connecting' if connect else 'Disconnecting'} network interface"
            f" of node {node_ip} on IBM Cloud cluster {self.cluster_name}"
        )
        # Add IBM Cloud-specific logic here

    def network_split(self, nodes):
        log.info(
            f"Simulating network split on nodes {nodes} in IBM Cloud cluster {self.cluster_name}"
        )
        # Add IBM Cloud-specific network split logic


class AwsClusterFailures(ClusterFailures):
    def __init__(self):
        super().__init__(cluster_name="AWS")

    def shutdown_node(self, node_ip=None, node_type="worker"):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        log.info(f"Shutting down node {node_ip} on AWS cluster {self.cluster_name}")
        # Add AWS-specific shutdown logic

    def change_node_network_interface_state(
        self, node_ip=None, node_type="worker", interface_name=None, connect=False
    ):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        log.info(
            f"{'Connecting' if connect else 'Disconnecting'} network interface"
            f"of node {node_ip} on AWS cluster {self.cluster_name}"
        )
        # Add AWS-specific logic here

    def network_split(self, nodes):
        log.info(
            f"Simulating network split on nodes {nodes} in AWS cluster {self.cluster_name}"
        )
        # Add AWS-specific network split logic


class BaremetalClusterFailures(ClusterFailures):
    def __init__(self):
        super().__init__(cluster_name="Bare Metal")

    def shutdown_node(self, node_ip=None, node_type="worker"):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        log.info(
            f"Shutting down node {node_ip} on Bare Metal cluster {self.cluster_name}"
        )
        # Add bare metal-specific shutdown logic

    def change_node_network_interface_state(
        self, node_ip=None, node_type="worker", interface_name=None, connect=False
    ):
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)
        log.info(
            f"{'Connecting' if connect else 'Disconnecting'} network interface"
            f" of node {node_ip} on Bare Metal cluster {self.cluster_name}"
        )
        # Add bare metal-specific logic here

    def network_split(self, nodes):
        log.info(
            f"Simulating network split on nodes {nodes} in Bare Metal cluster {self.cluster_name}"
        )
        # Add bare metal-specific network split logic


def get_cluster_object():
    platform = config.ENV_DATA["platform"].lower()
    if platform == constants.VSPHERE_PLATFORM:
        return VsphereClusterFailures()
    elif platform == constants.AWS_PLATFORM:
        return AwsClusterFailures()
    elif platform == constants.IBMCLOUD_PLATFORM:
        return IbmCloudClusterFailures()
    elif platform == constants.BAREMETAL_PLATFORM:
        return BaremetalClusterFailures()
    else:
        raise ValueError(f"Unsupported platform: {platform}")
