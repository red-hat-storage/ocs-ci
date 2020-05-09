import logging

from ocs_ci.framework import config
from ocs_ci.utility import vsphere
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_deviceset_pvs
from .platform import PlatfromBase


logger = logging.getLogger(__name__)


class VSPHERE(PlatfromBase):
    """
    VMWare nodes class

    """
    def __init__(self):
        super(VSPHERE, self).__init__()
        self.cluster_name = config.ENV_DATA.get("cluster_name")
        self.server = config.ENV_DATA['vsphere_server']
        self.user = config.ENV_DATA['vsphere_user']
        self.password = config.ENV_DATA['vsphere_password']
        self.cluster = config.ENV_DATA['vsphere_cluster']
        self.datacenter = config.ENV_DATA['vsphere_datacenter']
        self.datastore = config.ENV_DATA['vsphere_datastore']
        self.vsphere = vsphere.VSPHERE(self.server, self.user, self.password)

    def get_vms(self, nodes):
        """
        Get vSphere vm objects list

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            list: vSphere vm objects list

        """
        vms_in_pool = self.vsphere.get_all_vms_in_pool(
            self.cluster_name, self.datacenter, self.cluster
        )
        node_names = [node.get().get('metadata').get('name') for node in nodes]
        vms = []
        for node in node_names:
            node_vms = [vm for vm in vms_in_pool if vm.name in node]
            vms.extend(node_vms)
        return vms

    def get_data_volumes(self, pvs=None):
        """
        Get the data vSphere volumes

        Args:
            pvs (list): PV OCS objects

        Returns:
            list: vSphere volumes

        """
        if not pvs:
            pvs = get_deviceset_pvs()
        return [
            pv.get().get('spec').get('vsphereVolume').get('volumePath') for pv in pvs
        ]

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "get node by attached volume functionality is not implemented"
        )

    def stop_nodes(self, nodes, force=True):
        """
        Stop vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.stop_vms(vms, force=force)

    def start_nodes(self, nodes, wait=True):
        """
        Start vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.start_vms(vms)

    def restart_nodes(self, nodes, force=True):
        """
        Restart vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.restart_vms(vms, force=force)

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        """
        Detach disk from a VM and delete from datastore if specified

        Args:
            volume (str): Volume path
            node (OCS): The OCS object representing the node
            delete_from_backend (bool): True for deleting the disk (vmdk)
                from backend datastore, False otherwise

        """
        vm = self.get_vms([node])[0]
        self.vsphere.remove_disk(
            vm=vm, identifier=volume, key='volume_path',
            datastore=delete_from_backend
        )

    def create_and_attach_volume(self, node, size):
        """
        Create a new volume and attach it to the given VM

        Args:
            node (OCS): The OCS object representing the node
            size (int): The size in GB for the new volume

        """
        vm = self.get_vms([node])[0]
        self.vsphere.add_disk(vm, size)

    def attach_volume(self, node, volume):
        raise NotImplementedError(
            "Attach volume functionality is not implemented for VMWare"
        )

    def wait_for_volume_attach(self, volume):
        logger.info("Not waiting for volume to get re-attached")
        pass

    def restart_nodes_teardown(self):
        """
        Make sure all VMs are up by the end of the test

        """
        vms = self.get_vms(self.cluster_nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in self.cluster_nodes]}"
        )
        stopped_vms = [
            vm for vm in vms if self.vsphere.get_vm_power_status(vm) == constants.VM_POWERED_OFF
        ]
        # Start the VMs
        if stopped_vms:
            logger.info(f"The following VMs are powered off: {stopped_vms}")
            self.vsphere.start_vms(stopped_vms)
