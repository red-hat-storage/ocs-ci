"""
This module contains the vSphere related methods
"""
import atexit
import logging
import ssl

from pyVmomi import vim, vmodl
from pyVim.task import WaitForTask
from pyVim.connect import Disconnect, SmartStubAdapter, VimSessionOrientedStub
from ocs_ci.ocs.exceptions import VMMaxDisksReachedException
from ocs_ci.ocs.constants import GB2KB, VM_DISK_TYPE, VM_DISK_MODE

logger = logging.getLogger(__name__)


class VSPHERE(object):
    """
    wrapper for vSphere
    """
    def __init__(self, host, user, password, port=443):
        """
        Initialize the variables required for vCenter server

        Args:
             host (str): Host name
             user (str): User name
             password (): Password for the Host
             port (int): Port number

        """
        self._host = host
        self._user = user
        self._password = password
        self._port = port
        self.sslContext = ssl._create_unverified_context()
        self._si = self._get_service_instance()

    def _get_service_instance(self):
        """
        Gets the service instance

        Returns:
            vim.ServiceInstance: Service Instance for Host

        """
        try:
            smart_stub = SmartStubAdapter(
                host=self._host,
                port=int(self._port),
                sslContext=self.sslContext,
                connectionPoolTimeout=0
            )
            session_stub = VimSessionOrientedStub(
                smart_stub,
                VimSessionOrientedStub.makeUserLoginMethod(self._user, self._password))
            service_instance = vim.ServiceInstance('ServiceInstance', session_stub)

            # Ensure connection to server is closed on program exit
            atexit.register(Disconnect, service_instance)
            return service_instance
        except vmodl.MethodFault as error:
            logger.error(f"Caught vmodl fault : {error.msg}")
            raise

    @property
    def get_content(self):
        """
        Retrieves the content

        Returns:
            vim.ServiceInstanceContent: Service Instance Content for Host

        """
        return self._si.RetrieveContent()

    @property
    def get_search_index(self):
        """
        Get the search index

        Returns:
            vim.SearchIndex: Instance of Search Index

        """
        return self.get_content.searchIndex

    def get_vm_by_ip(self, ip, dc, vm_search=True):
        """
        Gets the VM using IP address

        Args:
            ip (str): IP address
            dc (str): Datacenter name
            vm_search (bool): Search for VMs if True, Hosts if False

        Returns:
            vim.VirtualMachine: VM instance

        """
        return self.get_search_index.FindByIp(datacenter=self.get_dc(dc), ip=str(ip), vmSearch=vm_search)

    def get_dc(self, name):
        """
        Gets the Datacenter

        Args:
            name (str): Datacenter name

        Returns:
            vim.Datacenter: Datacenter instance

        """
        for dc in self.get_content.rootFolder.childEntity:
            if dc.name == name:
                return dc

    def get_cluster(self, name, dc):
        """
        Gets the cluster

        Args:
            name (str): Cluster name
            dc (str): Datacenter name

        Returns:
            vim.ClusterComputeResource: Cluster instance

        """
        for cluster in self.get_dc(dc).hostFolder.childEntity:
            if cluster.name == name:
                return cluster

    def get_pool(self, name, dc, cluster):
        """
        Gets the Resource pool

        Args:
            name (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            vim.ResourcePool: Resource pool instance

        """
        cluster_obj = self.get_cluster(cluster, dc)
        for rp in cluster_obj.resourcePool.resourcePool:
            if rp.name == name:
                return rp

    def get_all_vms_in_pool(self, name, dc, cluster):
        """
        Gets all VM's in Resource pool

        Args:
            name (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            list: VM instances (vim.VirtualMachine)

        """
        rp = self.get_pool(name, dc, cluster)
        return [vm for vm in rp.vm]

    def get_vm_in_pool_by_name(self, name, dc, cluster, pool):
        """
        Gets the VM instance in a resource pool

        Args:
            name (str): VM name
            dc (str): Datacenter name
            cluster (str): Cluster name
            pool (str): pool name

        Returns:
            vim.VirtualMachine: VM instances

        """
        vms = self.get_all_vms_in_pool(pool, dc, cluster)
        for vm in vms:
            if vm.name == name:
                return vm

    def get_controllers(self, vm):
        """
        Get the controllers for VM

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
            list: list of controllers

        """
        return [device for device in vm.config.hardware.device
                if (isinstance(device, vim.vm.device.VirtualSCSIController)
                    or isinstance(device, vim.vm.device.VirtualSCSIController))
                ]

    def get_controller_for_adding_disk(self, vm):
        """
        Gets the controller for adding disk

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
            controller instance

        """
        controllers = self.get_controllers(vm)
        for controller in controllers:
            if len(controller.device) < 15:
                return controller

    def get_unit_number(self, vm):
        """
        Gets the available unit number for the disk

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
            int: available unit number for disk

        """
        unit_number = 0
        for device in vm.config.hardware.device:
            if hasattr(device.backing, 'fileName'):
                unit_number = max(unit_number, int(device.unitNumber) + 1)
                # unit_number 7 reserved for scsi controller
                if unit_number == 7:
                    unit_number += 1
                # TODO: Add new contoller if there are more than 15 disks
                if unit_number >= 16:
                    logger.error("More than 15 disks for controller is not supported")
                    raise VMMaxDisksReachedException
        return unit_number

    def add_disk(self, vm, size, disk_type='thin'):
        """
        Attaches disk to VM

        Args:
            vm (vim.VirtualMachine): VM instance
            size (int) : size of disk in GB
            disk_type (str) : disk type

        """
        logger.info(f"Adding disk to {vm.config.name}")
        spec = vim.vm.ConfigSpec()
        controller = self.get_controller_for_adding_disk(vm)
        unit_number = self.get_unit_number(vm)
        logger.info(f"Unit number for new disk: {unit_number}")

        device_changes = []
        new_disk_kb = int(size) * GB2KB
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.fileOperation = "create"
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        disk_spec.device = vim.vm.device.VirtualDisk()
        disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        if disk_type == VM_DISK_TYPE:
            disk_spec.device.backing.thinProvisioned = True
        disk_spec.device.backing.diskMode = VM_DISK_MODE
        disk_spec.device.unitNumber = unit_number
        disk_spec.device.capacityInKB = new_disk_kb
        disk_spec.device.controllerKey = controller.key
        device_changes.append(disk_spec)
        spec.deviceChange = device_changes
        WaitForTask(vm.ReconfigVM_Task(spec=spec))
        logger.info(f"{size}GB disk added successfully to {vm.config.name}")

    def add_disks(self, num_disks, vm, size, disk_type='thin'):
        """
        Adds multiple disks to the VM

        Args:
            num_disks: number of disks to add
            vm (vim.VirtualMachine): VM instance
            size (int) : size of disk in GB
            disk_type (str) : disk type

        """
        for _ in range(int(num_disks)):
            self.add_disk(vm, size, disk_type)
