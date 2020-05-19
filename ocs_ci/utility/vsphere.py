"""
This module contains the vSphere related methods
"""
import logging
import ssl

import atexit

from pyVmomi import vim, vmodl
from pyVim.task import WaitForTask, WaitForTasks
from pyVim.connect import Disconnect, SmartStubAdapter, VimSessionOrientedStub
from ocs_ci.ocs.exceptions import VMMaxDisksReachedException
from ocs_ci.ocs.constants import GB2KB, VM_DISK_TYPE, VM_DISK_MODE, VM_POWERED_OFF
from ocs_ci.utility.utils import TimeoutSampler

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

    def get_vm_power_status(self, vm):
        """
        Get the VM power status

        Args:
            vm (vm): VM object

        Returns:
            str: VM power status

        """
        return vm.summary.runtime.powerState

    def get_vms_ips(self, vms):
        """
        Get VMs IPs

        Args:
            vms (list): VM (vm) objects

        Returns:
            list: VMs IPs

        """
        return [vm.summary.guest.ipAddress for vm in vms]

    def stop_vms(self, vms, force=True):
        """
        Stop VMs

        Args:
            vms (list): VM (vm) objects
            force (bool): True for VM ungraceful power off, False for
                graceful VM shutdown

        """
        if force:
            logger.info(f"Powering off VMs: {[vm.name for vm in vms]}")
            tasks = [vm.PowerOff() for vm in vms]
            WaitForTasks(tasks, self._si)

        else:
            logger.info(f"Gracefully shutting down VMs: {[vm.name for vm in vms]}")

            # Can't use WaitForTasks as it requires VMWare tools installed
            # on the guests to check for Shutdown task completion
            _ = [vm.ShutdownGuest() for vm in vms]

            def get_vms_power_status(vms):
                return [self.get_vm_power_status(vm) for vm in vms]

            for statuses in TimeoutSampler(600, 5, get_vms_power_status, vms):
                logger.info(
                    f"Waiting for VMs {[vm.name for vm in vms]} to power off. "
                    f"Current VMs statuses: {statuses}"
                )
                if all(status == VM_POWERED_OFF for status in statuses):
                    logger.info("All VMs reached poweredOff off status")
                    break

    def start_vms(self, vms, wait=True):
        """
        Start VMs

        Args:
            vms (list): VM (vm) objects
            wait (bool): Wait for VMs to start

        """
        logger.info(f"Powering on VMs: {[vm.name for vm in vms]}")
        tasks = [vm.PowerOn() for vm in vms]
        WaitForTasks(tasks, self._si)

        if wait:
            for ips in TimeoutSampler(240, 3, self.get_vms_ips, vms):
                logger.info(
                    f"Waiting for VMs {[vm.name for vm in vms]} to power on "
                    f"based on network connectivity. Current VMs IPs: {ips}"
                )
                if not (None in ips or '<unset>' in ips):
                    break

    def restart_vms_by_stop_and_start(self, vms, force=True):
        """
        Stop and Start VMs

        Args:
            vms (list): VM (vm) objects
            force (bool): True for VM ungraceful power off, False for
                graceful VM shutdown

        """
        self.stop_vms(vms, force=force)
        self.start_vms(vms)

    def restart_vms(self, vms, force=False):
        """
        Restart VMs

        Args:
            vms (list): VM (vm) objects
            force (bool): True for VM ungraceful power off, False for
                graceful VM shutdown

        """
        logger.info(f"Rebooting VMs: {[vm.name for vm in vms]}")
        if force:
            tasks = [vm.ResetVM_Task() for vm in vms]
        else:
            tasks = [vm.RebootGuest() for vm in vms]
        WaitForTasks(tasks, self._si)


    def is_resource_pool_exist(self, pool, dc, cluster):
        """
        Check whether resource pool exists in cluster or not

        Args:
            pool (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            bool: True if resource pool exists, otherwise False

        """
        return True if self.get_pool(pool, dc, cluster) else False

    def poweroff_vms(self, vms):
        """
        Powers off the VM and wait for operation to complete

        Args:
            vms (list): VM instance list

        """
        to_poweroff_vms = []
        for vm in vms:
            status = self.get_vm_power_status(vm)
            logger.info(f"power state of {vm.name}: {status}")
            if status == "poweredOn":
                to_poweroff_vms.append(vm)
        logger.info(f"Powering off VMs: {[vm.name for vm in to_poweroff_vms]}")
        tasks = [vm.PowerOff() for vm in to_poweroff_vms]
        WaitForTasks(tasks, self._si)

    def poweron_vms(self, vms):
        """
        Powers on the VM and wait for operation to complete

        Args:
            vms (list): VM instance list

        """
        to_poweron_vms = []
        for vm in vms:
            status = self.get_vm_power_status(vm)
            logger.info(f"power state of {vm.name}: {status}")
            if status == "poweredOff":
                to_poweron_vms.append(vm)
        logger.info(f"Powering on VMs: {[vm.name for vm in to_poweron_vms]}")
        tasks = [vm.PowerOn() for vm in to_poweron_vms]
        WaitForTasks(tasks, self._si)

    def destroy_vms(self, vms):
        """
        Destroys the VM's

        Args:
             vms (list): VM instance list

        """
        self.poweroff_vms(vms)
        logger.info(f"Destroying VM's: {[vm.name for vm in vms]}")
        tasks = [vm.Destroy_Task() for vm in vms]
        WaitForTasks(tasks, self._si)

    def destroy_pool(self, pool, dc, cluster):
        """
        Deletes the Resource Pool

        Args:
            pool (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        """
        vms_in_pool = self.get_all_vms_in_pool(pool, dc, cluster)
        logger.info(f"VM's in resource pool {pool}: {[vm.name for vm in vms_in_pool]}")
        self.destroy_vms(vms_in_pool)

        # get resource pool instance
        pi = self.get_pool(pool, dc, cluster)
        WaitForTask(pi.Destroy())
        logger.info(f"Successfully deleted resource pool {pool}")

    def remove_disk(self, vm, identifier, key='unit_number', datastore=True):
        """
        Removes the Disk from VM and datastore. By default, it will delete
        the disk ( vmdk ) from VM and backend datastore. If datastore parameter
        is set to False, then it will ONLY removes the disk from VM

        Args:
            vm (vim.VirtualMachine): VM instance
            identifier (str): The value of either 'unit_number'
                (Disk unit number to remove) or 'volume_path'
                (The volume path in the datastore (i.e,
                '[vsanDatastore] d4210a5e-40ce-efb8-c87e-040973d176e1/control-plane-1.vmdk')
            key (str): Either 'unit_number' 'volume_path'
            datastore (bool): Delete the disk (vmdk) from backend datastore
                if True

        """
        virtual_disk_device = None
        virtual_disk_spec = vim.vm.device.VirtualDeviceSpec()
        if datastore:
            virtual_disk_spec.fileOperation = (
                vim.vm.device.VirtualDeviceSpec.FileOperation.destroy
            )
        virtual_disk_spec.operation = (
            vim.vm.device.VirtualDeviceSpec.Operation.remove
        )

        if key == 'unit_number':
            disk_prefix = "Hard disk "
            for dev in vm.config.hardware.device:
                # choose the device based on unit number instead of
                # deviceInfo.label. labels can change if a disk is removed
                if (
                    isinstance(dev, vim.vm.device.VirtualDisk)
                    and dev.unitNumber == identifier
                    and disk_prefix in dev.deviceInfo.label
                ):
                    virtual_disk_device = dev

        elif key == 'volume_path':
            vm_volumes = [
                device for device in vm.config.hardware.device if isinstance(
                    device, vim.vm.device.VirtualDisk
                )
            ]
            for vol in vm_volumes:
                if vol.backing.fileName == identifier:
                    virtual_disk_device = vol
                    break

        if not virtual_disk_device:
            logger.warning(f"Volume with {key} {identifier} for {vm.name} could not be found")

        virtual_disk_spec.device = virtual_disk_device
        spec = vim.vm.ConfigSpec()
        spec.deviceChange = [virtual_disk_spec]
        logger.info(
            f"Detaching Disk with identifier: {identifier}"
            f" from {vm.name} and remove from datastore={datastore}"
        )
        WaitForTask(vm.ReconfigVM_Task(spec=spec))

    def remove_disks(self, vm):
        """
        Removes all the extra disks for a VM

        Args:
            vm (vim.VirtualMachine): VM instance

        """
        extra_disk_unit_numbers = self.get_used_unit_number(vm)
        if extra_disk_unit_numbers:
            for each_disk_unit_number in extra_disk_unit_numbers:
                self.remove_disk(vm=vm, identifier=each_disk_unit_number)

    def get_used_unit_number(self, vm):
        """
        Gets the used unit numbers for a VM

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
            list: list of unit numbers

        """
        return [
            device.unitNumber for device in vm.config.hardware.device
            if hasattr(device.backing, 'fileName') and device.unitNumber != 0
        ]

    def check_folder_exists(self, name, cluster, dc):
        """
        Checks whether folder exists in Templates

        Args:
            name (str): Folder name
            cluster (str): Cluster name
            dc (str): Datacenter name

        Returns:
            bool: True if folder exists, False otherwise

        """
        _rc = False
        dc = self.get_dc(dc)

        vms = dc.vmFolder.childEntity
        for vm in vms:
            if vm.name == name:
                _rc = True
        return _rc

    def destroy_folder(self, name, cluster, dc):
        """
        Removes the folder from Templates

        Args:
            name (str): Folder name
            cluster (str): Cluster name
            dc (str): Datacenter name

        """
        if self.check_folder_exists(name, cluster, dc):
            dc = self.get_dc(dc)
            vms = dc.vmFolder.childEntity
            for vm in vms:
                if vm.name == name:
                    for dvm in vm.childEntity:
                        self.poweroff_vms([dvm])
                    logger.info(f"Destroying folder {name} in templates")
                    WaitForTask(vm.Destroy())
        else:
            logger.info(f"Folder {name} doesn't exist in templates")
