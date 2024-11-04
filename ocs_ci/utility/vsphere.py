"""
This module contains the vSphere related methods
"""
import logging
import os
import ssl

import atexit

from copy import deepcopy
from pyVmomi import vim, vmodl
from pyVim.task import WaitForTask, WaitForTasks
from pyVim.connect import Disconnect, SmartStubAdapter, VimSessionOrientedStub
from ocs_ci.ocs.exceptions import (
    ResourcePoolNotFound,
    VMMaxDisksReachedException,
    VSLMNotFoundException,
)
from ocs_ci.ocs.constants import (
    GB2KB,
    VM_DISK_TYPE,
    VM_DISK_MODE,
    VM_POWERED_OFF,
    DISK_MODE,
    COMPATABILITY_MODE,
    DISK_PATH_PREFIX,
    VMFS,
    VM_DEFAULT_NETWORK,
    VM_DEFAULT_NETWORK_ADAPTER,
)
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
                connectionPoolTimeout=0,
            )
            session_stub = VimSessionOrientedStub(
                smart_stub,
                VimSessionOrientedStub.makeUserLoginMethod(self._user, self._password),
            )
            service_instance = vim.ServiceInstance("ServiceInstance", session_stub)

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

    def get_all_objs(self, content, vimtype, folder=None, recurse=True):
        """
        Generate objects of type vimtype

        Args:
            content (vim.ServiceInstanceContent): Service Instance Content
            vimtype (vim.type): Type of vim
                (e.g: For VM's, type is vim.VirtualMachine
                For Hosts, type is vim.HostSystem)
            folder (str): Folder name
            recurse (bool): True for recursive search

        Returns:
            dict: Dictionary of objects and corresponding name
               e.g:{
                   'vim.Datastore:datastore-12158': 'datastore1 (1)',
                   'vim.Datastore:datastore-12157': 'datastore1 (2)'
                   }

        """
        if not folder:
            folder = content.rootFolder

        obj = {}
        container = content.viewManager.CreateContainerView(folder, vimtype, recurse)
        for managed_object_ref in container.view:
            obj.update({managed_object_ref: managed_object_ref.name})
        container.Destroy()
        return obj

    def find_object_by_name(self, content, name, obj_type, folder=None, recurse=True):
        """
        Finds object by given name

        Args:
            content (vim.ServiceInstanceContent): Service Instance Content
            name (str): Name to search
            obj_type (list): list of vim.type
                (e.g: For VM's, type is vim.VirtualMachine
                For Hosts, type is vim.HostSystem)
            folder (str): Folder name
            recurse (bool): True for recursive search

        Returns:
            vim.type: Type of vim instance
            None: If vim.type doesn't exists

        """
        if not isinstance(obj_type, list):
            obj_type = [obj_type]

        objects = self.get_all_objs(content, obj_type, folder=folder, recurse=recurse)
        for obj in objects:
            if obj.name == name:
                return obj

        return None

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
        return self.get_search_index.FindByIp(
            datacenter=self.get_dc(dc), ip=str(ip), vmSearch=vm_search
        )

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

        Raises:
            ResourcePoolNotFound: when Resource pool doesn't exist

        """
        rp = self.get_pool(name, dc, cluster)
        if not self.is_resource_pool_exist(name, dc, cluster):
            raise ResourcePoolNotFound
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
        return [
            device
            for device in vm.config.hardware.device
            if (
                isinstance(device, vim.vm.device.VirtualSCSIController)
                or isinstance(device, vim.vm.device.VirtualSCSIController)
            )
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
            if hasattr(device.backing, "fileName"):
                unit_number = max(unit_number, int(device.unitNumber) + 1)
                # unit_number 7 reserved for scsi controller
                if unit_number == 7:
                    unit_number += 1
                # TODO: Add new contoller if there are more than 15 disks
                if unit_number >= 16:
                    logger.error("More than 15 disks for controller is not supported")
                    raise VMMaxDisksReachedException
        return unit_number

    def add_disk(self, vm, size, disk_type="thin"):
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

    def add_disks(self, num_disks, vm, size, disk_type="thin"):
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

    def add_rdm_disk(self, vm, device_name, disk_mode=None, compatibility_mode=None):
        """
        Attaches RDM disk to vm

        Args:
            vm (vim.VirtualMachine): VM instance
            device_name (str): Device name to add to VM.
                e.g:"/vmfs/devices/disks/naa.600304801b540c0125ef160f3048faba"
            disk_mode (str): Disk mode. By default it will
                add 'independent_persistent'. Available modes are 'append',
                'independent_nonpersistent', 'independent_persistent',
                'nonpersistent', 'persistent', and 'undoable'
            compatibility_mode (str): Compatabilty mode. Either 'physicalMode'
                or 'virtualMode'. By default it will add 'physicalMode'.

        """
        logger.info(f"Adding RDM disk {device_name} to {vm.config.name}")
        if not disk_mode:
            disk_mode = DISK_MODE
        if not compatibility_mode:
            compatibility_mode = COMPATABILITY_MODE

        spec = vim.vm.ConfigSpec()
        controller = self.get_controller_for_adding_disk(vm)
        unit_number = self.get_unit_number(vm)
        logger.info(f"Unit number for new disk: {unit_number}")

        device_changes = []
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.fileOperation = "create"
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        disk_spec.device = vim.vm.device.VirtualDisk()
        rdm_info = vim.vm.device.VirtualDisk.RawDiskMappingVer1BackingInfo()
        disk_spec.device.backing = rdm_info
        disk_spec.device.backing.compatibilityMode = compatibility_mode
        disk_spec.device.backing.diskMode = disk_mode
        disk_spec.device.backing.deviceName = device_name
        disk_spec.device.unitNumber = unit_number
        disk_spec.device.controllerKey = controller.key

        device_changes.append(disk_spec)
        spec.deviceChange = device_changes
        WaitForTask(vm.ReconfigVM_Task(spec=spec))
        logger.info(f"RDM disk {device_name} added successfully to {vm.config.name}")

    def add_pci_device(self, vm, pci_device):
        """
        Attaches PCI device to VM

        Args:
            vm (vim.VirtualMachine): VM instance
            pci_device (vim.vm.PciPassthroughInfo): PCI device to add

        """
        host = vm.runtime.host.name
        logger.info(
            f"Adding PCI device with ID:{pci_device.pciDevice.id} on host {host} to {vm.name}"
        )
        deviceId = hex(pci_device.pciDevice.deviceId % 2**16).lstrip("0x")
        backing = vim.VirtualPCIPassthroughDeviceBackingInfo(
            deviceId=deviceId,
            id=pci_device.pciDevice.id,
            systemId=pci_device.systemId,
            vendorId=pci_device.pciDevice.vendorId,
            deviceName=pci_device.pciDevice.deviceName,
        )

        hba_object = vim.VirtualPCIPassthrough(key=-100, backing=backing)
        new_device_config = vim.VirtualDeviceConfigSpec(device=hba_object)
        new_device_config.operation = "add"

        vmConfigSpec = vim.vm.ConfigSpec()
        vmConfigSpec.memoryReservationLockedToMax = True
        vmConfigSpec.deviceChange = [new_device_config]
        WaitForTask(vm.ReconfigVM_Task(spec=vmConfigSpec))

    def get_passthrough_enabled_devices(self, vm):
        """
        Fetches the passthrough enabled devices

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
            list: List of PciPassthroughInfo

        """
        return vm.environmentBrowser.QueryConfigTarget(host=None).pciPassthrough

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

    def stop_vms(self, vms, force=True, wait=True):
        """
        Stop VMs

        Args:
            vms (list): VM (vm) objects
            force (bool): True for VM ungraceful power off, False for
                graceful VM shutdown
            wait (bool): Wait for the VMs to stop

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

            if wait:
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
                if not (None in ips or "<unset>" in ips):
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
        Restart VMs by VM Reset or Guest reboot

        Args:
            vms (list): VM (vm) objects
            force (bool): True for Hard reboot(VM Reset),
                False for Soft reboot(Guest Reboot)

        """
        logger.info(f"Rebooting VMs: {[vm.name for vm in vms]}")
        if force:
            tasks = [vm.ResetVM_Task() for vm in vms]
            WaitForTasks(tasks, self._si)
        else:
            [vm.RebootGuest() for vm in vms]

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

    def is_resource_pool_prefix_exist(self, pool_prefix, dc, cluster):
        """
        Check whether or not resource pool with the provided prefix exist

        Args:
            pool_prefix (str): The prefix to look for
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            bool: True if a resource pool with the same name prefix exists, False otherwise

        """
        cluster_obj = self.get_cluster(cluster, dc)
        for rp in cluster_obj.resourcePool.resourcePool:
            if rp.name.startswith(pool_prefix):
                return True
        return False

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

    def remove_vms_from_inventory(self, vms):
        """
        Remove the VM's from inventory

        Args:
            vms (list): VM instance list

        """
        self.poweroff_vms(vms)
        for vm in vms:
            logger.info(f"Removing VM from inventory: {vm.name}")
            vm.UnregisterVM()

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

    def get_disks(self, vm):
        """
        Fetches the information of all disks in a VM

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
            list: List which contains disk related information

        """
        disks = []
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualDisk):
                backing = device.backing
                if hasattr(backing, "uuid"):
                    wwn = getattr(backing, "uuid", "N/A")
                    disk_info = {
                        "deviceName": device.deviceInfo.label,
                        "capacityInKB": device.capacityInKB,
                        "unitNumber": device.unitNumber,
                        "datastore": backing.datastore,
                        "isthinProvisioned": backing.thinProvisioned
                        if hasattr(backing, "thinProvisioned")
                        else False,
                        "uuid": backing.uuid,
                        "eagerlyScrub": backing.eagerlyScrub,
                        "fileName": backing.fileName
                        if hasattr(backing, "fileName")
                        else "N/A",
                        "wwn": wwn,
                    }
                    disks.append(disk_info)
        logger.debug(f"Disks on node {vm.name} is {disks}")
        return disks

    def remove_disk(self, vm, identifier, key="unit_number", datastore=True):
        """
        Removes the Disk from VM and datastore. By default, it will delete
        the disk ( vmdk ) from VM and backend datastore. If datastore parameter
        is set to False, then it will ONLY removes the disk from VM

        Args:
            vm (vim.VirtualMachine): VM instance
            identifier (str): The value of either 'unit_number'
                (Disk unit number to remove), 'volume_path'
                (The volume path in the datastore (i.e,
                '[vsanDatastore] d4210a5e-40ce-efb8-c87e-040973d176e1/control-plane-1.vmdk'),
                or 'disk_name'(The disk name (i.e, 'scsi-36000c290a2cffeb9fcf4a5f748e21909')
            key (str): Either 'unit_number', 'volume_path', or 'disk_name'
            datastore (bool): Delete the disk (vmdk) from backend datastore
                if True

        """
        virtual_disk_spec = vim.vm.device.VirtualDeviceSpec()
        if datastore:
            virtual_disk_spec.fileOperation = (
                vim.vm.device.VirtualDeviceSpec.FileOperation.destroy
            )
        virtual_disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.remove

        virtual_disk_device = self.get_device_by_key(vm, identifier, key)

        if not virtual_disk_device:
            logger.warning(
                f"Volume with {key} {identifier} for {vm.name} could not be found"
            )

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
            device.unitNumber
            for device in vm.config.hardware.device
            if hasattr(device.backing, "fileName") and device.unitNumber != 0
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
            try:
                if vm.name == name:
                    _rc = True
            except vmodl.fault.ManagedObjectNotFound as ex:
                logger.exception(
                    "There was an exception hit while attempting to check if folder exists!"
                )
                if (
                    "has already been deleted or has not been completely create"
                    not in str(ex)
                ):
                    raise

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

    def get_host(self, vm):
        """
        Fetches the Host for the VM. Host where VM resides

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
             vim.HostSystem: Host instance

        """
        return vm.runtime.host

    def get_storage_devices(self, host):
        """
        Fetches all the storage devices in the Host. It excludes
        the enclosures.

        Args:
            host (vim.HostSystem): Host instance

        Returns:
            list: List of storage devices in Host

        """
        logger.debug(f"Fetching all the storage devices in host {host.name}")
        storage_system = host.configManager.storageSystem
        storage_device_info = storage_system.storageDeviceInfo
        return [
            ScsiDisk.deviceName
            for ScsiDisk in storage_device_info.scsiLun
            if ScsiDisk.deviceType == "disk"
        ]

    def get_mounted_devices_in_vsan(self, host):
        """
        Fetches the devices which was mounted in VSAN

        Args:
            host (vim.HostSystem): Host instance

        Returns:
            list: List of storage devices which was mounted

        """
        device_list = []
        logger.debug(f"Fetching all the storage devices mounted in host {host.name}")
        disk_mapping = host.config.vsanHostConfig.storageInfo.diskMapping
        for each in disk_mapping:
            device_list.append(each.ssd.devicePath)
            for hd in each.nonSsd:
                device_list.append(hd.devicePath)
        logger.debug(f"Mounted devices in Host {host.name}: {device_list}")
        return device_list

    def get_mounted_devices_in_vmfs(self, host):
        """
        Fetches the devices which was mounted in VMFS

        Args:
            host (vim.HostSystem): Host instance

        Returns:
            list: List of storage devices which was mounted

        """
        device_list = []
        logger.debug(f"Fetching all the storage devices mounted in host {host.name}")
        mount_info = host.config.fileSystemVolume.mountInfo
        for each in mount_info:
            try:
                if each.volume.extent:
                    extent = each.volume.extent
                    for scsidisk in extent:
                        disk_path = os.path.join(DISK_PATH_PREFIX, scsidisk.diskName)
                        device_list.append(disk_path)
            except AttributeError:
                continue
        logger.debug(f"Mounted devices in Host {host.name}: {device_list}")
        return device_list

    def get_mounted_devices(self, host, datastore_type="VMFS"):
        """
        Fetches the available storage devices on Host.

        Args:
            host (vim.HostSystem): Host instance
            datastore_type (str): Type of datastore. Either VMFS or vsan
                By default, it will take VMFS as datastore type.

        Returns:
            list: List of storage devices available for use

        """
        if datastore_type == VMFS:
            return self.get_mounted_devices_in_vmfs(host)
        else:
            return self.get_mounted_devices_in_vsan(host)

    def get_active_partition(self, host):
        """
        Fetches the active partition disk on Host

        Args:
            host (vim.HostSystem): Host instance

        Returns:
            str: Active partition disk

        """
        logger.debug(f"Getting the active partition device in host {host.name}")
        active_partition = host.config.activeDiagnosticPartition
        if not active_partition:
            active_partition = self.get_active_partition_from_mount_info(host)
            return active_partition
        return active_partition.id.diskName

    def available_storage_devices(self, host, datastore_type="VMFS"):
        """
        Fetches the available storage devices on Host.

        Args:
            host (vim.HostSystem): Host instance
            datastore_type (str): Type of datastore. Either VMFS or vsan
                By default, it will take VMFS as datastore type.

        Returns:
            list: List of storage devices available for use

        """
        storage_devices = self.get_storage_devices(host)
        mounted_devices = self.get_mounted_devices(host, datastore_type)
        used_devices = self.get_used_devices(host)
        active_partition = self.get_active_partition(host)

        used_devices_all = deepcopy(mounted_devices)
        if used_devices:
            used_devices_all += used_devices

        devices_with_active_disk = list(set(storage_devices) - set(used_devices_all))
        logger.debug(f"Host {host.name} Storage Devices information:")
        logger.debug(f"Available Storage Devices: {storage_devices}")
        logger.debug(f"Mounted Storage Devices: {mounted_devices}")
        logger.debug(f"Used Storage Devices: {used_devices}")
        logger.debug(
            f"Storage Devices with active partition:" f" {devices_with_active_disk}"
        )

        return [
            device
            for device in devices_with_active_disk
            if active_partition not in device
        ]

    def get_all_vms_in_dc(self, dc):
        """
        Fetches all VMs in Datacenter

        Args:
            dc (str): Datacenter name

        Returns:
            list: List of VMs instance in a Datacenter

        """
        vms = []
        dc = self.get_dc(dc)
        vmfolder = dc.vmFolder
        vmlist = vmfolder.childEntity
        for each in vmlist:
            if hasattr(each, "childEntity"):
                for vm in each.childEntity:
                    vms.append(vm)
            else:
                # Direct VMs created in cluster
                # This are the VMs created directly on cluster
                # without ResourcePool
                vms.append(each)
        return vms

    def get_lunids(self, dc):
        """
        Fetches the LUN ids from the Datacenter

        Args:
            dc (str): Datacenter name

        Returns:
            dict: Dictionary contains host name as key and
                values as list lun ids
                    e.g:{
                        'HostName1': ['02000000193035e73d534'],
                        'HostName2': ['020000000060034d43333']
                        }

        """
        lunids = {}
        vms = self.get_all_vms_in_dc(dc)
        for vm in vms:
            for device in vm.config.hardware.device:
                if hasattr(device.backing, "lunUuid"):
                    host = self.get_host(vm).name
                    if host not in lunids.keys():
                        lunids[host] = []
                    lunids[host].append(device.backing.lunUuid)
        return lunids

    def map_lunids_to_devices(self, **kwargs):
        """
        Maps the LUN ids to storage devices

        Args:
            **kwargs (dict): Host to LUN mapping
                e.g:
                ``data = get_lunids(dc)
                map_lunids_to_devices(**data)``

        Returns:
            dict: Dictionary contains host instance as key and
                value as list of device path

        """
        data = kwargs
        host_devices_mapping = {}
        logger.debug("Mapping LUN ids to Devices")
        for each_host in data:
            host = self.get_host_obj(each_host)
            if host not in host_devices_mapping.keys():
                host_devices_mapping[host] = []
            storagedeviceinfo = host.configManager.storageSystem.storageDeviceInfo
            scsilun = storagedeviceinfo.scsiLun
            for scsidisk in scsilun:
                if scsidisk.uuid in data[each_host]:
                    host_devices_mapping[host].append(scsidisk.devicePath)
        logger.debug(f"Host to Device Mapping: {host_devices_mapping}")
        return host_devices_mapping

    def get_host_obj(self, host_name):
        """
        Fetches the Host object

        Args:
            host_name (str): Host name

        Returns:
            vim.HostSystem: Host instance

        """
        content = self.get_content
        host_view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.HostSystem], True
        )
        host_obj = [host for host in host_view.view]
        host_view.Destroy()
        for host in host_obj:
            if host.name == host_name:
                return host

    def get_used_devices(self, host):
        """
        Fetches the used storage devices in Host.

        Note: Storage devices may be used in different Resource Pools
        of OCS clusters.

        Args:
             host (vim.HostSystem): Host instance

        Returns:
            list: List of storage devices used

        """
        logger.debug(f"Fetching all the storage devices used in host {host.name}")
        cluster = host.parent
        dc = cluster.parent.parent.name
        lunids = self.get_lunids(dc)
        host_devices_mapping = self.map_lunids_to_devices(**lunids)
        return host_devices_mapping.get(host)

    def get_active_partition_from_mount_info(self, host):
        """
        Gets the active partition from mount info

        Args:
            host (vim.HostSystem): Host instance

        Returns:
            str: Active partition disk

        """
        logger.debug("Fetching active partition from fileSystemVolume information")
        mount_info = host.config.fileSystemVolume.mountInfo
        for each in mount_info:
            try:
                if each.volume.extent:
                    return each.volume.extent[0].diskName
            except AttributeError:
                continue

    def erase_partition(self, host, device_path):
        """
        Erase the partitions on the disk

        Args:
            host (vim.HostSystem): Host instance
            device_path (str): Device path to erase the partition
               e.g:"/vmfs/devices/disks/naa.910229801b540c0125ef160f3048faba"

        """
        # set empty partition spec
        spec = vim.HostDiskPartitionSpec()
        host.configManager.storageSystem.UpdateDiskPartitions(device_path, spec)

    def find_datastore_by_name(self, datastore_name, datacenter_name):
        """
        Fetches the Datastore

        Args:
            datastore_name (str): Name of the Datastore
            datacenter_name (str): Name of the Datacenter

        Returns:
            vim.Datastore: Datastore instance

        """
        dc = self.find_datacenter_by_name(datacenter_name)
        for ds in dc.datastore:
            if ds.name == datastore_name:
                return ds

    def find_datacenter_by_name(self, datacenter_name):
        """
        Fetches the Datacenter

        Args:
            datacenter_name (str): Name of the Datacenter

        Returns:
            vim.Datacenter: Datacenter instance

        """
        return self.find_object_by_name(
            self.get_content, datacenter_name, [vim.Datacenter]
        )

    def get_datastore_type(self, datastore):
        """
        Gets the Datastore Type

        Args:
            datastore (vim.Datastore): Datastore instance

        Returns:
            str: Datastore type. Either VMFS or vsan

        """
        return datastore.summary.type

    def get_datastore_type_by_name(self, datastore_name, datacenter_name):
        """
        Gets the Datastore Type

        Args:
            datastore_name (str): Name of the Datastore
            datacenter_name (str): Name of the Datacenter

        Returns:
            str: Datastore type. Either VMFS or vsan

        """
        datastore = self.find_datastore_by_name(datastore_name, datacenter_name)
        return self.get_datastore_type(datastore)

    def get_datastore_free_capacity(self, datastore_name, datacenter_name):
        """
        Gets the Datastore capacity

        Args:
            datastore_name (str): Name of the Datastore
            datacenter_name (str): Name of the Datacenter

        Returns:
            int: Datastore capacity in bytes

        """
        ds_obj = self.find_datastore_by_name(datastore_name, datacenter_name)
        return ds_obj.summary.freeSpace

    def clone_vm(
        self,
        vm_name,
        template_name,
        datacenter_name,
        resource_pool_name,
        datastore_name,
        cluster_name,
        cpus=4,
        memory=8,
        root_disk_size=125829120,
        network_adapter="VM Network",
        power_on=True,
        **kwargs,
    ):
        """
        Clones the VM from template

        Args:
            vm_name (str): Name of the VM to create
            template_name (str): Template name to clone
            datacenter_name (str): Name of the Datacenter
            resource_pool_name (str): Name of the Resource Pool
            datastore_name (str): Name of the Datastore
            cluster_name (str): Name of the Cluster in Datacenter
            cpus (int): Number of CPU's
            memory (int): Memory in MB
            root_disk_size (int): Root Disk size in KB
            network_adapter (str): Name of the Network Adapter
            power_on (bool): True to power on the VM after cloning

        """
        data = kwargs
        datacenter = self.find_datacenter_by_name(datacenter_name)
        datastore = self.find_datastore_by_name(datastore_name, datacenter_name)
        dest_folder = datacenter.vmFolder

        resource_pool = self.get_pool(resource_pool_name, datacenter_name, cluster_name)
        template = self.find_object_by_name(
            self.get_content, template_name, [vim.VirtualMachine], dest_folder
        )

        # set relocate specification
        relocate_spec = vim.vm.RelocateSpec()
        relocate_spec.datastore = datastore
        relocate_spec.pool = resource_pool

        # clonespec arguments
        clonespec_kwargs = {}
        clonespec_kwargs["location"] = relocate_spec
        clonespec_kwargs["powerOn"] = power_on

        # update the root disk size
        disks = [
            x
            for x in template.config.hardware.device
            if isinstance(x, vim.vm.device.VirtualDisk)
        ]
        diskspec = vim.vm.device.VirtualDeviceSpec()
        diskspec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
        diskspec.device = disks[0]
        diskspec.device.capacityInKB = root_disk_size
        configspec = vim.vm.ConfigSpec()
        clonespec_kwargs["config"] = configspec

        # update the network adapter
        devices = []
        for device in template.config.hardware.device:
            if hasattr(device, "addressType"):
                nic = vim.vm.device.VirtualDeviceSpec()
                nic.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                nic.device = device
                nic.device.deviceInfo.summary = network_adapter
                device.backing.deviceName = network_adapter
                devices.append(nic)
        devices.append(diskspec)

        # Update the spec with the added network adapter
        clonespec_kwargs["config"].deviceChange = devices

        # Update the spec with the cpus and memory
        clonespec_kwargs["config"].numCPUs = cpus
        clonespec_kwargs["config"].memoryMB = memory

        # VM config specifications
        vm_conf = vim.vm.ConfigSpec()
        if data:
            vm_conf.extraConfig = []
            for index, param in enumerate(data):
                option_value = vim.option.OptionValue()
                option_value.key = param
                option_value.value = data[param]
                vm_conf.extraConfig.append(option_value)

        clonespec_kwargs["config"].extraConfig = vm_conf.extraConfig
        clonespec = vim.vm.CloneSpec(**clonespec_kwargs)

        # get the folder to place the VM
        vm_folder = datacenter.vmFolder
        for each_folder in datacenter.vmFolder.childEntity:
            if resource_pool_name in each_folder.name:
                vm_folder = each_folder
                break

        # clone the template
        logger.debug(f"Cloning VM with name {vm_name}")
        task = template.Clone(folder=vm_folder, name=vm_name, spec=clonespec)
        logger.debug("waiting for cloning to complete")
        self.wait_for_task(task)

    def wait_for_task(self, task):
        """
        Wait for a task to finish

        Args:
            task (instance): Instance for the task

        Returns:
            instance: VM instance

        """
        task_done = False
        while not task_done:
            if task.info.state == "success":
                logger.debug("Cloning VM completed successfully")
                return task.info.result

            if task.info.state == "error":
                logger.error(f"Error while cloning the VM : {task.info.error.msg}")
                task_done = True

    def find_resource_pool_by_name(self, resource_pool_name):
        """
        Fetches the Resource Pool

        Args:
            resource_pool_name (str): Name of the Resource Pool

        Returns:
            instance: Resource Pool instance

        """
        return self.find_object_by_name(
            self.get_content, resource_pool_name, [vim.ResourcePool]
        )

    def find_ip_by_vm(self, vm, datacenter_name, cluster_name, resource_pool_name):
        """
        Fetches the IP for the VM

        Args:
            vm (str): Name of VM
            datacenter_name: Name of the Datacenter
            cluster_name: Name of the cluster
            resource_pool_name: Name of the Resource Pool

        Returns:
            str: IP of the VM

        """
        vm = self.get_vm_in_pool_by_name(
            vm, datacenter_name, cluster_name, resource_pool_name
        )
        return vm.summary.guest.ipAddress

    def find_vms_without_ip(self, name, datacenter_name, cluster_name):
        """
        Find all VMs without IP from resource pool

        Args:
            name (str): Resource pool name
            datacenter_name (str): Datacenter name
            cluster_name (str): vSphere Cluster name

        Returns:
            list: VM instances (vim.VirtualMachine)

        """
        all_vms = self.get_all_vms_in_pool(
            name,
            datacenter_name,
            cluster_name,
        )
        for vm in all_vms:
            logger.info(f"vm name: {vm.name} , IP: {vm.summary.guest.ipAddress}")
        vms_without_ip = [vm for vm in all_vms if not vm.summary.guest.ipAddress]
        for vm in vms_without_ip:
            logger.info(f"VM: {vm.name} doesn't have IP")
        if vms_without_ip:
            return vms_without_ip

    def get_device_by_key(self, vm, identifier, key="unit_number"):
        """
        Get the device by key, and a specific identifier

        Args:
            vm (vim.VirtualMachine): VM instance
            identifier (str): The value of either 'unit_number'
                (Disk unit number to remove), 'volume_path'
                (The volume path in the datastore (i.e,
                '[vsanDatastore] d4210a5e-40ce-efb8-c87e-040973d176e1/control-plane-1.vmdk'),
                or 'disk_name'(The disk name (i.e, 'scsi-36000c290a2cffeb9fcf4a5f748e21909')
            key (str): Either 'unit_number', 'volume_path', or 'disk_name'

        Returns:
            pyVmomi.VmomiSupport.vim.vm.device.VirtualDisk: The virtual disk device object that
                matches the key and the identifier

        """
        virtual_disk_device = None
        vm_volumes = [
            device
            for device in vm.config.hardware.device
            if isinstance(device, vim.vm.device.VirtualDisk)
        ]

        if key == "unit_number":
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

        elif key == "volume_path":
            for vol in vm_volumes:
                if vol.backing.fileName == identifier:
                    virtual_disk_device = vol
                    break

        elif key == "disk_name":
            for vol in vm_volumes:
                uuid_suffix = "".join(vol.backing.uuid.split("-")[1:])
                identifier_suffix = identifier[-len(uuid_suffix) :]
                if uuid_suffix == identifier_suffix:
                    virtual_disk_device = vol
                    break

        return virtual_disk_device

    def get_compute_vms_in_pool(self, name, dc, cluster):
        """
        Gets all compute VM's in Resource pool

        Args:
            name (str): Resource pool name
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            list: VM instances (vim.VirtualMachine)

        """
        vms = self.get_all_vms_in_pool(name, dc, cluster)
        return [vm for vm in vms if vm.name.startswith("compute")]

    def is_template_exist(self, template_name, dc):
        """
        Checks whether template exists in Datacenter

        Args:
            template_name (str): template name
            dc (str): Datacenter name

        Returns:
            bool: True if template exists, otherwise False

        """
        datacenter = self.find_datacenter_by_name(dc)
        dest_folder = datacenter.vmFolder

        return (
            True
            if self.find_object_by_name(
                self.get_content, template_name, [vim.VirtualMachine], dest_folder
            )
            else False
        )

    def is_vm_obj_exist(self, vm):
        """
        Check if the vm object exists.

        Args:
            vm (vim.VirtualMachine): VM instance

        Returns:
            bool: True if the VM object exists, False otherwise.

        """
        vm_name = None
        try:
            logger.info("Trying to get the vm name to see if the vm object exists")
            vm_name = vm.name
        except vmodl.fault.ManagedObjectNotFound:
            logger.info("The vm object is not exist")

        return True if vm_name else False

    def wait_for_vm_delete(self, vm, timeout=60):
        """
        Wait for the vm object to delete.

        Args:
            vm (vim.VirtualMachine): VM instance
            timeout (int): Time to wait for the VM object to delete.

        Returns:
            bool: True if the VM object is deleted in the given timeout, False otherwise.

        """
        sample = TimeoutSampler(
            timeout=timeout, sleep=10, func=self.is_vm_obj_exist, vm=vm
        )
        return sample.wait_for_func_status(result=False)

    def get_network_device(
        self, ip, dc, label=VM_DEFAULT_NETWORK_ADAPTER, network=VM_DEFAULT_NETWORK
    ):
        """
        Get the network adapter for a VM.

        Args:
            ip (str): The IP address of the VM.
            dc (str): The datacenter name where the VM is located.
            label (str, optional): The label of the network adapter. Defaults to "Network adapter 1".
            network (str, optional): The name of the network. Defaults to "VM Network".

        Returns:
            vim.vm.device.VirtualEthernetCard: The network adapter, or None if not found.
        """

        vm_network_spec = vim.vm.device.VirtualEthernetCard
        vm = self.get_vm_by_ip(ip, dc)

        logger.info(f"Finding Network Adapter '{label}' on Virtual Machine '{vm.name}'")
        # Find the network adapter.
        for dev in vm.config.hardware.device:
            if (
                isinstance(dev, vm_network_spec)
                and dev.deviceInfo.label == label
                and dev.deviceInfo.summary == network
            ):
                logger.info(
                    f"Network adapter: '{label}' Found on Virtial Machine: '{vm.name}'."
                )
                return dev

        logger.error(
            f"Network adapter: '{label}' Not Found on Virtial Machine: '{vm.name}'."
        )
        return None

    def change_vm_network_state(
        self,
        ip,
        dc,
        label=VM_DEFAULT_NETWORK_ADAPTER,
        network=VM_DEFAULT_NETWORK,
        timeout=10,
        connect=True,
    ):
        """
        Connects or disconnects a VM's network adapter.

        Args:
            ip (str): The IP address of the VM.
            dc (str): The datacenter name where the VM is located.
            label (str, optional): The label of the network adapter to disconnect. Defaults to "Network adapter 1".
            network (str, optional): The name of the network to disconnect from. Defaults to "VM Network".
            timeout (int, optional): The maximum time to wait for the operation to complete. Defaults to 10 seconds.
            connect (bool, optional): True to connect the network adapter, False to disconnect. Defaults to True.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """

        action = "connected" if connect else "disconnected"
        action_verb = "Connecting" if connect else "Disconnecting"

        # Find the network adapter to change state
        vm_device = self.get_network_device(ip, dc, label=label, network=network)

        if vm_device is None:
            logger.error(
                f"No network adapter found to {action} for Virtual Machine with IP: {ip}."
            )
            return False

        if vm_device.connectable.connected == connect:
            logger.info(
                f"Network adapter is already {action} for Virtual Machine with IP: {ip}."
            )
            return True

        # Change the network adapter state
        vm = self.get_vm_by_ip(ip, dc)
        vm_device.connectable.connected = connect
        spec = vim.vm.ConfigSpec()
        spec.deviceChange = [
            vim.vm.device.VirtualDeviceSpec(device=vm_device, operation="edit")
        ]
        task = vm.ReconfigVM_Task(spec=spec)
        logger.info(
            f"{action_verb} Network adapter '{label}' on Virtual Machine with IP: {ip}."
        )

        # Wait for the task to complete or timeout
        sampler = TimeoutSampler(
            timeout, 1, lambda: task.info.state == vim.TaskInfo.State.success
        )

        if sampler:
            logger.info(
                f"Network adapter '{label}' {action} for Virtual Machine with IP: {ip}."
            )
            return True

        logger.error(
            f"Timeout error: Failed to {action} Network adapter '{label}' on Virtual Machine with IP: {ip}."
        )
        return False

    def get_storage_object_manger(self):
        """
        Gets the vStorageObjectManager

        Returns:
             vim.vslm.vcenter.VStorageObjectManager: vStorageObjectManager

        """
        return self.get_content.vStorageObjectManager

    def get_vslm_id(self, volume_id, datastore, storage):
        """
        Gets the VSLM ID

        Args:
            volume_id (str): Volume ID
            datastore (vim.Datastore): Datastore instance
            storage (vim.vslm.vcenter.VStorageObjectManager): vStorageObjectManager

        Returns:
            vim.vslm.ID

        Raises:
            VSLMNotFoundException: In case VSLM not found

        """
        vslms = storage.ListVStorageObject(datastore)
        for vslm in vslms:
            if vslm.id == volume_id:
                return vslm
        else:
            logger.error(f"vslm not found for volume {volume_id}")
            raise VSLMNotFoundException

    def get_volume_path(self, volume_id, datastore_name, datacenter_name):
        """
        Gets the Volume path

        Args:
            volume_id (str): Volume ID
            datastore_name (str): Name of the Datastore
            datacenter_name (str): Name of the Datacenter

        Returns:
            str: Path to the volume

        """
        ds = self.find_datastore_by_name(datastore_name, datacenter_name)
        storage = self.get_storage_object_manger()
        vslm = self.get_vslm_id(volume_id, ds, storage)
        vstorage_object = storage.RetrieveVStorageObject(vslm, ds)
        volume_path = vstorage_object.config.backing.filePath
        logger.debug(f"File path for volume {volume_id} is `{volume_path}`")
        return volume_path
