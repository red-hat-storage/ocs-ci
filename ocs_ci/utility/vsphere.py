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
from ocs_ci.ocs.exceptions import VMMaxDisksReachedException
from ocs_ci.ocs.constants import (
    GB2KB, VM_DISK_TYPE, VM_DISK_MODE, VM_POWERED_OFF,
    DISK_MODE, COMPATABILITY_MODE, DISK_PATH_PREFIX,
    VMFS,
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
        container = content.viewManager.CreateContainerView(
            folder,
            vimtype,
            recurse
        )
        for managed_object_ref in container.view:
            obj.update({managed_object_ref: managed_object_ref.name})
        container.Destroy()
        return obj

    def find_object_by_name(
            self,
            content,
            name,
            obj_type,
            folder=None,
            recurse=True
    ):
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

        objects = self.get_all_objs(
            content,
            obj_type,
            folder=folder,
            recurse=recurse
        )
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
            ScsiDisk.deviceName for ScsiDisk in storage_device_info.scsiLun
            if ScsiDisk.deviceType == 'disk'
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
        logger.debug(
            f"Fetching all the storage devices mounted in host {host.name}"
        )
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
        logger.debug(
            f"Fetching all the storage devices mounted in host {host.name}"
        )
        mount_info = host.config.fileSystemVolume.mountInfo
        for each in mount_info:
            try:
                if each.volume.extent:
                    extent = each.volume.extent
                    for scsidisk in extent:
                        disk_path = os.path.join(
                            DISK_PATH_PREFIX,
                            scsidisk.diskName
                        )
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
        logger.debug(
            f"Getting the active partition device in host {host.name}"
        )
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

        devices_with_active_disk = list(
            set(storage_devices) - set(used_devices_all)
        )
        logger.debug(f"Host {host.name} Storage Devices information:")
        logger.debug(f"Available Storage Devices: {storage_devices}")
        logger.debug(f"Mounted Storage Devices: {mounted_devices}")
        logger.debug(f"Used Storage Devices: {used_devices}")
        logger.debug(
            f"Storage Devices with active partition:"
            f" {devices_with_active_disk}"
        )

        return [
            device for device in devices_with_active_disk
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
            if hasattr(each, 'childEntity'):
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
                if hasattr(device.backing, 'lunUuid'):
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
            storagedeviceinfo = (
                host.configManager.storageSystem.storageDeviceInfo
            )
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
            content.rootFolder,
            [vim.HostSystem],
            True
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
        logger.debug(
            f"Fetching all the storage devices used in host {host.name}"
        )
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
        logger.debug(
            "Fetching active partition from fileSystemVolume information"
        )
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
        host.configManager.storageSystem.UpdateDiskPartitions(
            device_path,
            spec
        )

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
        folder = dc.datastoreFolder
        return self.find_object_by_name(
            self.get_content,
            datastore_name,
            [vim.Datastore],
            folder=folder
        )

    def find_datacenter_by_name(self, datacenter_name):
        """
        Fetches the Datacenter

        Args:
            datacenter_name (str): Name of the Datacenter

        Returns:
            vim.Datacenter: Datacenter instance

        """
        return self.find_object_by_name(
            self.get_content,
            datacenter_name,
            [vim.Datacenter]
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
        datastore = self.find_datastore_by_name(
            datastore_name,
            datacenter_name
        )
        return self.get_datastore_type(datastore)
