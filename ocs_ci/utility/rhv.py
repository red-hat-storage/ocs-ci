"""
Module for interactions with RHV Cluster

"""
import atexit
import logging

import ovirtsdk4 as sdk
import ovirtsdk4.types as types
from ocs_ci.framework import config
from ocs_ci.ocs.constants import GB, RHV_DISK_FORMAT_RAW, RHV_DISK_INTERFACE_VIRTIO_SCSI
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class RHV(object):
    """
    Wrapper for  RHV
    """

    _engine_connection = None

    def __init__(
        self, url=None, username=None, password=None, insecure=True, ca_file=None
    ):
        """
        Initialize the variables required to connection to the Red Hat Virtualization
        Manager

        Args:
             url (str): Base URL of the manager
                example: https://server.example.com/ovirt-engine/api
             username (str): Name of the user
             password (str): Password of the user
             insecure (bool): True to check server’s TLS certificate and host name,
                False otherwise (default: True)
             ca_file (str): PEM file containing the trusted CA certificates.
                 If not set, system wide CA certificate store is used.
                 (default: None)

        """
        self._ovirt_url = url or config.ENV_DATA["ovirt_url"]
        self._ovirt_username = username or config.ENV_DATA["ovirt_username"]
        self._ovirt_password = password or config.ENV_DATA["ovirt_password"]
        self._insecure = insecure
        self._ca_file = ca_file

    @property
    def engine_connection(self):
        """
        Get the connection to the engine server

        Returns:
            connection: Connection to the engine server

        """
        if not self._engine_connection:
            self._engine_connection = self.create_engine_connection()
        return self._engine_connection

    @property
    def system_service(self):
        """
        Reference to the root of the services tree

        The returned value is an instance of the SystemService class
        """
        return self.engine_connection.system_service()

    @property
    def disks_service(self):
        """
        Reference to the disks service
        """
        return self.system_service.disks_service()

    @property
    def vms_service(self):
        """
        Reference to the virtual machines service
        """
        return self.system_service.vms_service()

    def create_engine_connection(self):
        """
        Create the connection to the engine server

        Returns:
            connection: Connection to the engine server

        """
        try:
            connection = sdk.Connection(
                url=self._ovirt_url,
                username=self._ovirt_username,
                password=self._ovirt_password,
                insecure=self._insecure,
                ca_file=self._ca_file,
            )

            if connection.test(raise_exception=True):
                logger.info("Connected to the engine server")

            # Ensure connection to server is closed on program exit
            atexit.register(self.close_engine_connection, connection)

            return connection
        except sdk.Error as exception:
            logger.error(f"Connection to the engine server failed: {exception}")
            raise exception

    def close_engine_connection(self, connection):
        """
        Releases the resources used by this connection.
        """
        connection.close()

    def get_vm_service(self, vm_id):
        """
        Get the service that manages a specific virtual machine

        Args:
            vm_id (str): unique identifier of the vm

        """
        return self.vms_service.vm_service(vm_id)

    def get_disk_service(self, disk_id):
        """
        Get the disk service managing a specific disk

        Args:
            disk_id (str): unique identifier of the disk

        """
        return self.disks_service.disk_service(disk_id)

    def get_disk_attachments_service(self, vm_id):
        """
        Get the service that manages the disk attachments of the vm

        Args:
            vm_id (str): unique identifier of the vm

        """
        return self.get_vm_service(vm_id).disk_attachments_service()

    def get_disk_attachment_service(self, vm_id, disk_id):
        """
        Get the service that manages a specific disk attachment

        Args:
            vm_id (str): unique identifier of the vm
            disk_id (str): unique identifier of the disk

        """
        return self.get_disk_attachments_service(vm_id).attachment_service(disk_id)

    def get_vms_by_pattern(
        self, pattern=None, case_sensitive=True, key="name", filter_by_cluster_name=True
    ):
        """
        Get the list of virtual machines of the system by pattern.
        If not specified it will return all the vms, or will return the list
        filtered by the cluster name.

        Args:
            pattern (str): query string to restrict the returned vms
            case_sensitive (bool): True to take case into account, False to
                ignore case (default: True)
            key (str): Either 'name', or 'id' (default: name)
            filter_by_cluster_name (bool): Will be used only if the 'pattern' param
                not specified. If True it filters the Vm by the cluster name,
                else if False it returns all Vm.

        Returns:
            list: list of Vm instances

        """
        if not pattern:
            if filter_by_cluster_name:
                pattern = f"{config.ENV_DATA['cluster_name']}*"

        if key == "name":
            pattern = f"{pattern}*"

        vms = self.vms_service.list(
            case_sensitive=case_sensitive, search=f"{key}={pattern}"
        )
        return vms

    def get_vm_names(self):
        """
        Returns:
            list: list of VMs name

        """
        return [
            vm.name
            for vm in self.get_vms_by_pattern(
                pattern=config.ENV_DATA["default_cluster_name"],
                filter_by_cluster_name=True,
            )
        ]

    def get_disks_by_pattern(
        self, pattern=None, case_sensitive=True, key="name", filter_by_cluster_name=True
    ):
        """
        Get the list of disks by pattern.
        If not specified it will return all the disks, or will return the list
        filtered by the cluster name.

        Args:
            pattern (str): query string to restrict the returned disks
            case_sensitive (bool): True to take case into account, False to
                ignore case (default: True)
            key (str): Either 'name', or 'id' (default: name)
            filter_by_cluster_name (bool): Will be used only if the 'pattern' param
                not specified. If True it filters the disks by the cluster name,
                else if False it returns all disks.

        Returns:
            list: list of disks

        """
        if not pattern:
            if filter_by_cluster_name:
                pattern = f"{config.ENV_DATA['cluster_name']}*"

        if key == "name":
            pattern = f"{pattern}*"

        disks = self.disks_service.list(
            case_sensitive=case_sensitive, search=f"{key}={pattern}"
        )
        return disks

    def get_disk_attachments(self, vm_id):
        """
        Get the disks that are attached to the virtual machine

        Args:
            vm_id (str): unique identifier of the vm

        Returns:
            list: list of disks attachments
        """
        return self.get_disk_attachments_service(vm_id).list()

    def get_compute_vms(self):
        """
        Gets all compute VM's

        Returns:
            list: list of compute Vm instances

        """
        vms = self.get_vms_by_pattern(filter_by_cluster_name=True)
        return [vm for vm in vms if "worker" in vm.name]

    def get_rhv_vm_instance(self, vm_name):
        """
        Get RHV VM instance

        args:
            vm_name: name of RHV VM

        Returns:
            vm (types.Vm): Vm instance
        """
        return self.get_vms_by_pattern(pattern=vm_name)[0]

    def add_disk(
        self,
        vm,
        size,
        disk_format=RHV_DISK_FORMAT_RAW,
        disk_interface=RHV_DISK_INTERFACE_VIRTIO_SCSI,
        sparse=None,
        pass_discard=None,
        storage_domain_id=None,
        timeout=120,
    ):
        """
        Attaches disk to VM

        Args:
            vm (types.Vm): Vm instance
            size (int) : size of disk in GB
            disk_format (str): underlying storage format of disks (default: "RAW")
            disk_interface (str): underlying storage interface of disks
                communication with controller (default: 'VIRTIO_SCSI')
            sparse (bool): disk allocation policy. True for sparse,
                false for preallocated (default: None)
            pass_discard (bool): True if the virtual machine passes discard
                commands to the storage, False otherwise (default: None)
            storage_domain_id (str): A unique identifier for the storage domain
            timeout (int): The timeout in seconds for disk status OK (default: 120)

        """
        logger.info(f"Adding disk to {vm.name}")
        disk_size_bytes = int(size) * GB
        storage_domain_id = (
            storage_domain_id or config.ENV_DATA["ovirt_storage_domain_id"]
        )
        disk_attachments_service = self.get_disk_attachments_service(vm.id)
        disk_attachment = disk_attachments_service.add(
            types.DiskAttachment(
                disk=types.Disk(
                    format=getattr(types.DiskFormat, disk_format),
                    provisioned_size=disk_size_bytes,
                    sparse=sparse,
                    storage_domains=[
                        types.StorageDomain(
                            id=storage_domain_id,
                        ),
                    ],
                ),
                interface=getattr(types.DiskInterface, disk_interface),
                bootable=False,
                active=True,
                pass_discard=pass_discard,
            ),
        )

        # Wait for the disk to reach OK:
        disk_service = self.get_disk_service(disk_attachment.disk.id)
        try:
            for sample in TimeoutSampler(timeout, 3, disk_service.get):
                logger.info(
                    f"Waiting for disk status to be OK. "
                    f"Current disk status: {sample.status}"
                )
                if sample.status == types.DiskStatus.OK:
                    logger.info(f"Disk {sample.name} reached OK status")
                    break
        except TimeoutExpiredError:
            logger.error(f"Disk {sample.name} failed to get attached to {vm.name}")
            raise
        logger.info(f"{size}GB disk added successfully to {vm.name}")

    def add_disks(
        self,
        num_disks,
        vm,
        size,
        disk_format=RHV_DISK_FORMAT_RAW,
        disk_interface=RHV_DISK_INTERFACE_VIRTIO_SCSI,
        sparse=None,
        pass_discard=None,
        storage_domain_id=None,
        timeout=120,
    ):
        """
        Adds multiple disks to the VM

        Args:
            num_disks: number of disks to add
            vm (types.Vm): Vm instance
            size (int) : size of disk in GB
            disk_format (str): underlying storage format of disks (default: "RAW")
            disk_interface (str): underlying storage interface of disks
                communication with controller (default: 'VIRTIO_SCSI')
            sparse (bool): disk allocation policy. True for sparse,
                false for preallocated (default: None)
            pass_discard (bool): True if the virtual machine passes discard
                commands to the storage, False otherwise (default: None)
            storage_domain_id (str): A unique identifier for the storage domain
            timeout (int): The timeout in seconds for disk status OK (default: 120)

        """
        for _ in range(int(num_disks)):
            self.add_disk(
                vm,
                size,
                disk_format,
                disk_interface,
                sparse,
                pass_discard,
                storage_domain_id,
                timeout,
            )

    def remove_disk(self, vm, identifier, key="name", detach_only=True):
        """
        Removes the disk attachment. By default, only detach the disk from the
        virtual machine, but won’t remove it from the system, unless the
        detach_only parameter is False

        Args:
            vm (types.Vm): Vm instance
            identifier (str): unique identifier of the disk
                Either disk name or disk id
            key (str): Either 'name' or 'id' (default: name)
            detach_only (bool): True to only detach the disk from the vm but not
                removed from the system, False otherwise (default: True)

        """
        logger.info(f"Removing disk from {vm.name}")
        disk = self.get_disks_by_pattern(pattern=identifier, key=key)[0]
        if disk:
            disk_attachment_service = self.get_disk_attachment_service(vm.id, disk.id)
            disk_attachment_service.update(types.DiskAttachment(active=False))
            for sample in TimeoutSampler(30, 3, disk_attachment_service.get):
                logger.info(
                    f"Waiting for disk attachment to become not active. "
                    f"Current status: Active={sample.active}"
                )
                if not sample.active:
                    logger.info(f"Disk {identifier} marked as Inactive")
                    break
            disk_attachment_service.remove(detach_only)
        else:
            logging.warning(
                f"There's no disk attachment of {disk.name} with ID: {disk.id}"
                f"for {vm.name}"
            )

    def remove_disks(self, vm, detach_only=True):
        """
        Removes all the extra disks for a VM

        Args:
            vm (types.Vm): Vm instance
            detach_only (bool): True to only detach the disk from the vm but not
                removed from the system, False otherwise (default: True)
        """
        all_disks = self.get_disk_attachments(vm.id)
        for disk in all_disks:
            # Ignore bootable devices
            if disk.bootable is not True:
                self.remove_disk(
                    vm=vm, identifier=disk.id, key="id", detach_only=detach_only
                )

    def get_vm_status(self, vm):
        """
        Get the power status of RHV VM

        Args:
           vm (str): RHV VM instance

        Returns :
           str: Power status of RHV VM

        """
        vm_service = self.get_vm_service(vm.id)
        vm_info = vm_service.get()
        return vm_info.status

    def stop_rhv_vms(self, vms, timeout=600, force=False):
        """
        Shutdown the RHV virtual machines

        Args:
            vms (list): list of RHV vm instances
            force (bool): True for non-graceful VM shutdown, False for
                graceful VM shutdown.
            timeout (int): time in seconds to wait for VM to reach 'down' status.

        """
        for vm in vms:
            # Find the virtual machine
            vm_service = self.get_vm_service(vm.id)
            vm_service.stop(force=force)
            # Wait till the virtual machine is down:
            try:
                for status in TimeoutSampler(timeout, 5, self.get_vm_status, vm):
                    logger.info(
                        f"Waiting for RHV Machine {vm.name} to shutdown"
                        f"Current status is : {status}"
                    )
                    if status == types.VmStatus.DOWN:
                        logger.info(f"RHV Machine {vm.name} reached down status")
                        break
            except TimeoutExpiredError:
                logger.error(f"RHV VM {vm.name} is still Running")
                raise

    def start_rhv_vms(self, vms, wait=True, timeout=600):
        """
         Run the RHV virtual machines

        Args:
            vms (list): list of RHV vm instances
            wait (bool): Wait for RHV VMs to start
            timeout (int): time in seconds to wait for VM to reach 'up' status.

        """
        for vm in vms:
            # Find the virtual machine
            vm_service = self.get_vm_service(vm.id)
            vm_service.start()

        if wait:
            # Wait till the virtual machine is UP:
            for vm in vms:
                try:
                    for status in TimeoutSampler(timeout, 5, self.get_vm_status, vm):
                        logger.info(
                            f"Waiting for RHV Machine {vm.name} to Power ON "
                            f"Current status is : {status}"
                        )
                        if status == types.VmStatus.UP:
                            logger.info(f"RHV Machine {vm.name} reached UP status")
                            break
                except TimeoutExpiredError:
                    logger.error(f"RHV VM {vm.name} is not UP")
                    raise

    def reboot_rhv_vms(self, vm_names, timeout=600, wait=True, force=True):
        """
        Reboot the RHV virtual machines

        Args:
            vm_names (list): Names of RHV vms
            timeout (int): time in seconds to wait for VM to reboot
            wait (bool): Wait for RHV VMs to reboot
            force (bool): True to reboot vm forcibly False otherwise

        """
        for vm in vm_names:
            # Find the virtual machine
            vm_service = self.get_vm_service(vm.id)
            vm_service.reboot(force=force)

            if wait:
                # Wait till the virtual machine will start rebooting and then UP
                try:
                    expc_status = types.VmStatus.REBOOT_IN_PROGRESS
                    look_up = False
                    for status in TimeoutSampler(timeout, 5, self.get_vm_status, vm):
                        if not look_up and status != expc_status:
                            logger.info(
                                f"Waiting for RHV Machine {vm.name} to {expc_status}"
                                f" Current status is : {status}"
                            )
                            continue
                        elif not look_up and status == expc_status:
                            expc_status = types.VmStatus.UP
                            look_up = True
                            logger.info(f"RHV Machine {vm} is now {expc_status} status")
                            continue
                        else:
                            logger.info(
                                f"Waiting for RHV Machine {vm.name} to Power ON"
                                f" Current status is : {status}"
                            )
                            if status == types.VmStatus.UP:
                                logger.info(f"RHV Machine {vm.name} reached UP status")
                                break
                except TimeoutExpiredError:
                    logger.error(
                        f"RHV VM {vm.name} is still not {expc_status} after "
                        f"initiating reboot"
                    )
                    raise
