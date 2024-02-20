"""
Virtual machine class
"""
import os
import yaml
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.cnv.virtctl import Virtctl
from ocs_ci.ocs.cnv.virtual_machine_instance import VirtualMachineInstance
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import UsernameNotFoundException


logger = logging.getLogger(__name__)


class VirtualMachine(Virtctl):
    """
    Virtual Machine class which provides VM information and handles various VM related operations
    like start / stop / status / restart/ etc
    """

    def __init__(
        self,
        vm_name,
        namespace=None,
    ):
        """
        Initialize the VirtualMachine object.

        Args:
            vm_name (str): Name of the VirtualMachine.
            namespace (str): Namespace for the VirtualMachine.

        """
        super().__init__(namespace=namespace)
        self._vm_name = vm_name
        self.vm_ocp_obj = OCP(
            kind=constants.VIRTUAL_MACHINE,
            namespace=namespace,
        )
        self.vmi_ocp_obj = OCP(
            kind=constants.VIRTUAL_MACHINE_INSTANCE,
            namespace=namespace,
        )
        self.vmi_obj = VirtualMachineInstance(
            vmi_name=self._vm_name, namespace=namespace
        )
        self.ssh_private_key_path = self._get_ssh_private_key_path()

    def _get_ssh_private_key_path(self):
        """
        Get the full path of the derived private key file from the associated SSH public key file

        Returns:
            str: The full path of the derived private key file

        """
        # To handle circular imports
        from ocs_ci.helpers.cnv_helpers import get_ssh_pub_key_with_filename

        ssh_dir = os.path.expanduser("~/.ssh/")
        _, ssh_pub_key_name = get_ssh_pub_key_with_filename()

        # Derive private key path by replacing the extension (if present)
        private_key_name, _ = os.path.splitext(ssh_pub_key_name)
        private_key_path = os.path.join(ssh_dir, private_key_name)

        # Handling both with and without .pem file extension case
        pem_private_key_path = private_key_path + ".pem"
        if os.path.exists(pem_private_key_path):
            private_key_path = pem_private_key_path
        logger.info(
            f"The private key used for authenticating to the server: {private_key_path}"
        )

        return private_key_path

    @property
    def name(self):
        return self._vm_name

    def get(self, out_yaml_format=True):
        """
        Get information about the VirtualMachine.

        Args:
            out_yaml_format (bool): True to get the output in YAML format.

        Returns:
            dict: Information about the VirtualMachine.

        """
        return self.vm_ocp_obj.get(
            resource_name=self._vm_name, out_yaml_format=out_yaml_format
        )

    def get_os_username(self):
        """
        Retrieve the operating system username from the cloud-init data associated with the virtual machine

        Returns:
            str: The operating system username

        Raises:
            UsernameNotFoundException: If the 'user' key is not present in the VM userData

        """
        vm_get_out = self.get()
        volumes = (
            vm_get_out.get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("volumes", [])
        )
        for volume in volumes:
            cloud_init_data = volume.get("cloudInitNoCloud") or volume.get(
                "cloudInitConfigDrive"
            )
            if cloud_init_data:
                user_data = cloud_init_data.get("userData", {})
                user_data_dict = yaml.safe_load(user_data)
                username = user_data_dict.get("user")
                if username is not None:
                    return username
                else:
                    raise UsernameNotFoundException(
                        f"Username not found in the {self.name} user data"
                    )

    def wait_for_vm_status(self, status=constants.VM_RUNNING, timeout=600):
        """
        Wait for the VirtualMachine to reach the specified status.

        Args:
            status (str): The desired status to wait for - Running/Stopped/Paused. default is "Running").
            timeout (int): Timeout value in seconds.

        Raises:
            TimeoutExpiredError: If the timeout is reached.

        """
        self.vm_ocp_obj.wait_for_resource(
            resource_name=self._vm_name, condition=status, timeout=timeout
        )

    def start(self, timeout=600, wait=True):
        """
        Start the VirtualMachine.

        Args:
            timeout (int): Timeout value in seconds.
            wait (bool): True to wait for the VirtualMachine to reach the "Running" status.

        """
        if (
            self.printableStatus() == constants.CNV_VM_STOPPED
            and self.check_if_vmi_does_not_exist()
        ):
            logger.info(
                f"{self._vm_name} is in stopped state and vmi does not exists, starting {self._vm_name}"
            )
        elif not self.check_if_vmi_does_not_exist():
            logger.info(
                f"VMI for this {self._vm_name} is still running, waiting for the vmi to "
                f"delete before starting the {self._vm_name}"
            )
            self.vmi_obj.wait_for_vmi_delete()
        self.start_vm(self._vm_name)
        logger.info(f"Successfully started VM: {self._vm_name}")

        if wait:
            self.wait_for_vm_status(status=constants.VM_RUNNING, timeout=timeout)
            logger.info(f"VM:{self._vm_name} reached Running state")

    def check_if_vmi_does_not_exist(self):
        """
        Check if the VirtualMachineInstance (VMI) does not exist.

        Returns:
            bool: True if the VMI does not exist.

        """
        status_conditions_out = self.get().get("status").get("conditions")[0]
        return status_conditions_out["reason"] == "VMINotExists"

    def wait_for_ssh_connectivity(self, username=None, timeout=600):
        """
        Wait for the SSH connectivity to establish to the virtual machine

        Args:
            vm_obj (vm object): The virtual machine object.
            username (str): The username to use for SSH. If None, it will use the OS username from vm_obj if exists
            timeout (int): The maximum time to wait for SSH connectivity in seconds

        """
        username = username if username else self.get_os_username()
        logger.info(f"Waiting for the SSH connectivity to establish to {self.name} ")
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=30,
            func=self.run_ssh_cmd,
            username=username,
            command="exit",
            use_sudo=False,
        ):
            if sample == "":
                logger.info(f"{self.name} is ready for SSH connection")
                return

    def stop(self, force=False, wait=True):
        """
        Stop the VirtualMachine.

        Args:
            force (bool): True to forcefully stop the VirtualMachine.
            wait (bool): True to wait for the VirtualMachine to reach the "Stopped" status.

        """
        self.stop_vm(self._vm_name, force=force)
        logger.info(f"Successfully stopped VM: {self._vm_name}")
        if wait:
            self.vmi_obj.wait_for_virt_launcher_pod_delete()
            self.vmi_obj.wait_for_vmi_delete()
            self.wait_for_vm_status(status=constants.CNV_VM_STOPPED)
            logger.info(f"VM: {self._vm_name} reached Stopped state")

    def restart(self, wait=True):
        """
        Restart the VirtualMachine.

        Args:
            wait (bool): True to wait for the VirtualMachine to reach the "Running" status.

        """
        self.restart_vm(self._vm_name)
        logger.info(f"Successfully restarted VM: {self._vm_name}")
        if wait:
            self.vmi_obj.wait_for_virt_launcher_pod_delete()
            self.vmi_obj.wait_for_vmi_to_be_running()
            logger.info(
                f"VM: {self._vm_name} reached Running state state after restart operation"
            )

    def addvolme(self, volume_name, persist=True, serial=None):
        """
        Add a volume to a VM

        Args:
            volume_name (str): Name of the volume/PVC to add.
            persist (bool): True to persist the volume.
            serial (str): Serial number for the volume.

        Returns:
             str: stdout of command

        """
        logger.info(f"Adding {volume_name} to {self._vm_name}")
        self.add_volume(
            vm_name=self._vm_name,
            volume_name=volume_name,
            persist=persist,
            serial=serial,
        )
        logger.info(f"Successfully HotPlugged disk {volume_name} to {self._vm_name}")

    def removevolume(self, volume_name):
        """
        Remove a volume from a VM

        Args:
            volume_name (str): Name of the volume to remove.

        Returns:
             str: stdout of command

        """
        logger.info(f"Removing {volume_name} from {self._vm_name}")
        self.remove_volume(vm_name=self._vm_name, volume_name=volume_name)
        logger.info(
            f"Successfully HotUnplugged disk {volume_name} from {self._vm_name}"
        )

    def scp_to_vm(
        self,
        local_path,
        vm_username=None,
        identity_file=None,
        vm_dest_path=None,
        recursive=False,
    ):
        """
        Copy files/directories from the local machine to the VirtualMachine using SCP.

        Args:
            local_path (str): Path to the local file/directory.
            vm_username (str): Username for SSH connection to the VirtualMachine.
            identity_file (str): Path to the SSH private key file.
            vm_dest_path (str): Destination path on the VirtualMachine.
            recursive (bool): True to copy directories recursively.

        Returns:
             str: stdout of command

        """
        vm_username = vm_username if vm_username else self.get_os_username()
        vm_dest_path = vm_dest_path if vm_dest_path else "."
        identity_file = identity_file if identity_file else self.ssh_private_key_path
        logger.info(
            f"Starting scp from local machine path: {local_path} to VM path: {vm_dest_path}"
        )
        return self.scp(
            local_path,
            vm_username,
            self._vm_name,
            identity_file=identity_file,
            vm_dest_path=vm_dest_path,
            to_vm=True,
            recursive=recursive,
        )

    def scp_from_vm(
        self,
        local_path,
        vm_src_path,
        vm_username=None,
        identity_file=None,
        recursive=False,
    ):
        """
        Copy files/directories from the VirtualMachine to the local machine using SCP.

        Args:
            local_path (str): Path to the local destination.
            vm_username (str): Username for SSH connection to the VirtualMachine.
            identity_file (str): Path to the SSH private key file.
            vm_src_path (str): Source path on the VirtualMachine.
            recursive (bool): True to copy directories recursively.

        Returns:
             str: stdout of command

        """
        vm_username = vm_username if vm_username else self.get_os_username()
        identity_file = identity_file if identity_file else self.ssh_private_key_path
        logger.info(
            f"Starting scp from VM path: {vm_src_path} to local machine path: {local_path}"
        )
        return self.scp(
            local_path,
            vm_username,
            self._vm_name,
            identity_file=identity_file,
            vm_dest_path=vm_src_path,
            to_vm=False,
            recursive=recursive,
        )

    def run_ssh_cmd(self, command, username=None, use_sudo=True, identity_file=None):
        """
        Connect to the VirtualMachine using SSH and execute a command.

        Args:
            username (str): SSH username for the VirtualMachine.
            command (str): Command to execute
            identity_file (str): Path to the SSH private key file.
            use_sudo (bool): True to run the command with sudo.

        Returns:
             str: stdout of command

        """
        logger.info(f"Executing {command} command on the {self._vm_name} VM using SSH")
        username = username if username else self.get_os_username()
        identity_file = identity_file if identity_file else self.ssh_private_key_path
        return self.run_ssh_command(
            self._vm_name,
            username,
            command,
            use_sudo=use_sudo,
            identity_file=identity_file,
        )

    def pause(self, wait=True):
        """
        Pause the VirtualMachine.

        Args:
            wait (bool): True to wait for the VirtualMachine to reach the "Paused" status.

        """
        self._pause("vm", self._vm_name)
        logger.info(f"Successfully Paused VM: {self._vm_name}")
        if wait:
            self.wait_for_vm_status(status=constants.VM_PAUSED)
            logger.info(f"VM: {self._vm_name} reached Paused state")

    def unpause(self, wait=True):
        """
        Unpause the VirtualMachine.

        Args:
            wait (bool): True to wait for the VirtualMachine to reach the "Running" status.

        """
        self.unpause_vm(self._vm_name)
        logger.info(f"Successfully UnPaused VM: {self._vm_name}")
        if wait:
            self.wait_for_vm_status(status=constants.VM_RUNNING)
            logger.info(f"VM: {self._vm_name} reached Running state")

    def ready(self):
        """
        Get the readiness status of the VirtualMachine.

        Returns:
            bool: True if the VirtualMachine is ready.

        """
        return self.get().get("status", {}).get("ready")

    def printableStatus(self):
        """
        Get the printable status of the VirtualMachine.

        Returns:
            str: Printable status of the VirtualMachine.

        """
        return self.get().get("status").get("printableStatus")

    def delete(self):
        """
        Delete the VirtualMachine
        """
        self.vm_ocp_obj.delete(resource_name=self._vm_name)
        self.vm_ocp_obj.wait_for_delete(resource_name=self._vm_name, timeout=180)
