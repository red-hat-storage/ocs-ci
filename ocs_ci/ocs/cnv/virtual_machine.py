"""
Virtual machine class
"""
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.cnv.virtctl import Virtctl
from ocs_ci.ocs.cnv.virtual_machine_instance import VirtualMachineInstance
from ocs_ci.ocs import constants


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
            self.printableStatus() == constants.VM_STOPPED
            and self.check_if_vmi_does_not_exist()
        ):
            logger.info(
                f"{self._vm_name} is in stopped state and vmi does not exists, starting {self._vm_name}"
            )
            self.start_vm(self._vm_name)
            logger.info(f"Successfully started VM: {self._vm_name}")
        else:
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
            self.wait_for_vm_status(status=constants.VM_STOPPED)
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

    def scp_to_vm(
        self,
        local_path,
        vm_username,
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
        vm_dest_path = vm_dest_path if vm_dest_path else "."
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
        self, local_path, vm_username, vm_src_path, identity_file=None, recursive=False
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

    def run_ssh_cmd(self, username, command, use_sudo=True, identity_file=None):
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
        logger.info(f"Executing {command} on the {self._vm_name} VM using SSH")
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
