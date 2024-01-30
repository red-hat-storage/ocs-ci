"""
Virtctl class
"""
import json

from ocs_ci.utility.utils import run_cmd


class Virtctl(object):
    """
    Virtctl class for interacting with the virtctl command-line tool.
    It provides various methods for managing KubeVirt VMs.
    """

    def __init__(self, namespace=None):
        """
        Initialize the Virtctl object.

        Args:
            namespace (str): The namespace for KubeVirt VMs.

        """
        self.namespace = namespace
        self.base_command = f"virtctl --namespace {self.namespace}"

    def add_volume(self, vm_name, volume_name, persist=False, serial=None):
        """
        Add a volume to a VM.

        Args:
            vm_name (str): Name of the VM.
            volume_name (str): Name of the volume to add.
            persist (bool): True to persist the volume.
            serial (str): Serial number for the volume.

        Returns:
             str: stdout of command

        """
        base_command = (
            f"{self.base_command} addvolume {vm_name} --volume-name={volume_name}"
        )
        optional_flags = []
        if persist:
            optional_flags.append("--persist")
        if serial:
            optional_flags.append(f"--serial={serial}")

        command = f"{base_command} {' '.join(optional_flags)}"

        return run_cmd(command)

    def remove_volume(self, vm_name, volume_name):
        """
        Remove a volume from a VM.

        Args:
            vm_name (str): Name of the VM.
            volume_name (str): Name of the volume to remove.

        Returns:
             str: stdout of command

        """
        command = (
            f"{self.base_command} removevolume {vm_name} --volume-name={volume_name}"
        )
        return run_cmd(command)

    def guest_os_info(self, vm_name, dict_out=True):
        """
        Get information about the guest OS running on a VM.

        Args:
            vm_name (str): Name of the VM.

        Returns:
            dict: output of the executed command in dict format else an ouput in json format

        """
        command = f"{self.base_command} guestosinfo {vm_name}"
        json_out = run_cmd(command)
        return json.loads(json_out) if dict_out else json_out

    def image_upload(self, dv_name, size, image_path, insecure=False):
        """
        Upload an image to a DataVolume.

        Args:
            dv_name (str): Name of the DataVolume.
            size (int): Size of the image.
            image_path (str): Path to the image file.
            insecure (bool): True to upload the image insecurely.

        Returns:
             str: stdout of command

        """
        base_command = f"{self.base_command} image-upload dv {dv_name} --size={size} --image-path={image_path}"
        optional_params = ["--insecure"] if insecure else []
        command = f"{base_command} {' '.join(optional_params)}"
        return run_cmd(command)

    def _pause(self, entity_type, entity_name):
        """
        Pause a specified entity.

        Args:
            entity_type (str): Type of the entity ('vm' or 'vmi').
            entity_name (str): Name of the entity.

        Returns:
             str: stdout of command

        """
        command = f"{self.base_command} pause {entity_type} {entity_name}"
        return run_cmd(command)

    def pause_vm(self, vm_name):
        """
        Pause a VM.

        Args:
            vm_name (str): Name of the VM.

        Returns:
             str: stdout of command

        """
        return self._pause("vm", vm_name)

    def pause_vmi(self, vm_name):
        """
        Pause a VirtualMachineInstance (VMI).

        Args:
            vm_name (str): Name of the VMI.

        Returns:
             str: stdout of command

        """
        return self._pause("vmi", vm_name)

    def restart_vm(self, vm_name):
        """
        Restart a VM.

        Args:
            vm_name (str): Name of the VM.

        Returns:
             str: stdout of command

        """
        command = f"{self.base_command} restart {vm_name}"
        return run_cmd(command)

    def scp(
        self,
        local_path,
        vm_username,
        vm_name,
        identity_file=None,
        vm_dest_path=None,
        to_vm=True,
        recursive=False,
    ):
        """
        Copy files between local and VM using SCP.

        Args:
            local_path (str): Local path of the file or directory.
            vm_username (str): Username to connect to the VM.
            vm_name (str): Name of the VM.
            identity_file (str): Path to the SSH private key.
            vm_dest_path (str): Destination path on the VM.
            to_vm (bool): True to copy to VM, False to copy from VM.
            recursive (bool): True for recursive copying.

        Returns:
             str: stdout of command

        """
        base_command = f"{self.base_command} scp"

        extra_params = ["--recursive"] if recursive else []
        extra_params.append("--local-ssh-opts='-o StrictHostKeyChecking=no'")

        if identity_file:
            extra_params.append(f" --identity-file={identity_file}")

        vm_dest_path = vm_dest_path if vm_dest_path else "."
        if to_vm:
            mandatory_params = [
                f"{local_path}",
                f"{vm_username}@{vm_name}:{vm_dest_path}",
            ]
        else:
            mandatory_params = [
                f"{vm_username}@{vm_name}:{vm_dest_path} " f"{local_path}",
            ]

        command = f"{base_command} {' '.join(extra_params + mandatory_params)}"

        return run_cmd(command)

    def run_ssh_command(self, vm, username, command, use_sudo=True, identity_file=None):
        """
        SSH into a VM and execute a command

        Args:
            vm (str): Name of the VM.
            username (str): SSH username.
            command (str): Command to run on the VM.
            use_sudo (bool): True to run the command with sudo.
            identity_file (str): Path to the SSH private key.

        Returns:
             str: stdout of command

        """
        base_command = f"{self.base_command} ssh {vm}"

        if use_sudo:
            command = f"sudo {command}"

        mandatory_flags = [
            f"--username={username}",
            "--port=22",  # Default port for VM
            f'-c "{command}"',
        ]

        if identity_file:

            mandatory_flags.insert(1, f"--identity-file={identity_file}")

        full_command = f"{base_command} {' '.join(mandatory_flags)}"
        out = run_cmd(full_command)

        return out

    def start_vm(self, vm_name):
        """
        Start a VM.

        Args:
            vm_name (str): Name of the VM.

        Returns:
             str: stdout of command

        """
        command = f"{self.base_command} start {vm_name}"
        return run_cmd(command)

    def stop_vm(self, vm_name, force=False):
        """
        Stop a VM.

        Args:
            vm_name (str): Name of the VM.
            force (bool): True to forcefully stop the VM.

        Returns:
             str: stdout of command

        """
        force_flag = "--force" if force else ""
        command = f"{self.base_command} stop {vm_name} {force_flag}"
        return run_cmd(command)

    def unpause_vm(self, vm_name):
        """
        Unpause a VM.

        Args:
            vm_name (str): Name of the VM.

        Returns:
             str: stdout of command

        """
        command = f"{self.base_command} unpause vm {vm_name}"
        return run_cmd(command)

    def unpause_vmi(self, vm_name):
        """
        Unpause a VirtualMachineInstance (VMI).

        Args:
            vm_name (str): Name of the VMI.

        Returns:
             str: stdout of command

        """
        command = f"{self.base_command} unpause vmi {vm_name}"
        return run_cmd(command)

    def version(self):
        """
        Get the version information.

        Returns:
             str: stdout of command

        """
        command = f"{self.base_command} version"
        return run_cmd(command)
