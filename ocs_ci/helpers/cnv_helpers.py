"""
Helper functions specific for CNV
"""
import os
import base64
import logging

from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.ocs.cnv.virtual_machine import VirtualMachine
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def create_vm(
    namespace,
    vm_name=None,
    existing_data_volume_name=None,
    ssh=True,
    vm_dict_path=None,
):
    """
    Create a Virtual Machine (VM) in the specified namespace.

    Args:
        namespace (str): The namespace in which to create the VM.
        vm_name (str): Name for the VM. If not provided, a unique name will be generated.
        existing_data_volume_name (str): Existing DataVolume name to attach to the VM
        ssh (bool): If set to True, it adds a statically manged public SSH key during the VM creation at the first boot
        vm_dict_path (str): Path to the VM YAML file

    Returns:
        vm_obj: The VirtualMachine object

    Raises:
        CommandFailed: If an error occurs during the creation of the VM

    """
    vm_dict_path = vm_dict_path if vm_dict_path else constants.CNV_VM_CENTOS_YAML
    vm_data = templating.load_yaml(vm_dict_path)
    vm_name = vm_name if vm_name else create_unique_resource_name("test", "vm")
    # data volume is created with the same vm name
    vm_data["metadata"]["name"] = vm_name
    vm_data["metadata"]["namespace"] = namespace
    vm_data["spec"]["dataVolumeTemplates"][0]["metadata"]["name"] = vm_name
    vm_data["spec"]["template"]["spec"]["volumes"][0]["dataVolume"]["name"] = vm_name
    if existing_data_volume_name:
        del vm_data["spec"]["dataVolumeTemplates"]
        vm_data["spec"]["template"]["spec"]["volumes"]["dataVolume"][
            "name"
        ] = existing_data_volume_name.name

    if ssh:
        ssh_secret = create_vm_secret()
        ssh_secret_dict = [
            {
                "sshPublicKey": {
                    "propagationMethod": {"noCloud": {}},
                    "source": {"secret": {"secretName": f"{ssh_secret.name}"}},
                }
            }
        ]
        vm_data["spec"]["template"]["spec"]["accessCredentials"] = ssh_secret_dict

    vm_ocs_obj = create_resource(**vm_data)
    logger.info(f"Successfully created VM: {vm_ocs_obj.name}")

    vm_obj = VirtualMachine(vm_name=vm_ocs_obj.name, namespace=namespace)
    vm_obj.wait_for_vm_status(status=constants.VM_RUNNING)

    return vm_obj


def get_ssh_pub_key(path=None):
    """
    Retrieve the content of the SSH public key.

    Args:
        path (str): Path to the SSH public key file - Optional

    Returns:
        str: The content of the SSH public key.

    """
    logger.info("Retrieving the content of the SSH public key from the client machine")
    username = os.getlogin()
    ssh_dir = os.path.expanduser("~/.ssh/")
    if path and os.path.exists(path):
        ssh_key_path = path
        logger.info(f"The provided ssh pub key path:{path} exists")
    else:
        id_rsa_path = os.path.join(ssh_dir, "id_rsa.pub")
        if os.path.exists(id_rsa_path):
            ssh_key_path = id_rsa_path
            logger.info("id_rsa.pub exists")
        else:
            logger.info(
                "id_rsa.pub does not exist, filtering the pub key based on username"
            )
            ssh_key_path_list = [
                file
                for file in os.listdir(ssh_dir)
                if file.endswith(".pub") and username in file
            ]
            ssh_key_path = os.path.join(ssh_dir, ssh_key_path_list[0])

    with open(ssh_key_path, "r") as ssh_key:
        content = ssh_key.read().strip()
        return content


def convert_ssh_key_to_base64(ssh_key):
    """
    Convert SSH key to base64 encoding

    Args:
        ssh_key (str): SSH key

    Returns:
        str: Base64 encoded SSH key

    """
    logger.info("Converting SSH key to base64")
    base64_key = base64.b64encode(ssh_key.encode()).decode()
    return base64_key


def create_vm_secret(path=None):
    """
    Create an SSH secret for the VM

    Args:
        path (str): Path to the SSH public key file - optional

    Returns:
        secret_obj: An OCS instance

    """
    secret_data = templating.load_yaml(constants.CNV_VM_SECRET_YAML)
    secret_data["metadata"]["name"] = create_unique_resource_name("vm-test", "secret")
    ssh_pub_key = get_ssh_pub_key(path=path)
    base64_key = convert_ssh_key_to_base64(ssh_key=ssh_pub_key)
    secret_data["data"]["key"] = base64_key
    secret_obj = create_resource(**secret_data)
    logger.info(f"Successfully created an SSH secret for the VM - {secret_obj.name}")

    return secret_obj


def wait_for_ssh_connectivity(vm_obj, username=None, timeout=600):
    """
    Wait for the SSH connectivity to establish to the virtual machine

    Args:
        vm_obj (vm object): The virtual machine object.
        username (str): The username to use for SSH. If None, it will use the OS username from vm_obj if exists
        timeout (int): The maximum time to wait for SSH connectivity in seconds

    """
    username = username if username else vm_obj.get_os_username()
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=30,
        func=vm_obj.run_ssh_cmd,
        username=username,
        command="exit",
        use_sudo=False,
    ):
        if sample == "":
            logger.info(f"{vm_obj.name} is ready for SSH connection")
            return
