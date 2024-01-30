"""
Helper functions specific for CNV
"""
import logging

from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


def create_vm(
    namespace,
    vm_name=None,
    existing_data_volume=None,
    dv_name=None,
    ssh_secret=None,
    vm_dict_path=None,
):
    """
    Create a Virtual Machine (VM) in the specified namespace.

    Args:
        namespace (str): The namespace in which to create the VM.
        vm_name (str): Name for the VM. If not provided, a unique name will be generated.
        existing_data_volume (DataVolume): Existing DataVolume to attach to the VM.
        dv_name (str): Name for the DataVolume. If not provided, a unique name will be generated.
        ssh_secret (str): Name of the SSH secret to be used for the VM.
        vm_dict_path (str): Path to the VM YAML file

    Returns:
        OCS: The OCS object

    Raises:
        CommandFailed: If an error occurs during the creation of the VM

    """
    vm_dict_path = vm_dict_path if vm_dict_path else constants.CNV_VM_CENTOS_YAML
    vm_data = templating.load_yaml(vm_dict_path)
    if not vm_name:
        vm_name = create_unique_resource_name("test", "vm")
    vm_data["metadata"]["name"] = vm_name
    vm_data["metadata"]["namespace"] = namespace
    if dv_name:
        dv_name = create_unique_resource_name("test", "dv")
        vm_data["spec"]["dataVolumeTemplates"][0]["metadata"]["name"] = dv_name
        vm_data["spec"]["template"]["spec"]["volumes"][0]["dataVolume"][
            "name"
        ] = dv_name
    if existing_data_volume:
        del vm_data["spec"]["dataVolumeTemplates"]
        vm_data["spec"]["template"]["spec"]["volumes"]["dataVolume"][
            "name"
        ] = existing_data_volume.name

    if ssh_secret:
        # Create a statically managed SSH key when creating a VM
        ssh_secret_dict = {
            "accessCredentials": [
                {
                    "sshPublicKey": {
                        "propagationMethod": {"configDrive": {}},
                        "source": {"secret": {"secretName": f"{ssh_secret}"}},
                    }
                }
            ]
        }
        vm_data["spec"]["template"]["spec"]["accessCredentials"] = ssh_secret_dict

    vm_obj = create_resource(**vm_data)
    logger.info(f"Successfully created VM: {vm_obj.name}")

    return vm_obj
