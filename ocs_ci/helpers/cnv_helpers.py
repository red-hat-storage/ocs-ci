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
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    create_ocs_object_from_kind_and_name,
)
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def create_vm_using_standalone_pvc(
    namespace=constants.CNV_NAMESPACE,
    vm_name=None,
    pvc_size=None,
    sc_name=None,
    ssh=True,
    running=True,
    wait_for_vm_boot=True,
    vm_dict_path=None,
):
    """
    Create a Virtual Machine (VM) in the specified namespace using a standalone Persistent Volume Claim (PVC)

    Args:
        namespace (str): The namespace in which to create the VM.
        vm_name (str): Name for the VM. If not provided, a unique name will be generated.
        existing_data_volume_name (str): Existing DataVolume name to attach to the VM
        ssh (bool): If set to True, it adds a statically manged public SSH key during the VM creation at the first boot
        wait_for_vm_boot (bool): If True and running is True, wait for the VM to finish booting and
        ensure SSH connectivity
        vm_dict_path (str): Path to the VM YAML file

    Returns:
        vm_obj: The VirtualMachine object

    Raises:
        CommandFailed: If an error occurs during the creation of the VM

    """
    namespace = (
        namespace if namespace else create_unique_resource_name("test-vm", "namespace")
    )
    source_data_obj = create_volume_import_source()
    pvc_data_obj = create_pvc_using_data_source(
        source_name=source_data_obj.name,
        pvc_size=pvc_size,
        sc_name=sc_name,
        namespace=namespace,
    )

    vm_dict_path = (
        vm_dict_path if vm_dict_path else constants.CNV_VM_STANDALONE_PVC_VM_YAML
    )
    vm_data = templating.load_yaml(vm_dict_path)
    vm_name = vm_name if vm_name else create_unique_resource_name("test", "vm")
    vm_data["metadata"]["name"] = vm_name
    vm_data["metadata"]["namespace"] = namespace
    if not running:
        vm_data["spec"]["running"] = False
    vm_data["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
        "claimName"
    ] = pvc_data_obj.name

    if ssh:
        ssh_secret = create_vm_secret(namespace=namespace)
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

    wait_for_resource_state(
        resource=pvc_data_obj, state=constants.STATUS_BOUND, timeout=300
    )
    vm_obj = VirtualMachine(vm_name=vm_ocs_obj.name, namespace=namespace)
    if running:
        vm_obj.wait_for_vm_status(status=constants.VM_RUNNING)
        if wait_for_vm_boot:
            vm_obj.wait_for_ssh_connectivity(timeout=1200)

    return vm_obj


def get_ssh_pub_key_with_filename(path=None):
    """
    Retrieve the content of the SSH public key and its file name

    Args:
        path (str): Path to the SSH public key file - Optional

    Returns:
        tuple: A tuple containing the content of the SSH public key and the file name

    """
    logger.info(
        "Retrieving the content and file name of the SSH public key from the client machine"
    )
    ssh_dir = os.path.expanduser("~/.ssh/")
    if path:
        if os.path.exists(path):
            ssh_key_path = path
            logger.info(f"The provided ssh pub key path:{path} exists")
        else:
            raise FileNotFoundError(
                f"The provided ssh pub key path:{path} does not exist"
            )
    else:
        id_rsa_path = os.path.join(ssh_dir, "id_rsa.pub")
        config_ssh_key = config.DEPLOYMENT.get("ssh_key")
        if os.path.exists(id_rsa_path):
            ssh_key_path = id_rsa_path
            logger.info("Default id_rsa.pub exists")
        elif config_ssh_key and os.path.exists(config_ssh_key):
            ssh_key_path = config_ssh_key
            logger.info(f"Using ssh key from ocs-ci default config: {config_ssh_key}")
        else:
            raise FileNotFoundError(
                "Neither id_rsa.pub nor ssh_key in ocs-ci default config is present"
            )

    with open(ssh_key_path, "r") as ssh_key:
        content = ssh_key.read().strip()
        key_name = os.path.basename(ssh_key_path)

        return content, key_name


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


def create_vm_secret(path=None, namespace=constants.CNV_NAMESPACE):
    """
    Create an SSH secret for the VM

    Args:
        path (str): Path to the SSH public key file - optional

    Returns:
        secret_obj: An OCS instance

    """
    secret_data = templating.load_yaml(constants.CNV_VM_SECRET_YAML)
    secret_data["metadata"]["name"] = create_unique_resource_name("vm-test", "secret")
    secret_data["metadata"]["namespace"] = namespace
    ssh_pub_key, _ = get_ssh_pub_key_with_filename(path=path)
    base64_key = convert_ssh_key_to_base64(ssh_key=ssh_pub_key)
    secret_data["data"]["key"] = base64_key
    secret_obj = create_resource(**secret_data)
    logger.info(f"Successfully created an SSH secret for the VM - {secret_obj.name}")

    return secret_obj


def create_volume_import_source(name=None, url=None):
    """
    Create a VolumeImportSource object

    Args:
        name (str): Name for the VolumeImportSource. If not provided, a unique name will be generated
        url (str): URL for the registry source

    Returns:
        source_data_obj: The created VolumeImportSource object

    """
    source_data = templating.load_yaml(constants.CNV_VM_STANDALONE_PVC_SOURCE_YAML)
    name = name if name else create_unique_resource_name("source", "volumeimportsource")
    source_data["metadata"]["name"] = name
    if url:
        source_data["spec"]["source"]["registry"]["url"]
    source_data_obj = create_resource(**source_data)
    logger.info(f"Successfully created VolumeImportSource - {source_data_obj.name}")

    return source_data_obj


def create_pvc_using_data_source(
    source_name, pvc_size=None, sc_name=None, namespace=constants.CNV_NAMESPACE
):
    """
    Create a PVC using a specified data source

    Args:
        source_name (str): Name of the data source (VolumeImportSource) for the PVC
        pvc_size (str): Size of the PVC
        sc_name (str): StorageClass name for the PVC
        namespace (str): The namespace in which to create the PVC

    Returns:
        pvc_data_obj: PVC object

    """
    pvc_data = templating.load_yaml(constants.CNV_VM_STANDALONE_PVC_PVC_YAML)
    pvc_name = create_unique_resource_name("test", "pvc")
    pvc_data["metadata"]["name"] = pvc_name
    pvc_data["metadata"]["namespace"] = namespace
    pvc_data["spec"]["dataSourceRef"]["name"] = source_name
    if pvc_size:
        pvc_data["spec"]["resource"]["requests"]["storage"] = pvc_size
    if sc_name:
        pvc_data["spec"]["storageClassName"] = sc_name
    pvc_data_obj = create_resource(**pvc_data)
    logger.info(f"Successfully created PVC - {pvc_data_obj.name} using data source")

    return pvc_data_obj


def get_pvc_from_vm(vm_obj):
    """
    Get the PVC name from VM obj

    Returns:
        ocs_ci.ocs.resources.ocs.OCS (obj): PVC in the form of ocs object

    """
    vm_data = vm_obj.get()
    pvc_name = (
        vm_data.get("spec")
        .get("template")
        .get("spec")
        .get("volumes")[0]
        .get("persistentVolumeClaim")
        .get("claimName")
    )
    return create_ocs_object_from_kind_and_name(
        kind=constants.PVC, resource_name=pvc_name, namespace=vm_obj.namespace
    )


def get_secret_from_vm(vm_obj):
    """
    Get the secret name from VM obj

    Returns:
        ocs_ci.ocs.resources.ocs.OCS (obj): Secret in the form of ocs object

    """
    vm_data = vm_obj.get()
    secret_name = (
        vm_data.get("spec")
        .get("template")
        .get("spec")
        .get("accessCredentials")[0]
        .get("sshPublicKey")
        .get("source")
        .get("secret")
        .get("secretName")
    )
    return create_ocs_object_from_kind_and_name(
        kind=constants.SECRET, resource_name=secret_name, namespace=vm_obj.namespace
    )


def get_volumeimportsource(pvc_obj):
    """
    Get the volumeimportsource name from PVC obj

    Returns:
        ocs_ci.ocs.resources.ocs.OCS (obj): volumeimportsource in the form of ocs object

    """
    pvc_data = pvc_obj.get()
    volumeimportsource_name = pvc_data.get("spec").get("dataSource").get("name")
    return create_ocs_object_from_kind_and_name(
        kind=constants.VOLUME_IMPORT_SOURCE,
        resource_name=volumeimportsource_name,
        namespace=pvc_obj.namespace,
    )


def get_ssh_private_key_path():
    """
    Get the full path of the derived private key file from the associated SSH public key file

    Returns:
        str: The full path of the derived private key file

    """
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
