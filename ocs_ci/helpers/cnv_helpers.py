"""
Helper functions specific for CNV
"""
import os
import base64
import logging

from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating
from ocs_ci.helpers.helpers import (
    create_ocs_object_from_kind_and_name,
)
from ocs_ci.framework import config
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


def get_ssh_pub_key_with_filename(path=None):
    """
    Retrieve the content of the SSH public key and its file name

    Args:
        path (str): Path to the SSH public key file - Optional

    Returns:
        tuple: A tuple containing the content of the SSH public key and the file name

    Raises:
    FileNotFoundError: If the provided ssh pub key path does not exist

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
        config_ssh_key_path = os.path.expanduser(config_ssh_key)
        if os.path.exists(id_rsa_path):
            ssh_key_path = id_rsa_path
            logger.info("Default id_rsa.pub exists")
        elif config_ssh_key and os.path.exists(config_ssh_key_path):
            ssh_key_path = config_ssh_key_path
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


def create_vm_secret(path=None, secret_name=None, namespace=constants.CNV_NAMESPACE):
    """
    Create an SSH secret for the VM

    Args:
        path (str): Path to the SSH public key file - optional
        secret_name (str, optional): Name of the secret. If not provided, a unique name will be generated.
        namespace (str, optional): Namespace in which the secret will be created. Defaults to constants.CNV_NAMESPACE.

    Returns:
        secret_obj: An OCS instance

    """
    secret_data = templating.load_yaml(constants.CNV_VM_SECRET_YAML)
    secret_name = (
        secret_name if secret_name else create_unique_resource_name("vm-test", "secret")
    )
    secret_data["metadata"]["name"] = secret_name
    secret_data["metadata"]["namespace"] = namespace
    ssh_pub_key, _ = get_ssh_pub_key_with_filename(path=path)
    base64_key = convert_ssh_key_to_base64(ssh_key=ssh_pub_key)
    secret_data["data"]["key"] = base64_key
    secret_obj = create_resource(**secret_data)
    logger.info(f"Successfully created an SSH secret for the VM - {secret_obj.name}")

    return secret_obj


def create_volume_import_source(name=None, url=constants.CNV_CENTOS_SOURCE):
    """
    Create a VolumeImportSource object

    Args:
        name (str): Name for the VolumeImportSource. If not provided, a unique name will be generated
        url (str): URL for the registry source

    Returns:
        source_data_obj: The created VolumeImportSource object

    """
    source_data = templating.load_yaml(constants.CNV_VM_SOURCE_YAML)
    name = name if name else create_unique_resource_name("source", "volumeimportsource")
    source_data["metadata"]["name"] = name
    source_data["spec"]["source"]["registry"]["url"] = url
    source_data_obj = create_resource(**source_data)
    logger.info(f"Successfully created VolumeImportSource - {source_data_obj.name}")

    return source_data_obj


def create_pvc_using_data_source(
    source_name,
    access_mode=constants.ACCESS_MODE_RWX,
    sc_name=constants.DEFAULT_CNV_CEPH_RBD_SC,
    pvc_size="30Gi",
    namespace=constants.CNV_NAMESPACE,
):
    """
    Create a PVC using a specified data source

    Args:
        access_mode (str): The access mode for the volume. Default is `constants.ACCESS_MODE_RWX`
        source_name (str): Name of the data source (VolumeImportSource) for the PVC
        pvc_size (str): Size of the PVC
        sc_name (str): StorageClass name for the PVC
        namespace (str): The namespace in which to create the PVC

    Returns:
        pvc_obj: PVC object

    """
    pvc_data = templating.load_yaml(constants.CNV_VM_PVC_YAML)
    pvc_name = create_unique_resource_name("test", "pvc")
    pvc_data["metadata"]["name"] = pvc_name
    pvc_data["metadata"]["namespace"] = namespace
    pvc_data["spec"]["dataSourceRef"]["name"] = source_name
    pvc_data["spec"]["accessModes"] = [access_mode]
    pvc_data["spec"]["resources"]["requests"]["storage"] = pvc_size
    pvc_data["spec"]["storageClassName"] = sc_name
    pvc_data_obj = create_resource(**pvc_data)
    logger.info(f"Successfully created PVC - {pvc_data_obj.name} using data source")

    return pvc_data_obj


def create_dv(
    access_mode=constants.ACCESS_MODE_RWX,
    sc_name=constants.DEFAULT_CNV_CEPH_RBD_SC,
    pvc_size="30Gi",
    source_url=constants.CNV_CENTOS_SOURCE,
    namespace=constants.CNV_NAMESPACE,
):
    """
    Create a PVC using a specified data source

    Args:
        access_mode (str): The access mode for the volume. Default is `constants.ACCESS_MODE_RWX`
        sc_name (str): The name of the storage class to use. Default is `constants.DEFAULT_CNV_CEPH_RBD_SC`.
        pvc_size (str): The size of the PVC. Default is "30Gi".
        source_url (str): The URL of the vm registry image. Default is `constants.CNV_CENTOS_SOURCE`.
        namespace (str, optional): The namespace to create the vm on.


    Returns:
        dv_obj: DV object

    """
    dv_data = templating.load_yaml(constants.CNV_VM_DV_YAML)
    dv_name = create_unique_resource_name("test", "dv")
    dv_data["metadata"]["name"] = dv_name
    dv_data["metadata"]["namespace"] = namespace
    dv_data["spec"]["storage"]["accessModes"] = [access_mode]
    dv_data["spec"]["storage"]["resources"]["requests"]["storage"] = pvc_size
    dv_data["spec"]["storage"]["storageClassName"] = sc_name
    dv_data["spec"]["source"]["registry"]["url"] = source_url
    dv_data_obj = create_resource(**dv_data)
    logger.info(f"Successfully created DV - {dv_data_obj.name}")

    return dv_data_obj


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


@retry(CommandFailed, tries=10, delay=5, backoff=1)
def cal_md5sum_vm(vm_obj, file_path, username=None):
    """
    Calculate the MD5 checksum of a file via SSH on a virtual machine.

    Args:
        vm_obj (obj): The virtual machine object.
        file_path (str): Full path to the file to calculate the MD5 checksum for.
        username (str, optional): The username to use for SSH authentication. Defaults to None.

    Returns:
        str: The MD5 checksum of the specified file.

    """
    md5sum_out = vm_obj.run_ssh_cmd(
        command=f"md5sum {file_path}",
        username=username,
    )
    return md5sum_out.split()[0]


@retry(CommandFailed, tries=10, delay=5, backoff=1)
def run_dd_io(vm_obj, file_path, size="10240", username=None, verify=False):
    """
    Perform input/output (I/O) operation using dd command via SSH on a virtual machine.

    Args:
        vm_obj (obj): The virtual machine object.
        file_path (str): The full path of the file to write on
        size (str, optional): Size in MB. Defaults to "102400" which is 10GB.
        username (str, optional): The username to use for SSH authentication. Defaults to None.
        verify (bool, optional): Whether to verify the I/O operation by calculating MD5 checksum.
            Defaults to False.

    Returns:
        str or None: If verify is True, returns the MD5 checksum of the written file. Otherwise, None.

    """
    # Block size defaults to 1MB
    bs = 1024
    vm_obj.run_ssh_cmd(
        command=f"dd if=/dev/urandom of={file_path} bs={bs} count={size}",
        username=username,
    )
    if verify:
        return cal_md5sum_vm(
            vm_obj=vm_obj,
            file_path=file_path,
            username=username,
        )
