"""
Helper functions specific for CNV
"""

import os
import base64
import logging
import re
import time

from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
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
    Creates a DV using a specified data source

    Args:
        access_mode (str): The access mode for the volume. Default is `constants.ACCESS_MODE_RWX`
        sc_name (str): The name of the storage class to use. Default is `constants.DEFAULT_CNV_CEPH_RBD_SC`.
        pvc_size (str): The size of the PVC. Default is "30Gi".
        source_url (str): The URL of the vm registry image. Default is `constants.CNV_CENTOS_SOURCE`.
        namespace (str, optional): The namespace to create the DV on.

    Returns:
        dv_obj: DV object

    """
    dv_name = create_unique_resource_name("test", "dv")
    dv_data = templating.load_yaml(constants.CNV_VM_DV_YAML)
    dv_data["spec"]["storage"]["accessModes"] = [access_mode]
    dv_data["spec"]["storage"]["resources"]["requests"]["storage"] = pvc_size
    dv_data["spec"]["storage"]["storageClassName"] = sc_name
    dv_data["spec"]["source"]["registry"]["url"] = source_url
    dv_data["metadata"]["name"] = dv_name
    dv_data["metadata"]["namespace"] = namespace
    dv_data_obj = create_resource(**dv_data)
    logger.info(f"Successfully created DV - {dv_data_obj.name}")
    return dv_data_obj


def clone_dv(source_pvc_name, source_pvc_ns, destination_ns):
    """
    Clones a DV using a specified data source

    Args:
        source_pvc_name (str): PVC name of source vm used for cloning.
        source_pvc_ns (str):  PVC namespace of source vm used for cloning.
        destination_ns (str): Namespace of cloned dv to be created on

    Returns:
        dv_obj: Cloned DV object

    """
    dv_name = create_unique_resource_name("clone", "dv")
    dv_data = templating.load_yaml(constants.CNV_VM_DV_CLONE_YAML)
    dv_data["spec"]["source"]["pvc"]["name"] = source_pvc_name
    dv_data["spec"]["source"]["pvc"]["namespace"] = source_pvc_ns
    dv_data["metadata"]["name"] = dv_name
    dv_data["metadata"]["namespace"] = destination_ns
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


def verifyvolume(vm_name, volume_name, namespace):
    """
    Verify a volume in VM.

    Args:
        vm_name (str): Name of the virtual machine
        volume_name (str): Name of the volume (PVC) to verify
        namespace (str): Virtual Machine Namespace

    Returns:
        bool: True if the volume (PVC) is found, False otherwise

    """
    cmd = (
        f"get vm {vm_name} -n {namespace} -o "
        + "jsonpath='{.spec.template.spec.volumes}'"
    )
    try:
        output = OCP().exec_oc_cmd(command=cmd)
        logger.info(f"Output of the command '{cmd}': {output}")
        for volume in output:
            if volume.get("persistentVolumeClaim", {}).get("claimName") == volume_name:
                logger.info(
                    f"Hotpluggable PVC {volume_name} is visible inside the VM {vm_name}"
                )
                return True
        logger.warning(f"PVC {volume_name} not found inside the VM {vm_name}")
        return False
    except Exception as e:
        logger.error(f"Error executing command '{cmd}': {e}")
        return False


def verify_hotplug(vm_obj, disks_before_hotplug):
    """
    Verifies if a disk has been hot-plugged into/removed from a VM.

    Args:
        disks_before_hotplug (str): Set of disk information before hot-plug or add.
        vm_obj (VM object): The virtual machine object to check.

    Returns:
        bool: True if a hot-plugged disk is detected, False otherwise.

    """
    try:
        disks_after_hotplug_raw = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
        disks_after_hotplug = set(
            re.findall(r'NAME="([^"]+)"', disks_after_hotplug_raw)
        )
        disks_before_hotplug = set(re.findall(r'NAME="([^"]+)"', disks_before_hotplug))

        logger.info(f"Disks before hotplug:\n{disks_before_hotplug}")
        logger.info(f"Disks found after hotplug:\n{disks_after_hotplug}")

        added_disks = disks_after_hotplug - disks_before_hotplug
        removed_disks = disks_before_hotplug - disks_after_hotplug

        if added_disks or removed_disks:
            logger.info(
                f"Hotplug difference detected: Added: {added_disks}, "
                f"Removed: {removed_disks}"
            )
            return True
        logger.info(f"No hotplug difference detected in VM {vm_obj.name}")
        return False
    except Exception as error:
        logger.error(
            f"Error occurred while verifying hotplug in VM {vm_obj.name}: {str(error)}"
        )
        return False


def expand_pvc_and_verify(vm_obj, new_size):
    """
    Expands the PVC for a VM and verifies the new size of pvc from inside the VM.

    Args:
        vm_obj: The VM object.
        new_size (int): The new PVC size in GB.

    Returns:
        bool: True if expansion is successful.

    Raises:
        ValueError: If the pvc size is not expanded.

    """

    # Expand PVC
    pvc_obj = vm_obj.get_vm_pvc_obj()
    pvc_obj.resize_pvc(new_size=new_size, verify=True)

    # Refresh PVC object after resizing
    pvc_obj = vm_obj.get_vm_pvc_obj()

    logger.info("Get root disk name")
    disk = vm_obj.vmi_obj.get().get("status").get("volumeStatus")[1]["target"]
    devicename = f"/dev/{disk}"

    # Verify the new size inside the VM
    result = vm_obj.run_ssh_cmd(command=f"lsblk -d -n -o SIZE {devicename}").strip()
    if result != f"{new_size}G":
        raise ValueError(
            "Expanded PVC size is not showing on VM. "
            "Please verify the disk rescan and filesystem resize."
        )
    logger.info(f"PVC expansion successful for VM {vm_obj.name}.")
    return True


def install_fio_on_vm(vm_obj):
    """
    Detects the OS distribution of a virtual machine running in OpenShift Virtualization (CNV)
    and installs the 'fio' package using the appropriate package manager.

    Args:
        vm_obj (str): Name of the virtual machine object.

    Returns:
        str: Output of the installation command.

    """
    PACKAGE_MANAGERS_BY_DISTRO = {
        "fedora": "dnf",
        "Debian": "apt-get",
        "RHEL": "yum",
        "Alpine": "apk",
        "centos": "dnf",
    }

    # Extract OS from labels if available
    os_distro = vm_obj.vmi_obj.get().get("status").get("guestOSInfo").get("id")

    pkg_mgr = PACKAGE_MANAGERS_BY_DISTRO[os_distro]

    if os_distro == "Debian":
        cmd = f"{pkg_mgr} update"
        vm_obj.run_ssh_cmd(cmd)
        logger.info("Sleep 5 seconds after update to make sure the lock is released")
        time.sleep(5)

    cmd = f"{pkg_mgr} -y install fio"
    return vm_obj.run_ssh_cmd(cmd)


def run_fio(
    vm_obj,
    size="1G",
    io_direction="randrw",
    jobs=1,
    runtime=300,
    depth=4,
    rate="1m",
    bs="4K",
    direct=1,
    verify=True,
    verify_method="crc32c",
    filename="/testfile",
    fio_log_path="/tmp/fio_output.log",
    fio_service_name="fio_test",
):
    """
    Execute FIO on a CNV Virtual Machine with data integrity checks.

    Args:
        vm_obj: Name of the virtual machine object
        size (str): Size of the test file (e.g., '1G').
        io_direction (str): Read/write mode ('rw', 'randwrite', 'randread').
        jobs (int): Number of FIO jobs to run.
        runtime (int): Duration of IO test (seconds).
        depth (int): I/O depth.
        rate (str): I/O rate limit.
        bs (str): Block size (default: '4K').
        direct (int): Use direct I/O (1 = Yes, 0 = No).
        verify (bool): Enable data integrity verification.
        verify_method (str): Data integrity check method ('crc32c', 'md5', etc.).
        filename (str): Path of the test file in the VM.
        fio_log_path (str): Path where FIO logs will be stored.
        fio_service_name(str): name of fio service to be create

    """
    install_fio_on_vm(vm_obj)

    # Construct the FIO command
    fio_cmd = (
        f"fio --name=cnv_fio_test "
        f"--rw={io_direction} --bs={bs} --size={size} --numjobs={jobs} "
        f"--iodepth={depth} --rate={rate} --runtime={runtime} --time_based --filename={filename} "
        f"--direct={direct} "
    )

    if verify:
        fio_cmd += f" --verify={verify_method} --verify_fatal=1"

    try:
        logger.info(f" Starting FIO on VM: {vm_obj.name}")
        create_fio_service(vm_obj, fio_cmd, fio_service_name)
        logger.info("FIO execution started successfully!")

    except Exception as e:
        logger.error(f" Error: {e}")


def create_fio_service(vm_obj, fio_cmd, fio_service_name):
    """
    Creates a systemd service on the given VM to run the specified FIO command persistently,
    ensuring it starts automatically after VM reboots.

    Args:
        vm_obj (str): Name or reference to the virtual machine object.
        fio_cmd (str): The FIO command to be executed by the service.
        fio_service_name (str): Name of the systemd service to be created.

    """
    service_content = f"""[Unit]
    Description=Persistent FIO Workload
    After=network.target

    [Service]
    Type=simple
    WorkingDirectory=/tmp
    ExecStart={fio_cmd}
    Restart=always
    RestartSec=5

    [Install]
    WantedBy=multi-user.target
    """

    # Write the systemd service file
    vm_obj.run_ssh_cmd(
        f"echo '{service_content}' | sudo tee /etc/systemd/system/{fio_service_name}.service"
    )

    # Enable and start the service
    vm_obj.run_ssh_cmd("systemctl daemon-reload")
    vm_obj.run_ssh_cmd(f"systemctl enable {fio_service_name}")
    vm_obj.run_ssh_cmd(f"systemctl start {fio_service_name}")

    logger.info("FIO service setup complete.")


def check_fio_status(vm_obj, fio_service_name="fio_test"):
    """
    Check if FIO is running after restart.
    """
    output = vm_obj.run_ssh_cmd(f"systemctl status {fio_service_name}")
    return "running" in output
