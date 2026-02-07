"""
This module is used for configuring iscsi.
"""

import logging
import re

from ocs_ci.ocs import node, ocp, constants
from ocs_ci.framework import config
from ocs_ci.utility.connection import Connection

log = logging.getLogger(__name__)


def setup_target_environment(target_node_ssh):
    """
    Set up target environment on remote target VM via SSH.

    Parameters:
        target_node_ssh (Connection): An established SSH connection to the target VM.
    """
    log.info("Setting up target environment")
    commands = [
        # Create target directory
        "mkdir -p /etc/target",
        # Start and enable target service
        "systemctl start target",
        "systemctl enable target",
        # Verify setup
        "systemctl status target --no-pager",
    ]

    for cmd in commands:
        log.info(f"Executing on target VM: {cmd}")

        retcode, stdout, stderr = target_node_ssh.exec_cmd(cmd)

        if not retcode and "already" not in stderr.lower():
            log.warning(f"Command warning: {stderr}")
        if stdout:
            log.debug(f"Output: {stdout.strip()}")


def get_worker_node_names():
    """
    Get worker node names using ocs-ci node utilities.
    This function works on pure OCP clusters without OCS installed.

    Returns:
        list: A list of worker node names (strings).
    """

    try:
        worker_node_names = node.get_worker_nodes()
        return worker_node_names
    except Exception as e:
        log.error(f"Error getting worker node names: {e}")
        return []


# --------------------------------------------------------
# STEP 1: Collect worker node initiator IQNs
# --------------------------------------------------------
def get_worker_iqns(worker_node_names):
    """
    Collects iSCSI initiator IQNs from worker nodes.
    This function ensures the iSCSI service is started before reading the IQN.

    Parameters:
        worker_node_names (list): List of worker node names (strings).

    Returns:
        list: A list of IQNs of worker nodes or an empty list on failure.
    """
    iqns = []
    log.info("=== Collecting Worker IQNs ===")

    ocp_obj = ocp.OCP()

    start_service_cmd = (
        "systemctl start iscsid 2>/dev/null || "
        "systemctl enable --now iscsid 2>/dev/null || "
        "true"
    )
    cmd = "grep InitiatorName /etc/iscsi/initiatorname.iscsi | cut -d= -f2"
    for node_name in worker_node_names:
        log.info(f"Getting IQN from worker node {node_name}...")

        try:
            # First, ensure iSCSI service is started
            ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[start_service_cmd], use_root=True, timeout=120
            )
            log.debug(f"Started iSCSI service on {node_name}")
            # Now read the IQN from the initiatorname.iscsi file
            stdout = ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[cmd], use_root=True, timeout=120
            )
            iqn = stdout.strip()
            if iqn:
                iqns.append(iqn)
                log.info(f"Worker {node_name} IQN: {iqn}")
            else:
                log.warning(f"No IQN found for worker node {node_name}")
        except Exception as e:
            log.error(f"Failed to get IQN from {node_name}: {e}")
            # Fallback to IP-based connection if oc debug fails
            try:
                # Get node IP from OCP API
                ocp_node_obj = ocp.OCP(kind=constants.NODE)
                node_data = ocp_node_obj.get(resource_name=node_name)
                node_ip = (
                    node_data.get("status", {}).get("addresses", [{}])[0].get("address")
                )

                if node_ip:
                    node_ssh = Connection(
                        host=node_ip,
                        user="core",
                        private_key=config.DEPLOYMENT["ssh_key_private"],
                    )
                    # Start iSCSI service via SSH
                    start_cmd = (
                        "sudo systemctl start iscsid 2>/dev/null || "
                        "sudo systemctl enable --now iscsid 2>/dev/null || "
                        "true"
                    )
                    node_ssh.exec_cmd(start_cmd)

                    retcode, stdout, stderr = node_ssh.exec_cmd(
                        cmd="grep InitiatorName /etc/iscsi/initiatorname.iscsi | cut -d= -f2"
                    )
                    if retcode == 0 and stdout.strip():
                        iqns.append(stdout.strip())
                        log.info(
                            f"Worker {node_ip} IQN: {stdout.strip()} (via SSH fallback)"
                        )
            except Exception as fallback_error:
                log.error(f"SSH fallback also failed for {node_name}: {fallback_error}")

    return iqns


# --------------------------------------------------------
# STEP 2: Configure Target VM
# --------------------------------------------------------
def configure_target(target_node_ssh, target_iqn, worker_iqns):
    """
    Configures the iSCSI target with given IQNs and IP.

    Parameters:
        target_node_ssh (object): An established SSH connection to the target VM.
        target_iqn (str): Target iSCSI IQN.
        worker_iqns (list): List of IQNs of worker nodes.

    """

    # Setup environment first
    # setup_target_environment(target_node_ssh)

    # Check if target exists
    check_cmd = f"targetcli /iscsi/{target_iqn} ls 2>/dev/null"
    retcode, stdout, stderr = target_node_ssh.exec_cmd(check_cmd)
    if retcode != 0:
        create_target_cmd = f"targetcli /iscsi create {target_iqn}"
        retcode, stdout, stderr = target_node_ssh.exec_cmd(create_target_cmd)
        if retcode != 0 and "already exists" not in (stdout + stderr).lower():
            log.error(f"Failed to create iSCSI target {target_iqn}: {stderr or stdout}")
            return
    else:
        log.info(f"iSCSI target {target_iqn} already exists. Adding workers...")

    # Add LUNs and ACLs for each worker IQN
    for worker_iqn in worker_iqns:
        # Add ACL
        acl_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/acls create {worker_iqn}"
        retcode, stdout, stderr = target_node_ssh.exec_cmd(acl_cmd)
        if retcode != 0:
            # targetcli returns non-zero if ACL already exists (idempotency case)
            msg = (stdout + stderr).strip()
            if "already exists" in msg.lower():
                log.info(f"ACL already exists for worker IQN: {worker_iqn}")
            else:
                log.error(f"Failed to add ACL for {worker_iqn}: {msg}")
                continue
        else:
            log.info(f"Added ACL for worker IQN: {worker_iqn}")

        # Get list of LUNs to map
        lun_list_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/luns ls"
        retcode, stdout, stderr = target_node_ssh.exec_cmd(lun_list_cmd)
        if retcode != 0:
            log.error(f"Failed to get list of LUNs: {stderr}")
            continue

        lun_numbers = re.findall(r"lun(\d+)", stdout)
        if not lun_numbers:
            log.warning(
                f"No LUNs found under /iscsi/{target_iqn}/tpg1/luns. "
                f"Create backstores/LUNs before attempting ACL mapping."
            )
            continue

        for lun_num in lun_numbers:
            log.info(f"Mapping LUN{lun_num} to {worker_iqn}...")

            # Map LUN to ACL
            map_cmd = (
                f"targetcli /iscsi/{target_iqn}/tpg1/acls/{worker_iqn} "
                f"create {lun_num} {lun_num} 2>&1 || "
                f"echo 'LUN mapping may already exist'"
            )
            retcode, map_stdout, map_stderr = target_node_ssh.exec_cmd(map_cmd)
            log.info(
                f"LUN{lun_num} mapping result (rc={retcode}): "
                f"{(map_stdout + map_stderr).strip()}"
            )

    # Save configuration
    log.info("Saving target configuration...")
    save_cmd = "targetcli saveconfig"
    retcode, stdout, stderr = target_node_ssh.exec_cmd(save_cmd)

    if retcode == 0:
        log.info("Configuration saved successfully")
    else:
        log.warning(f"Save warning: {stderr or stdout}")


# --------------------------------------------------------
# STEP 3: Configure iSCSI Initiator on each Worker
# --------------------------------------------------------
def configure_initiators(worker_node_names):
    """
    Configures a worker node as an iSCSI initiator.
    This function installs necessary packages, discovers targets, logs in to the target,
    and enables the iSCSI service.

    Parameters:
        worker_node_names (list): List of worker node names (strings).

    Returns:
        None

    Raises:
        Exception: If there is an error while configuring the initiator.
        Exception: If there is an error while SSH fallback fails.
        Exception: If there is an error while getting the node IP.
        Exception: If there is an error while deleting the StorageClasses or LocalDisks.

    """

    log.info("\n=== Configuring Worker Nodes as Initiators ===")

    ocp_obj = ocp.OCP()
    target_ip = config.ENV_DATA["iscsi_target_ip"]
    target_iqn = config.ENV_DATA["iscsi_target_iqn"]

    cmds = [
        f"iscsiadm -m discovery -t sendtargets -p {target_ip}:3260",
        f"iscsiadm -m node -T {target_iqn} -p {target_ip}:3260 --login",
        "systemctl enable --now iscsid || systemctl enable --now open-iscsi",
    ]

    for node_name in worker_node_names:
        log.info(f"Configuring initiator on worker node {node_name}...")

        try:
            for cmd in cmds:
                stdout = ocp_obj.exec_oc_debug_cmd(
                    node=node_name, cmd_list=[cmd], use_root=True, timeout=120
                )
                log.debug(f"Command output on {node_name}: {stdout}")
        except Exception as e:
            log.error(f"Failed to configure initiator on {node_name}: {e}")
            # Fallback to SSH if oc debug fails
            try:
                # Get node IP from OCP API
                ocp_node_obj = ocp.OCP(kind=constants.NODE)
                node_data = ocp_node_obj.get(resource_name=node_name)
                node_ip = (
                    node_data.get("status", {}).get("addresses", [{}])[0].get("address")
                )

                if node_ip:
                    worker_node_ssh = Connection(
                        host=node_ip,
                        user="core",
                        private_key=config.DEPLOYMENT["ssh_key_private"],
                    )
                    log.info(f"Using SSH fallback for {node_ip}...")
                    for cmd in cmds:
                        retcode, stdout, stderr = worker_node_ssh.exec_cmd(cmd)
                        if retcode != 0:
                            log.warning(f"Command failed on {node_ip}: {stderr}")
                        else:
                            log.debug(f"Command output: {stdout}")
                else:
                    log.error(f"Node IP is none or empty for {node_name}")
                    raise Exception(f"Node IP is none or empty for {node_name}")
            except Exception as fallback_error:
                log.error(f"SSH fallback also failed for {node_name}: {fallback_error}")


def remove_acls_from_target(target_node_ssh, target_iqn, worker_iqns, username):
    """
    Remove ACLs and LUN mappings from target using targetcli.

    Parameters:
        target_node_ssh (object): An established SSH connection to the target VM.
        target_iqn (str): Target iSCSI IQN.
        worker_iqns (list): List of IQNs of worker nodes.
        username (str): Username for the target VM.

    """

    log.info("\n" + "=" * 70)
    log.info("STEP 3: Removing ACLs from target")
    log.info("=" * 70)

    for worker_iqn in worker_iqns:
        log.info(f"\nRemoving ACL: {worker_iqn}")

        # Delete ACL (this also removes all LUN mappings)
        delete_acl_cmd = (
            f"targetcli /iscsi/{target_iqn}/tpg1/acls delete {worker_iqn} 2>&1"
        )
        retcode, stdout, stderr = target_node_ssh.exec_cmd(delete_acl_cmd)

        if retcode == 0 or "does not exist" in stdout.lower():
            log.info(" ACL is deleted")
        else:
            log.warning(f"Warning: {stdout}")

    # Save configuration
    save_cmd = "targetcli saveconfig 2>&1"
    target_node_ssh.exec_cmd(save_cmd)
    log.info("\n Configuration saved")


def wipe_luns_on_target(target_node_ssh, target_iqn, username):
    """
    Wipe LUN data on target VM.
    Also deletes IBM Spectrum Scale related resources.
    Only wipes devices that are referenced in LocalDisks resources.

    Parameters:
        target_node_ssh (object): An established SSH connection to the target VM.
        target_iqn (str): Target iSCSI IQN.
        username (str): Username for the target VM.

    Returns:
        None

    Raises:
        Exception: If there is an error while deleting StorageClasses or LocalDisks.
        Exception: If there is an error while wiping the devices.
        Exception: If there is an error while removing signatures from the devices.
        Exception: If there is an error while verifying the cleanup.
        Exception: If there is an error while deleting the StorageClasses or LocalDisks.

    """

    log.info("STEP 4: Wiping LUN data on target")

    # Collect device paths from LocalDisks before deletion
    devices_to_wipe = set()

    # Delete IBM Spectrum Scale StorageClasses
    try:
        log.info(
            "Deleting StorageClasses with provisioner spectrumscale.csi.ibm.com..."
        )
        sc_obj = ocp.OCP(kind=constants.STORAGECLASS)
        storageclasses = sc_obj.get(all_namespaces=True)

        for sc in storageclasses.get("items", []):
            provisioner = sc.get("provisioner", "")
            if provisioner == "spectrumscale.csi.ibm.com":
                sc_name = sc.get("metadata", {}).get("name")
                if sc_name:
                    try:
                        sc_obj.delete(resource_name=sc_name, wait=False)
                        log.info(f"Deleted StorageClass: {sc_name}")
                    except Exception as e:
                        log.warning(f"Failed to delete StorageClass {sc_name}: {e}")
    except Exception as e:
        log.warning(f"Error while deleting StorageClasses: {e}")

    # Delete LocalDisks custom resource from ibm-spectrum-scale namespace
    try:
        log.info("Deleting LocalDisks resource from ibm-spectrum-scale namespace...")
        localdisk_obj = ocp.OCP(
            kind="localdisks.scale.spectrum.ibm.com", namespace="ibm-spectrum-scale"
        )
        try:
            localdisks = localdisk_obj.get(all_namespaces=False)
            for ld in localdisks.get("items", []):
                device_path = ld.get("spec", {}).get("device")
                if device_path:
                    devices_to_wipe.add(device_path)
                    log.info(
                        f"Found device in LocalDisk {ld.get('metadata', {}).get('name')}: {device_path}"
                    )
                ld_name = ld.get("metadata", {}).get("name")
                if ld_name:
                    try:
                        localdisk_obj.delete(resource_name=ld_name, wait=False)
                        log.info(f"Deleted LocalDisk: {ld_name}")
                    except Exception as e:
                        log.warning(f"Failed to delete LocalDisk {ld_name}: {e}")
        except Exception as e:
            # Resource might not exist, which is fine
            log.debug(f"LocalDisks resource not found or already deleted: {e}")
    except Exception as e:
        log.warning(f"Error while deleting LocalDisks: {e}")

    log.info(
        f"Will wipe {len(devices_to_wipe)} device(s) from LocalDisks: {devices_to_wipe}"
    )
    # Wipe only the devices collected from LocalDisks
    for device in devices_to_wipe:
        log.info(f"\n  Device: {device}")

        # Wipe device
        wipe_cmd = f"dd if=/dev/zero of={device} bs=1M count=100 2>&1"
        retcode, stdout, stderr = target_node_ssh.exec_cmd(wipe_cmd)
        if retcode == 0:
            log.info(f" Wiped {device}")
        else:
            log.warning(f" Failed to wipe {device}: {stderr}")

        # Remove signatures
        wipefs_cmd = f"wipefs -a -f {device} 2>&1"
        retcode, stdout, stderr = target_node_ssh.exec_cmd(wipefs_cmd)
        if retcode == 0:
            log.info(f" Removed signatures from {device}")
        else:
            log.warning(f" Failed to remove signatures from {device}: {stderr}")


def verify_cleanup(target_node_ssh, target_iqn, username_target, worker_iqns):
    """
    Verify cleanup was successful.

    Parameters:
        target_node_ssh (object): An established SSH connection to the target VM.
        target_iqn (str): Target iSCSI IQN.
        username_target (str): Username for the target VM.
        worker_iqns (list): List of IQNs of worker nodes.

    Raises:
        Exception: If there is an error while verifying the cleanup.
    """
    log.info("STEP 5: Verifying cleanup")

    # Check ACLs on target
    log.info("\nChecking target ACLs...")
    for iqn in worker_iqns:
        check_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/acls/{iqn} ls 2>&1"
        success, stdout, stderr = target_node_ssh.exec_cmd(check_cmd)

        if "No such path" in stdout:
            log.info(f" ACL for {iqn} successfully removed")
        else:
            raise Exception(f"ACL for {iqn} still exists:\n{stdout}")
    log.info(" All ACLs verified as removed.")


def cleanup_iscsi_target(
    target_node_ssh,
    target_iqn,
    worker_iqns,
    wipe_data,
    username_target,
):
    """
    Complete cleanup of iSCSI target.

    Parameters:
        target_node_ssh (object): An established SSH connection to the target VM.
        target_iqn (str): Target IQN
        worker_iqns (list): List of worker IQNs to remove
        wipe_data (bool): Whether to wipe data (default: True)
        username_target (str): SSH user for target VM

    """

    log.info("iSCSI TARGET CLEANUP - START")
    log.info(f"Target IQN: {target_iqn}")
    log.info(f"Wipe data: {wipe_data}")

    try:

        # Step 1: Remove ACLs
        remove_acls_from_target(
            target_node_ssh, target_iqn, worker_iqns, username_target
        )

        # Step 2: Wipe target LUNs
        if wipe_data:
            wipe_luns_on_target(target_node_ssh, target_iqn, username_target)

        # Step 3: Verify cleanup
        verify_cleanup(target_node_ssh, target_iqn, username_target, worker_iqns)

        log.info("\n" + "=" * 70)
        log.info("CLEANUP COMPLETED SUCCESSFULLY")
        log.info("=" * 70)

    except Exception as e:
        log.error(f"\n Cleanup failed: {e}")
        import traceback

        traceback.print_exc()
        raise e


def iscsi_setup():
    """
    Setup iSCSI target and initiators.
    """
    log.info("Setting up iSCSI configuration...")

    # Get worker node names (works on pure OCP clusters without OCS)
    worker_node_names = get_worker_node_names()
    if not worker_node_names:
        log.error("No worker nodes found!")
        raise ValueError("No worker nodes found!")

    log.info(f"Current available worker nodes: {worker_node_names}")
    worker_iqns = get_worker_iqns(worker_node_names)
    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        raise ValueError("No IQNs found!")

    target_node_ssh = Connection(
        host=config.ENV_DATA.get("iscsi_target_ip"),
        user=config.ENV_DATA.get("iscsi_target_username"),
        password=config.ENV_DATA.get("iscsi_target_password"),
        stdout=True,
    )

    configure_target(
        target_node_ssh, config.ENV_DATA.get("iscsi_target_iqn"), worker_iqns
    )
    configure_initiators(worker_node_names)

    # Run validations to confirm setup succeeded
    run_iscsi_setup_validations()


def iscsi_teardown():
    """
    Tear down iSCSI target.
    """
    log.info("Tearing down iSCSI target...")
    target_iqn = config.ENV_DATA.get("iscsi_target_iqn")
    target_ip = config.ENV_DATA.get("iscsi_target_ip")
    # Get worker nodes
    worker_node_names = get_worker_node_names()
    if not worker_node_names:
        log.error("No worker nodes found!")
        raise ValueError("No worker nodes found!")

    log.info(f"Current available worker nodes: {worker_node_names}")
    worker_iqns = get_worker_iqns(worker_node_names)
    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        raise ValueError("No IQNs found!")

    target_node_ssh = Connection(
        host=target_ip,
        user=config.ENV_DATA.get("iscsi_target_username"),
        password=config.ENV_DATA.get("iscsi_target_password"),
        stdout=True,
    )

    cleanup_iscsi_target(
        target_node_ssh,
        target_iqn,
        worker_iqns,
        username_target=config.ENV_DATA.get("iscsi_target_username"),
        wipe_data=True,
    )


def verify_iscsi_sessions(worker_node_names, target_iqn):
    """
    Verify iSCSI sessions are established on worker nodes.

    Parameters:
        worker_node_names (list): List of worker node names (strings).
        target_iqn (str): Target iSCSI IQN to verify.

    Returns:
        dict: Dictionary with node name as key and verification result as value.
    """
    results = {}
    log.info("\n=== Verifying iSCSI Sessions ===")

    ocp_obj = ocp.OCP()

    for node_name in worker_node_names:
        log.info(f"Checking iSCSI session on worker node {node_name}...")

        try:
            # Check if session exists
            session_cmd = f"iscsiadm -m session -P 3 | grep -i {target_iqn}"
            stdout = ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[session_cmd], use_root=True, timeout=120
            )

            if target_iqn.lower() in stdout.lower():
                log.info(f"✓ iSCSI session found on {node_name}")
                results[node_name] = {"session": True, "details": stdout}
            else:
                log.warning(f"✗ No iSCSI session found on {node_name}")
                results[node_name] = {"session": False, "details": stdout}
        except Exception as e:
            log.error(f"Failed to check session on {node_name}: {e}")
            results[node_name] = {"session": False, "details": str(e)}

    return results


def verify_iscsi_devices(worker_node_names, target_iqn):
    """
    Verify iSCSI devices are visible on worker nodes.

    This function detects iSCSI block devices by:
    1. Using lsblk to find devices with transport type 'iscsi'

    Parameters:
        worker_node_names (list): List of worker node names (strings).
        target_iqn (str): Target iSCSI IQN to verify.

    Returns:
        dict: Dictionary with node name as key and device information as value.
    """
    results = {}
    log.info("\n=== Verifying iSCSI Devices ===")

    ocp_obj = ocp.OCP()

    for node_name in worker_node_names:
        log.info(f"Checking iSCSI devices on worker node {node_name}...")

        devices_found = []
        block_devices = []

        try:
            lsblk_cmd = "lsblk -o NAME,TYPE,TRAN,SIZE,MODEL -n | grep -i iscsi"
            stdout = ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[lsblk_cmd], use_root=True, timeout=120
            )
            if stdout:
                lsblk_devices = [
                    line.strip() for line in stdout.split("\n") if line.strip()
                ]
                devices_found.extend(lsblk_devices)
                # Extract device names (first column)
                for line in lsblk_devices:
                    parts = line.split()
                    if parts:
                        block_devices.append(parts[0])

            # Count unique devices
            unique_devices = list(set(block_devices))
            device_count = len(unique_devices)

            if device_count > 0:
                log.info(f"✓ Found {device_count} iSCSI device(s) on {node_name}")
                log.debug(f"  Block devices: {unique_devices}")
                results[node_name] = {
                    "devices": unique_devices,
                    "lsblk_output": devices_found,
                    "count": device_count,
                }
            else:
                log.warning(f"✗ No iSCSI devices found on {node_name}")
                results[node_name] = {
                    "devices": [],
                    "lsblk_output": [],
                    "count": 0,
                }
        except Exception as e:
            log.error(f"Failed to check devices on {node_name}: {e}")
            results[node_name] = {
                "devices": [],
                "lsblk_output": [],
                "count": 0,
                "error": str(e),
            }

    return results


def verify_iscsi_target_connectivity(worker_node_names, target_ip, target_port=3260):
    """
    Verify network connectivity to iSCSI target from worker nodes.

    Parameters:
        worker_node_names (list): List of worker node names (strings).
        target_ip (str): iSCSI target IP address.
        target_port (int): iSCSI target port (default: 3260).

    Returns:
        dict: Dictionary with node name as key and connectivity result as value.
    """
    results = {}
    log.info("\n=== Verifying iSCSI Target Connectivity ===")

    ocp_obj = ocp.OCP()

    for node_name in worker_node_names:
        log.info(
            f"Checking connectivity from {node_name} to {target_ip}:{target_port}..."
        )

        try:
            # Test connectivity using iscsiadm discovery (most reliable for iSCSI)
            connectivity_cmd = (
                f"timeout 10 iscsiadm -m discovery -t sendtargets "
                f"-p {target_ip}:{target_port} 2>&1 | grep -q {target_ip} && echo 'Connected' || echo 'Failed'"
            )
            stdout = ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[connectivity_cmd], use_root=True, timeout=120
            )

            if "Connected" in stdout:
                log.info(
                    f"✓ Connectivity verified from {node_name} to {target_ip}:{target_port}"
                )
                results[node_name] = {"connected": True, "method": "iscsi"}
            else:
                # Fallback to TCP connectivity test
                tcp_cmd = (
                    f"timeout 5 bash -c "
                    f"'cat < /dev/null > /dev/tcp/{target_ip}/{target_port}' 2>/dev/null "
                    f"&& echo 'TCP_Connected' || echo 'TCP_Failed'"
                )
                stdout2 = ocp_obj.exec_oc_debug_cmd(
                    node=node_name, cmd_list=[tcp_cmd], use_root=True, timeout=30
                )
                if "TCP_Connected" in stdout2:
                    log.info(
                        f"✓ TCP connectivity verified from {node_name} to {target_ip}:{target_port}"
                    )
                    results[node_name] = {"connected": True, "method": "tcp"}
                else:
                    log.warning(
                        f"✗ Connectivity failed from {node_name} to {target_ip}:{target_port}"
                    )
                    results[node_name] = {"connected": False, "error": stdout2}
        except Exception as e:
            log.error(f"Failed to check connectivity on {node_name}: {e}")
            results[node_name] = {"connected": False, "error": str(e)}

    return results


def verify_iscsi_setup():
    """
    Comprehensive verification of iSCSI setup.

    Returns:
        dict: Dictionary containing all verification results.

    """

    log.info("iSCSI SETUP VERIFICATION - START")

    if not config.ENV_DATA.get("iscsi_setup"):
        log.warning("iSCSI configuration not found in ENV_DATA, skipping verification")
        return {"skipped": True, "reason": "iSCSI not configured"}

    try:
        # Get worker node names
        worker_node_names = get_worker_node_names()

        if not worker_node_names:
            log.warning("No worker nodes found, skipping iSCSI verification")
            return {"skipped": True, "reason": "No worker nodes"}

        target_ip = config.ENV_DATA.get("iscsi_target_ip")
        target_iqn = config.ENV_DATA.get("iscsi_target_iqn")

        verification_results = {
            "worker_nodes": worker_node_names,
            "target_ip": target_ip,
            "target_iqn": target_iqn,
        }

        # Verify connectivity
        connectivity_results = verify_iscsi_target_connectivity(
            worker_node_names, target_ip
        )
        verification_results["connectivity"] = connectivity_results

        # Verify sessions
        session_results = verify_iscsi_sessions(worker_node_names, target_iqn)
        verification_results["sessions"] = session_results

        # Verify devices
        device_results = verify_iscsi_devices(worker_node_names, target_iqn)
        verification_results["devices"] = device_results

        # Summary
        all_sessions_ok = all(
            result.get("session", False) for result in session_results.values()
        )
        all_connectivity_ok = all(
            result.get("connected", False) for result in connectivity_results.values()
        )
        has_devices = any(
            result.get("count", 0) > 0 for result in device_results.values()
        )

        verification_results["summary"] = {
            "all_sessions_established": all_sessions_ok,
            "all_nodes_connected": all_connectivity_ok,
            "devices_found": has_devices,
            "overall_status": all_sessions_ok and all_connectivity_ok and has_devices,
        }

        log.info("iSCSI SETUP VERIFICATION - SUMMARY")
        log.info(f"Sessions established: {all_sessions_ok}")
        log.info(f"Connectivity verified: {all_connectivity_ok}")
        log.info(f"Devices found: {has_devices}")
        log.info(
            f"Overall status: {'PASS' if verification_results['summary']['overall_status'] else 'FAIL'}"
        )
        log.info("=" * 70)

        return verification_results

    except Exception as e:
        log.error(f"Error during iSCSI verification: {e}")
        import traceback

        traceback.print_exc()
        return {"error": str(e)}


# --------------------------------------------------------
# Validation functions (run after iscsi_setup or from tests)
# --------------------------------------------------------
def validate_iscsi_connectivity():
    """
    Validate network connectivity to iSCSI target from all worker nodes.

    Raises:
        AssertionError: If any worker node cannot connect.
        ValueError: If iSCSI target IP is not configured in ENV_DATA.
        ValueError: If no worker nodes are found.

    """
    target_ip = config.ENV_DATA.get("iscsi_target_ip")
    if not target_ip:
        raise ValueError("iSCSI target IP not configured in ENV_DATA")

    worker_node_names = get_worker_node_names()
    if not worker_node_names:
        raise ValueError("No worker nodes found")

    log.info("Validating iSCSI connectivity...")
    connectivity_results = verify_iscsi_target_connectivity(
        worker_node_names, target_ip
    )

    failed_nodes = [
        node_name
        for node_name, result in connectivity_results.items()
        if not result.get("connected", False)
    ]
    if failed_nodes:
        raise AssertionError(
            f"Connectivity check failed for worker nodes: {failed_nodes}. "
            f"Full results: {connectivity_results}"
        )
    log.info(f"Connectivity validation passed for all {len(worker_node_names)} node(s)")


def validate_iscsi_sessions():
    """
    Validate iSCSI sessions are established on all worker nodes.

    Raises:
        AssertionError: If any worker node does not have an iSCSI session.

    """
    target_iqn = config.ENV_DATA.get("iscsi_target_iqn")
    if not config.ENV_DATA.get("iscsi_target_ip") or not target_iqn:
        raise ValueError("iSCSI target IP or IQN not configured in ENV_DATA")

    worker_node_names = get_worker_node_names()
    if not worker_node_names:
        raise ValueError("No worker nodes found")

    log.info("Validating iSCSI sessions...")
    session_results = verify_iscsi_sessions(worker_node_names, target_iqn)

    failed_nodes = [
        node_name
        for node_name, result in session_results.items()
        if not result.get("session", False)
    ]
    if failed_nodes:
        raise AssertionError(
            f"iSCSI session check failed for worker nodes: {failed_nodes}. "
            f"Full results: {session_results}"
        )
    log.info(f"Session validation passed for all {len(worker_node_names)} node(s)")


def validate_iscsi_devices():
    """
    Validate iSCSI devices are visible on worker nodes.

    Raises:
        AssertionError: If no iSCSI devices are found on any node.
        ValueError: If iSCSI target IP or IQN is not configured in ENV_DATA.
        ValueError: If no worker nodes are found.

    """
    target_iqn = config.ENV_DATA.get("iscsi_target_iqn")
    if not config.ENV_DATA.get("iscsi_target_ip") or not target_iqn:
        raise ValueError("iSCSI target IP or IQN not configured in ENV_DATA")

    worker_node_names = get_worker_node_names()
    if not worker_node_names:
        raise ValueError("No worker nodes found")

    log.info("Validating iSCSI devices...")
    device_results = verify_iscsi_devices(worker_node_names, target_iqn)

    total_devices = sum(result.get("count", 0) for result in device_results.values())
    if total_devices <= 0:
        raise AssertionError(
            f"No iSCSI devices found on any worker node. Full results: {device_results}"
        )
    log.info(f"Device validation passed. Total devices found: {total_devices}")


def run_iscsi_setup_validations():
    """
    Run all three iSCSI validations: connectivity, sessions, and devices.
    Called at the end of iscsi_setup() and can be used by tests for post-deploy verification.

    Raises:
        AssertionError: If any validation fails.
        ValueError: If iSCSI not configured in ENV_DATA.
        ValueError: If no worker nodes are found.

    """
    if not config.ENV_DATA.get("iscsi_setup"):
        log.warning("iSCSI not configured in ENV_DATA, skipping validations")
        return

    worker_node_names = get_worker_node_names()
    if not worker_node_names:
        log.warning("No worker nodes found, skipping validations")
        return

    log.info("Running iSCSI setup validations (connectivity, sessions, devices)...")
    validate_iscsi_connectivity()
    validate_iscsi_sessions()
    validate_iscsi_devices()
    log.info("All iSCSI setup validations passed.")
