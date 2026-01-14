"""
This module is used for configuring iscsi.
"""

import sys
import logging
import os
import re
import argparse

from ocs_ci.ocs import node, ocp, constants
from ocs_ci.framework import config
from ocs_ci.utility.connection import Connection

log = logging.getLogger(__name__)


key_path = "~/.ssh/openshift-dev.pem"
key_path = os.path.expanduser(key_path)


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


def get_worker_node_ips(kubeconfig_path):
    """
    Collects worker node IPs from the Kubernetes cluster using oc command.

    Parameters:
    kubeconfig_path (str): Path to the kubeconfig file.

    Returns:
    list: A list of IP addresses of worker nodes or an empty list on failure.
    """
    try:
        # Use ocs_ci.utils.node.get_node_ips to get worker node IPs
        worker_ips = node.get_node_ips(
            kubeconfig_path,
            label_selector="node-role.kubernetes.io/worker",
            field_selector="status.phase=Ready",
        )
        return worker_ips
    except Exception as e:
        log.error("Error getting worker node IPs:", e)
        return []


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
    log.info("\n=== Collecting Worker IQNs ===")

    ocp_obj = ocp.OCP()

    for node_name in worker_node_names:
        log.info(f"Getting IQN from worker node {node_name}...")

        try:
            # First, ensure iSCSI service is started
            # Try both iscsid and open-iscsi service names (different distros use different names)
            start_service_cmd = (
                "systemctl start iscsid 2>/dev/null || "
                "systemctl start open-iscsi 2>/dev/null || "
                "systemctl enable --now iscsid 2>/dev/null || "
                "systemctl enable --now open-iscsi 2>/dev/null || "
                "true"
            )
            ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[start_service_cmd], use_root=True, timeout=60
            )
            log.debug(f"Started iSCSI service on {node_name}")

            # Now read the IQN from the initiatorname.iscsi file
            cmd = "grep InitiatorName /etc/iscsi/initiatorname.iscsi | cut -d= -f2"
            stdout = ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[cmd], use_root=True, timeout=60
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
                        private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
                    )
                    # Start iSCSI service via SSH
                    start_cmd = (
                        "sudo systemctl start iscsid 2>/dev/null || "
                        "sudo systemctl start open-iscsi 2>/dev/null || "
                        "sudo systemctl enable --now iscsid 2>/dev/null || "
                        "sudo systemctl enable --now open-iscsi 2>/dev/null || "
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
    This function is a placeholder and requires implementation.

    Parameters:
    target_node_ssh (object): An established SSH connection to the target VM.
    target_iqn (str): Target iSCSI IQN.
    worker_iqns (list): List of IQNs of worker nodes.
    """

    # Setup environment first
    # setup_target_environment(target_node_ssh)
    # Check if target exists
    check_cmd = f"targetcli ls /iscsi/{target_iqn} 2>/dev/null"
    success, stdout, stderr = target_node_ssh(check_cmd)
    if not success:
        create_target_cmd = f"targetcli /iscsi create {target_iqn}"
        target_node_ssh(create_target_cmd)
    else:
        log.info(f"iSCSI target {target_iqn} already exists")
        log.info("Adding workers...")

    # Add LUNs and ACLs for each worker IQN
    for worker_iqn in worker_iqns:
        # Add ACL
        acl_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/acls create {worker_iqn}"
        target_node_ssh(acl_cmd)
        log.info(f"AddedACL for worker IQN: {worker_iqn}")

        # Get list of LUNs to map
        lun_list_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/luns ls"
        target_node_ssh(lun_list_cmd)

        lun_numbers = re.findall(r"lun(\d+)", stdout)

        for lun_num in lun_numbers:
            log.info(f"Mapping LUN{lun_num} to {worker_iqn}...")

            # Map LUN to ACL
            map_cmd = (
                f"targetcli /iscsi/{target_iqn}/tpg1/acls/{worker_iqn} "
                f"create {lun_num} {lun_num} 2>&1 || "
                f"echo 'LUN mapping may already exist'"
            )
            target_node_ssh(map_cmd)
            log.info(f"LUN{lun_num} mapping result: {stdout.strip()}")

    # Save configuration
    log.info("Saving target configuration...")
    save_cmd = "targetcli saveconfig"
    success, stdout, stderr = target_node_ssh(save_cmd)

    if success:
        log.info("Configuration saved successfully")
    else:
        log.warning(f"Save warning: {stderr}")


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
                        private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
                    )
                    log.info(f"Using SSH fallback for {node_ip}...")
                    for cmd in cmds:
                        retcode, stdout, stderr = worker_node_ssh.exec_cmd(cmd)
                        if retcode != 0:
                            log.warning(f"Command failed on {node_ip}: {stderr}")
                        else:
                            log.debug(f"Command output: {stdout}")
            except Exception as fallback_error:
                log.error(f"SSH fallback also failed for {node_name}: {fallback_error}")


def remove_acls_from_target(target_node_ssh, target_iqn, worker_iqns, username):
    """
    Remove ACLs and LUN mappings from target using targetcli.
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
        success, stdout, stderr = target_node_ssh(delete_acl_cmd)

        if success or "does not exist" in stdout.lower():
            log.info(" ACL is deleted")
        else:
            log.warning(f"Warning: {stdout}")

    # Save configuration
    save_cmd = "targetcli saveconfig 2>&1"
    target_node_ssh(save_cmd)
    log.info("\n Configuration saved")


def wipe_luns_on_target(target_node_ssh, target_iqn, username):
    """
    Wipe LUN data on target VM.
    """
    log.info("\n" + "=" * 70)
    log.info("STEP 4: Wiping LUN data on target")
    log.info("=" * 70)

    # Get LUN paths
    get_luns_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/luns ls 2>&1"
    success, stdout, stderr = target_node_ssh(get_luns_cmd)

    if not success or not stdout:
        log.info(" No LUNs found")
        return

    # Parse output to find device paths
    # Example: lun0 -> /backstores/block/disk1 (/dev/sdb)
    for line in stdout.split("\n"):
        if "/dev/" in line:
            # Extract device path

            match = re.search(r"(/dev/[a-z0-9]+)", line)
            if match:
                device = match.group(1)
                log.info(f"\n  Device: {device}")

                # Wipe device
                wipe_cmd = f"dd if=/dev/zero of={device} bs=1M count=100 2>&1"
                target_node_ssh(wipe_cmd)
                log.info(f" Wiped {device}")

                # Remove signatures
                wipefs_cmd = f"wipefs -a {device} 2>&1"
                target_node_ssh(wipefs_cmd)
                log.info(" Removed signatures")


def verify_cleanup(target_node_ssh, target_iqn, username_target, worker_iqns):
    """
    Verify cleanup was successful.
    """
    log.info("\n" + "=" * 70)
    log.info("STEP 5: Verifying cleanup")
    log.info("=" * 70)

    # Check ACLs on target
    log.info("\nChecking target ACLs...")
    for iqn in worker_iqns:
        check_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/acls/{iqn} ls 2>&1"
        success, stdout, stderr = target_node_ssh(check_cmd)

        if "No such path" in stdout:
            log.info(f" ACL for {iqn} successfully removed")
        else:
            raise Exception(f"ACL for {iqn} still exists:\n{stdout}")
    log.info(" All ACLs verified as removed.")


def cleanup_iscsi_target(
    target_node_ssh,
    target_iqn,
    worker_iqns,
    worker_ips,
    wipe_data,
    username_target,
):
    """
    Complete cleanup of iSCSI target.

    Parameters:
    target_vm_ip: Target VM IP address
    target_iqn: Target IQN
    worker_iqns: List of worker IQNs to remove
    worker_ips: List of worker IP addresses
    wipe_data: Whether to wipe data (default: True)
    username_target: SSH user for target VM
    username_worker: SSH user for workers
    """
    log.info("\n" + "=" * 70)
    log.info("iSCSI TARGET CLEANUP - START")
    log.info("=" * 70)
    log.info(f"Target IQN: {target_iqn}")
    log.info(f"Workers: {len(worker_ips)}")
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
    log.info("Setting up iSCSI configuration...")

    # Get worker node names (works on pure OCP clusters without OCS)
    worker_node_names = get_worker_node_names()
    if not worker_node_names:
        log.error("No worker nodes found!")
        sys.exit(1)

    log.info(f"Current available worker nodes: {worker_node_names}")
    worker_iqns = get_worker_iqns(worker_node_names)
    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        sys.exit(1)

    target_node_ssh = Connection(
        host=config.ENV_DATA.get("iscsi_target_ip"),
        user=config.ENV_DATA.get("iscsi_target_username"),
        private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
    )

    configure_target(
        target_node_ssh, config.ENV_DATA.get("iscsi_target_iqn"), worker_iqns
    )
    configure_initiators(worker_node_names)


def iscsi_teardown():
    log.info("Tearing down iSCSI target...")
    # Get worker nodes
    kubeconfig_path = os.path.join(
        config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
    )
    worker_node_ips = get_worker_node_ips(kubeconfig_path)
    log.info(f"Current available worker nodes are {worker_node_ips}")
    worker_iqns = get_worker_iqns(worker_node_ips)
    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        sys.exit(1)

    target_node_ssh = Connection(
        host=config.ENV_DATA.get("iscsi_target_ip"),
        user=config.ENV_DATA.get("iscsi_target_username"),
        private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
    )

    cleanup_iscsi_target(
        target_node_ssh,
        config.ENV_DATA.get("iscsi_target_iqn"),
        worker_iqns,
        worker_node_ips,
        wipe_data=True,
    )


def init_arg_parser():
    """
    Initialize argument parser for iscsi setup/teardown

    Returns:
        object: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="iSCSI Setup/Teardown Utility",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Set up iSCSI configuration",
    )
    parser.add_argument(
        "--teardown",
        action="store_true",
        help="Tear down iSCSI configuration instead of setting it up",
    )
    parser.add_argument(
        "--target-ip",
        type=str,
        help="IP address of the iSCSI target node",
    )
    parser.add_argument(
        "--target-iqn",
        type=str,
        help="IQN of the iSCSI target node",
    )
    parser.add_argument(
        "--target-username",
        type=str,
        help="SSH username for the iSCSI target node",
    )
    parser.add_argument(
        "--password",
        type=str,
        help="SSH password for the iSCSI target node",
    )

    return parser.parse_args()


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
                node=node_name, cmd_list=[session_cmd], use_root=True, timeout=60
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
    2. Getting device information from iscsiadm session details

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
            # Method 1: Use lsblk to find devices with transport type 'iscsi'
            lsblk_cmd = "lsblk -o NAME,TYPE,TRAN,SIZE,MODEL -n | grep -i iscsi"
            stdout = ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[lsblk_cmd], use_root=True, timeout=60
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

            # Method 2: Get device information from iscsiadm session details
            # This is the most reliable method as it directly queries the iSCSI session
            device_name_cmd = (
                "iscsiadm -m session -P 3 2>/dev/null | "
                "grep 'Attached scsi disk' | awk '{print $4}' | "
                "sed 's|/dev/||' | sort -u"
            )
            stdout2 = ocp_obj.exec_oc_debug_cmd(
                node=node_name, cmd_list=[device_name_cmd], use_root=True, timeout=60
            )
            if stdout2:
                session_devices = [
                    line.strip() for line in stdout2.split("\n") if line.strip()
                ]
                for dev_name in session_devices:
                    if dev_name and dev_name not in block_devices:
                        block_devices.append(dev_name)

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
                node=node_name, cmd_list=[connectivity_cmd], use_root=True, timeout=60
            )

            if "Connected" in stdout:
                log.info(
                    f"✓ Connectivity verified from {node_name} to {target_ip}:{target_port}"
                )
                results[node_name] = {"connected": True, "method": "iscsi"}
            else:
                # Fallback to TCP connectivity test
                tcp_cmd = (
                    f"timeout 5 bash -c 'cat < /dev/null > /dev/tcp/{target_ip}/{target_port}' 2>/dev/null "
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
    log.info("\n" + "=" * 70)
    log.info("iSCSI SETUP VERIFICATION - START")
    log.info("=" * 70)

    if not config.ENV_DATA.get("iscsi_target_ip") or not config.ENV_DATA.get(
        "iscsi_target_iqn"
    ):
        log.warning("iSCSI configuration not found in ENV_DATA, skipping verification")
        return {"skipped": True, "reason": "iSCSI not configured"}

    try:
        # Get worker node names (works on pure OCP clusters without OCS)
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

        log.info("\n" + "=" * 70)
        log.info("iSCSI SETUP VERIFICATION - SUMMARY")
        log.info("=" * 70)
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


def main():
    "main Function to setup iscsi target and initiators"
    args = init_arg_parser()
    if args.setup:
        iscsi_setup()
    elif args.teardown:
        iscsi_teardown()
    else:
        log.error("Please provide either --setup or --teardown argument")
        sys.exit(1)


if __name__ == "__main__":
    main()
