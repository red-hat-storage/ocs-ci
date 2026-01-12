#!/usr/bin/env python3

import paramiko
import sys
import logging
import os
import re

from ocs_ci.ocs import node


log = logging.getLogger(__name__)


USERNAME = "root"
key_path = "~/.ssh/openshift-dev.pem"
key_path = os.path.expanduser(key_path)

TARGET_IQN = "iqn.2003-01.org.linux-iscsi.localhost.x8664:sn.d7a7c8437192"
TARGET_IP = "10.1.161.239"

BACKSTORES = ["disk0", "disk1", "disk2"]  # Already created on target
MOUNT_BASE = "/mnt/iscsi_lun"


def ssh_run(host, cmd, username=USERNAME):
    """
    Executes a command on a remote host via SSH using Paramiko.

    Parameters:
    host (str): The hostname or IP address of the remote server.
    cmd (str): The command to execute on the remote server.

    Returns:
    (str): stdout objects from the command execution
    """
    log.info(f"\n[{host}] ➜ {cmd}")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        if username == "core":
            private_key = paramiko.RSAKey.from_private_key_file(key_path)
            client.connect(host, username=username, pkey=private_key, timeout=10)
        else:
            client.connect(
                host,
                username=username,
                password=os.environ.get("TARGET_PASSWORD"),
                timeout=10,
            )
        stdin, stdout, stderr = client.exec_command(cmd)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()

        if out:
            log.info(out)
        if err:
            log.error("ERROR:", err)
        return True, out, err

    except Exception as e:
        log.error(f"SSH ERROR on {host}: {e}")
        return None
    finally:
        client.close()


def setup_target_environment(target_vm_ip, username=USERNAME):
    """
    Set up target environment on remote target VM via SSH.

    Parameters:
    target_vm_ip (str): IP address of the target VM
    username (str): SSH username (default: root)
    """
    log.info(f"Setting up target environment on {target_vm_ip}...")

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
        success, stdout, stderr = ssh_run(target_vm_ip, cmd, username)
        if not success and "already" not in stderr.lower():
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


# --------------------------------------------------------
# STEP 1: Collect worker node initiator IQNs
# --------------------------------------------------------
def get_worker_iqns(worker_node_ips):
    """
    Collects iSCSI initiator IQNs from worker nodes.

    Parameters:
    worker_node_ips (list): List of IP addresses of worker nodes.

    Returns:
    list: A list of IQNs of worker nodes or an empty list on failure.
    """
    iqns = []
    log.info("\n=== Collecting Worker IQNs ===")

    for node_ip in worker_node_ips:
        iqns.append(
            ssh_run(
                node_ip,
                command="grep InitiatorName /etc/iscsi/initiatorname.iscsi | cut -d= -f2",
                username="core",
            )[0]
            .stdout.decode()
            .strip()
        )
        log.info(f"Worker {node_ip} IQN: {iqns[-1]}")
    return iqns


# --------------------------------------------------------
# STEP 2: Configure Target VM
# --------------------------------------------------------
def configure_target(target_iqn, target_ip, worker_iqns, username=USERNAME):
    """
    Configures the iSCSI target with given IQNs and IP.
    This function is a placeholder and requires implementation.

    Parameters:
    target_iqn (str): The IQN of the target node.
    target_ip (str): The IP address of the target node.
    worker_iqns (list): List of IQNs of worker nodes.
    """

    # Setup environment first

    # Check if target exists
    check_cmd = f"targetcli ls /iscsi/{target_iqn} 2>/dev/null"
    success, stdout, stderr = ssh_run(target_ip, check_cmd, username)
    if not success:
        log.info(f"Creating iSCSI target {target_iqn} on {target_ip}...")
        create_target_cmd = f"targetcli /iscsi create {target_iqn}"
        ssh_run(target_ip, create_target_cmd, username)
    else:
        log.info(f"iSCSI target {target_iqn} already exists on {target_ip}.")
        log.info("Adding workers...")

    # Add LUNs and ACLs for each worker IQN
    for worker_iqn in worker_iqns:
        # Add ACL
        acl_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/acls create {worker_iqn}"
        ssh_run(target_ip, acl_cmd, username)
        log.info(f"AddedACL for worker IQN: {worker_iqn}")

        # Get list of LUNs to map
        lun_list_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/luns ls"
        success, stdout, stderr = ssh_run(target_ip, lun_list_cmd, username)

        lun_numbers = re.findall(r"lun(\d+)", stdout)

        for lun_num in lun_numbers:
            log.info(f"Mapping LUN{lun_num} to {worker_iqn}...")

            # Map LUN to ACL
            map_cmd = (
                f"targetcli /iscsi/{target_iqn}/tpg1/acls/{worker_iqn} "
                f"create {lun_num} {lun_num} 2>&1 || "
                f"echo 'LUN mapping may already exist'"
            )
            success, stdout, stderr = ssh_run(target_ip, map_cmd, username)
            log.info(f"LUN{lun_num} mapping result: {stdout.strip()}")

    # Save configuration
    log.info("Saving target configuration...")
    save_cmd = "targetcli saveconfig"
    success, stdout, stderr = ssh_run(target_ip, save_cmd, username)

    if success:
        log.info("Configuration saved successfully")
    else:
        log.warning(f"Save warning: {stderr}")


# --------------------------------------------------------
# STEP 3: Configure iSCSI Initiator on each Worker
# --------------------------------------------------------
def configure_initiators(worker_ip):
    """
    Configures a worker node as an iSCSI initiator.
    This function installs necessary packages, discovers targets, logs in to the target,
    and enables the iSCSI service.

    Parameters:
    worker_ip (str): The IP address of the worker node to configure.
    """

    log.info("\n=== Configuring Worker Nodes as Initiators ===")
    cmds = [
        f"sudo iscsiadm -m discovery -t sendtargets -p {TARGET_IP}:3260",
        f"sudo iscsiadm -m node -T {TARGET_IQN} -p {TARGET_IP}:3260 --login",
        "sudo systemctl enable --now iscsid || sudo systemctl enable --now open-iscsi",
    ]
    for cmd in cmds:
        ssh_run(worker_ip, cmd, username="core")
    log.info(f"Worker {worker_ip} successfully logged in to target.")


def remove_acls_from_target(target_vm_ip, target_iqn, worker_iqns, username):
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
        success, stdout, stderr = ssh_run(target_vm_ip, delete_acl_cmd, username)

        if success or "does not exist" in stdout.lower():
            log.info(" ACL is deleted")
        else:
            log.warning(f"Warning: {stdout}")

    # Save configuration
    save_cmd = "targetcli saveconfig 2>&1"
    ssh_run(target_vm_ip, save_cmd, username)
    log.info("\n Configuration saved")


def wipe_luns_on_target(target_vm_ip, target_iqn, username):
    """
    Wipe LUN data on target VM.
    """
    log.info("\n" + "=" * 70)
    log.info("STEP 4: Wiping LUN data on target")
    log.info("=" * 70)

    # Get LUN paths
    get_luns_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/luns ls 2>&1"
    success, stdout, stderr = ssh_run(target_vm_ip, get_luns_cmd, username)

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
                ssh_run(target_vm_ip, wipe_cmd, username)
                log.info(f" Wiped {device}")

                # Remove signatures
                wipefs_cmd = f"wipefs -a {device} 2>&1"
                ssh_run(target_vm_ip, wipefs_cmd, username)
                log.info(" Removed signatures")


def verify_cleanup(target_vm_ip, target_iqn, username_target, worker_iqns):
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
        success, stdout, stderr = ssh_run(target_vm_ip, check_cmd, username_target)

        if "No such path" in stdout:
            log.info(f" ACL for {iqn} successfully removed")
        else:
            raise Exception(f"ACL for {iqn} still exists:\n{stdout}")
    success, stdout, stderr = ssh_run(target_vm_ip, check_cmd, username_target)
    log.info(" All ACLs verified as removed.")


def cleanup_iscsi_target(
    target_vm_ip,
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
    log.info(f"Target VM: {target_vm_ip}")
    log.info(f"Target IQN: {target_iqn}")
    log.info(f"Workers: {len(worker_ips)}")
    log.info(f"Wipe data: {wipe_data}")

    try:

        # Step 3: Remove ACLs
        remove_acls_from_target(target_vm_ip, target_iqn, worker_iqns, username_target)

        # Step 4: Wipe target LUNs
        if wipe_data:
            wipe_luns_on_target(target_vm_ip, target_iqn, username_target)

        # Step 5: Verify
        verify_cleanup(target_vm_ip, target_iqn, username_target, worker_iqns)

        log.info("\n" + "=" * 70)
        log.info("✓ CLEANUP COMPLETED SUCCESSFULLY")
        log.info("=" * 70)

    except Exception as e:
        log.error(f"\n Cleanup failed: {e}")
        import traceback

        traceback.print_exc()
        raise e


if __name__ == "__main__":
    KUBECONFIG_PATH = "/Users/avdhootsagare/auth_odf/auth/kubeconfig"
    # Get worker nodes
    worker_node_ips = get_worker_node_ips(KUBECONFIG_PATH)
    log.info(f"Current available worker nodes are {worker_node_ips}")
    worker_iqns = get_worker_iqns(worker_node_ips)
    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        sys.exit(1)

    # Setup
    # setup_target_environment(TARGET_IP)
    configure_target(TARGET_IQN, TARGET_IP, worker_iqns)
    for worker_ip in worker_node_ips:
        configure_initiators(worker_ip)

    # Teardown
    cleanup_iscsi_target(
        target_vm_ip=TARGET_IP,
        target_iqn=TARGET_IQN,
        worker_iqns=worker_iqns,
        worker_ips=worker_node_ips,
        wipe_data=False,
        username_target="root",
        username_worker="core",
    )
