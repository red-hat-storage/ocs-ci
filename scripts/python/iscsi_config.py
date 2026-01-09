#!/usr/bin/env python3

import paramiko
import sys
import logging
import subprocess
import json
import os
import re


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
        # Mount configfs
        "mount -t configfs configfs /sys/kernel/config 2>/dev/null || true",
        # Load kernel modules
        "modprobe target_core_mod",
        "modprobe iscsi_target_mod",
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
    # Command to get node details in JSON
    cmd = [
        "oc",
        "--kubeconfig",
        kubeconfig_path,
        "get",
        "nodes",
        "-l",
        "node-role.kubernetes.io/worker",
        "-o",
        "json",
    ]

    try:
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        log.error("Error running oc command:", e.output.decode())
        return []

    data = json.loads(result)

    worker_ips = []

    # Parse each worker node
    for node in data.get("items", []):
        for addr in node["status"]["addresses"]:
            if addr["type"] == "InternalIP":
                worker_ips.append(addr["address"])

    return worker_ips


# --------------------------------------------------------
# STEP 1: Collect worker node initiator IQNs
# --------------------------------------------------------
def get_worker_iqns(worker_node_ips):
    """
    Configures the iSCSI target with given IQNs and IP.
    This function is a placeholder and requires implementation.

    Parameters:
    target_iqn (str): The IQN of the target node.
    target_ip (str): The IP address of the target node.
    worker_iqns (list): List of IQNs of worker nodes.
    """
    iqns = []
    ("\n=== Collecting Worker IQNs ===")
    for node_ip in worker_node_ips:
        start_iscsi_command = (
            "sudo systemctl enable iscsid && sudo systemctl start iscsid"
        )
        ssh_run(node_ip, start_iscsi_command, username="core")

        success, iqn, err = ssh_run(
            node_ip,
            "grep InitiatorName /etc/iscsi/initiatorname.iscsi | cut -d= -f2",
            username="core",
        )
        if iqn:
            log.info(f"[{node_ip}] Found IQN: {iqn}")
            iqns.append(iqn)
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
    # setup_target_environment(target_ip)

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


if __name__ == "__main__":
    KUBECONFIG_PATH = "/Users/avdhootsagare/auth_odf/auth/kubeconfig"
    # Get worker nodes
    worker_node_ips = get_worker_node_ips(KUBECONFIG_PATH)
    log.info(f"Current available worker nodes are {worker_node_ips}")
    worker_iqns = get_worker_iqns(worker_node_ips)
    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        sys.exit(1)
    configure_target(TARGET_IQN, TARGET_IP, worker_iqns)
    for worker_ip in worker_node_ips:
        configure_initiators(worker_ip)
