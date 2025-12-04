#!/usr/bin/env python3

import paramiko
import sys
import logging
import subprocess
import json
import argparse

from rtslib_fb import Root, NetworkPortal, ACL

log = logging.getLogger(__name__)


TARGET_VM = "10.1.161.239"

USERNAME = "root"
SSH_KEY = "/Users/avdhootsagare/.ssh/id_rsa"

TARGET_IQN = "iqn.2003-01.org.linux-iscsi.localhost.x8664:sn.d7a7c8437192 "
TARGET_IP = "10.1.161.239"

BACKSTORES = ["disk0", "disk1", "disk2"]  # Already created on target
MOUNT_BASE = "/mnt/iscsi_lun"


def parse_args():
    """
    Parses command-line arguments using argparse.

    Returns:
    argparse.Namespace: An object containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(description="iSCSI Automation Script")
    parser.add_argument(
        "--kubeconfig",
        required=True,
        help="Path to the kubeconfig file used to discover OCP worker nodes",
    )
    return parser.parse_args()


def ssh_run(host, cmd):
    """ "
    Executes a command on a remote host via SSH using Paramiko.

    Parameters:
    host (str): The hostname or IP address of the remote server.
    cmd (str): The command to execute on the remote server.

    Returns:
    (str): stdout objects from the command execution
    """
    log.info(f"\n[{host}] ➜ {cmd}")
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=USERNAME, key_filename=SSH_KEY, timeout=10)
        stdin, stdout, stderr = client.exec_command(cmd)

        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()

        if out:
            log.info(out)
        if err:
            log.error("ERROR:", err)

        client.close()
        return out

    except Exception as e:
        log.error(f"SSH ERROR on {host}: {e}")
        return None


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
        check_iscsi_command = "which iscsiadm"
        iscsiadm_installed = ssh_run(node_ip, check_iscsi_command)
        if not iscsiadm_installed:
            log.info(f"[{node_ip}] iscsiadm not found. Installing...")
            ssh_run(
                node_ip,
                "sudo yum install -y iscsi-initiator-utils || sudo apt install -y open-iscsi",
            )
            start_iscsi_command = (
                "sudo systemctl enable iscsid && sudo systemctl start iscsid"
            )
            ssh_run(node_ip, start_iscsi_command)

        iqn = ssh_run(
            node_ip, "grep InitiatorName /etc/iscsi/initiatorname.iscsi | cut -d= -f2"
        )
        if iqn:
            log.info(f"[{node_ip}] Found IQN: {iqn}")
            iqns.append(iqn)
    return iqns


# --------------------------------------------------------
# STEP 2: Configure Target VM
# --------------------------------------------------------
def configure_target(target_iqn, target_ip, worker_iqns):
    """
    Configures the iSCSI target with given IQNs and IP.
    This function is a placeholder and requires implementation.

    Parameters:
    target_iqn (str): The IQN of the target node.
    target_ip (str): The IP address of the target node.
    worker_iqns (list): List of IQNs of worker nodes.
    """

    root = Root()
    # Find existing target
    target = None
    for t in root.targets:
        if t.wwn == target_iqn:
            target = t
            break

    if target is None:
        raise Exception(f"Target IQN not found: {target_iqn}")

    # Create TPG
    tpg = target.tpgs[0]

    log.info(f"Adding portal {target_ip}:3260 if missing...")
    try:
        NetworkPortal(tpg, target_ip, 3260)
    except Exception as e:
        if "already exists" in str(e):
            log.exception("Portal already exists — OK")

    # ACL creation
    for iqn in worker_iqns:
        log.info(f"Adding ACL entry for worker IQN: {iqn}")
        try:
            acl = ACL(tpg, iqn)
        except Exception as e:
            if "already exists" in str(e):
                log.exception("ACL already exists — OK")
            acl = [a for a in tpg.node_acls if a.node_wwn == iqn][0]

    # Map all LUNs under ACL
    for lun in tpg.luns:
        try:
            acl.lun_map(lun.storage_object, lun.lun)
            log.info(f"Mapped LUN{lun.lun} under ACL")
        except Exception as e:
            if "already mapped" in str(e):
                log.exception(f"LUN{lun.lun} already mapped — OK")

    log.info("\n Target configuration complete")


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
        "sudo yum install -y iscsi-initiator-utils || sudo apt install -y open-iscsi",
        f"sudo iscsiadm -m discovery -t sendtargets -p {TARGET_IP}:3260",
        f"sudo iscsiadm -m node -T {TARGET_IQN} -p {TARGET_IP}:3260 --login",
        "sudo systemctl enable --now iscsid || sudo systemctl enable --now open-iscsi",
    ]
    for cmd in cmds:
        ssh_run(worker_ip, cmd)
    log.info(f"Worker {worker_ip} successfully logged in to target.")


if __name__ == "__main__":
    args = parse_args()
    KUBECONFIG = args.kubeconfig

    # Get worker nodes
    worker_node_ips = get_worker_node_ips(KUBECONFIG)
    log.info(f"Current available worker nodes are {worker_node_ips}")
    worker_iqns = get_worker_iqns(worker_node_ips)

    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        sys.exit(1)

    configure_target(TARGET_IQN, TARGET_IP, worker_iqns)
    for worker_ip in worker_node_ips:
        configure_initiators(worker_ip)
