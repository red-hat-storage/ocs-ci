#!/usr/bin/env python3
import paramiko
import sys
import time
import logging
import subprocess
import json

log = logging.getLogger(__name__)

# -----------------------------
# USER CONFIGURATION
# -----------------------------
TARGET_VM = "10.1.161.239"

USERNAME = "root"
SSH_KEY = "/Users/avdhootsagare/.ssh/id_rsa"

TARGET_IQN = "iqn.2003-01.org.linux-iscsi.localhost.x8664:sn.d7a7c8437192 "
PORTAL_IP = "10.1.161.239"

BACKSTORES = ["disk0", "disk1", "disk2"]  # Already created on target
MOUNT_BASE = "/mnt/iscsi_lun"


def ssh_run(host, cmd):
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


def get_worker_node_ips(kubeconfig_path="/User/auth_odf/auth/kubeconfig"):
    """
    collect worker node IPs from the cluster
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
        print("Error running oc command:", e.output.decode())
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
def configure_target(worker_iqns):

    log.info("\n=== Configuring iSCSI Target ===")

    # ssh_run(TARGET_VM, f"targetcli /iscsi/{TARGET_IQN} create")

    # ssh_run(TARGET_VM,
    #         f"targetcli /iscsi/{TARGET_IQN}/tpg1/portals create {PORTAL_IP} {PORTAL_PORT}")

    # # LUN creation
    # for i, disk in enumerate(BACKSTORES):
    #     ssh_run(TARGET_VM,
    #             f"targetcli /iscsi/{TARGET_IQN}/tpg1/luns create /backstores/block/{disk}")

    # ACL creation
    for iqn in worker_iqns:
        ssh_run(TARGET_VM, f"targetcli /iscsi/{TARGET_IQN}/tpg1/acls create {iqn}")

    ssh_run(TARGET_VM, "targetcli saveconfig")
    ssh_run(TARGET_VM, "systemctl enable --now target")

    log.info("\n✔ Target configuration complete")


# --------------------------------------------------------
# STEP 3: Configure iSCSI Initiator on each Worker
# --------------------------------------------------------
def configure_initiators(worker_node_ips):
    log.info("\n=== Configuring Worker Nodes as Initiators ===")

    for idx, node_ip in enumerate(worker_node_ips):

        # Discover target
        ssh_run(node_ip, f"iscsiadm -m discovery -t sendtargets -p {PORTAL_IP}")

        # Login
        ssh_run(node_ip, f"iscsiadm -m node -T {TARGET_IQN} -p {PORTAL_IP} --login")

        time.sleep(5)

        # Identify new disk (the one without a filesystem)
        # new_disk = ssh_run(
        #     node, "lsblk -o NAME,TYPE | grep disk | awk '{print $1}' | tail -n 1"
        # )


if __name__ == "__main__":
    # Get worker nodes
    worker_node_ips = get_worker_node_ips()
    log.info(f"Current available worker nodes are {worker_node_ips}")
    worker_iqns = get_worker_iqns(worker_node_ips)

    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        sys.exit(1)

    configure_target(worker_iqns)
    configure_initiators(worker_node_ips)
