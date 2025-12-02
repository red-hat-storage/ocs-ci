#!/usr/bin/env python3
import paramiko
import sys
import time

# -----------------------------
# USER CONFIGURATION
# -----------------------------
OCP_WORKERS = ["worker1.example.com", "worker2.example.com", "worker3.example.com"]
TARGET_VM = "10.1.161.239"

USERNAME = "root"
SSH_KEY = "/Users/avdhootsagare/.ssh/id_rsa"

TARGET_IQN = "iqn.2003-01.org.linux-iscsi.localhost.x8664:sn.d7a7c8437192 "
PORTAL_IP = "10.1.161.239"

BACKSTORES = ["disk0", "disk1", "disk2"]  # Already created on target
MOUNT_BASE = "/mnt/iscsi_lun"


# -----------------------------


def ssh_run(host, cmd):
    print(f"\n[{host}] ➜ {cmd}")
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=USERNAME, key_filename=SSH_KEY, timeout=10)
        stdin, stdout, stderr = client.exec_command(cmd)

        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()

        if out:
            print(out)
        if err:
            print("ERROR:", err)

        client.close()
        return out

    except Exception as e:
        print(f"SSH ERROR on {host}: {e}")
        return None


# --------------------------------------------------------
# STEP 1: Collect worker node initiator IQNs
# --------------------------------------------------------
def get_worker_iqns():
    iqns = []
    print("\n=== Collecting Worker IQNs ===")
    for node in OCP_WORKERS:
        check_iscsi_command = "which iscsiadm"
        iscsiadm_installed = ssh_run(node, check_iscsi_command)
        if not iscsiadm_installed:
            print(f"[{node}] iscsiadm not found. Installing...")
            ssh_run(
                node,
                "sudo yum install -y iscsi-initiator-utils || sudo apt install -y open-iscsi",
            )
            start_iscsi_command = (
                "sudo systemctl enable iscsid && sudo systemctl start iscsid"
            )
            ssh_run(node, start_iscsi_command)

        iqn = ssh_run(
            node, "grep InitiatorName /etc/iscsi/initiatorname.iscsi | cut -d= -f2"
        )
        if iqn:
            print(f"[{node}] Found IQN: {iqn}")
            iqns.append(iqn)
    return iqns


# --------------------------------------------------------
# STEP 2: Configure Target VM
# --------------------------------------------------------
def configure_target(worker_iqns):

    print("\n=== Configuring iSCSI Target ===")

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

    print("\n✔ Target configuration complete")


# --------------------------------------------------------
# STEP 3: Configure iSCSI Initiator on each Worker
# --------------------------------------------------------
def configure_initiators():
    print("\n=== Configuring Worker Nodes as Initiators ===")

    for idx, node in enumerate(OCP_WORKERS):

        # Discover target
        ssh_run(node, f"iscsiadm -m discovery -t sendtargets -p {PORTAL_IP}")

        # Login
        ssh_run(node, f"iscsiadm -m node -T {TARGET_IQN} -p {PORTAL_IP} --login")

        time.sleep(5)

        # Identify new disk (the one without a filesystem)
        # new_disk = ssh_run(
        #     node, "lsblk -o NAME,TYPE | grep disk | awk '{print $1}' | tail -n 1"
        # )


if __name__ == "__main__":
    worker_iqns = get_worker_iqns()

    if not worker_iqns:
        print("No IQNs found! Exiting...")
        sys.exit(1)

    configure_target(worker_iqns)
    configure_initiators()

    print("\n\n🎉 END-TO-END iSCSI Provisioning Completed Successfully!")
