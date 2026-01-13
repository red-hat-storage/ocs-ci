"""
This module is used for configuring iscsi.
"""

import sys
import logging
import os
import re
import argparse

from ocs_ci.ocs import node
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
        node_ssh = Connection(
            host=node_ip,
            user="core",
            private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
        )
        log.info(f"Connecting to worker node {node_ip} to get IQN...")

        retcode, stdout, stderr = node_ssh.exec_cmd(
            cmd="grep InitiatorName /etc/iscsi/initiatorname.iscsi | cut -d= -f2"
        )
        iqns.append(stdout.strip())
        log.info(f"Worker {node_ip} IQN: {stdout.strip()}")
    return iqns


# --------------------------------------------------------
# STEP 2: Configure Target VM
# --------------------------------------------------------
def configure_target(target_node_ssh, target_iqn, worker_iqns):
    """
    Configures the iSCSI target with given IQNs and IP.
    This function is a placeholder and requires implementation.

    Parameters:
    target_iqn (str): The IQN of the target node.
    target_ip (str): The IP address of the target node.
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
def configure_initiators(worker_ips):
    """
    Configures a worker node as an iSCSI initiator.
    This function installs necessary packages, discovers targets, logs in to the target,
    and enables the iSCSI service.

    Parameters:
    worker_ip (str): The IP address of the worker node to configure.
    """

    log.info("\n=== Configuring Worker Nodes as Initiators ===")

    cmds = [
        f"sudo iscsiadm -m discovery -t sendtargets -p {config.ENV_DATA['iscsi_target_ip']}:3260",
        f"sudo iscsiadm -m node -T {config.ENV_DATA['iscsi_target_iqn']} -p \
        {config.ENV_DATA['iscsi_target_ip']}:3260 --login",
        "sudo systemctl enable --now iscsid || sudo systemctl enable --now open-iscsi",
    ]

    for worker_ip in worker_ips:
        worker_node_ssh = Connection(
            host=worker_ip,
            user="core",
            private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
        )
        for cmd in cmds:
            worker_node_ssh(worker_ip, cmd, username="core")


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

    configure_target(
        target_node_ssh, config.ENV_DATA.get("iscsi_target_iqn"), worker_iqns
    )

    configure_initiators(worker_node_ips)


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
