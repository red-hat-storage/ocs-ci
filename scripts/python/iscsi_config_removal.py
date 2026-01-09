import sys
import logging
import os
import re

from iscsi_config import (
    get_worker_node_ips,
    get_worker_iqns,
    TARGET_IQN,
    TARGET_IP,
    ssh_run,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


USERNAME = "root"
key_path = "~/.ssh/openshift-dev.pem"
key_path = os.path.expanduser(key_path)


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


def verify_cleanup(target_vm_ip, target_iqn, username_target):
    """
    Verify cleanup was successful.
    """
    log.info("\n" + "=" * 70)
    log.info("STEP 5: Verifying cleanup")
    log.info("=" * 70)

    # Check ACLs on target
    log.info("\nChecking target ACLs...")
    check_cmd = f"targetcli /iscsi/{target_iqn}/tpg1/acls ls 2>&1"
    success, stdout, stderr = ssh_run(target_vm_ip, check_cmd, username_target)

    if "No ACLs" in stdout or not stdout:
        log.info(" No ACLs remaining")
    else:
        raise Exception(f"ACLs still exist:\n{stdout}")


def cleanup_iscsi_target(
    target_vm_ip,
    target_iqn,
    worker_iqns,
    worker_ips,
    wipe_data,
    username_target,
    username_worker,
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
        verify_cleanup(
            target_vm_ip, target_iqn, worker_ips, username_target, username_worker
        )

        log.info("\n" + "=" * 70)
        log.info("✓ CLEANUP COMPLETED SUCCESSFULLY")
        log.info("=" * 70)

    except Exception as e:
        log.error(f"\n Cleanup failed: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":

    KUBECONFIG_PATH = "/Users/avdhootsagare/auth_odf/auth/kubeconfig"
    # Get worker nodes
    worker_node_ips = get_worker_node_ips(KUBECONFIG_PATH)
    log.info(f"Current available worker nodes are {worker_node_ips}")
    worker_iqns = get_worker_iqns(worker_node_ips)
    if not worker_iqns:
        log.info("No IQNs found! Exiting...")
        sys.exit(1)

    # Run cleanup
    cleanup_iscsi_target(
        target_vm_ip=TARGET_IP,
        target_iqn=TARGET_IQN,
        worker_iqns=worker_iqns,
        worker_ips=worker_node_ips,
        wipe_data=True,
        username_target="root",
        username_worker="core",
    )
