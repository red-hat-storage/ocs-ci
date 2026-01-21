import os
import logging
from pathlib import Path

from ocs_ci.deployment.baremetal import (
    detect_simulation_disk_on_node,
    disks_available_to_cleanup,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_nodes, get_node_internal_ip, Node
from ocs_ci.framework import config
from ocs_ci.deployment.helpers.lso_helpers import add_disk_for_vsphere_platform

logger = logging.getLogger(__name__)


def get_ceph_admin_key(host_node, namespace=None):
    """
    Retrieves the Ceph admin key from the specified host node.

    Args:
        host_node (ocs_ci.ocs.resources.ocs.OCS): The host node object from
            which to retrieve the Ceph admin key.
        namespace (str): Namespace for the debug pod.

    Returns:
        str: The Ceph admin key.

    Raises:
        RuntimeError: If the key cannot be parsed from the output.

    """
    node_obj = Node(host_node.name, namespace, use_root=True)

    cmd = "cephadm shell -- ceph auth get-key client.bootstrap-osd"
    out = node_obj.run_cmd(cmd, timeout=300)
    # Get the key from the last line of the output
    if out:
        key = str(out).strip().splitlines()[-1]
        return key

    raise RuntimeError("Failed to parse Ceph admin key from output")


def get_ceph_fsid(host_node, namespace=None):
    """
    Retrieves the Ceph FSID from the specified host node.

    Args:
        host_node (ocs_ci.ocs.resources.ocs.OCS): The host node object from which to retrieve the FSID.
        namespace (str): Namespace for the debug pod.

    Returns:
        str: The Ceph FSID.

    Raises:
        RuntimeError: If the FSID cannot be parsed from the output.

    """
    node_obj = Node(host_node.name, namespace, use_root=True)

    cmd = "cephadm shell -- ceph fsid"
    out = node_obj.run_cmd(cmd, timeout=300)
    # Get the FSID from the last line of the output
    if out:
        fsid = str(out).strip().splitlines()[-1]
        return fsid

    raise RuntimeError("Failed to parse Ceph FSID from output")


def remove_minimal_ceph_cluster(host_node, namespace=None):
    """
    Removes a minimal Ceph cluster from the specified host node.

    Args:
        host_node (ocs_ci.ocs.resources.ocs.OCS): The host node object from
            which to remove the Ceph cluster.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the removal succeeded, False otherwise.

    """
    node_obj = Node(host_node.name, namespace, use_root=True)

    fsid = get_ceph_fsid(host_node, namespace)
    logger.info(f"Removing minimal Ceph cluster with FSID: {fsid}")
    cmd = f"cephadm rm-cluster --fsid {fsid} --force"
    out = node_obj.run_cmd(cmd, timeout=300)

    logger.info(f"Removal command output:\n{out}")
    success_msg = "Deleting cluster"
    return success_msg in out


def clear_ceph_bluestore_signature_on_wnodes(wnodes, disk_name=None, namespace=None):
    """
    Clears Ceph BlueStore signatures on the specified worker nodes.

    Args:
        wnodes (list): List of worker node objects where the Ceph BlueStore
            signatures will be cleared.
        disk_name (str): Specific disk name to clear the signature from.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the clearing succeeded on all nodes, False otherwise.

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE

    for wnode in wnodes:
        logger.info(f"Clearing Ceph BlueStore signature on worker node: {wnode.name}")
        if not disk_name:
            disk_name = detect_simulation_disk_on_node(wnode, namespace)
            if not disk_name:
                logger.warning(
                    f"Skipping Ceph BlueStore signature clearing on node {wnode.name} "
                    f"as no suitable disk was detected."
                )
                return False

        node_obj = Node(wnode.name, namespace, use_root=True)
        cmd = f"wipefs -a -f {disk_name} || true"
        out = node_obj.run_cmd(cmd, timeout=300)

        logger.info(f"Command output:\n{out}")
        success_msg = "erased"
        if success_msg not in out:
            logger.warning(
                f"Failed to clear Ceph BlueStore signature on worker node: {wnode.name}"
            )
            return False

    logger.info("Ceph BlueStore signatures cleared successfully on all worker nodes.")
    return True


def install_minimal_ceph_cluster(wnode, namespace=None):
    """
    Installs a minimal Ceph cluster on the specified worker node.

    Args:
        wnode (ocs_ci.ocs.resources.ocs.OCS): The worker node object where
            the Ceph cluster will be installed.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the installation succeeded, False otherwise.

    """
    node_obj = Node(wnode.name, namespace, use_root=True)

    script_name = "install_minimal_ceph_cluster.sh"
    top_dir = Path(constants.TOP_DIR)
    script_src_path = os.path.join(top_dir, "scripts", "bash", script_name)
    script_dest_path = f"/tmp/{script_name}"

    # Upload the script directly on the node
    logger.info(
        f"Uploading install minimal ceph cluster script to the worker node {wnode.name}"
    )
    node_obj.upload_script(
        script_src_path=script_src_path, script_dest_path=script_dest_path, timeout=300
    )
    # Run the script on the node
    logger.info(
        f"Running install minimal ceph cluster script on the worker node {wnode.name}. "
        f"This may take a few minutes..."
    )
    out = node_obj.run_script(script_path=script_dest_path, timeout=600)
    logger.info(f"result = {out}")
    success_msg = "completed successfully"
    return success_msg in out


def install_minimal_ceph_conf(wnode, host_node, namespace=None):
    """
    Installs a minimal Ceph configuration on the specified worker node.

    Args:
        wnode (ocs_ci.ocs.resources.ocs.OCS): The worker node object where
            the Ceph configuration will be installed.
        host_node (ocs_ci.ocs.resources.ocs.OCS): The host node object from
            which to copy the Ceph configuration.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the installation succeeded, False otherwise.

    """
    node_obj = Node(wnode.name, namespace, use_root=True)

    ceph_key = get_ceph_admin_key(host_node, namespace)
    ceph_fsid = get_ceph_fsid(host_node, namespace)
    host_node_ip = get_node_internal_ip(host_node)
    logger.info(
        f"Ceph FSID: {ceph_fsid}, Ceph Key: {ceph_key}, Host Node IP: {host_node_ip}"
    )

    script_name = "install_minimal_ceph_conf.sh"
    top_dir = Path(constants.TOP_DIR)
    script_src_path = os.path.join(top_dir, "scripts", "bash", script_name)
    script_dest_path = f"/tmp/{script_name}"

    # Upload the script directly on the node
    logger.info(
        f"Uploading install minimal ceph conf script to the worker node {wnode.name}"
    )
    node_obj.upload_script(
        script_src_path=script_src_path, script_dest_path=script_dest_path, timeout=300
    )

    # Run the script on the node
    args = f"{ceph_fsid} {ceph_key} {host_node_ip}"
    logger.info(
        f"Running install minimal ceph conf script on the worker node {wnode.name} "
        f"with the args: {args}"
    )
    out = node_obj.run_script(script_path=script_dest_path, timeout=300)
    logger.info("Script output:\n" + out)
    success_msg = "created successfully"
    return success_msg in out


def install_minimal_ceph_cluster_and_conf_on_wnodes(
    wnodes, host_node=None, namespace=None
):
    """
    Installs a minimal Ceph cluster and configuration on the specified worker nodes.

    Args:
        wnodes (list): List of worker node objects where the Ceph cluster and configuration
            will be installed.
        host_node (ocs_ci.ocs.resources.ocs.OCS): The host node object where the Ceph cluster
            will be installed. If None, the first worker node in the list will be used as the host node.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the installation succeeded on all nodes, False otherwise.

    Raises:
        RuntimeError: If the Ceph cluster or configuration installation fails on any node.

    """
    host_node = host_node or wnodes[0]
    logger.info(f"Installing minimal Ceph cluster on host node: {host_node.name}")
    result = install_minimal_ceph_cluster(host_node, namespace)
    if not result:
        logger.warning(
            f"Failed to install minimal Ceph cluster on host node: {host_node.name}"
        )
        return False
    logger.info("Minimal Ceph cluster installed successfully on host node.")

    for wnode in wnodes[1:]:
        logger.info(
            f"Installing minimal Ceph configuration on worker node: {wnode.name}"
        )
        result = install_minimal_ceph_conf(wnode, host_node, namespace)
        if not result:
            logger.warning(
                f"Failed to install minimal Ceph configuration on worker node: {wnode.name}"
            )
            return False

    logger.info(
        "Minimal Ceph cluster and ceph configuration installed successfully "
        "on all worker nodes."
    )
    return True


def simulate_ceph_bluestore_on_node_disk(wnode, disk_name=None, namespace=None):
    """
    Simulates a Ceph BlueStore label on a specified disk of a given worker node.


    This function uploads a local shell script to the node using base64 encoding,
    and executes it to simulate a BlueStore label on the specified disk. If no disk is specified,
    the function attempts to auto-detect a suitable disk. The output is parsed to determine
    whether the simulation was successful.

    Args:
        wnode (ocs_ci.ocs.resources.ocs.OCS): The worker node object where the simulation
            should be performed.
        disk_name (str, optional): The disk device name to simulate the label on.
            If not provided, the function auto-detects the last /dev/sd* or the nvme disk on the node.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the simulation succeeded (BlueStore label detected), False otherwise.

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE

    if not disk_name:
        disk_name = detect_simulation_disk_on_node(wnode, namespace, timeout=300)

    if not disk_name:
        logger.error("Disk detection failed. Aborting BlueStore simulation.")
        return False

    node_obj = Node(wnode.name, namespace, use_root=True)

    script_name = "simulate_bluestore_label.sh"
    top_dir = Path(constants.TOP_DIR)
    script_src_path = os.path.join(top_dir, "scripts", "bash", script_name)
    script_dest_path = f"/tmp/{script_name}"

    # Upload the script directly on the node
    logger.info(
        f"Uploading BlueStore simulation script to the worker node {wnode.name}"
    )
    node_obj.upload_script(
        script_src_path=script_src_path, script_dest_path=script_dest_path, timeout=300
    )
    # Run the script on the node
    logger.info(
        f"Running BlueStore simulation script on disk: {disk_name}. "
        f"This may take 1-2 minutes..."
    )
    args = f"{disk_name}"
    out = node_obj.run_script(script_path=script_dest_path, args=args, timeout=300)

    logger.info("Script output:\n" + out)
    result = (
        "BlueStore simulation complete" in out or ("bluestore" and "osd_uuid") in out
    )
    if result:
        logger.info(
            f"BlueStore label simulation succeeded on the worker node {wnode.name}"
        )
    else:
        logger.warning(
            f"BlueStore label simulation failed on the worker node {wnode.name}"
        )
    return result


def simulate_ceph_bluestore_on_wnodes(wnodes, namespace=None):
    """
    Simulates Ceph BlueStore labels on the specified worker nodes.

    This function iterates over a list of worker nodes and simulates a BlueStore label
    on each node's disk. It uses the `simulate_ceph_bluestore_on_node_disk` function
    to perform the simulation on each node.

    Args:
        wnodes (list): List of worker node objects where the BlueStore simulation
            should be performed.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the simulation succeeded on all nodes, False otherwise.

    """
    for wnode in wnodes:
        logger.info(f"Simulating Ceph BlueStore on worker node: {wnode.name}")
        result = simulate_ceph_bluestore_on_node_disk(wnode, namespace=namespace)
        if not result:
            logger.warning(
                f"Failed to simulate Ceph BlueStore on worker node: {wnode.name}"
            )
            return False

    logger.info("Ceph BlueStore simulation succeeded on all worker nodes.")
    return True


def simulate_full_ceph_bluestore_process_on_wnodes(
    wnodes=None,
    namespace=None,
    add_disks=True,
    remove_ceph_cluster=True,
    clear_signatures=True,
):
    """
    Simulates the full Ceph BlueStore process on the specified worker nodes.

    This function performs the following steps on each worker node:
    1. Adds disks to the nodes if specified.
    2. Installs a minimal Ceph cluster and configuration.
    3. Simulates a BlueStore label on the node's disk.
    4. Removes the minimal Ceph cluster if specified.

    Args:
        wnodes (list): List of worker node objects where the full BlueStore simulation
            should be performed.
        namespace (str): Namespace for the debug pod.
        add_disks (bool): Whether to add disks to the nodes before simulation.
        remove_ceph_cluster (bool): Whether to remove the minimal Ceph cluster after simulation.
        clear_signatures (bool): Whether to clear BlueStore signatures from disks after simulation.

    Returns:
        bool: True if all steps succeeded on all nodes, False otherwise.

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE
    wnodes = wnodes or get_nodes()
    wnode_names = [wnode.name for wnode in wnodes]
    logger.info(
        f"Starting full Ceph BlueStore simulation process on worker nodes : {wnode_names}"
    )

    # Step 1: Optionally add disks to worker nodes
    platform = config.ENV_DATA["platform"].lower()
    if add_disks and platform == constants.VSPHERE_PLATFORM:
        logger.info("Adding disks to worker nodes before simulation.")
        # Check if disks are already added
        disks_already_added = all(
            disks_available_to_cleanup(wnode, namespace) for wnode in wnodes
        )
        if disks_already_added:
            logger.info(
                "Disks are already added to all worker nodes. Skipping addition."
            )
        else:
            add_disk_for_vsphere_platform()

    # Step 2: Install minimal Ceph cluster and configuration
    host_node = wnodes[0]
    result = install_minimal_ceph_cluster_and_conf_on_wnodes(
        wnodes, host_node, namespace
    )
    if not result:
        logger.warning("Failed to install minimal Ceph cluster and configuration.")
        return False

    # Step 3: Simulate BlueStore labels on all worker nodes
    result = simulate_ceph_bluestore_on_wnodes(wnodes, namespace)
    if not result:
        logger.warning("Failed to simulate Ceph BlueStore on worker nodes.")
        return False

    # Step 4: Optionally remove the minimal Ceph cluster
    if remove_ceph_cluster:
        logger.info("Removing minimal Ceph cluster from host node.")
        host_node = wnodes[0]
        result = remove_minimal_ceph_cluster(host_node, namespace)
        if not result:
            logger.warning("Failed to remove minimal Ceph cluster from host node.")
            return False

    # Step 5: Optionally clear BlueStore signatures from disks
    if clear_signatures:
        logger.info("Clearing Ceph BlueStore signatures from worker node disks.")
        result = clear_ceph_bluestore_signature_on_wnodes(wnodes, namespace=namespace)
        if not result:
            logger.warning(
                "Failed to clear Ceph BlueStore signatures from worker nodes."
            )
            return False

    logger.info("Full Ceph BlueStore simulation process completed successfully.")
    return True
