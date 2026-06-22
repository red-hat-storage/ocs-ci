import os
import re
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


def clear_ceph_bluestore_signature_on_wnodes(wnodes, disk_names=None, namespace=None):
    """
    Clears Ceph BlueStore signatures on the specified worker nodes.

    Args:
        wnodes (list): List of worker node objects where the Ceph BlueStore
            signatures will be cleared.
        disk_names (dict): Map of node name to disk name to clear. When
            absent, the disk is auto-detected per node.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the clearing succeeded on all nodes, False otherwise.

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE

    for wnode in wnodes:
        logger.info(f"Clearing Ceph BlueStore signature on worker node: {wnode.name}")
        node_disk = (disk_names or {}).get(
            wnode.name
        ) or detect_simulation_disk_on_node(wnode, namespace)
        if not node_disk:
            logger.warning(
                f"Skipping Ceph BlueStore signature clearing on node {wnode.name} "
                f"as no suitable disk was detected."
            )
            return False

        node_obj = Node(wnode.name, namespace, use_root=True)
        cmd = f"wipefs -a -f {node_disk}"
        out = node_obj.run_cmd(cmd, timeout=300)

        logger.info(f"Command output:\n{out}")

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
        script_src_path=script_src_path,
        script_dest_path=script_dest_path,
        timeout=300,
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
    logger.info(f"Ceph FSID: {ceph_fsid}, Host Node IP: {host_node_ip}")

    script_name = "install_minimal_ceph_conf.sh"
    top_dir = Path(constants.TOP_DIR)
    script_src_path = os.path.join(top_dir, "scripts", "bash", script_name)
    script_dest_path = f"/tmp/{script_name}"

    # Upload the script directly on the node
    logger.info(
        f"Uploading install minimal ceph conf script to the worker node {wnode.name}"
    )
    node_obj.upload_script(
        script_src_path=script_src_path,
        script_dest_path=script_dest_path,
        timeout=300,
    )

    # Run the script on the node
    args = [ceph_fsid, ceph_key, host_node_ip]
    logger.info(
        f"Running install minimal ceph conf script on the worker node {wnode.name} "
        f"with fsid={ceph_fsid}, host_node_ip={host_node_ip}"
    )
    out = node_obj.run_script(
        script_path=script_dest_path,
        args=args,
        timeout=300,
        secrets=[ceph_key],
    )
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

    for wnode in [wn for wn in wnodes if wn.name != host_node.name]:
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
        script_src_path=script_src_path,
        script_dest_path=script_dest_path,
        timeout=300,
    )
    # Run the script on the node
    logger.info(
        f"Running BlueStore simulation script on disk: {disk_name}. "
        f"This may take 1-2 minutes..."
    )
    out = node_obj.run_script(
        script_path=script_dest_path, args=[disk_name], timeout=300
    )

    logger.info("Script output:\n" + out)
    result = "BlueStore simulation complete" in out or (
        "bluestore" in out and "osd_uuid" in out
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


def simulate_ceph_bluestore_on_wnodes(wnodes, namespace=None, disk_map=None):
    """
    Simulates Ceph BlueStore labels on the specified worker nodes.

    This function iterates over a list of worker nodes and simulates a BlueStore label
    on each node's disk. It uses the `simulate_ceph_bluestore_on_node_disk` function
    to perform the simulation on each node.

    Args:
        wnodes (list): List of worker node objects where the BlueStore simulation
            should be performed.
        namespace (str): Namespace for the debug pod.
        disk_map (dict): Map of node name to pre-detected disk name. When
            provided, skips per-node disk detection.

    Returns:
        bool: True if the simulation succeeded on all nodes, False otherwise.

    """
    for wnode in wnodes:
        logger.info(f"Simulating Ceph BlueStore on worker node: {wnode.name}")
        disk_name = (disk_map or {}).get(wnode.name)
        result = simulate_ceph_bluestore_on_node_disk(
            wnode, disk_name=disk_name, namespace=namespace
        )
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

    # Step 2: Detect disks once so the same device is used for simulation and cleanup
    disk_map = {}
    for wnode in wnodes:
        disk = detect_simulation_disk_on_node(wnode, namespace, timeout=300)
        if not disk:
            logger.error(f"Disk detection failed on node {wnode.name}. Aborting.")
            return False
        disk_map[wnode.name] = disk

    # Step 3: Install minimal Ceph cluster and configuration
    host_node = wnodes[0]
    result = install_minimal_ceph_cluster_and_conf_on_wnodes(
        wnodes, host_node, namespace
    )
    if not result:
        logger.warning("Failed to install minimal Ceph cluster and configuration.")
        return False

    # Steps 4-6: simulation + cleanup (cleanup always runs when flags are set)
    simulation_result = False
    cleanup_ok = True
    try:
        # Step 4: Simulate BlueStore labels on all worker nodes
        simulation_result = simulate_ceph_bluestore_on_wnodes(
            wnodes, namespace, disk_map
        )
        if not simulation_result:
            logger.warning("Failed to simulate Ceph BlueStore on worker nodes.")
    finally:
        # Step 5: Optionally remove the minimal Ceph cluster
        if remove_ceph_cluster:
            logger.info("Removing minimal Ceph cluster from host node.")
            if not remove_minimal_ceph_cluster(host_node, namespace):
                logger.warning("Failed to remove minimal Ceph cluster from host node.")
                cleanup_ok = False

        # Step 6: Optionally clear BlueStore signatures from disks
        if clear_signatures:
            logger.info("Clearing Ceph BlueStore signatures from worker node disks.")
            if not clear_ceph_bluestore_signature_on_wnodes(
                wnodes, disk_names=disk_map, namespace=namespace
            ):
                logger.warning(
                    "Failed to clear Ceph BlueStore signatures from worker nodes."
                )
                cleanup_ok = False

    if not simulation_result or not cleanup_ok:
        return False

    logger.info("Full Ceph BlueStore simulation process completed successfully.")
    return True


def get_wipe_devices_from_other_clusters_flag():
    """
    Read the ``wipeDevicesFromOtherClusters`` flag from the StorageCluster CR.

    Returns:
        object: The raw value of
            ``spec.managedResources.cephCluster.cleanupPolicy
            .wipeDevicesFromOtherClusters`` — ``True``, ``False``, or
            ``None`` when the key is absent.

    """
    from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster

    sc_obj = get_storage_cluster()
    sc_data = sc_obj.get(resource_name=constants.DEFAULT_STORAGE_CLUSTER)
    return (
        sc_data.get("spec", {})
        .get("managedResources", {})
        .get("cephCluster", {})
        .get("cleanupPolicy", {})
        .get("wipeDevicesFromOtherClusters")
    )


def verify_no_wipe_devices_from_other_clusters():
    """
    Verify that no bluestore wipe of foreign OSD data occurred when neither
    ``wipe_devices_from_other_clusters`` nor ``odf_forceful_deployment`` was
    requested.

    Performs two checks:

    1. **StorageCluster CR** — asserts that
       ``spec.managedResources.cephCluster.cleanupPolicy.wipeDevicesFromOtherClusters``
       is **not** ``true``.

    2. **OSD prepare logs** — checks each ``rook-ceph-osd-prepare`` pod and
       asserts that the following pattern is **absent**:

       - ``completed wiping OSD <id> device "<dev>" belonging to a different
         ceph cluster`` — would indicate an unexpected wipe occurred

    Returns:
        bool: True if both checks pass (no wipe found), False otherwise.

    """
    from ocs_ci.ocs.resources.pod import get_osd_prepare_pods, get_pod_logs

    # Check 1: StorageCluster CR flag must not be True
    flag = get_wipe_devices_from_other_clusters_flag()
    if flag is True:
        logger.error(
            "StorageCluster wipeDevicesFromOtherClusters is True"
            " but wipe was not requested"
        )
        return False
    logger.info(
        "StorageCluster wipeDevicesFromOtherClusters is %r — no wipe expected",
        flag,
    )

    # Check 2: wipe pattern must be absent from OSD prepare pod logs
    wipe_pattern = re.compile(
        r'completed wiping OSD \d+ device ".+" belonging to a'
        r" different ceph cluster"
    )
    osd_prepare_pods = get_osd_prepare_pods()
    if not osd_prepare_pods:
        logger.warning("No rook-ceph-osd-prepare pods found")
        return True

    for pod in osd_prepare_pods:
        logs = get_pod_logs(pod.name)
        if wipe_pattern.search(logs):
            logger.error(
                "Unexpected wipe of foreign bluestore data found in pod %r logs",
                pod.name,
            )
            return False

    logger.info("Confirmed: no bluestore wipe in OSD prepare logs")
    return True


def verify_wipe_unencrypted_devices_from_other_clusters():
    """
    Verify that the cluster correctly wiped pre-existing bluestore OSDs from
    another cluster before provisioning new OSDs.

    Performs two checks:

    1. **StorageCluster CR** — asserts that
       ``spec.managedResources.cephCluster.cleanupPolicy.wipeDevicesFromOtherClusters``
       is exactly ``true``.

    2. **OSD prepare logs** — checks each ``rook-ceph-osd-prepare`` pod for
       the following regex patterns, which confirm that foreign bluestore data
       was detected and zapped:

       - ``completed wiping OSD <id> device "<dev>" belonging to a different
         ceph cluster`` — foreign bluestore data detected and zapped
       - ``successfully zapped osd.<id> path "<dev>"`` — low-level wipe
         succeeded
       - ``ceph-volume raw dmcrypt prepare successful`` — new OSD prepared
         (emitted for both encrypted and unencrypted OSDs)

    Returns:
        bool: True if both checks pass, False otherwise.

    """
    from ocs_ci.ocs.resources.pod import get_osd_prepare_pods, get_pod_logs

    # Check 1: StorageCluster CR flag
    flag = get_wipe_devices_from_other_clusters_flag()
    if flag is True:
        logger.info("StorageCluster has wipeDevicesFromOtherClusters set to true")
    else:
        logger.error(
            "StorageCluster wipeDevicesFromOtherClusters is %r, expected true",
            flag,
        )
        return False

    # Check 2: OSD prepare pod logs
    expected_patterns = [
        re.compile(
            r'completed wiping OSD \d+ device ".+" belonging to a different'
            r" ceph cluster"
        ),
        re.compile(r'successfully zapped osd\.\d+ path ".+"'),
        re.compile(r"ceph-volume raw dmcrypt prepare successful"),
    ]

    osd_prepare_pods = get_osd_prepare_pods()
    if not osd_prepare_pods:
        logger.error("No rook-ceph-osd-prepare pods found")
        return False

    for pod in osd_prepare_pods:
        logs = get_pod_logs(pod.name)
        logger.info("Verifying bluestore wipe log entries in pod %s", pod.name)
        for pattern in expected_patterns:
            if not pattern.search(logs):
                logger.error(
                    "Expected log pattern not found in pod %r: %r",
                    pod.name,
                    pattern.pattern,
                )
                return False

    return True


def verify_wipe_encrypted_devices_from_other_clusters():
    """
    Verify that the cluster correctly wiped pre-existing encrypted (LUKS)
    OSDs from another cluster before provisioning new encrypted OSDs.

    Used when ``simulate_bluestore_label_dmcrypt`` is set — the simulation
    stamps LUKS headers on each disk, and this function confirms Rook
    detected and wiped them before creating fresh encrypted OSDs.

    Performs two checks:

    1. **StorageCluster CR** — asserts that
       ``spec.managedResources.cephCluster.cleanupPolicy.wipeDevicesFromOtherClusters``
       is exactly ``true``.

    2. **OSD prepare logs** — checks each ``rook-ceph-osd-prepare`` pod for
       the following regex patterns:

       - ``cleaning encrypted disk ".+" that is part of a different ceph
         cluster`` — LUKS header detected as belonging to a foreign cluster
       - ``successfully zapped device ".+"`` — low-level wipe succeeded
       - ``completed wiping device ".+" belonging to a different ceph
         cluster`` — wipe fully complete
       - ``ceph-volume raw prepare .+ --dmcrypt`` — new encrypted OSD
         prepared after the wipe

    Returns:
        bool: True if both checks pass, False otherwise.

    """
    from ocs_ci.ocs.resources.pod import get_osd_prepare_pods, get_pod_logs

    # Check 1: StorageCluster CR flag
    flag = get_wipe_devices_from_other_clusters_flag()
    if flag is True:
        logger.info("StorageCluster has wipeDevicesFromOtherClusters set to true")
    else:
        logger.error(
            "StorageCluster wipeDevicesFromOtherClusters is %r, expected true",
            flag,
        )
        return False

    # Check 2: OSD prepare pod logs
    expected_patterns = [
        re.compile(
            r'cleaning encrypted disk ".+" that is part of a different ceph cluster'
        ),
        re.compile(r'successfully zapped device ".+"'),
        re.compile(
            r'completed wiping device ".+" belonging to a different ceph cluster'
        ),
        re.compile(r"ceph-volume raw prepare .+ --dmcrypt"),
    ]

    osd_prepare_pods = get_osd_prepare_pods()
    if not osd_prepare_pods:
        logger.error("No rook-ceph-osd-prepare pods found")
        return False

    for pod in osd_prepare_pods:
        logs = get_pod_logs(pod.name)
        logger.info(
            "Verifying encrypted bluestore wipe log entries in pod %s", pod.name
        )
        for pattern in expected_patterns:
            if not pattern.search(logs):
                logger.error(
                    "Expected log pattern not found in pod %r: %r",
                    pod.name,
                    pattern.pattern,
                )
                return False

    return True


def post_deployment_verify_wipe_devices():
    """
    Post-deployment wipe verification dispatcher.

    Reads ``wipe_devices_from_other_clusters`` and
    ``odf_forceful_deployment`` from config to decide which verification
    path to take:

    * If either flag is set, verifies the correct wipe scenario occurred
      (unencrypted or dm-crypt, depending on ``simulate_bluestore_label``
      / ``simulate_bluestore_label_dmcrypt``).
    * Otherwise, asserts that no wipe of foreign bluestore data took place.

    Raises:
        AssertionError: When the no-wipe check fails.

    """
    wipe_devices_from_other_clusters = config.ENV_DATA.get(
        "wipe_devices_from_other_clusters", False
    )
    odf_forceful_deployment = config.DEPLOYMENT.get("odf_forceful_deployment", False)
    simulate_bluestore_label = config.ENV_DATA.get("simulate_bluestore_label", False)
    simulate_bluestore_label_dmcrypt = config.ENV_DATA.get(
        "simulate_bluestore_label_dmcrypt", False
    )
    if wipe_devices_from_other_clusters or odf_forceful_deployment:
        if simulate_bluestore_label:
            logger.info(
                "Verify wipe devices from other clusters "
                "(StorageCluster CR flag and OSD prepare logs)"
            )
            assert (
                verify_wipe_unencrypted_devices_from_other_clusters()
            ), "Unencrypted wipe verification failed"
        elif simulate_bluestore_label_dmcrypt:
            logger.info(
                "Verify wipe of encrypted (LUKS) devices from other "
                "clusters (StorageCluster CR flag and OSD prepare logs)"
            )
            assert (
                verify_wipe_encrypted_devices_from_other_clusters()
            ), "Encrypted wipe verification failed"
        else:
            logger.info(
                "Neither simulate_bluestore_label nor "
                "simulate_bluestore_label_dmcrypt is set. "
                "Skipping simulation-specific wipe verification."
            )
    else:
        logger.info(
            "Verify no wipe of foreign bluestore data occurred "
            "(StorageCluster CR flag and OSD prepare logs)"
        )
        assert (
            verify_no_wipe_devices_from_other_clusters()
        ), "No-wipe verification failed"


def _detect_ceph_image_tag(node_obj):
    """
    Detect the Ceph container image tag from a node using cephadm.

    Args:
        node_obj (Node): Node object to run the command on.

    Returns:
        str: Ceph image tag (e.g. "v18.2.0"), or empty string if undetectable.

    """
    try:
        tag_out = node_obj.run_cmd("cephadm version", timeout=60)
        m = re.search(r"cephadm version\s+(\S+)", tag_out or "")
        if m:
            ceph_tag = f"v{m.group(1)}"
            logger.info("Detected Ceph image tag: %s", ceph_tag)
            return ceph_tag
    except Exception:
        logger.warning("Could not detect Ceph version; script will auto-detect the tag")
    return ""


def simulate_ceph_bluestore_dmcrypt_on_node_disk(wnode, disk_name=None, namespace=None):
    """
    Simulates an encrypted Ceph BlueStore OSD (dm-crypt/LUKS) on a disk of a
    given worker node.

    Uploads the simulate_bluestore_label_dmcrypt.sh script to the node and
    executes it, creating a LUKS container with BlueStore metadata inside and
    LUKS header metadata matching a real Rook encrypted OSD layout.

    Args:
        wnode (ocs_ci.ocs.resources.ocs.OCS): The worker node object where the
            simulation should be performed.
        disk_name (str, optional): The disk device name to simulate on.
            If not provided, auto-detects the last /dev/sd* or nvme disk.
        namespace (str): Namespace for the debug pod.

    Returns:
        bool: True if the simulation succeeded, False otherwise.

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE

    if not disk_name:
        disk_name = detect_simulation_disk_on_node(wnode, namespace, timeout=300)

    if not disk_name:
        logger.error("Disk detection failed. Aborting encrypted BlueStore simulation.")
        return False

    node_obj = Node(wnode.name, namespace, use_root=True)

    script_name = "simulate_bluestore_label_dmcrypt.sh"
    top_dir = Path(constants.TOP_DIR)
    script_src_path = os.path.join(top_dir, "scripts", "bash", script_name)
    script_dest_path = f"/tmp/{script_name}"

    logger.info(
        f"Uploading encrypted BlueStore simulation script to "
        f"worker node {wnode.name}"
    )
    node_obj.upload_script(
        script_src_path=script_src_path,
        script_dest_path=script_dest_path,
        timeout=300,
    )

    ceph_tag = _detect_ceph_image_tag(node_obj)

    # Pass VERIFY_DISK_EMPTY=false: Stage 3 wipes the disk anyway, and a
    # prior failed attempt may have left partial LUKS data at LBA0.
    logger.info(
        f"Running encrypted BlueStore simulation script on {wnode.name}. "
        f"ceph-volume prepare can take 5-10 min..."
    )
    out = ""
    try:
        out = node_obj.run_script(
            script_dest_path,
            args=[disk_name, ceph_tag, "false"],
            timeout=660,
        )
    except Exception as e:
        logger.warning("Simulation script failed on %s: %s", wnode.name, e)

    logger.info("Script output:\n" + out)
    result = "Encrypted BlueStore simulation complete" in out
    if result:
        logger.info(
            f"Encrypted BlueStore simulation succeeded on worker node {wnode.name}"
        )
    else:
        logger.warning(
            f"Encrypted BlueStore simulation failed on worker node {wnode.name}"
        )
    return result


def simulate_ceph_bluestore_dmcrypt_on_wnodes(wnodes, namespace=None, disk_map=None):
    """
    Simulates encrypted Ceph BlueStore OSDs (dm-crypt/LUKS) on worker nodes.

    Args:
        wnodes (list): List of worker node objects where the simulation
            should be performed.
        namespace (str): Namespace for the debug pod.
        disk_map (dict): Map of node name to pre-detected disk name. When
            provided, skips per-node disk detection.

    Returns:
        bool: True if the simulation succeeded on all nodes, False otherwise.

    """
    for wnode in wnodes:
        logger.info(f"Simulating encrypted Ceph BlueStore on worker node: {wnode.name}")
        disk_name = (disk_map or {}).get(wnode.name)
        result = simulate_ceph_bluestore_dmcrypt_on_node_disk(
            wnode, disk_name=disk_name, namespace=namespace
        )
        if not result:
            logger.warning(
                f"Failed to simulate encrypted Ceph BlueStore on "
                f"worker node: {wnode.name}"
            )
            return False

    logger.info("Encrypted Ceph BlueStore simulation succeeded on all worker nodes.")
    return True


def simulate_full_ceph_bluestore_dmcrypt_process_on_wnodes(
    wnodes=None,
    namespace=None,
    add_disks=True,
    remove_ceph_cluster=True,
    clear_signatures=True,
):
    """
    Simulates the full encrypted Ceph BlueStore (dm-crypt/LUKS) process on
    the specified worker nodes.

    Steps performed on each worker node:
    1. Adds disks to the nodes if specified.
    2. Installs a minimal Ceph cluster and configuration.
    3. Simulates an encrypted BlueStore OSD on the node's disk.
    4. Removes the minimal Ceph cluster if specified.
    5. Clears BlueStore signatures from disks if specified.

    Args:
        wnodes (list): List of worker node objects. Defaults to all worker nodes.
        namespace (str): Namespace for the debug pod.
        add_disks (bool): Whether to add disks to the nodes before simulation.
        remove_ceph_cluster (bool): Whether to remove the minimal Ceph cluster
            after simulation.
        clear_signatures (bool): Whether to clear BlueStore signatures from
            disks after simulation.

    Returns:
        bool: True if all steps succeeded on all nodes, False otherwise.

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE
    wnodes = wnodes or get_nodes()
    wnode_names = [wnode.name for wnode in wnodes]
    logger.info(
        "Starting full encrypted Ceph BlueStore simulation process on "
        f"worker nodes: {wnode_names}"
    )

    # Step 1: Optionally add disks to worker nodes
    platform = config.ENV_DATA["platform"].lower()
    if add_disks and platform == constants.VSPHERE_PLATFORM:
        logger.info("Adding disks to worker nodes before simulation.")
        disks_already_added = all(
            disks_available_to_cleanup(wnode, namespace) for wnode in wnodes
        )
        if disks_already_added:
            logger.info(
                "Disks are already added to all worker nodes. Skipping addition."
            )
        else:
            add_disk_for_vsphere_platform()

    # Step 2: Detect disks once so the same device is used for simulation and cleanup
    disk_map = {}
    for wnode in wnodes:
        disk = detect_simulation_disk_on_node(wnode, namespace, timeout=300)
        if not disk:
            logger.error(f"Disk detection failed on node {wnode.name}. Aborting.")
            return False
        disk_map[wnode.name] = disk

    # Step 3: Install minimal Ceph cluster and configuration
    host_node = wnodes[0]
    result = install_minimal_ceph_cluster_and_conf_on_wnodes(
        wnodes, host_node, namespace
    )
    if not result:
        logger.warning("Failed to install minimal Ceph cluster and configuration.")
        return False

    # Steps 4-6: simulation + cleanup (cleanup always runs when flags are set)
    simulation_result = False
    cleanup_ok = True
    try:
        # Step 4: Simulate encrypted BlueStore OSDs on all worker nodes
        simulation_result = simulate_ceph_bluestore_dmcrypt_on_wnodes(
            wnodes, namespace, disk_map
        )
        if not simulation_result:
            logger.warning(
                "Failed to simulate encrypted Ceph BlueStore on worker nodes."
            )
    finally:
        # Step 5: Optionally remove the minimal Ceph cluster
        if remove_ceph_cluster:
            logger.info("Removing minimal Ceph cluster from host node.")
            if not remove_minimal_ceph_cluster(host_node, namespace):
                logger.warning("Failed to remove minimal Ceph cluster from host node.")
                cleanup_ok = False

        # Step 6: Optionally clear BlueStore signatures from disks
        if clear_signatures:
            logger.info("Clearing Ceph BlueStore signatures from worker node disks.")
            if not clear_ceph_bluestore_signature_on_wnodes(
                wnodes, disk_names=disk_map, namespace=namespace
            ):
                logger.warning(
                    "Failed to clear Ceph BlueStore signatures from worker nodes."
                )
                cleanup_ok = False

    if not simulation_result or not cleanup_ok:
        return False

    logger.info(
        "Full encrypted Ceph BlueStore simulation process completed successfully."
    )
    return True
