"""
Helper functions for Two-Node Fencing (TNF) cluster deployment
"""

import base64
import logging
import os
import tempfile

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedDeploymentConfiguration
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.templating import Templating
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


def verify_tnf_cluster_topology():
    """
    Verify that the cluster has DualReplica topology (two-node cluster).

    Returns:
        bool: True if cluster is DualReplica, False otherwise

    Raises:
        CommandFailed: If unable to get cluster topology
    """
    logger.info("Verifying two-node cluster topology...")
    try:
        cmd = (
            "oc get infrastructure cluster "
            "-o jsonpath='{.status.controlPlaneTopology}'"
        )
        result = exec_cmd(cmd, shell=True)
        topology = result.stdout.decode().strip().strip("'")
        logger.info(f"Cluster topology: {topology}")

        if topology == constants.TNF_CONTROL_PLANE_TOPOLOGY:
            logger.info("Cluster topology verified: DualReplica")
            return True
        else:
            logger.warning(f"Expected DualReplica topology but found: {topology}")
            return False
    except Exception as e:
        logger.error(f"Failed to verify cluster topology: {e}")
        raise


def get_tnf_node_info():
    """
    Get information about the two nodes in the TNF cluster.

    Returns:
        list: List of dictionaries containing node information
            [{'name': str, 'ip': str, 'role': str}, ...]

    Raises:
        CommandFailed: If nodes cannot be retrieved
    """
    logger.info("Getting TNF node information...")
    ocp_node = OCP(kind=constants.NODE)
    nodes = ocp_node.get()["items"]

    if len(nodes) != 2:
        logger.warning(f"Expected 2 nodes for TNF cluster, found {len(nodes)}")

    node_info = []
    for node in nodes:
        name = node["metadata"]["name"]
        addresses = node["status"]["addresses"]
        ip = next(
            (addr["address"] for addr in addresses if addr["type"] == "InternalIP"),
            None,
        )
        role = (
            "master"
            if "node-role.kubernetes.io/master" in node["metadata"].get("labels", {})
            else "worker"
        )

        node_info.append({"name": name, "ip": ip, "role": role})
        logger.info(f"Node: {name}, IP: {ip}, Role: {role}")

    return sorted(node_info, key=lambda x: x["name"])


def create_local_storage_class():
    """
    Create local block storage class for TNF deployment.

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Creating local block storage class...")
    try:
        _templating = Templating()
        sc_data = _templating.render_template(
            "tnf-deployment/lso-storageclass.yaml.j2",
            {"storage_class_name": constants.TNF_LOCALBLOCK_SC},
        )

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as temp_file:
            temp_file.write(sc_data)
            temp_file_path = temp_file.name

        exec_cmd(f"oc create -f {temp_file_path}")
        logger.info(
            f"Storage class '{constants.TNF_LOCALBLOCK_SC}' created successfully"
        )
        return True

    except CommandFailed as e:
        if "already exists" in str(e):
            logger.info(f"Storage class '{constants.TNF_LOCALBLOCK_SC}' already exists")
            return True
        logger.error(f"Failed to create storage class: {e}")
        return False


def create_persistent_volumes(device_mappings):
    """
    Create persistent volumes for OSD disks on both nodes.

    Args:
        device_mappings (list): List of dictionaries with device mapping info
            [{'node_name': str, 'device_path': str, 'size': str, 'pv_name': str}, ...]

    Returns:
        list: List of created PV names

    Raises:
        CommandFailed: If PV creation fails
    """
    logger.info("Creating persistent volumes for OSD disks...")
    created_pvs = []

    for mapping in device_mappings:
        try:
            _templating = Templating()
            pv_data = _templating.render_template(
                "tnf-deployment/local-pv.yaml.j2",
                {
                    "pv_name": mapping["pv_name"],
                    "storage_size": mapping.get("size", "500Gi"),
                    "device_path": mapping["device_path"],
                    "node_name": mapping["node_name"],
                    "storage_class_name": constants.TNF_LOCALBLOCK_SC,
                },
            )

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            ) as temp_file:
                temp_file.write(pv_data)
                temp_file_path = temp_file.name

            exec_cmd(f"oc create -f {temp_file_path}")
            logger.info(f"PV '{mapping['pv_name']}' created successfully")
            created_pvs.append(mapping["pv_name"])

        except CommandFailed as e:
            if "already exists" in str(e):
                logger.info(f"PV '{mapping['pv_name']}' already exists")
                created_pvs.append(mapping["pv_name"])
            else:
                logger.error(f"Failed to create PV '{mapping['pv_name']}': {e}")
                raise

    logger.info(f"Created {len(created_pvs)} persistent volumes")
    return created_pvs


def configure_drbd(node_0_info, node_1_info, monitor_disk_node_0, monitor_disk_node_1):
    """
    Configure DRBD for the floating monitor.

    1. Fetch drbd-setup script from the rook-ceph-drbd-setup-script ConfigMap
       (created by the ODF operator after installation)
    2. Run with -d/-d0/-d1 flags for the floating monitor disk
    3. The script handles KMM, DRBD kmod, resource config, sync, and ConfigMap

    Args:
        node_0_info (dict): Node 0 information {'name': str, 'ip': str}
        node_1_info (dict): Node 1 information {'name': str, 'ip': str}
        monitor_disk_node_0 (str): Device path for monitor disk on node 0
        monitor_disk_node_1 (str): Device path for monitor disk on node 1

    Returns:
        bool: True if successful

    Raises:
        CommandFailed: If DRBD configuration fails
    """
    logger.info("Configuring DRBD for floating monitor...")

    # Check if DRBD is already configured
    try:
        ocp_cm = OCP(
            kind=constants.CONFIGMAP,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        ocp_cm.get(resource_name=constants.TNF_DRBD_CONFIGURE_CM)
        logger.info("DRBD already configured (drbd-configure ConfigMap exists)")
        return True
    except CommandFailed:
        logger.info("DRBD not yet configured, proceeding with setup...")

    # Validate monitor disks exist on nodes before running the script
    _validate_device_on_node(node_0_info["name"], monitor_disk_node_0)
    _validate_device_on_node(node_1_info["name"], monitor_disk_node_1)

    # Fix virtual disk issues: rotational flag and missing /dev/disk/by-id symlinks
    for node_name, disk_path in [
        (node_0_info["name"], monitor_disk_node_0),
        (node_1_info["name"], monitor_disk_node_1),
    ]:
        _fix_rotational_flag_if_virtual(node_name, disk_path)
        _ensure_disk_by_id_symlink(node_name, disk_path)

    # Fetch the drbd-setup script from the ODF operator ConfigMap
    script_content = _fetch_drbd_setup_script()

    # Save script to temp file
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sh", delete=False, prefix="drbd_setup_"
    ) as f:
        f.write(script_content)
        script_path = f.name
    exec_cmd(f"chmod +x {script_path}")
    logger.info(f"DRBD setup script saved to {script_path}")

    # Build disk flags: -d for same path on both nodes, -d0/-d1 for different
    if monitor_disk_node_0 == monitor_disk_node_1:
        disk_flag = f"-d {monitor_disk_node_0}"
    else:
        disk_flag = f"-d0 {monitor_disk_node_0} -d1 {monitor_disk_node_1}"

    logger.info(f"Running DRBD setup script with: {disk_flag}")
    result = exec_cmd(
        f"bash {script_path} {disk_flag}",
        shell=True,
        timeout=3600,
        ignore_error=True,
    )
    stdout = result.stdout.decode().strip()
    stderr = result.stderr.decode().strip()
    if stdout:
        logger.info(f"DRBD setup script output:\n{stdout}")
    if result.returncode:
        error_detail = stderr or stdout or "no output captured"
        raise CommandFailed(
            f"DRBD setup script failed (exit code {result.returncode}). "
            f"Output: {error_detail}"
        )

    logger.info("DRBD configuration completed successfully")
    return True


def _validate_device_on_node(node_name, device_path):
    """
    Validate that a block device exists on the given node.

    Args:
        node_name (str): Node name
        device_path (str): Device path (e.g. /dev/vdb)

    Raises:
        CommandFailed: If device does not exist or node is not accessible
    """
    logger.info(f"Validating device {device_path} on node {node_name}...")
    try:
        result = exec_cmd(
            f"oc debug -q node/{node_name} -- chroot /host "
            f"lsblk -ndo NAME,SIZE,ROTA,TYPE {device_path}",
            shell=True,
            timeout=120,
        )
        output = result.stdout.decode().strip()
        if output:
            logger.info(f"Device {device_path} on {node_name}: {output}")
        else:
            raise CommandFailed(
                f"Device {device_path} not found on node {node_name}. "
                f"List available devices with: "
                f"oc debug -q node/{node_name} -- chroot /host "
                f"lsblk -o NAME,PATH,SIZE,ROTA,TYPE,FSTYPE"
            )
    except CommandFailed:
        logger.error(
            f"Device {device_path} not accessible on node {node_name}. "
            f"Check that the device exists and the node is reachable."
        )
        raise


def _fix_rotational_flag_if_virtual(node_name, device_path):
    """
    Set rotational flag to 0 on disks that report ROTA=1 in VM environments.

    VM-backed disks (virtio, SCSI-emulated, xen) may falsely report as
    rotational even when backed by SSD storage. The drbd-setup script
    requires ROTA=0.

    Args:
        node_name (str): Node name
        device_path (str): Device path (e.g. /dev/vda, /dev/sdb)
    """

    try:
        resolved = exec_cmd(
            f"oc debug -q node/{node_name} -- chroot /host "
            f"readlink -f {device_path}",
            shell=True,
            timeout=60,
        )
        dev_name = os.path.basename(resolved.stdout.decode().strip())
        rotational_path = f"/sys/block/{dev_name}/queue/rotational"
        result = exec_cmd(
            f"oc debug -q node/{node_name} -- chroot /host cat {rotational_path}",
            shell=True,
            timeout=60,
        )
        current = result.stdout.decode().strip()
        if current == "0":
            return

        logger.info(
            f"Setting {device_path} on {node_name} as non-rotational "
            f"(ROTA=1, likely virtual disk backed by SSD)"
        )
        exec_cmd(
            f"oc debug -q node/{node_name} -- chroot /host "
            f"bash -c 'echo 0 > {rotational_path}'",
            shell=True,
            timeout=60,
        )
    except CommandFailed as e:
        logger.warning(
            f"Could not fix rotational flag for {device_path} on {node_name}: {e}"
        )


def _ensure_disk_by_id_symlink(node_name, device_path):
    """
    Ensure /dev/disk/by-id/ symlink exists for the device.

    The drbd-setup script resolves device paths to /dev/disk/by-id/ for stable
    DRBD configuration. Virtual disks (virtio) often lack by-id entries.
    Creates a virtio-based symlink if none exists.

    Args:
        node_name (str): Node name
        device_path (str): Device path (e.g. /dev/vda)
    """

    dev_name = os.path.basename(device_path)

    try:
        result = exec_cmd(
            f"oc debug -q node/{node_name} -- chroot /host "
            f"bash -c 'for id in /dev/disk/by-id/*; do "
            f'[ -e "$id" ] && [ "$(readlink -f "$id")" = '
            f'"$(readlink -f {device_path})" ] && echo "$id"; '
            f"done'",
            shell=True,
            timeout=60,
            ignore_error=True,
        )
        existing = result.stdout.decode().strip()
        if existing:
            logger.info(
                f"Device {device_path} on {node_name} has by-id: "
                f"{existing.split(chr(10))[0]}"
            )
            return

        symlink_name = f"/dev/disk/by-id/virtio-{dev_name}"
        logger.info(
            f"Creating /dev/disk/by-id symlink for {device_path} "
            f"on {node_name}: {symlink_name}"
        )
        exec_cmd(
            f"oc debug -q node/{node_name} -- chroot /host "
            f"bash -c 'mkdir -p /dev/disk/by-id && "
            f"ln -sf {device_path} {symlink_name}'",
            shell=True,
            timeout=60,
        )
    except CommandFailed as e:
        logger.warning(
            f"Could not ensure by-id symlink for {device_path} " f"on {node_name}: {e}"
        )


def resolve_disk_by_id_path(node_name, device_path):
    """
    Resolve a device path to its /dev/disk/by-id/ path on a node.

    Ensures a by-id symlink exists (creating one for virtual disks if needed),
    then returns the by-id path. Used for PV creation per ODF 4.22 docs which
    require stable by-id paths.

    Args:
        node_name (str): Node name
        device_path (str): Device path (e.g. /dev/vdb)

    Returns:
        str: The /dev/disk/by-id/ path, or the original path if resolution fails
    """
    _fix_rotational_flag_if_virtual(node_name, device_path)
    _ensure_disk_by_id_symlink(node_name, device_path)

    try:
        result = exec_cmd(
            f"oc debug -q node/{node_name} -- chroot /host "
            f"bash -c 'for id in /dev/disk/by-id/*; do "
            f'[ -e "$id" ] && [ "$(readlink -f "$id")" = '
            f'"$(readlink -f {device_path})" ] && echo "$id"; '
            f"done'",
            shell=True,
            timeout=60,
            ignore_error=True,
        )
        by_id = result.stdout.decode().strip()
        if by_id:
            by_id_path = by_id.split("\n")[0]
            logger.info(f"Resolved {device_path} on {node_name} -> {by_id_path}")
            return by_id_path
    except CommandFailed as e:
        logger.warning(
            f"Could not resolve by-id path for {device_path} on {node_name}: {e}"
        )

    logger.warning(
        f"No by-id path found for {device_path} on {node_name}, "
        f"using raw device path"
    )
    return device_path


def _fetch_drbd_setup_script():
    """
    Fetch the drbd-setup script from the ODF operator ConfigMap.

    The ODF operator creates a ConfigMap 'rook-ceph-drbd-setup-script'
    in openshift-storage namespace with the script in .data.script (base64).

    Returns:
        str: The decoded script content

    Raises:
        CommandFailed: If ConfigMap not found after timeout
    """
    logger.info(
        f"Waiting for ConfigMap '{constants.TNF_DRBD_SETUP_SCRIPT_CM}' "
        f"in {constants.OPENSHIFT_STORAGE_NAMESPACE}..."
    )

    for sample in TimeoutSampler(
        timeout=600,
        sleep=15,
        func=_try_get_drbd_script,
    ):
        if sample:
            logger.info("DRBD setup script fetched from ODF operator ConfigMap")
            return sample


def _try_get_drbd_script():
    """Try to fetch and decode the drbd-setup script from ConfigMap."""
    try:
        result = exec_cmd(
            f"oc get configmap {constants.TNF_DRBD_SETUP_SCRIPT_CM} "
            f"-n {constants.OPENSHIFT_STORAGE_NAMESPACE} "
            f"-o jsonpath='{{.data.script}}'",
            shell=True,
        )
        script_b64 = result.stdout.decode().strip().strip("'")
        if script_b64:
            return base64.b64decode(script_b64).decode()
    except CommandFailed:
        logger.debug(
            f"ConfigMap '{constants.TNF_DRBD_SETUP_SCRIPT_CM}' not yet available"
        )
    return None


def _run_drbd_cmd(node_name, drbd_image, drbd_cmd, timeout=300):
    """
    Run a DRBD command on a node using the DRBD utils container.

    Args:
        node_name (str): Node name
        drbd_image (str): DRBD utils container image
        drbd_cmd (str): DRBD command to run (e.g. 'drbdadm status r0')
        timeout (int): Timeout in seconds

    Returns:
        CompletedProcess: Command result
    """
    cmd = (
        f"oc debug -q node/{node_name} -- chroot /host "
        f"podman run --rm --privileged "
        f"--authfile /var/lib/kubelet/config.json "
        f"-v /dev:/dev "
        f"-v {constants.TNF_DRBD_CONF_PATH}:{constants.TNF_DRBD_CONF_PATH} "
        f"-v {constants.TNF_DRBD_DIR_PATH}:{constants.TNF_DRBD_DIR_PATH} "
        f"--net host --hostname {node_name} "
        f"{drbd_image} {drbd_cmd}"
    )
    return exec_cmd(cmd, shell=True, timeout=timeout)


def verify_drbd_configuration():
    """
    Verify DRBD configuration is correct by checking the drbd-configure ConfigMap.

    The ConfigMap is created by the drbd-setup script and contains node IPs,
    disk paths, DRBD device/resource names, and the DRBD utils image reference.

    Returns:
        bool: True if DRBD is configured correctly
    """
    logger.info("Verifying DRBD configuration...")
    try:
        ocp_cm = OCP(
            kind=constants.CONFIGMAP, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        drbd_cm = ocp_cm.get(resource_name=constants.TNF_DRBD_CONFIGURE_CM)

        required_keys = [
            "DRBD_DEVICE_NAME",
            "DRBD_RESOURCE_NAME",
            "DRBD_PORT",
            "DRBD_UTILS_IMAGE",
            "NODE_0_NAME",
            "NODE_0_IP",
            "NODE_1_NAME",
            "NODE_1_IP",
            "BLOCK_DEVICE_PATH_NODE_0",
            "BLOCK_DEVICE_PATH_NODE_1",
        ]

        for key in required_keys:
            if key not in drbd_cm.get("data", {}):
                logger.error(f"Required key '{key}' not found in DRBD ConfigMap")
                return False

        logger.info("DRBD configuration verified successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to verify DRBD configuration: {e}")
        return False


def verify_drbd_status(node_name):
    """
    Verify DRBD status on a specific node.

    Gets the DRBD utils image from the drbd-configure ConfigMap.

    Args:
        node_name (str): Name of the node to check

    Returns:
        bool: True if DRBD is running correctly on the node
    """
    logger.info(f"Checking DRBD status on node {node_name}...")

    try:
        ocp_cm = OCP(
            kind=constants.CONFIGMAP,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        drbd_cm = ocp_cm.get(resource_name=constants.TNF_DRBD_CONFIGURE_CM)
        drbd_image = drbd_cm["data"]["DRBD_UTILS_IMAGE"]
    except (CommandFailed, KeyError) as e:
        logger.error(f"Cannot get DRBD image from ConfigMap: {e}")
        return False

    try:
        result = _run_drbd_cmd(
            node_name,
            drbd_image,
            f"drbdadm -c {constants.TNF_DRBD_CONF_PATH} "
            f"status {constants.TNF_DRBD_RESOURCE_NAME}",
        )
        status = result.stdout.decode()
        logger.info(f"DRBD status on {node_name}: {status.strip()}")
        return "UpToDate" in status

    except CommandFailed as e:
        logger.error(f"Failed to check DRBD status on {node_name}: {e}")
        return False


def verify_port_connectivity(source_node, target_ip, port=constants.TNF_DRBD_PORT):
    """
    Verify network connectivity on DRBD port between nodes.

    Args:
        source_node (str): Source node name
        target_ip (str): Target node IP address
        port (int): Port to check (default: 7794)

    Returns:
        bool: True if port is reachable

    """
    logger.info(
        f"Verifying port {port} connectivity from {source_node} to {target_ip}..."
    )
    try:
        cmd = (
            f"oc debug -q node/{source_node} -- chroot /host "
            f"nc -zv {target_ip} {port}"
        )
        exec_cmd(cmd, shell=True, timeout=120)
        logger.info(f"Port {port} connectivity verified")
        return True

    except Exception as e:
        logger.warning(
            f"Port {port} connectivity check from {source_node} to {target_ip}: {e}"
        )
        return False


def get_block_devices_on_node(node_name):
    """
    List block devices on a specific node.

    Args:
        node_name (str): Name of the node

    Returns:
        list: List of block device information

    Raises:
        CommandFailed: If unable to list devices
    """
    logger.info(f"Listing block devices on node {node_name}...")
    try:
        cmd = (
            f"oc debug -q node/{node_name} -- chroot /host "
            f"lsblk -ndo NAME,PATH,SIZE,ROTA,TYPE,FSTYPE"
        )
        result = exec_cmd(cmd, shell=True)
        devices_output = result.stdout.decode().strip().split("\n")

        devices = []
        for line in devices_output:
            parts = line.split()
            if len(parts) >= 5:
                devices.append(
                    {
                        "name": parts[0],
                        "path": parts[1],
                        "size": parts[2],
                        "rota": parts[3],
                        "type": parts[4],
                        "fstype": parts[5] if len(parts) > 5 else "",
                    }
                )

        logger.info(f"Found {len(devices)} block devices on {node_name}")
        return devices

    except Exception as e:
        logger.error(f"Failed to list block devices on {node_name}: {e}")
        raise


def _parse_size_gb(size_str):
    """
    Parse lsblk size string to GB.

    Args:
        size_str (str): Size string (e.g., "500G", "1T", "20G")

    Returns:
        float: Size in GB
    """
    if not size_str:
        return 0.0
    size_str = size_str.strip().upper()
    multipliers = {"K": 1 / (1024 * 1024), "M": 1 / 1024, "G": 1, "T": 1024}
    for unit, multiplier in multipliers.items():
        if unit in size_str:
            try:
                return float(size_str.replace(unit, "").strip()) * multiplier
            except ValueError:
                return 0.0
    return 0.0


def discover_available_disks(node_info):
    """
    Discover available (unused) disks on TNF nodes for monitor and OSD use.

    Filters out:
    - OS disks (have partitions)
    - Loop devices, CD-ROM (sr*), and other non-disk types
    - Disks with existing filesystems

    Args:
        node_info (list): List of node info dicts [{'name': str, 'ip': str}, ...]

    Returns:
        dict: Per-node disk info {'node-name': {'all': [device_dicts]}}
    """
    logger.info("Discovering available disks on TNF nodes...")
    result = {}

    for node in node_info:
        node_name = node["name"]

        # lsblk WITHOUT -d to include partitions for parent disk detection
        cmd = (
            f"oc debug -q node/{node_name} -- chroot /host "
            f"lsblk -no NAME,PATH,SIZE,ROTA,TYPE,FSTYPE"
        )
        lsblk_result = exec_cmd(cmd, shell=True)
        lines = lsblk_result.stdout.decode().strip().split("\n")

        all_entries = []
        for line in lines:
            parts = line.split()
            if len(parts) >= 5:
                name = parts[0].lstrip("│├└─ ").lstrip("|-`- ")
                all_entries.append(
                    {
                        "name": name,
                        "path": parts[1],
                        "size": parts[2],
                        "rota": parts[3],
                        "type": parts[4],
                        "fstype": parts[5] if len(parts) > 5 else "",
                    }
                )

        # Find parent disks that have partitions (OS/boot disks)
        partitioned_disks = set()
        for entry in all_entries:
            if entry["type"] == "part":
                parent = entry["name"].rstrip("0123456789")
                partitioned_disks.add(parent)

        # Filter to only whole, unpartitioned disks without filesystems
        unused_disks = []
        for entry in all_entries:
            if entry["type"] != "disk":
                continue
            if entry["name"] in partitioned_disks:
                continue
            if entry["fstype"]:
                continue
            unused_disks.append(entry)

        result[node_name] = {"all": unused_disks}

        logger.info(f"Node {node_name}: {len(unused_disks)} unused disk(s)")
        for d in unused_disks:
            logger.info(
                f"  {d['path']}: {d['size']} " f"(ROTA={d['rota']}, TYPE={d['type']})"
            )

    return result


def validate_tnf_prerequisites():
    """
    Validate all prerequisites for TNF deployment.

    Checks cluster topology, node count, network connectivity,
    and storage requirements.

    Returns:
        dict: Validation results

    Raises:
        UnexpectedDeploymentConfiguration: If critical validations fail
    """
    logger.info("Validating TNF deployment prerequisites...")
    validation_results = {
        "topology": False,
        "node_count": False,
        "storage": False,
        "network": False,
        "errors": [],
    }

    try:
        if verify_tnf_cluster_topology():
            validation_results["topology"] = True
        else:
            validation_results["errors"].append("Cluster topology is not DualReplica.")
    except Exception as e:
        validation_results["errors"].append(f"Failed to verify topology: {e}")

    try:
        node_info = get_tnf_node_info()
        if len(node_info) == 2:
            validation_results["node_count"] = True
            validation_results["nodes"] = node_info
        else:
            validation_results["errors"].append(
                f"Expected 2 nodes, found {len(node_info)}"
            )
    except Exception as e:
        validation_results["errors"].append(f"Failed to get node info: {e}")

    if validation_results["node_count"]:
        try:
            network_ok = True
            for i, node in enumerate(validation_results["nodes"]):
                peer = validation_results["nodes"][1 - i]
                if not verify_port_connectivity(
                    node["name"], peer["ip"], constants.TNF_DRBD_PORT
                ):
                    network_ok = False
                    validation_results["errors"].append(
                        f"Port {constants.TNF_DRBD_PORT} not reachable "
                        f"from {node['name']} to {peer['ip']}"
                    )
            if network_ok:
                validation_results["network"] = True
        except Exception as e:
            validation_results["errors"].append(f"Network validation failed: {e}")

    if validation_results["errors"]:
        raise UnexpectedDeploymentConfiguration(
            f"TNF validation failed: {validation_results['errors']}"
        )

    return validation_results
