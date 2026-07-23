"""
Helper functions for Two-Node Failover (TNF) cluster deployment
"""

import logging
import tempfile

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.templating import Templating
from ocs_ci.utility.utils import exec_cmd

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
        sc_data = Templating.render_template(
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
            pv_data = Templating.render_template(
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
    Configure DRBD for the floating monitor using ODF operator's script.

    This function follows the Red Hat ODF 4.22 documentation:
    https://docs.redhat.com/en/documentation/red_hat_openshift_data_foundation/4.22/html-single/deploying_openshift_data_foundation_on_two-node_clusters/index

    The DRBD setup script is provided by the ODF operator in a ConfigMap.

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

    # First, ensure openshift-storage namespace exists
    try:
        exec_cmd(
            f"oc create namespace {constants.OPENSHIFT_STORAGE_NAMESPACE}", shell=True
        )
    except CommandFailed as e:
        if "already exists" in str(e):
            logger.info("openshift-storage namespace already exists")
        else:
            raise

    # Get DRBD setup script from ConfigMap (provided by ODF operator)
    try:
        logger.info("Retrieving DRBD setup script from ODF operator ConfigMap...")

        # Extract script from ConfigMap as per ODF documentation
        script_cmd = (
            f"oc get configmap {constants.TNF_DRBD_SETUP_SCRIPT_CM} "
            f"-n {constants.OPENSHIFT_STORAGE_NAMESPACE} "
            f"-o jsonpath='{{.data.script}}' | base64 -d"
        )
        drbd_script = exec_cmd(script_cmd, shell=True)

        # Write script to temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sh", delete=False
        ) as temp_file:
            temp_file.write(drbd_script)
            temp_file_path = temp_file.name

        # Make script executable
        exec_cmd(f"chmod +x {temp_file_path}")

        # Execute DRBD setup script with required parameters
        logger.info("Executing DRBD setup script...")
        exec_cmd(
            f"{temp_file_path} "
            f"{node_0_info['name']} {node_0_info['ip']} {monitor_disk_node_0} "
            f"{node_1_info['name']} {node_1_info['ip']} {monitor_disk_node_1}"
        )

        logger.info("DRBD configuration completed successfully")
        return True

    except CommandFailed as e:
        logger.error(
            f"Failed to retrieve or execute DRBD setup script. "
            f"Ensure ODF operator is installed and ConfigMap "
            f"'{constants.TNF_DRBD_SETUP_SCRIPT_CM}' exists: {e}"
        )
        raise


def verify_drbd_configuration():
    """
    Verify DRBD configuration is correct.

    Returns:
        bool: True if DRBD is configured correctly

    Raises:
        CommandFailed: If DRBD ConfigMap not found
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
            "NODE_0_NAME",
            "NODE_0_IP",
            "NODE_1_NAME",
            "NODE_1_IP",
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

    Args:
        node_name (str): Name of the node to check

    Returns:
        bool: True if DRBD is running correctly on the node

    Raises:
        CommandFailed: If unable to check DRBD status
    """
    logger.info(f"Checking DRBD status on node {node_name}...")
    try:
        cmd = (
            f"oc debug -q node/{node_name} -- chroot /host podman run --rm --privileged "
            f"-v /dev:/dev -v {constants.TNF_DRBD_CONF_PATH}:{constants.TNF_DRBD_CONF_PATH} "
            f"-v {constants.TNF_DRBD_DIR_PATH}:{constants.TNF_DRBD_DIR_PATH} "
            f"--hostname {node_name} --net host {constants.TNF_DRBD_UTILS_IMAGE} "
            f"drbdadm -c {constants.TNF_DRBD_CONF_PATH} status {constants.TNF_DRBD_RESOURCE_NAME}"
        )

        drbd_result = exec_cmd(cmd, shell=True)
        logger.info(f"DRBD status on {node_name}: {drbd_result.stdout.decode()}")
        return True

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

    Raises:
        CommandFailed: If connectivity check fails
    """
    logger.info(
        f"Verifying port {port} connectivity from {source_node} to {target_ip}..."
    )
    try:
        cmd = (
            f"oc debug -q node/{source_node} -- chroot /host "
            f"nc -zv {target_ip} {port}"
        )
        exec_cmd(cmd, shell=True, timeout=10)
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
