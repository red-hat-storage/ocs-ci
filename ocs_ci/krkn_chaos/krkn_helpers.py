import yaml
import os
import logging
from ocs_ci.ocs.constants import (
    KRKN_CHAOS_DIR,
)
from ocs_ci.ocs import ocp
from ocs_ci.ocs.node import get_worker_nodes, get_master_nodes
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


def krkn_scenarios_list():
    """
    Load the hog_scenarios YAML configuration into a Python dictionary.

    Returns:
        dict: Parsed hog_scenarios content
    """
    config_path = os.path.join(KRKN_CHAOS_DIR, "config", "chaos_scenarios_list.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Scenario YAML not found at {config_path}")

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    return data


def get_default_network_interfaces(node_type="worker"):
    """
    Get all active network interfaces from nodes in the cluster.

    Args:
        node_type (str): Type of nodes to query - "worker", "master", or "all"

    Returns:
        list: List of unique network interface names found across the specified nodes
              (e.g., ['eth0', 'enp1s0', 'ens3'])
    """
    try:
        # Get nodes based on type
        if node_type == "worker":
            nodes = get_worker_nodes()
        elif node_type == "master":
            nodes = get_master_nodes()
        elif node_type == "all":
            nodes = get_worker_nodes() + get_master_nodes()
        else:
            raise ValueError(
                f"Invalid node_type: {node_type}. Must be 'worker', 'master', or 'all'"
            )

        if not nodes:
            log.warning(f"No {node_type} nodes found, falling back to ['eth0']")
            return ["eth0"]

        ocp_obj = ocp.OCP()
        all_interfaces = set()

        # Command to get all active network interfaces
        # This gets interfaces that are UP (both with and without IP addresses)
        cmd = "ip -o link show | awk '/state UP/ {print $2}' | sed 's/:$//' | sort -u"

        log.info(f"Getting network interfaces from {len(nodes)} {node_type} node(s)")

        for node_name in nodes:
            try:
                log.debug(f"Querying network interfaces from node: {node_name}")
                output = ocp_obj.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd])

                interfaces = [
                    iface.strip() for iface in output.splitlines() if iface.strip()
                ]
                if interfaces:
                    all_interfaces.update(interfaces)
                    log.debug(f"Found interfaces on {node_name}: {interfaces}")
                else:
                    log.warning(f"No active interfaces found on node {node_name}")

            except (CommandFailed, Exception) as e:
                log.error(f"Error retrieving interfaces from node {node_name}: {e}")
                continue

        # Convert to sorted list and filter out loopback and virtual interfaces
        interface_list = sorted(list(all_interfaces))

        # Filter out common virtual/unwanted interfaces
        filtered_interfaces = [
            iface
            for iface in interface_list
            if not any(
                pattern in iface.lower()
                for pattern in [
                    "lo",
                    "docker",
                    "br-",
                    "veth",
                    "flannel",
                    "cni",
                    "ovn-k8s",
                    "@if",  # Filter out virtual interfaces with @if suffix
                    "genev_sys",  # Filter out Geneve system interfaces
                    "ovs-system",  # Filter out OVS system interfaces
                ]
            )
        ]

        if filtered_interfaces:
            log.info(f"Found active network interfaces: {filtered_interfaces}")
            return filtered_interfaces
        else:
            log.warning(
                "No suitable network interfaces found, falling back to ['eth0']"
            )
            return ["eth0"]

    except Exception as e:
        log.error(f"Error getting network interfaces for {node_type} nodes: {e}")
        log.warning("Falling back to ['eth0']")
        return ["eth0"]


def get_ceph_service_ports(service_type="all", namespace="openshift-storage"):
    """
    Dynamically discover actual ports used by Ceph services in the cluster.

    Args:
        service_type (str): Type of service to get ports for:
                           "mon", "mgr", "osd", "rgw", "mds", "all"
        namespace (str): Namespace where Ceph services are running

    Returns:
        dict: Dictionary mapping service types to their actual ports
              e.g., {"mon": [3300], "mgr": [9283], "rgw": [80, 443]}
    """
    from ocs_ci.ocs import ocp
    from ocs_ci.ocs.exceptions import CommandFailed

    log = logging.getLogger(__name__)

    try:
        ocp_obj = ocp.OCP(kind="service", namespace=namespace)
        services = ocp_obj.get()["items"]

        service_ports = {
            "mon": [],
            "mgr": [],
            "osd": [],
            "rgw": [],
            "mds": [],
            "noobaa": [],
            "metrics": [],
        }

        for service in services:
            service_name = service["metadata"]["name"]
            ports = []

            # Extract ports from service spec
            if "spec" in service and "ports" in service["spec"]:
                for port_spec in service["spec"]["ports"]:
                    if "port" in port_spec:
                        ports.append(port_spec["port"])

            # Categorize services by type based on name patterns
            if "rook-ceph-mon" in service_name:
                service_ports["mon"].extend(ports)
                log.debug(f"Found MON service {service_name} with ports: {ports}")
            elif "rook-ceph-mgr" in service_name:
                service_ports["mgr"].extend(ports)
                log.debug(f"Found MGR service {service_name} with ports: {ports}")
            elif "rook-ceph-osd" in service_name:
                service_ports["osd"].extend(ports)
                log.debug(f"Found OSD service {service_name} with ports: {ports}")
            elif "rook-ceph-rgw" in service_name:
                service_ports["rgw"].extend(ports)
                log.debug(f"Found RGW service {service_name} with ports: {ports}")
            elif "rook-ceph-mds" in service_name:
                service_ports["mds"].extend(ports)
                log.debug(f"Found MDS service {service_name} with ports: {ports}")
            elif "noobaa" in service_name:
                service_ports["noobaa"].extend(ports)
                log.debug(f"Found NooBaa service {service_name} with ports: {ports}")
            elif any(keyword in service_name for keyword in ["metrics", "exporter"]):
                service_ports["metrics"].extend(ports)
                log.debug(f"Found metrics service {service_name} with ports: {ports}")

        # Remove duplicates and sort
        for svc_type in service_ports:
            service_ports[svc_type] = sorted(list(set(service_ports[svc_type])))

        log.info(f"Discovered Ceph service ports: {service_ports}")

        # Return specific service type or all
        if service_type == "all":
            return service_ports
        elif service_type in service_ports:
            return {service_type: service_ports[service_type]}
        else:
            log.warning(f"Unknown service type: {service_type}")
            return {}

    except (CommandFailed, Exception) as e:
        log.error(f"Error discovering Ceph service ports: {e}")
        # Return fallback default ports
        fallback_ports = {
            "mon": [3300],
            "mgr": [9283],
            "osd": [6800, 6801, 6802, 6803],
            "rgw": [80, 443],
            "mds": [6800],
            "noobaa": [80, 443, 8445, 8446],
            "metrics": [8443, 9443, 9926],
        }

        if service_type == "all":
            return fallback_ports
        elif service_type in fallback_ports:
            return {service_type: fallback_ports[service_type]}
        else:
            return {}


def get_pod_container_ports(label_selector, namespace="openshift-storage"):
    """
    Get actual container ports from running pods based on label selector.

    This discovers ports that pods are actually listening on, which may
    include additional ports not exposed via services.

    Args:
        label_selector (str): Kubernetes label selector (e.g., "app=rook-ceph-mon")
        namespace (str): Namespace to search in

    Returns:
        list: List of unique ports found across matching pods
    """
    from ocs_ci.ocs import ocp
    from ocs_ci.ocs.exceptions import CommandFailed

    log = logging.getLogger(__name__)

    try:
        ocp_obj = ocp.OCP(kind="pod", namespace=namespace)
        pods = ocp_obj.get(selector=label_selector)["items"]

        all_ports = set()

        for pod in pods:
            pod_name = pod["metadata"]["name"]

            # Get ports from container specs
            if "spec" in pod and "containers" in pod["spec"]:
                for container in pod["spec"]["containers"]:
                    if "ports" in container:
                        for port_spec in container["ports"]:
                            if "containerPort" in port_spec:
                                all_ports.add(port_spec["containerPort"])
                                log.debug(
                                    f"Found container port {port_spec['containerPort']} in pod {pod_name}"
                                )

        ports_list = sorted(list(all_ports))
        log.info(f"Discovered container ports for {label_selector}: {ports_list}")
        return ports_list

    except (CommandFailed, Exception) as e:
        log.error(f"Error getting container ports for {label_selector}: {e}")
        return []


def get_dynamic_port_ranges():
    """
    Get comprehensive port information for all Ceph services dynamically.

    Returns:
        dict: Complete port mapping with both service and container ports
    """
    log = logging.getLogger(__name__)

    log.info("Discovering dynamic port ranges for Ceph services...")

    # Get service ports
    service_ports = get_ceph_service_ports("all")

    # Get additional container ports for key components
    from ocs_ci.ocs.constants import (
        MON_APP_LABEL,
        MGR_APP_LABEL,
        OSD_APP_LABEL,
        RGW_APP_LABEL,
    )

    container_ports = {}
    for component, label in [
        ("mon", MON_APP_LABEL),
        ("mgr", MGR_APP_LABEL),
        ("osd", OSD_APP_LABEL),
        ("rgw", RGW_APP_LABEL),
    ]:
        container_ports[component] = get_pod_container_ports(label)

    # Merge service and container ports
    dynamic_ports = {}
    for component in service_ports:
        all_component_ports = set(service_ports.get(component, []))
        all_component_ports.update(container_ports.get(component, []))
        dynamic_ports[component] = sorted(list(all_component_ports))

    log.info(f"Final dynamic port mapping: {dynamic_ports}")
    return dynamic_ports
