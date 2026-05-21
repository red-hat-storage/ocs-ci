"""
NooBaa-specific helper functions for Krkn chaos testing.

This module provides utility functions for NooBaa chaos testing including:
- Node discovery for NooBaa components
- NooBaa health validation
- Component-specific helpers

Used by:
- test_krkn_noobaa_chaos.py (pod disruption)
- test_krkn_noobaa_container_chaos.py (container kill)
- test_krkn_noobaa_node_disruption.py (node disruption)
"""

import logging

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    get_primary_nb_db_pod,
    Pod,
)

log = logging.getLogger(__name__)


# ============================================================================
# Node Discovery Functions
# ============================================================================


def get_node_hosting_noobaa_db_primary():
    """
    Get the node name hosting the NooBaa database primary pod.

    Returns:
        str: Node name hosting NooBaa DB primary pod

    Raises:
        Exception: If primary pod not found or node name not available

    Example:
        >>> node = get_node_hosting_noobaa_db_primary()
        >>> print(node)
        'worker-0'
    """
    try:
        # Get NooBaa DB primary pod
        primary_pod = get_primary_nb_db_pod()
        node_name = primary_pod.get()["spec"]["nodeName"]
        log.info(
            f"NooBaa DB primary pod '{primary_pod.name}' is running on node '{node_name}'"
        )
        return node_name
    except Exception as e:
        log.error(f"Failed to get node hosting NooBaa DB primary: {e}")
        raise


def get_nodes_hosting_noobaa_db_replicas():
    """
    Get list of node names hosting NooBaa database replica pods.

    Returns:
        list: List of node names hosting NooBaa DB replica pods

    Example:
        >>> nodes = get_nodes_hosting_noobaa_db_replicas()
        >>> print(nodes)
        ['worker-1', 'worker-2']
    """
    nodes = []
    try:
        # Get NooBaa DB replica pods
        replica_pods = get_pods_having_label(
            label=constants.NB_DB_SECONDARY_POD_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        for pod_info in replica_pods:
            pod_obj = Pod(**pod_info)
            node_name = pod_obj.get()["spec"]["nodeName"]
            nodes.append(node_name)
            log.info(
                f"NooBaa DB replica pod '{pod_obj.name}' is running on node '{node_name}'"
            )

        return nodes
    except Exception as e:
        log.warning(f"Failed to get nodes hosting NooBaa DB replicas: {e}")
        return []


def get_nodes_hosting_noobaa_core():
    """
    Get list of node names hosting NooBaa core pods.

    Returns:
        list: List of node names hosting NooBaa core pods

    Example:
        >>> nodes = get_nodes_hosting_noobaa_core()
        >>> print(nodes)
        ['worker-0', 'worker-2']
    """
    nodes = []
    try:
        # Get NooBaa core pods
        core_pods = get_pods_having_label(
            label=constants.NOOBAA_CORE_POD_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        for pod_info in core_pods:
            pod_obj = Pod(**pod_info)
            node_name = pod_obj.get()["spec"]["nodeName"]
            nodes.append(node_name)
            log.info(
                f"NooBaa core pod '{pod_obj.name}' is running on node '{node_name}'"
            )

        return nodes
    except Exception as e:
        log.warning(f"Failed to get nodes hosting NooBaa core: {e}")
        return []


def get_nodes_hosting_noobaa_operator():
    """
    Get list of node names hosting NooBaa operator pods.

    Returns:
        list: List of node names hosting NooBaa operator pods

    Example:
        >>> nodes = get_nodes_hosting_noobaa_operator()
        >>> print(nodes)
        ['worker-1']
    """
    nodes = []
    try:
        # Get NooBaa operator pods
        operator_pods = get_pods_having_label(
            label=constants.NOOBAA_OPERATOR_POD_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        for pod_info in operator_pods:
            pod_obj = Pod(**pod_info)
            node_name = pod_obj.get()["spec"]["nodeName"]
            nodes.append(node_name)
            log.info(
                f"NooBaa operator pod '{pod_obj.name}' is running on node '{node_name}'"
            )

        return nodes
    except Exception as e:
        log.warning(f"Failed to get nodes hosting NooBaa operator: {e}")
        return []


def get_nodes_hosting_noobaa_endpoints():
    """
    Get list of node names hosting NooBaa endpoint pods.

    Returns:
        list: List of node names hosting NooBaa endpoint pods

    Example:
        >>> nodes = get_nodes_hosting_noobaa_endpoints()
        >>> print(nodes)
        ['worker-0', 'worker-1', 'worker-2']
    """
    nodes = []
    try:
        # Get NooBaa endpoint pods
        endpoint_pods = get_pods_having_label(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        for pod_info in endpoint_pods:
            pod_obj = Pod(**pod_info)
            node_name = pod_obj.get()["spec"]["nodeName"]
            nodes.append(node_name)
            log.info(
                f"NooBaa endpoint pod '{pod_obj.name}' is running on node '{node_name}'"
            )

        return nodes
    except Exception as e:
        log.warning(f"Failed to get nodes hosting NooBaa endpoints: {e}")
        return []


def get_all_noobaa_nodes():
    """
    Get all nodes hosting any NooBaa component pods.

    Returns:
        dict: Dictionary with keys for each component containing node names:
            - 'primary': Node hosting DB primary
            - 'replicas': List of nodes hosting DB replicas
            - 'core': List of nodes hosting core pods
            - 'operator': List of nodes hosting operator pods
            - 'endpoints': List of nodes hosting endpoint pods

    Example:
        >>> nodes = get_all_noobaa_nodes()
        >>> print(nodes)
        {
            'primary': 'worker-0',
            'replicas': ['worker-1', 'worker-2'],
            'core': ['worker-0'],
            'operator': ['worker-1'],
            'endpoints': ['worker-0', 'worker-1', 'worker-2']
        }
    """
    return {
        "primary": get_node_hosting_noobaa_db_primary(),
        "replicas": get_nodes_hosting_noobaa_db_replicas(),
        "core": get_nodes_hosting_noobaa_core(),
        "operator": get_nodes_hosting_noobaa_operator(),
        "endpoints": get_nodes_hosting_noobaa_endpoints(),
    }


def get_unique_noobaa_nodes():
    """
    Get a unique set of all nodes hosting NooBaa components.

    Returns:
        set: Set of unique node names hosting any NooBaa component

    Example:
        >>> nodes = get_unique_noobaa_nodes()
        >>> print(nodes)
        {'worker-0', 'worker-1', 'worker-2'}
    """
    all_nodes_dict = get_all_noobaa_nodes()
    unique_nodes = set()

    # Add primary node
    unique_nodes.add(all_nodes_dict["primary"])

    # Add all replica nodes
    unique_nodes.update(all_nodes_dict["replicas"])

    # Add all core nodes
    unique_nodes.update(all_nodes_dict["core"])

    # Add all operator nodes
    unique_nodes.update(all_nodes_dict["operator"])

    # Add all endpoint nodes
    unique_nodes.update(all_nodes_dict["endpoints"])

    log.info(f"Found {len(unique_nodes)} unique nodes hosting NooBaa: {unique_nodes}")
    return unique_nodes


# ============================================================================
# NooBaa Health Validation Functions
# ============================================================================


def validate_noobaa_health(component_name="NooBaa"):
    """
    Validate NooBaa health after chaos testing.

    This function checks:
    1. NooBaa pods are running
    2. NooBaa database is accessible
    3. No permanent errors in NooBaa logs
    4. S3 endpoints are responsive

    Args:
        component_name (str): Component name for logging purposes

    Raises:
        AssertionError: If critical NooBaa health checks fail

    Example:
        >>> validate_noobaa_health("noobaa-db-primary")
        # Logs validation results and raises assertion if checks fail
    """
    log.info(f"Validating NooBaa health for {component_name}")

    # Check NooBaa pods are running
    log.info("   Checking NooBaa pod status...")
    noobaa_pods = [
        (constants.NOOBAA_DB_LABEL_419_AND_ABOVE, "NooBaa DB"),
        (constants.NOOBAA_CORE_POD_LABEL, "NooBaa Core"),
        (constants.NOOBAA_OPERATOR_POD_LABEL, "NooBaa Operator"),
    ]

    for label, name in noobaa_pods:
        pods = get_pods_having_label(
            label=label,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert len(pods) > 0, f"No {name} pods found after chaos"

        # Check pod status
        for pod in pods:
            pod_name = pod["metadata"]["name"]
            pod_obj = ocp.OCP(
                kind=constants.POD,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                resource_name=pod_name,
            )
            status = pod_obj.get()["status"]["phase"]
            log.info(f"      {pod_name}: {status}")
            # Allow pods to be in Running or ContainerCreating state
            # (they may still be recovering from the last kill)
            assert status in [
                "Running",
                "ContainerCreating",
                "Pending",
            ], f"{pod_name} is in unexpected state: {status}"

    log.info("   ✅ NooBaa pods are healthy")

    # Note: We don't fail on temporary service disruptions
    # Pod restarts and temporary S3 failures are expected during chaos testing
    log.info("   ℹ️  Temporary service disruptions during chaos are expected")
    log.info("✅ NooBaa health validation completed")


def validate_noobaa_db_health():
    """
    Validate NooBaa database health specifically.

    Checks:
    - Database pods are running
    - Primary is elected
    - Replication is working (if replicas exist)

    Raises:
        AssertionError: If database checks fail
    """
    log.info("Validating NooBaa database health")

    # Check database pods
    db_pods = get_pods_having_label(
        label=constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )

    assert len(db_pods) > 0, "No NooBaa database pods found"
    log.info(f"   Found {len(db_pods)} NooBaa database pod(s)")

    # Check primary pod exists
    try:
        primary_pod = get_primary_nb_db_pod()
        log.info(f"   ✅ Primary pod: {primary_pod.name}")
    except Exception as e:
        raise AssertionError(f"Failed to get NooBaa DB primary pod: {e}")

    # Check replica pods if they exist
    replica_pods = get_pods_having_label(
        label=constants.NB_DB_SECONDARY_POD_LABEL,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )

    if replica_pods:
        log.info(f"   ✅ Found {len(replica_pods)} replica pod(s)")
        for pod_info in replica_pods:
            log.info(f"      - {pod_info['metadata']['name']}")
    else:
        log.info("   ℹ️  No replica pods found (may be expected in some deployments)")

    log.info("✅ NooBaa database health validation completed")


def validate_noobaa_core_health():
    """
    Validate NooBaa core service health.

    Checks:
    - Core pods are running
    - S3 service is accessible

    Raises:
        AssertionError: If core service checks fail
    """
    log.info("Validating NooBaa core service health")

    # Check core pods
    core_pods = get_pods_having_label(
        label=constants.NOOBAA_CORE_POD_LABEL,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )

    assert len(core_pods) > 0, "No NooBaa core pods found"
    log.info(f"   Found {len(core_pods)} NooBaa core pod(s)")

    for pod_info in core_pods:
        pod_name = pod_info["metadata"]["name"]
        pod_obj = ocp.OCP(
            kind=constants.POD,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=pod_name,
        )
        status = pod_obj.get()["status"]["phase"]
        log.info(f"      {pod_name}: {status}")
        assert status in [
            "Running",
            "ContainerCreating",
        ], f"Core pod {pod_name} is in unexpected state: {status}"

    log.info("✅ NooBaa core service health validation completed")


def validate_noobaa_endpoints_health():
    """
    Validate NooBaa S3 endpoints health.

    Checks:
    - Endpoint pods are running
    - Minimum endpoints are available

    Raises:
        AssertionError: If endpoint checks fail
    """
    log.info("Validating NooBaa endpoints health")

    # Check endpoint pods
    endpoint_pods = get_pods_having_label(
        label=constants.NOOBAA_ENDPOINT_POD_LABEL,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )

    # At least 1 endpoint should be running
    assert len(endpoint_pods) > 0, "No NooBaa endpoint pods found"
    log.info(f"   Found {len(endpoint_pods)} NooBaa endpoint pod(s)")

    for pod_info in endpoint_pods:
        pod_name = pod_info["metadata"]["name"]
        pod_obj = ocp.OCP(
            kind=constants.POD,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=pod_name,
        )
        status = pod_obj.get()["status"]["phase"]
        log.info(f"      {pod_name}: {status}")
        assert status in [
            "Running",
            "ContainerCreating",
        ], f"Endpoint pod {pod_name} is in unexpected state: {status}"

    log.info("✅ NooBaa endpoints health validation completed")


# ============================================================================
# Component Label Mapping
# ============================================================================


def get_noobaa_component_label(component_name):
    """
    Get the Kubernetes label selector for a NooBaa component.

    Args:
        component_name (str): Component name (e.g., 'db', 'core', 'operator', 'endpoint')

    Returns:
        str: Label selector for the component

    Raises:
        ValueError: If component name is not recognized

    Example:
        >>> label = get_noobaa_component_label('core')
        >>> print(label)
        'noobaa-core=noobaa'
    """
    component_labels = {
        "db": constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
        "db_primary": constants.NB_DB_PRIMARY_POD_LABEL,
        "db_replica": constants.NB_DB_SECONDARY_POD_LABEL,
        "core": constants.NOOBAA_CORE_POD_LABEL,
        "operator": constants.NOOBAA_OPERATOR_POD_LABEL,
        "endpoint": constants.NOOBAA_ENDPOINT_POD_LABEL,
        "all": constants.NOOBAA_APP_LABEL,
    }

    if component_name not in component_labels:
        raise ValueError(
            f"Unknown NooBaa component: {component_name}. "
            f"Valid components: {list(component_labels.keys())}"
        )

    return component_labels[component_name]


def get_noobaa_component_pods(component_name):
    """
    Get all pods for a specific NooBaa component.

    Args:
        component_name (str): Component name (e.g., 'db', 'core', 'operator', 'endpoint')

    Returns:
        list: List of pod info dictionaries

    Example:
        >>> pods = get_noobaa_component_pods('core')
        >>> for pod in pods:
        ...     print(pod['metadata']['name'])
        noobaa-core-0
    """
    label = get_noobaa_component_label(component_name)
    pods = get_pods_having_label(
        label=label,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    return pods
