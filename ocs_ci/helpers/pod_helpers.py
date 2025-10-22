import logging

logger = logging.getLogger(__name__)


def get_pod_container_resource_details(pod_obj):
    """
    Extracts resource requests and limits for all containers within a single pod.

    Args:
        pod_obj (Pod): A Pod object containing metadata and spec information.

    Returns:
        list: A list of dictionaries, where each dictionary represents a container and its resource
            requests/limits. Returns an empty list if no containers are found.

    """
    pod_data = pod_obj.get()
    containers = pod_data.get("spec", {}).get("containers", [])
    pod_resource_list = []

    # Iterate over all containers defined in the pod spec
    for container in containers:
        container_name = container.get("name", "N/A")
        resources = container.get("resources", {})
        requests = resources.get("requests", {})
        limits = resources.get("limits", {})

        pod_resource_list.append(
            {
                "container": container_name,
                "requests": {
                    "cpu": requests.get("cpu", "null"),
                    "memory": requests.get("memory", "null"),
                },
                "limits": {
                    "cpu": limits.get("cpu", "null"),
                    "memory": limits.get("memory", "null"),
                },
            }
        )

    return pod_resource_list


def get_all_pods_container_resource_details(pod_objs):
    """
    Extracts resource requests and limits from the pod objects.

    Args:
        pod_objs (list): A list of pod objects from a live cluster.

    Returns:
        dict: A dictionary where keys are pod prefix names and values are a list of dictionaries with
            container resource details. This structure is used to group containers by their parent
            pod prefix.

    """
    pods_resources_details = {}
    for pod_obj in pod_objs:
        pod_resource_list = get_pod_container_resource_details(pod_obj)
        # Store the list of container resources with the pod's name as the key.
        pods_resources_details[pod_obj.name] = pod_resource_list
    return pods_resources_details


def validate_pod_container_resources(pod_name, pod_resources_details):
    """
    Validate that all container resource values in a pod exist and start with a digit.

    Args:
        pod_name (str): Name of the pod.
        pod_resources_details (list): List of container resource details.

    Returns:
        dict: Dictionary with validation results.

        Examples::

            {
                "result": bool,              # True if all values are valid
                "invalid_values": dict       # Containers and fields with invalid values
            }

    """
    invalid_values = {}

    for container in pod_resources_details:
        cname = container.get("container", "<unknown>")
        requests = container.get("requests") or {}
        limits = container.get("limits") or {}

        logger.info(f"Live data for '{pod_name}/{cname}': {container}")

        checks = [
            ("cpu_requests", requests.get("cpu")),
            ("memory_requests", requests.get("memory")),
            ("cpu_limits", limits.get("cpu")),
            ("memory_limits", limits.get("memory")),
        ]

        for key, value in checks:
            # Invalid if None or does not start with a digit
            if value is None or not str(value).strip()[:1].isdigit():
                logger.warning(
                    f"Invalid or missing live value for '{pod_name}/{cname}' key '{key}': {value}"
                )
                invalid_values.setdefault(cname, {})[key] = value

    result = len(invalid_values) == 0
    return {"result": result, "invalid_values": invalid_values}


def validate_all_pods_container_resources(pods_resources_details_dict):
    """
    Validate all live pods to ensure their container resource values exist
    and start with a digit.

    Args:
        pods_resources_details_dict (dict): Mapping of pod names to container resource details.

    Returns:
        dict: Dictionary with validation results.

        Examples::

            {
                "result": bool,  # True if all pods have valid values
                "invalid_values": dict  # Pods with invalid or missing resource values
            }

    """

    all_ok = True
    invalid_values = {}

    for pod_name, pod_resources_details in pods_resources_details_dict.items():
        res = validate_pod_container_resources(pod_name, pod_resources_details)

        if not res.get("result", False):
            all_ok = False
            if res.get("invalid_values"):
                invalid_values[pod_name] = res["invalid_values"]

    return {"result": all_ok, "invalid_values": invalid_values}
