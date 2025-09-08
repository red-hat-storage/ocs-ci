import logging
import json

from ocs_ci.ocs import constants
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def get_pod_resource_details(pod_obj):
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


def get_pods_resources_details(pod_objs):
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
        pod_resource_list = get_pod_resource_details(pod_obj)
        # Store the list of container resources with the pod's name as the key.
        pods_resources_details[pod_obj.name] = pod_resource_list
    return pods_resources_details


def get_pods_resources_from_json(file_path=constants.ODF_RESOURCES_REQUESTS_AND_LIMITS):
    """
    Parses a JSON file (in a dictionary of dictionaries formats) to extract resource
    requests and limits.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: A dictionary where keys are pod prefix names and values are a list of dictionaries with
            container resource details. Returns an empty dictionary if the file is not found or is invalid.

    """
    pod_resources_from_json = config.RUN.get("pods_resources_from_json", None)
    if pod_resources_from_json:
        return pod_resources_from_json

    try:
        with open(file_path, "r") as f:
            # Load the entire JSON file into a dictionary
            pods_data = json.load(f)
            # Access the nested dictionary with resource details
            pod_resources_from_json = pods_data.get("pod-resources-details", {})
            config.RUN["pods_resources_from_json"] = pod_resources_from_json
            return pod_resources_from_json

    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Error processing JSON file at '{file_path}'. Error: {e}")
        return {}


def get_expected_pod_resources_details(pod_name):
    """
    Finds the expected resource details for a pod by matching its name to a prefix
    in the JSON reference data.

    Args:
        pod_name (str): The full name of the pod from the live cluster.

    Returns:
        list: The list of dictionaries with expected container resource details for the pod,
            or None if no matching prefix is found.

    """
    # Get the dictionary of expected resources from the JSON file
    expected_pods_resources_details = get_pods_resources_from_json()
    pod_name_prefixes = list(expected_pods_resources_details.keys())

    pod_prefix = None
    # Iterate through prefixes to find a match for the given pod name
    for prefix in pod_name_prefixes:
        if pod_name.startswith(prefix):
            pod_prefix = prefix
            break

    if not pod_prefix:
        logger.warning(
            f"Didn't find a matching pod name prefix for '{pod_name}' in the expected data."
        )
        return None

    return expected_pods_resources_details.get(pod_prefix)


def check_pod_resources_details(pod_name, pod_resources_details):
    """
    Compares the live resource details of a single pod against the expected values from the
    JSON reference data.

    Args:
        pod_name (str): The full name of the pod from the live cluster.
        pod_resources_details (list): The list of dictionaries with container resource details for
            the live pod.

    Returns:
        bool: True if all resource values for all containers match, False otherwise.

    """
    # Get the expected resource details for the pod using its name
    expected_pod_resources_details = get_expected_pod_resources_details(pod_name)

    # Check if a matching pod prefix exists in the expected data
    if not expected_pod_resources_details:
        logger.warning(f"No expected resource details found for pod '{pod_name}'.")
        return False

    # Check if the number of containers is identical
    if len(pod_resources_details) != len(expected_pod_resources_details):
        logger.warning(
            f"The number of containers for pod '{pod_name}' does not match the expected count. "
            f"Found: {len(pod_resources_details)}, Expected: {len(expected_pod_resources_details)}."
        )
        return False

    # Convert the lists of container resources into dictionaries for easy key-based comparison
    pod_container_dict = {item["container"]: item for item in pod_resources_details}
    expected_container_dict = {
        item["container"]: item for item in expected_pod_resources_details
    }

    mismatches_found = False

    # Compare each container's resource values
    for container_name, container_details in pod_container_dict.items():
        expected_details = expected_container_dict.get(container_name)
        if not expected_details:
            logger.warning(
                f"Container '{container_name}' not found in expected data for pod '{pod_name}'. "
                f"Skipping comparison for this container."
            )
            mismatches_found = True
            continue

        # Log the full live and expected container data for debugging
        logger.info(f"Live data for '{pod_name}/{container_name}': {container_details}")
        logger.info(
            f"Expected data for '{pod_name}/{container_name}': {expected_details}"
        )

        for key in ["cpu_requests", "memory_requests", "cpu_limits", "memory_limits"]:
            if container_details.get(key) != expected_details.get(key):
                logger.warning(
                    f"Resource mismatch for '{pod_name}/{container_name}' on key '{key}'. "
                    f"Found: '{container_details.get(key)}', Expected: '{expected_details.get(key)}'."
                )
                mismatches_found = True

    return not mismatches_found


def check_odf_resources_requests_and_limits(pods_resources_details_dict):
    """
    Iterates through a dictionary of live pod resource details and checks them against
    the expected values.

    Args:
        pods_resources_details_dict (dict): A dictionary where keys are pod names and values are
            lists of dictionaries with container resource details.

    Returns:
        bool: True if no resource mismatches were found across all pods, False otherwise.

    """
    mismatches_found = False
    for pod_name, pod_resources_details in pods_resources_details_dict.items():
        # The check_pod_resources_details function now returns True/False
        if not check_pod_resources_details(pod_name, pod_resources_details):
            mismatches_found = True

    return not mismatches_found
