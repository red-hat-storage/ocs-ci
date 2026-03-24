import logging
import random
import time

from ocs_ci.ocs.constants import VOLUME_MODE_BLOCK
from ocs_ci.ocs.resources.pod import calculate_md5sum_of_pod_files

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


def run_io_on_pods(pods, pod_file_name, size="1G", runtime=30):
    """
    Helper function to run IO on the pods

    Args:
        pods (list): The list of pods for running the IO
        pod_file_name (str): The pod file name for fio
        size (str): Size in MB or Gi, e.g. '200M'.
            Default value is '1G'
        runtime (int): The number of seconds IO should run for

    """
    logger.info("Starting IO on all pods")
    for pod_obj in pods:
        storage_type = "block" if pod_obj.pvc.volume_mode == VOLUME_MODE_BLOCK else "fs"
        rate = f"{random.randint(1, 5)}M"
        pod_obj.run_io(
            storage_type=storage_type,
            size=size,
            runtime=runtime,
            rate=rate,
            fio_filename=pod_file_name,
            end_fsync=1,
        )
        logger.info("IO started on pod %s", pod_obj.name)
    logger.info("Started IO on all pods")


def run_io_on_small_groups_of_pods(
    pods,
    pod_file_name,
    size="1G",
    runtime=30,
    num_of_groups=3,
    do_md5sum=False,
    wait_between_groups=30,
):
    """
    Run IO on pods in smaller groups to avoid overwhelming
    the Ceph cluster with simultaneous writes.

    After each group starts, the function waits
    'wait_between_groups' seconds before starting the next
    group. If do_md5sum is True, it also waits for IO to
    complete and calculates md5sum per group.

    Args:
        pods (list): The list of pods for running the IO
        pod_file_name (str): The pod file name for fio
        size (str): Size in MB or Gi, e.g. '200M'.
            Default value is '1G'
        runtime (int): The number of seconds IO should
            run for
        num_of_groups (int): The number of groups to
            divide the pods into for running IO
        do_md5sum (bool): If True, wait for IO to
            complete and calculate md5sum per group
        wait_between_groups (int): Seconds to wait
            between starting each group

    """
    total_pods = len(pods)
    group_size = max(1, total_pods // num_of_groups)
    num_groups = (total_pods + group_size - 1) // group_size
    logger.info(
        "Dividing %d pods into %d groups of ~%d for running IO",
        total_pods,
        num_groups,
        group_size,
    )

    for i in range(0, total_pods, group_size):
        group_num = i // group_size + 1
        pod_group = pods[i : i + group_size]
        logger.info(
            "Running IO on pod group %d/%d (%d pods)",
            group_num,
            num_groups,
            len(pod_group),
        )
        run_io_on_pods(pod_group, pod_file_name, size=size, runtime=runtime)
        if do_md5sum:
            calculate_md5sum_of_pod_files(pod_group, pod_file_name)
        logger.info(
            "Waiting %d seconds between groups",
            wait_between_groups,
        )
        time.sleep(wait_between_groups)
