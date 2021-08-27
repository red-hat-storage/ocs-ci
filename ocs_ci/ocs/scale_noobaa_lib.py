import logging
import time
import re

from ocs_ci.helpers import helpers
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.utils import oc_get_all_obc_names
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources import pod

log = logging.getLogger(__name__)


def construct_obc_creation_yaml_bulk_for_kube_job(no_of_obc, sc_name, namespace):
    """
    Constructing obc.yaml file to create bulk of obc's using kube_job

    Args:
        no_of_obc(int): Bulk obc count
        sc_name (str): storage class name using for obc creation
        namespace(str): Namespace uses to create bulk of obc
    Returns:

         obc_dict_list (list): List of all obc.yaml dicts

    """

    # Construct obc.yaml for the no_of_obc count
    # append all the obc.yaml dict to obc_dict_list and return the list
    obc_dict_list = list()
    for i in range(no_of_obc):
        obc_name = helpers.create_unique_resource_name("test", "obc")
        obc_data = templating.load_yaml(constants.MCG_OBC_YAML)
        obc_data["metadata"]["name"] = obc_name
        obc_data["metadata"]["namespace"] = namespace
        obc_data["spec"]["storageClassName"] = sc_name
        obc_data["spec"]["bucketName"] = obc_name
        obc_dict_list.append(obc_data)

    return obc_dict_list


def check_all_obc_reached_bound_state_in_kube_job(
    kube_job_obj, namespace, no_of_obc, timeout=120, no_wait_time=20
):
    """
    Function to check either bulk created OBCs reached Bound state using kube_job

    Args:
        kube_job_obj (obj): Kube Job Object
        namespace (str): Namespace of OBC's created
        no_of_obc (int): Bulk OBC count
        timeout (second): a timeout for all the obc in kube job to reach bound state
        no_wait_time (int): number of wait time to ensure all OCBs to reach bound state

    Returns:
        obc_bound_list (list): List of all OBCs which is in Bound state.

    Raises:
        AssertionError: If not all OBC reached to Bound state

    """
    # Check all OBCs to reach Bound state
    obc_bound_list, obc_not_bound_list = ([] for i in range(2))
    while_iteration_count = 0
    while True:
        # Get kube_job obj and fetch either all OBC's are in Bound state
        # If not bound adding those OBCs to obc_not_bound_list
        job_get_output = kube_job_obj.get(namespace=namespace)
        if job_get_output is not None and len(job_get_output) == no_of_obc:
            for i in range(no_of_obc):
                status = job_get_output["items"][i]["status"]["phase"]
                log.info(
                    f"obc {job_get_output['items'][i]['metadata']['name']} status {status}"
                )
                if status != constants.STATUS_BOUND:
                    obc_not_bound_list.append(
                        job_get_output["items"][i]["metadata"]["name"]
                    )

        # Check the length of obc_not_bound_list to decide either all OBCs reached Bound state
        # If not then wait for timeout secs and re-iterate while loop
        if len(obc_not_bound_list):
            log.info(f"Number of OBCs are not in Bound state {len(obc_not_bound_list)}")
            time.sleep(timeout)
            while_iteration_count += 1
            if while_iteration_count >= no_wait_time:
                assert log.error(
                    f" Listed OBCs took more than {timeout*no_wait_time} "
                    f"secs to be bounded {obc_not_bound_list}"
                )
                break
            obc_not_bound_list.clear()
            continue
        elif not len(obc_not_bound_list):
            for i in range(no_of_obc):
                obc_bound_list.append(job_get_output["items"][i]["metadata"]["name"])
            log.info("All OBCs in Bound state")
            break
    return obc_bound_list


def cleanup(namespace):
    """
    Delete all OBCs created in the cluster

    Args:
        namespace (str): Namespace of OBC's deleting

    """
    obc_name_list = oc_get_all_obc_names()
    log.info(f"Deleting {len(obc_name_list)} OBCs")
    if obc_name_list:
        for i in obc_name_list:
            run_cmd(f"oc delete obc {i} -n {namespace}")


def get_endpoint_pod_count(namespace):
    """
    Function to query number of endpoint running in the cluster.

    Args:
        namespace (str): Namespace where endpoint is running

    Returns:
        endpoint_count (int): Number of endpoint pods

    """
    endpoint_count = pod.get_pod_count(
        label=constants.NOOBAA_ENDPOINT_POD_LABEL, namespace=namespace
    )
    log.info(f"Number of noobaa-endpoint pod(s) are running: {endpoint_count}")
    return endpoint_count


def get_hpa_utilization(namespace):
    """
    Function to get hpa utilization in the cluster.

    Args:
        namespace (str): Namespace where endpoint is running

    Returns:
         hpa_cpu_utilization (int): Percentage of CPU utilization on HPA.

    """
    obj = ocp.OCP()
    hpa_resources = obj.exec_oc_cmd(
        command=f"get hpa -n {namespace}", out_yaml_format=False
    ).split(",")
    for value in hpa_resources:
        value = re.findall(r"(\d{1,3})%/(\d{1,3})%", value.strip())
        value_list = [item for elem in value for item in elem]
        hpa_cpu_utilization = int(value_list[0])
        log.info(f"HPA CPU utilization by noobaa-endpoint is {hpa_cpu_utilization}%")
    return hpa_cpu_utilization
