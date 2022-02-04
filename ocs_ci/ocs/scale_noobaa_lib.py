import logging
import time
import re
import datetime

from ocs_ci.helpers import helpers
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, ocp, platform_nodes
from ocs_ci.ocs.utils import oc_get_all_obc_names
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.ocp import OCP

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
    kube_job_obj, namespace, no_of_obc, timeout=60, no_wait_time=20
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
    while_iteration_count_1 = 0
    while_iteration_count_2 = 0
    while True:
        # Get kube_job obj and check if all OBC's are in Bound state
        # If not bound adding those OBCs to obc_not_bound_list
        # import pdb; pdb.set_trace()
        job_get_output = kube_job_obj.get(namespace=namespace).get("items")
        if job_get_output is not None and len(job_get_output) == no_of_obc:
            for i in range(no_of_obc):
                if "status" not in job_get_output[i]:
                    log.info(f"XXX {job_get_output[i]}")
                    while_iteration_count_1 += 1
                    time.sleep(3)
                    continue
                status = job_get_output[i]["status"]["phase"]
                log.info(f"obc {job_get_output[i]['metadata']['name']} status {status}")
                if not status or status != constants.STATUS_BOUND:
                    obc_not_bound_list.append(job_get_output[i]["metadata"]["name"])
                    # Wait 20 secs to ensure the next obc on the list has status field populated
                    time.sleep(30)
                    job_get_output = kube_job_obj.get(namespace=namespace).get("items")
        else:
            while_iteration_count_1 += 1
            time.sleep(timeout)
            continue

        # Check the length of obc_not_bound_list to decide either all OBCs reached Bound state
        # If not then wait for timeout secs and re-iterate while loop
        if len(obc_not_bound_list):
            log.info(f"Number of OBCs are not in Bound state {len(obc_not_bound_list)}")
            time.sleep(timeout)
            while_iteration_count_2 += 1
            if while_iteration_count_2 >= no_wait_time:
                assert log.error(
                    f" Listed OBCs took more than {timeout*no_wait_time} "
                    f"secs to be bounded {obc_not_bound_list}"
                )
                break
            obc_not_bound_list.clear()
            continue
        elif not len(obc_not_bound_list):
            for i in range(no_of_obc):
                obc_bound_list.append(job_get_output[i]["metadata"]["name"])
            log.info("All OBCs in Bound state")
            break
    return obc_bound_list


def cleanup(namespace, obc_count=None):
    """
    Delete all OBCs created in the cluster

    Args:
        namespace (str): Namespace of OBC's deleting

    """
    if obc_count is not None:
        obc_name_list = obc_count
    else:
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
    return hpa_cpu_utilization


def measure_obc_creation_time(obc_name_list, timeout=60):
    """
    Measure OBC creation time
    Args:
        obc_name_list (list): List of obc names to measure creation time
        timeout (int): Wait time in second before collecting log
    Returns:
        obc_dict (dict): Dictionary of obcs and creation time in second

    """
    # Get obc creation logs
    nb_pod_name = get_pod_name_by_pattern("noobaa-operator-")
    nb_pod_log = pod.get_pod_logs(
        pod_name=nb_pod_name[0], namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    nb_pod_log = nb_pod_log.split("\n")

    loop_cnt = 0
    while True:
        no_data = list()
        for obc_name in obc_name_list:
            start = [
                i
                for i in nb_pod_log
                if re.search(f"provisioning.*{obc_name}.*bucket", i)
            ]
            end = [
                i
                for i in nb_pod_log
                if re.search(f"updating status.*{obc_name}.*Bound", i)
            ]
            if not start or not end:
                no_data.append(obc_name)
        if no_data:
            time.sleep(timeout)
            nb_pod_name = get_pod_name_by_pattern("noobaa-operator-")
            nb_pod_log = pod.get_pod_logs(
                pod_name=nb_pod_name[0], namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            nb_pod_log = nb_pod_log.split("\n")
            loop_cnt += 1
            if loop_cnt >= 10:
                log.info("Waited for more than 10 mins but still no data")
                raise UnexpectedBehaviour(
                    f"There is no obc creation data in noobaa-operator logs for {no_data}"
                )
            continue
        else:
            break
    obc_dict = dict()
    this_year = str(datetime.datetime.now().year)

    for obc_name in obc_name_list:
        # Extract obc creation start time
        start_item = [
            i for i in nb_pod_log if re.search(f"provisioning.*{obc_name}.*bucket", i)
        ]
        mon_day = " ".join(start_item[0].split(" ")[0:2])
        start = f"{this_year} {mon_day}"
        dt_start = datetime.datetime.strptime(start, "%Y I%m%d %H:%M:%S.%f")

        # Extract obc creation end time
        end_item = [
            i for i in nb_pod_log if re.search(f"updating status.*{obc_name}.*Bound", i)
        ]
        mon_day = " ".join(end_item[0].split(" ")[0:2])
        end = f"{this_year} {mon_day}"
        dt_end = datetime.datetime.strptime(end, "%Y I%m%d %H:%M:%S.%f")
        total = dt_end - dt_start
        log.info(f"{obc_name}: {total.total_seconds()} sec")
        obc_dict[obc_name] = total.total_seconds()

    return obc_dict


def measure_obc_deletion_time(obc_name_list, timeout=60):
    """
    Measure OBC deletion time
    Args:
        obc_name_list (list): List of obc names to measure deletion time
        timeout (int): Wait time in second before collecting log
    Returns:
        obc_dict (dict): Dictionary of obcs and deletion time in second

    """
    # Get obc deletion logs
    nb_pod_name = get_pod_name_by_pattern("noobaa-operator-")
    nb_pod_log = pod.get_pod_logs(
        pod_name=nb_pod_name[0], namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    nb_pod_log = nb_pod_log.split("\n")

    loop_cnt = 0
    while True:
        no_data = list()
        for obc_name in obc_name_list:
            start = [
                i
                for i in nb_pod_log
                if re.search(f"removing ObjectBucket.*{obc_name}", i)
            ]
            end = [
                i
                for i in nb_pod_log
                if re.search(f"ObjectBucket deleted.*{obc_name}", i)
            ]
            if not start or not end:
                no_data.append(obc_name)
        if no_data:
            time.sleep(timeout)
            nb_pod_name = get_pod_name_by_pattern("noobaa-operator-")
            nb_pod_log = pod.get_pod_logs(
                pod_name=nb_pod_name[0], namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            nb_pod_log = nb_pod_log.split("\n")
            loop_cnt += 1
            if loop_cnt >= 10:
                log.info("Waited for more than 10 mins but still no data")
                raise UnexpectedBehaviour(
                    f"There is no obc deletion data in noobaa-operator logs for {no_data}"
                )
            continue
        else:
            break

    obc_dict = dict()
    this_year = str(datetime.datetime.now().year)
    for obc_name in obc_name_list:
        # Extract obc deletion start time
        start_item = [
            i for i in nb_pod_log if re.search(f"removing ObjectBucket.*{obc_name}", i)
        ]
        mon_day = " ".join(start_item[0].split(" ")[0:2])
        start = f"{this_year} {mon_day}"
        dt_start = datetime.datetime.strptime(start, "%Y I%m%d %H:%M:%S.%f")

        # Extract obc deletion end time
        end_item = [
            i for i in nb_pod_log if re.search(f"ObjectBucket deleted.*{obc_name}", i)
        ]
        mon_day = " ".join(end_item[0].split(" ")[0:2])
        end = f"{this_year} {mon_day}"
        dt_end = datetime.datetime.strptime(end, "%Y I%m%d %H:%M:%S.%f")
        total = dt_end - dt_start
        log.info(f"{obc_name}: {total.total_seconds()} sec")
        obc_dict[obc_name] = total.total_seconds()

    return obc_dict


def noobaa_running_node_restart(pod_name):
    """
    Function to restart node which has noobaa pod's running

    Args:
        pod_name (str): Name of noobaa pod

    """

    nb_pod_obj = pod.get_pod_obj(
        (
            get_pod_name_by_pattern(
                pattern=pod_name, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
        )[0],
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    nb_node_name = pod.get_pod_node(nb_pod_obj).name
    factory = platform_nodes.PlatformNodesFactory()
    nodes = factory.get_nodes_platform()
    nb_nodes = get_node_objs(node_names=nb_node_name)
    log.info(f"{pod_name} is running on {nb_node_name}")
    log.info(f"Restating node: {nb_node_name}....")
    nodes.restart_nodes_by_stop_and_start(nodes=nb_nodes, force=True)

    # Validate nodes are up and running
    wait_for_nodes_status()
    ceph_health_check(tries=30, delay=60)
    helpers.wait_for_resource_state(nb_pod_obj, constants.STATUS_RUNNING, timeout=180)


def check_all_obcs_status(namespace=None):
    """
    Check all OBCs status in given namespace

    Args:
        namespace (str): Namespace where endpoint is running

    Returns:
        obc_bound_list: A list of all OBCs in Bound state

    """
    all_obcs_in_namespace = (
        OCP(namespace=namespace, kind="ObjectBucketClaim").get().get("items")
    )
    obc_bound_list, obc_not_bound_list = ([] for i in range(2))
    for obc in all_obcs_in_namespace:
        status = obc.get("status").get("phase")
        if status == constants.STATUS_BOUND:
            obc_bound_list.append(status)
        else:
            obc_not_bound_list.append(status)
    return obc_bound_list
