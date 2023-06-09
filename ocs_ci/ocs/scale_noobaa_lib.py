import logging
import time
import re
import datetime

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, ocp, platform_nodes
from ocs_ci.ocs.utils import oc_get_all_obc_names
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.utility.utils import ceph_health_check, run_cmd
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import hsbench

log = logging.getLogger(__name__)
hsbenchs3 = hsbench.HsBench()


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
        job_get_output = kube_job_obj.get(namespace=namespace).get("items")
        if job_get_output is not None and len(job_get_output) == no_of_obc:
            for i in range(no_of_obc):
                try:
                    # If the OBC status field is not yet populated and empty below line
                    # throws KeyError. Handling it in try:except
                    status = job_get_output[i]["status"]["phase"]
                    log.info(
                        f"obc {job_get_output[i]['metadata']['name']} status {status}"
                    )
                except KeyError as err:
                    if (
                        "status" in str(err)
                        or not status
                        or status != constants.STATUS_BOUND
                    ):
                        obc_not_bound_list.append(job_get_output[i]["metadata"]["name"])
                        # Wait 20 secs to ensure the next obc on the list has status field populated
                        time.sleep(30)
                        job_get_output = kube_job_obj.get(namespace=namespace).get(
                            "items"
                        )
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


def cleanup(namespace, obc_list=None):
    """
    Delete all OBCs created in the cluster

    Args:
        namespace (str): Namespace of OBC's deleting
        obc_list (string): List of OBCs to be deleted

    """
    obc_name_list = list()
    if obc_list is not None:
        obc_name_list = obc_list
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
        pod_name=nb_pod_name[0], namespace=config.ENV_DATA["cluster_namespace"]
    )
    nb_pod_log = nb_pod_log.split("\n")

    loop_cnt = 0
    while True:
        no_data = list()
        for obc_name in obc_name_list:
            start = [
                i
                for i in nb_pod_log
                if re.search(f"provisioning.*bucket.*{obc_name}", i)
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
                pod_name=nb_pod_name[0], namespace=config.ENV_DATA["cluster_namespace"]
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
            i for i in nb_pod_log if re.search(f"provisioning.*bucket.*{obc_name}", i)
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
        pod_name=nb_pod_name[0], namespace=config.ENV_DATA["cluster_namespace"]
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
                pod_name=nb_pod_name[0], namespace=config.ENV_DATA["cluster_namespace"]
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
                pattern=pod_name, namespace=config.ENV_DATA["cluster_namespace"]
            )
        )[0],
        namespace=config.ENV_DATA["cluster_namespace"],
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
    return obc_bound_list, obc_not_bound_list


def get_noobaa_pods_status():
    """
    Check Noobaa pod status to ensure it is in Running state.

    Args: None

    Returns:
        Boolean: True, if all Noobaa pods in Running state. False, otherwise

    """
    ret_val = True
    ocp_pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    pod_items = ocp_pod.get(selector=constants.NOOBAA_APP_LABEL).get("items")

    # Check if noobaa pods are in running state
    for nb_pod in pod_items:
        status = ocp_pod.get_resource_status(nb_pod.get("metadata").get("name"))
        if status == constants.STATUS_RUNNING:
            log.info("Noobaa pod in running state...")
        else:
            log.error(f"Noobaa pod is in {status}, expected in Running state")
            ret_val = False
    return ret_val


def check_memory_leak_in_noobaa_endpoint_log():
    """
    Check memory leak log in Noobaa endpoint logs.

    Raises:
        UnexpectedBehaviour: If memory leak error is existing in Noobaa endpoint logs.

    """
    # Get noobaa pod logs
    mem_leak = False
    pod_list = []
    searchstring = "Possible EventEmitter memory leak detected"
    nb_pods = pod.get_noobaa_pods()
    for p in nb_pods:
        pod_logs = pod.get_pod_logs(pod_name=p.name)
        for line in pod_logs:
            if searchstring in line:
                log.info(f"File Log contains memory leak: {p.name}")
                pod_list.append(p.name)
                mem_leak = True
    if mem_leak is True:
        raise UnexpectedBehaviour(f"Log contains memory leak: {pod_list}")
    else:
        log.info("No memory leak is seen in Noobaa endpoint logs")


def hsbench_setup():
    """
    Setup and install hsbench

    """
    hsbenchs3.create_test_user()
    hsbenchs3.create_resource_hsbench()
    hsbenchs3.install_hsbench()


def hsbench_io(
    namespace=None,
    num_obj=None,
    num_bucket=None,
    object_size=None,
    run_mode=None,
    bucket_prefix=None,
    result=None,
    validate=None,
    timeout=None,
):
    """
    Run hsbench s3 benchmark

    Args:
        namespace (str): namespace to run workload
        num_obj (int): Number of object(s)
        num_bucket (int): Number of bucket(s)
        object_size (str): Size of objects in bytes with postfix K, M, and G
        run_mode (str): run mode
        bucket_prefix (str): Prefix for buckets
        result (str): Write CSV output to this file
        validate (bool): Validates whether running workload is completed.
        timeout (int): timeout in second

    """
    hsbenchs3.run_benchmark(
        num_obj=num_obj,
        num_bucket=num_bucket,
        object_size=object_size,
        run_mode=run_mode,
        bucket_prefix=bucket_prefix,
        result=result,
        validate=validate,
        timeout=timeout,
    )


def validate_bucket(
    num_objs=None, upgrade=None, result=None, put=None, get=None, list_obj=None
):
    """
    Validate S3 objects created by hsbench on bucket(s)

    """
    hsbenchs3.validate_s3_objects(upgrade=upgrade)
    hsbenchs3.validate_hsbench_put_get_list_objects(
        num_objs=num_objs, result=result, put=put, get=get, list_obj=list_obj
    )


def delete_object(bucket_name=None):
    hsbenchs3.delete_objects_in_bucket(bucket_name=bucket_name)


def delete_bucket(bucket_name=None):
    hsbenchs3.delete_bucket(bucket_name=bucket_name)


def hsbench_cleanup():
    """
    Clean up deployment config, pvc, pod and test user

    """
    hsbenchs3.delete_test_user()
    hsbenchs3.cleanup()


def create_namespace():
    """
    Create and set namespace for obcs to be created
    """
    namespace_list = list()
    namespace_list.append(helpers.create_project())
    namespace = namespace_list[-1].namespace
    return namespace


def delete_namespace(namespace=None):
    """
    Delete namespace which was created for OBCs
    Args:
        namespace (str): Namespace where OBCs were created
    """
    ocp = OCP(kind=constants.NAMESPACE)
    ocp.delete(resource_name=namespace)
