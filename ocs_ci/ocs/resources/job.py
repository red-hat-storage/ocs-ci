import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import TimeoutIterator

log = logging.getLogger(__name__)


def get_job_obj(name, namespace=None):
    """
    Get OCS instance for job of given job name.

    Args:
        name (str): The name of the job
        namespace (str): The namespace to look in

    Returns:
        OCS: A job OCS instance
    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind=constants.JOB, namespace=namespace)
    ocp_dict = ocp_obj.get(resource_name=name)
    return OCS(**ocp_dict)


def get_all_jobs(namespace=None):
    """
    Get all the jobs in a specific namespace

    Args:
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list: list of dictionaries of the job OCS instances.

    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind=constants.JOB, namespace=namespace)
    return ocp_obj.get()["items"]


def get_jobs_with_prefix(prefix, namespace=None):
    """
    Get all the jobs that start with a specific prefix

    Args:
        prefix (str): The prefix to search in the job names
        namespace (str): Name of cluster namespace
            (default: config.ENV_DATA["cluster_namespace"] if None provided)

    Returns:
        list: list of dictionaries of the job OCS instances that start with the prefix

    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind=constants.JOB, namespace=namespace)
    jobs_dict = get_all_jobs(namespace)
    job_names = [item["metadata"]["name"] for item in jobs_dict]
    job_names_with_prefix = [
        job_name for job_name in job_names if job_name.startswith(prefix)
    ]

    jobs_with_prefix = []
    for job_name_with_prefix in job_names_with_prefix:
        ocp_dict = ocp_obj.get(resource_name=job_name_with_prefix)
        jobs_with_prefix.append(OCS(**ocp_dict))

    return jobs_with_prefix


def wait_for_job_completion(job_name, namespace, timeout=600, sleep_time=30):
    """
    Wait for given k8s Job to complete.

    Args:
        job_name (str): name of the job to wait for
        namespace (str): name of the namespace where the job is running
        timeout (int): timeout in seconds
        sleep_time (int): sleep time between consequent job status checks in
            seconds

    Raises:
        TimeoutExpiredError: When job fails to complete in given time
    """
    ocp_job = OCP(kind="Job", namespace=namespace, resource_name=job_name)
    try:
        for live_job_d in TimeoutIterator(
            timeout=timeout, sleep=sleep_time, func=ocp_job.get
        ):
            job_status = live_job_d.get("status")
            if job_status is None:
                log.debug("job status not (yet) available")
                continue
            if "completionTime" in job_status:
                log.info(
                    "job %s finished at %s", job_name, job_status["completionTime"]
                )
                break
    except exceptions.TimeoutExpiredError as ex:
        error_msg = f"job/{job_name} failed to complete in {timeout} seconds"
        log.warning(error_msg)
        raise exceptions.TimeoutExpiredError(error_msg) from ex


def get_job_pods(job_name, namespace, names_only=False):
    """
    Get list of pods of given job (via job-name pod selector).

    Args:
        job_name (str): name of the job to wait for
        namespace (str): name of the namespace where the job is running

    Returns:
        list: list of pod names (if names_only is True) or full item dicts
    """
    ocp_pod = OCP(kind="Pod", namespace=namespace)
    oc_result = ocp_pod.get(selector=f"job-name={job_name}")
    if oc_result["kind"] != "List":
        error_msg = "oc get should return List item"
        log.error(error_msg)
        log.debug(oc_result)
        raise exceptions.UnexpectedBehaviour(error_msg)
    if names_only:
        result = [item["metadata"]["name"] for item in oc_result["items"]]
    else:
        result = oc_result["items"]
    return result


def log_output_of_job_pods(job_name, namespace):
    """
    Log (via standard logger) output of all pods of given job. Expected to be
    used in case of error, when evidence needs to be captured in logs.

    Args:
        job_name (str): name of the job to wait for
        namespace (str): name of the namespace where the job is running
    """
    job_pods = get_job_pods(
        job_name=job_name,
        namespace=namespace,
        names_only=True,
    )
    ocp_pod = OCP(kind="Pod", namespace=namespace)
    for pod_name in job_pods:
        log.info(
            "fetching output of pod %s of job/%s (see DEBUG logs)",
            pod_name,
            job_name,
        )
        output = ocp_pod.get_logs(pod_name)
        log.debug(output)
