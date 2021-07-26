import logging

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS

log = logging.getLogger(__name__)


def get_job_obj(name, namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Get the job instance for the given job name

    Args:
        name (str): The name of the job
        namespace (str): The namespace to look in

    Returns:
        OCS: A job OCS instance
    """
    ocp_obj = OCP(kind=constants.JOB, namespace=namespace)
    ocp_dict = ocp_obj.get(resource_name=name)
    return OCS(**ocp_dict)


def get_all_jobs(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Get all the jobs in a specific namespace

    Args:
        namespace (str): Name of cluster namespace(default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list: list of dictionaries of the job OCS instances.

    """
    ocp_obj = OCP(kind=constants.JOB, namespace=namespace)
    return ocp_obj.get()["items"]


def get_jobs_with_prefix(prefix, namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Get all the jobs that start with a specific prefix

    Args:
        prefix (str): The prefix to search in the job names
        namespace (str): Name of cluster namespace(default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list: list of dictionaries of the job OCS instances that start with the prefix

    """
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
