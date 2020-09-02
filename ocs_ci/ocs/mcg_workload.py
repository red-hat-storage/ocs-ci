import logging

from ocs_ci.ocs import constants, ocp, fio_artefacts
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import TimeoutSampler
from tests.helpers import create_unique_resource_name

log = logging.getLogger(__name__)


def get_configmap_dict(fio_job, mcg_obj, bucket, custom_options=None):
    """
    Fio configmap dictionary with configuration set for MCG workload.

    Args:
        fio_job (dict): Definition of fio job
        mcg_obj (obj): instance of MCG class
        bucket (obj): MCG bucket to be used for workload
        custom_options (dict): Dictionary of lists containing tuples with
            additional configuration for fio in format:
            {'section': [('option', 'value'),...],...}
            e.g.
            {'global':[('name','bucketname')],'create':[('time_based','1'),('runtime','48h')]}
            Those values can be added to the config or rewrite already existing
            values

    Returns:
        dict: Configmap definition

    """
    configmap = fio_artefacts.get_configmap_dict()
    config_name = f"{fio_job['metadata']['name']}-config"
    configmap['metadata']['name'] = config_name
    configmap["data"]["workload.fio"] = fio_artefacts.get_mcg_conf(
        mcg_obj,
        bucket,
        custom_options
    )
    return configmap


def get_job_dict(job_name):
    """
    Fio job dictionary with configuration set for MCG workload.

    Args:
        job_name (str): Name of the workload job

    Returns:
        dict: Specification for the workload job
    """
    fio_job_dict = fio_artefacts.get_job_dict()

    config_name = f"{job_name}-config"
    volume_name = f"{config_name}-vol"

    fio_job_dict['metadata']['name'] = job_name
    fio_job_dict['spec']['template']['metadata']['name'] = job_name

    job_spec = fio_job_dict['spec']['template']['spec']
    job_spec['volumes'][1]['name'] = volume_name
    job_spec['volumes'][1]['configMap']['name'] = config_name
    job_spec['containers'][0]['volumeMounts'][1]['name'] = volume_name

    job_spec['volumes'].pop(0)
    job_spec['containers'][0]['volumeMounts'].pop(0)

    return fio_job_dict


def create_workload_job(
    job_name,
    bucket,
    project,
    mcg_obj,
    resource_path,
    custom_options=None
):
    """
    Creates kubernetes job that should utilize MCG bucket.

    Args:
        job_name (str): Name of the job
        bucket (objt): MCG bucket with S3 interface
        project (obj): OCP object representing OCP project which will be
            used for the job
        mcg_obj (obj): instance of MCG class
        resource_path (str): path to directory where should be created
            resources
        custom_options (dict): Dictionary of lists containing tuples with
            additional configuration for fio in format:
            {'section': [('option', 'value'),...],...}
            e.g.
            {'global':[('name','bucketname')],'create':[('time_based','1'),('runtime','48h')]}
            Those values can be added to the config or rewrite already existing
            values

    Returns:
        obj: Job object

    """
    fio_job_dict = get_job_dict(job_name)
    fio_configmap_dict = get_configmap_dict(
        fio_job_dict,
        mcg_obj,
        bucket,
        custom_options
    )
    fio_objs = [fio_configmap_dict, fio_job_dict]

    log.info(f"Creating MCG workload job {job_name}")
    job_file = ObjectConfFile(
        "fio_continuous",
        fio_objs,
        project,
        resource_path
    )

    # deploy the Job to the cluster and start it
    job_file.create()
    log.info(f"Job {job_name} created")

    # get job object
    ocp_job_obj = ocp.OCP(kind=constants.JOB, namespace=project.namespace)
    job = OCS(**ocp_job_obj.get(resource_name=job_name))

    return job


def mcg_job_factory(
    request,
    bucket_factory,
    project_factory,
    mcg_obj,
    resource_path
):
    """
    MCG IO workload factory. Calling this fixture creates a OpenShift Job.

    Args:
        request (obj): request fixture instance
        bucket_factory (func): factory function for bucket creation
        project_factory (func): factory function for project creation
        mcg_obj (obj): instance of MCG class
        resource_path (str): path to directory where should be created
            resources

    Returns:
        func: MCG workload job factory function

    """
    instances = []

    def _factory(
        job_name=None,
        bucket=None,
        project=None,
        custom_options=None
    ):
        """
        Args:
            job_name (str): Name of the job
            bucket (obj): MCG bucket with S3 interface
            project (obj): OCP object representing OCP project which will be
                used for the job
            mcg_obj (obj): instance of MCG class
            resource_path (str): path to directory where should be created
                resources
            custom_options (dict): Dictionary of lists containing tuples with
                additional configuration for fio in format:
                {'section': [('option', 'value'),...],...}
                e.g.
                {'global':[('name','bucketname')],'create':[('time_based','1'),('runtime','48h')]}
                Those values can be added to the config or rewrite already existing
                values

        Returns:
            func: MCG workload job factory function

        """
        job_name = job_name or create_unique_resource_name(
            resource_description='mcg-io',
            resource_type='job'
        )
        bucket = bucket or bucket_factory()
        project = project or project_factory()
        job = create_workload_job(
            job_name,
            bucket,
            project,
            mcg_obj,
            resource_path,
            custom_options
        )
        instances.append(job)
        return job

    def _finalizer():
        """
        Delete the RBD secrets
        """
        for instance in instances:
            instance.delete()
            instance.ocp.wait_for_delete(
                instance.name
            )

    request.addfinalizer(_finalizer)
    return _factory


def wait_for_active_pods(job, desired_count, timeout=3):
    """
    Wait for job to load desired number of active pods in time specified
    in timeout.

    Args:
        job (obj): OCS job object
        desired_count (str): Number of desired active pods for provided job
        timeout (int): Number of seconds to wait for the job to get into state

    Returns:
        bool: If job has desired number of active pods

    """
    job_name = job.name
    log.info(f"Checking number of active pods for job {job_name}")

    def _retrieve_job_state():
        job_obj = job.ocp.get(resource_name=job_name, out_yaml_format=True)
        return job_obj['status']['active']

    try:
        for state in TimeoutSampler(
            timeout=timeout,
            sleep=3,
            func=_retrieve_job_state
        ):
            if state == desired_count:
                return True
            else:
                log.debug(
                    f"Number of active pods for job {job_name}: {state}"
                )
    except TimeoutExpiredError:
        log.error(
            f"Job {job_name} doesn't have correct number of active pods ({desired_count})"
        )
        job_pods = pod.get_pods_having_label(
            f"job-name={job_name}",
            job.namespace
        )
        for job_pod in job_pods:
            log.info(
                f"Description of job pod {job_pod['metadata']['name']}: {job_pod}"
            )
            pod_logs = pod.get_pod_logs(
                job_pod['metadata']['name'],
                namespace=job_pod['metadata']['namespace']
            )
            log.info(
                f"Logs from job pod {job_pod['metadata']['name']}: {pod_logs}"
            )

        return False
