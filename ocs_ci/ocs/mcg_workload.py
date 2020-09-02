import logging

from ocs_ci.ocs import constants, ocp, fio_artefacts
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.resources.ocs import OCS
from tests.helpers import create_unique_resource_name


log = logging.getLogger(__name__)


def get_configmap_dict(fio_job, mcg_obj, bucket):
    """
    Fio configmap dictionary with configuration set for MCG workload.

    Args:
        fio_job (dict): Definition of fio job
        mcg_obj (obj): instance of MCG class
        bucket (obj): MCG bucket to be used for workload

    Returns:
        dict: Configmap definition

    """
    configmap = fio_artefacts.get_configmap_dict()
    config_name = f"{fio_job['metadata']['name']}-config"
    configmap['metadata']['name'] = config_name
    configmap["data"]["workload.fio"] = fio_artefacts.get_mcg_conf(
        mcg_obj,
        bucket
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
    resource_path
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

    Returns:
        obj: Job object

    """
    fio_job_dict = get_job_dict(job_name)
    fio_configmap_dict = get_configmap_dict(
        fio_job_dict,
        mcg_obj,
        bucket
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
            resource_path
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
