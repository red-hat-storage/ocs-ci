import copy
import logging
import textwrap

import pytest

from ocs_ci.ocs import constants, ocp, fio_artefacts
from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import Pod
from tests import helpers


log = logging.getLogger(__name__)


def get_configmap_dict(fio_job, mcg_obj, bucket):
    """
    Fio configmap dictionary with configuration set for MCG workload.

    Args:
        fio_job (dict): Definition of fio job
        mcg_obj (object): instance of MCG class
        bucket (object): MCG bucket to be used for workload

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
    tmp_path
):
    """
    Creates kubernetes job that should utilize MCG bucket.

    Args:
        job_name (str): Name of the job
        bucket (object): MCG bucket with S3 interface
        project (object): OCP object representing OCP project which will be
            used for the job

    Returns:
        object: Job object

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
        tmp_path
    )

    # deploy the Job to the cluster and start it
    job_file.create()
    log.info(f"Job {job_name} created")

    # get job object
    ocp_job_obj = ocp.OCP(kind=constants.JOB, namespace=project.namespace)
    job = OCS(**ocp_job_obj.get(resource_name=job_name))

    return job
