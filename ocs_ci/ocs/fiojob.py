# -*- coding: utf8 -*-

"""
This module contains functions which implements functionality necessary to run
general `fio`_ workloads as `k8s Jobs`_ in OCP/OCS cluster via workload
fixtures (see :py:mod:`ocs_ci.utility.workloadfixture`).

.. moduleauthor:: Martin Bukatovič

.. _`fio`: https://fio.readthedocs.io/en/latest/fio_doc.html
.. _`k8s Jobs`: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
"""


import logging
import time
import yaml
import yaml.parser

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


def get_storageutilization_size(target_percentage, ceph_pool_name):
    """
    For the purpose of the workload storage utilization fixtures, get expected
    pvc_size based on STORED and MAX AVAIL values (as reported by `ceph df`)
    for given ceph pool and target utilization percentage.

    This is only approximate, and it won't work eg. if each pool has different
    configuration of replication.

    Args:
        target_percentage (float): target total utilization, eg. 0.5 for 50%
        ceph_pool_name (str): name of ceph pool where you want to write data

    Returns:
        int: pvc_size for storage utilization job (in GiB, rounded)
    """
    # get STORED and MAX AVAIL of given ceph pool ...
    ct_pod = pod.get_ceph_tools_pod()
    ceph_df_dict = ct_pod.exec_ceph_cmd(ceph_cmd="ceph df")
    ceph_pool = None
    ceph_total_stored = 0
    for pool in ceph_df_dict["pools"]:
        ceph_total_stored += pool["stats"]["stored"]
        if pool["name"] == ceph_pool_name:
            ceph_pool = pool
    if ceph_pool is None:
        logger.error((
            f"pool {ceph_pool_name} was not found "
            f"in output of `ceph df`: {ceph_df_dict}"))
    # If the following assert fail, the problem is either:
    #  - name of the pool has changed (when this happens before GA, it's
    #    likely ocs-ci bug, after the release it's a product bug),
    #  - pool is missing (likely a product bug)
    # either way, the fixture can't continue ...
    assert ceph_pool is not None, f"pool {ceph_pool_name} should exist"
    # ... to compute PVC size (values in bytes)
    total = ceph_pool["stats"]["max_avail"] + ceph_total_stored
    max_avail_gi = ceph_pool['stats']['max_avail'] / 2**30
    logger.info(f"MAX AVAIL of {ceph_pool_name} is {max_avail_gi} Gi")
    target = total * target_percentage
    to_utilize = target - ceph_total_stored
    pvc_size = round(to_utilize / 2**30)  # GiB
    logger.info(
        f"to reach {target/2**30} Gi of total cluster utilization, "
        f"which is {target_percentage*100}% of the total capacity, "
        f"utilization job should request and fill {pvc_size} Gi volume")
    return pvc_size


def fio_to_dict(fio_output):
    """"
    Parse fio output and provide parsed dict it as a result.
    """
    fio_output_lines = fio_output.splitlines()
    line_num = 0
    for line_num, line in enumerate(fio_output_lines):
        if line == "{":
            break
        else:
            logger.info(line)
    fio_parseable_output = "\n".join(fio_output_lines[line_num:])
    try:
        fio_report = yaml.safe_load(fio_parseable_output)
    except yaml.parser.ParserError as ex:
        logger.error("json output from fio can't be parsed: %s", ex)
        raise ex
    return fio_report


def get_timeout(fio_min_mbps, pvc_size):
    """
    Compute how long we will let the job running while writing data to the
    volume.

    Args:
      fio_min_mbps (int): minimal write speed in MiB/s
      pvc_size (int): size of PVC in GiB, which will be used to writing

    Returns: write_timeout in seconds
    """
    # based on min. fio write speed of the enviroment ...
    logger.info(
        "Assuming %.2f MB/s is a minimal write speed of fio.", fio_min_mbps)
    # ... we compute max. time we are going to wait for fio to write all data
    min_time_to_write_gb = 1 / (fio_min_mbps / 2**10)
    write_timeout = pvc_size * min_time_to_write_gb  # seconds
    logger.info((
        f"fixture will wait {write_timeout} seconds for the Job "
        f"to write {pvc_size} Gi data on OCS backed volume"))
    return write_timeout


def wait_for_job_completion(namespace, timeout, error_msg):
    """
    This is a WORKAROUND of particular ocsci design choices: I just wait
    for one pod in the namespace, and then ask for the pod again to get
    it's name (but it would be much better to just wait for the job to
    finish instead, then ask for a name of the successful pod and use it
    to get logs ...)

    Returns: name of Pod resource of the finished job
    """
    ocp_pod = ocp.OCP(kind="Pod", namespace=namespace)
    try:
        ocp_pod.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_COMPLETED,
            timeout=timeout,
            sleep=30)
    except TimeoutExpiredError as ex:
        # report some high level error as well
        logger.error(error_msg)
        # TODO: log both describe and the output from the fio pods, as DEBUG
        ex.message = error_msg
        raise(ex)

    # indentify pod of the completed job
    pod_data = ocp_pod.get()
    # explicit list of assumptions, if these assumptions are not met, the
    # code won't work and it either means that something went terrible
    # wrong or that the code needs to be changed
    assert pod_data['kind'] == "List"
    pod_dict = pod_data['items'][0]
    assert pod_dict['kind'] == "Pod"
    pod_name = pod_dict['metadata']['name']
    logger.info(f"Identified pod name of the finished Job: {pod_name}")
    pod_name = pod_dict['metadata']['name']

    return pod_name


def write_data_via_fio(fio_job_file, write_timeout, pvc_size, target_percentage):
    """
    Write data via fio Job (specified in ``tf`` tmp file) to reach desired
    utilization level, and keep this level for ``minimal_time`` seconds.
    """
    # unix timestamp before starting the job so that one can check status
    # prior the fio job run
    fio_job_start_ts = time.time()

    # deploy the fio Job to the cluster
    fio_job_file.create()

    # high level description of the problem, reported in case of a job failure
    # or timeout
    error_msg = (
        f"Job fio failed to write {pvc_size} Gi data on OCS backed "
        f"volume in expected time {write_timeout} seconds."
        " If the fio pod were still runing"
        " (see 'last actual status was' in some previous log message),"
        " this is caused either by"
        " severe product performance regression"
        " or by a misconfiguration of the clusterr, ping infra team.")
    pod_name = wait_for_job_completion(
        fio_job_file.project.namespace, write_timeout, error_msg)

    ocp_pod = ocp.OCP(kind="Pod", namespace=fio_job_file.project.namespace)
    fio_output = ocp_pod.exec_oc_cmd(
        f"logs {pod_name}", out_yaml_format=False)

    # parse fio output
    fio_report = fio_to_dict(fio_output)

    logger.debug(fio_report)
    if fio_report is not None:
        disk_util = fio_report.get('disk_util')
        logger.info("fio disk_util stats: %s", disk_util)
    else:
        logger.warning("fio report is empty")

    # data which will be available to the test via:
    # fixture_name['result']
    result = {
        'fio_job_start': fio_job_start_ts,
        'fio': fio_report,
        'pvc_size': pvc_size,
        'target_p': target_percentage,
        'namespace': fio_job_file.project.namespace}

    return result


def delete_fio_data(fio_job_file, delete_check_func):
    """
    Delete fio data by removing the fio job resource, with a wait to
    make sure date were reclaimed on the ceph level.
    """
    # make sure we communicate what is going to happen
    logger.info(f"going to delete {fio_job_file.name} Job")
    fio_job_file.delete()
    logger.info(
        f"going to wait a bit to make sure that "
        f"data written by {fio_job_file.name} Job are really deleted")

    check_timeout = 660  # seconds
    check_sampler = TimeoutSampler(
        timeout=check_timeout, sleep=30, func=delete_check_func)
    finished_in_time = check_sampler.wait_for_func_status(result=True)
    if not finished_in_time:
        error_msg = (
            "it seems that the storage space was not reclaimed "
            f"within {check_timeout} seconds, "
            "this is most likely a product bug or misconfiguration")
        logger.error(error_msg)
        raise Exception(error_msg)
