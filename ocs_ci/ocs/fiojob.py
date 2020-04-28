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
import os
import textwrap
import time

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.exceptions import UnexpectedVolumeType
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility.workloadfixture import measure_operation


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
        logger.error(
            f"pool {ceph_pool_name} was not found "
            f"in output of `ceph df`: {ceph_df_dict}"
        )
    # If the following assert fail, the problem is either:
    #  - name of the pool has changed (when this happens before GA, it's
    #    likely ocs-ci bug, after the release it's a product bug),
    #  - pool is missing (likely a product bug)
    # either way, the fixture can't continue ...
    assert ceph_pool is not None, f"Pool: {ceph_pool_name} doesn't exist!"
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
        f"utilization job should request and fill {pvc_size} Gi volume"
    )
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

    Returns:
        int: write_timeout in seconds

    """
    # based on min. fio write speed of the enviroment ...
    logger.info(
        "Assuming %.2f MB/s is a minimal write speed of fio.", fio_min_mbps)
    # ... we compute max. time we are going to wait for fio to write all data
    min_time_to_write_gb = 1 / (fio_min_mbps / 2**10)
    write_timeout = pvc_size * min_time_to_write_gb  # seconds
    logger.info(
        f"fixture will wait {write_timeout} seconds for the Job "
        f"to write {pvc_size} Gi data on OCS backed volume"
    )
    return write_timeout


def wait_for_job_completion(namespace, timeout, error_msg):
    """
    This is a WORKAROUND of particular ocsci design choices: I just wait
    for one pod in the namespace, and then ask for the pod again to get
    it's name (but it would be much better to just wait for the job to
    finish instead, then ask for a name of the successful pod and use it
    to get logs ...)

    Returns:
        str: name of Pod resource of the finished job

    """
    ocp_pod = ocp.OCP(kind="Pod", namespace=namespace)
    try:
        ocp_pod.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_COMPLETED,
            timeout=timeout,
            sleep=30
        )
    except TimeoutExpiredError as ex:
        # report some high level error as well
        logger.error(error_msg)
        ex.message = error_msg
        # fetch log(s) of any fio pod(s) in the job namespace
        pod_data = ocp_pod.get()
        for pod_dict in pod_data.get('items', []):
            pod_name = pod_dict['metadata']['name']
            output = ocp_pod.get_logs(pod_name)
            if len(output) == 0:
                logger.error(
                    "Container log from pod '%s' is empty.", pod_name)
            else:
                logger.error(
                    "Container log from pod '%s' follows:\n%s",
                    pod_name,
                    output
                )
        # reraise the exception
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
    fio_output = ocp_pod.get_logs(pod_name)

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
            "this is most likely a product bug or misconfiguration"
        )
        logger.error(error_msg)
        raise Exception(error_msg)


def workload_fio_storageutilization(
    fixture_name,
    target_percentage,
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    with_checksum=False,
):
    """
    This function implements core functionality of fio storage utilization
    workload fixture. This is necessary because we can't parametrize single
    general fixture over multiple parameters (it would mess with test case id
    and polarion test case tracking).
    """
    # TODO: move out storage class names
    if fixture_name.endswith("rbd"):
        storage_class_name = "ocs-storagecluster-ceph-rbd"
        ceph_pool_name = "ocs-storagecluster-cephblockpool"
    elif fixture_name.endswith("cephfs"):
        storage_class_name = "ocs-storagecluster-cephfs"
        ceph_pool_name = "ocs-storagecluster-cephfilesystem-data0"
    else:
        raise UnexpectedVolumeType(
            "unexpected volume type, ocs-ci code is wrong")

    # make sure we communicate what is going to happen
    logger.info((
        f"starting {fixture_name} fixture, "
        f"using {storage_class_name} storage class "
        f"backed by {ceph_pool_name} ceph pool"))

    # log ceph mon_osd_*_ratio values for QE team to understand behaviour of
    # ceph cluster during high utilization levels (for expected values, consult
    # BZ 1775432 and check that there is no more recent BZ or JIRA in this
    # area)
    ceph_full_ratios = [
        'mon_osd_full_ratio',
        'mon_osd_backfillfull_ratio',
        'mon_osd_nearfull_ratio',
    ]
    ct_pod = pod.get_ceph_tools_pod()
    for ceph_ratio in ceph_full_ratios:
        logger.info("checking value of %s", ceph_ratio)
        value = ct_pod.exec_ceph_cmd(f'ceph config get mon.* {ceph_ratio}')
        logger.info(f"{ceph_ratio} is {value}")

    pvc_size = get_storageutilization_size(target_percentage, ceph_pool_name)

    # To handle use case of test_workload_rbd_cephfs_minimal which writes data
    # to reach a small fraction of the total capacity only (eg. 5%), the test
    # is going increase the target 2x and try again.
    if pvc_size <= 0 and target_percentage <= 0.10:
        new_target_percentage = 2 * target_percentage
        logger.info(
            "increasing storage utilization target percentage from %.2f to %.2f",
            target_percentage,
            new_target_percentage)
        target_percentage = new_target_percentage
        pvc_size = get_storageutilization_size(
            target_percentage,
            ceph_pool_name)
    # If this is still not enough, the test will be skipped, because the idea
    # of tests reaching a small total utilization is to do just that.
    # Moreover this will also skip this test case for any other utilization
    # level, which is easier to read in the test report than the actual
    # failure with negative pvc size.
    if pvc_size <= 0:
        skip_msg = (
            "current total storage utilization is too high, "
            f"the target utilization {target_percentage*100}% is already met")
        logger.warning(skip_msg)
        pytest.skip(skip_msg)

    fio_conf = textwrap.dedent("""
        [simple-write]
        readwrite=write
        buffered=1
        blocksize=4k
        ioengine=libaio
        directory=/mnt/target
        """)

    # When we ask for checksum to be generated for all files written in the
    # /mnt/target directory, we need to keep some space free so that the
    # checksum file would fit there. We overestimate this free space so that
    # it works both with CephFS and RBD volumes, as with RBD volumes actuall
    # usable capacity is smaller because of filesystem overhead (pvc size
    # defines size of a block device, on which local ext4 filesystem is
    # formatted).
    if with_checksum:
        # assume 4% fs overhead, and double to it make it safe
        fs_overhead = 0.08
        # size of file created by fio in MiB
        fio_size = int((pvc_size * (1 - fs_overhead)) * 2**10)
        fio_conf += f"size={fio_size}M\n"
    # Otherwise, we are tryting to write as much data as possible and fill the
    # persistent volume entirely.
    # For cephfs we can't use fill_fs because of BZ 1763808 (the process
    # will get *Disk quota exceeded* error instead of *No space left on
    # device* error).
    # On the other hand, we can't use size={pvc_size} for rbd, as we can't
    # write pvc_size bytes to a filesystem on a block device of {pvc_size}
    # size (obviously, some space is used by filesystem metadata).
    elif fixture_name.endswith("rbd"):
        fio_conf += "fill_fs=1\n"
    else:
        fio_conf += f"size={pvc_size}G\n"

    # When we ask for checksum to be generated for all files written in the
    # /mnt/target directory, we change the command of the container to run
    # both fio and sha1 checksum tool in the target directory. To do that,
    # we use '/bin/sh -c' hack.
    if with_checksum:
        container = fio_job_dict['spec']['template']['spec']['containers'][0]
        fio_command = " ".join(container['command'])
        sha_command = (
            "sha1sum /mnt/target/simple-write.*"
            " > /mnt/target/fio.sha1sum"
            " 2> /mnt/target/fio.stderr")
        shell_command = fio_command + " && " + sha_command
        container['command'] = ["/bin/bash", "-c", shell_command]

    # put the dicts together into yaml file of the Job
    fio_configmap_dict["data"]["workload.fio"] = fio_conf
    fio_pvc_dict["spec"]["storageClassName"] = storage_class_name
    fio_pvc_dict["spec"]["resources"]["requests"]["storage"] = f"{pvc_size}Gi"
    fio_objs = [fio_pvc_dict, fio_configmap_dict, fio_job_dict]
    fio_job_file = ObjectConfFile(fixture_name, fio_objs, project, tmp_path)

    fio_min_mbps = config.ENV_DATA['fio_storageutilization_min_mbps']
    write_timeout = get_timeout(fio_min_mbps, pvc_size)

    test_file = os.path.join(measurement_dir, f"{fixture_name}.json")

    measured_op = measure_operation(
        lambda: write_data_via_fio(
            fio_job_file, write_timeout, pvc_size, target_percentage),
        test_file,
        measure_after=True,
        minimal_time=480)

    # we don't need to delete anything if this fixture has been already
    # executed
    if not measured_op['first_run']:
        return measured_op

    def check_pvc_size():
        """
        Check whether data created by the Job were actually deleted.
        """
        # By asking again for pvc_size necessary to reach the target
        # cluster utilization, we can see how much data were already
        # deleted. Negative or small value of current pvc_size means that
        # the data were not yet deleted.
        pvc_size_tmp = get_storageutilization_size(
            target_percentage, ceph_pool_name)
        # If no other components were utilizing OCS storage, the space
        # would be considered reclaimed when current pvc_size reaches
        # it's original value again. But since this is not the case (eg.
        # constantly growing monitoring or log data are stored there),
        # we are ok with just 90% of the original value.
        result = pvc_size_tmp >= pvc_size * 0.90
        if result:
            logger.info("storage space was reclaimed")
        else:
            logger.info(
                "storage space was not yet fully reclaimed, "
                f"current pvc size {pvc_size_tmp} value "
                f"should be close to {pvc_size}")
        return result

    if with_checksum:
        # Let's get the name of the PV via the PVC.
        ocp_pvc = ocp.OCP(kind=constants.PVC, namespace=project.namespace)
        pvc_data = ocp_pvc.get()
        # Explicit list of assumptions, if these assumptions are not met, the
        # code won't work and it either means that something went terrible
        # wrong or that the code needs to be changed.
        assert pvc_data['kind'] == "List"
        assert len(pvc_data['items']) == 1
        pvc_dict = pvc_data['items'][0]
        assert pvc_dict['kind'] == constants.PVC
        pv_name = pvc_dict['spec']['volumeName']
        logger.info("Identified PV of the finished fio Job: %s", pv_name)
        # We change reclaim policy of the volume, so that we can reuse it
        # later, while everyting but the volume will be deleted during project
        # teardown. Note that while a standard way of doing this would be via
        # custom storage class with redefined reclaim policy, we need to do
        # this on this single volume only here, so editing volume directly is
        # more straightforward.
        logger.info("Changing persistentVolumeReclaimPolicy of %s", pv_name)
        ocp_pv = ocp.OCP(kind=constants.PV)
        patch_success = ocp_pv.patch(
            resource_name=pv_name,
            params='{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}')
        if patch_success:
            logger.info('Reclaim policy of %s was changed.', pv_name)
        else:
            logger.error('Reclaim policy of %s failed to be changed.', pv_name)
        label = f'fixture={fixture_name}'
        ocp_pv.add_label(pv_name, label)
    else:
        # Without checksum, we just need to make sure that data were deleted
        # and wait for this to happen to avoid conflicts with tests executed
        # right after this one.
        delete_fio_data(fio_job_file, check_pvc_size)

    return measured_op
