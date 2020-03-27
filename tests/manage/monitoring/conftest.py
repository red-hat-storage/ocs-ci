# -*- coding: utf8 -*-

import logging
import os
import textwrap
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import fiojob
from ocs_ci.ocs.exceptions import UnexpectedVolumeType
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.mcg_bucket import S3Bucket
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.utility import workloadfixture
from ocs_ci.utility.workloadfixture import measure_operation
from tests import helpers
from tests.helpers import create_unique_resource_name


logger = logging.getLogger(__name__)


@pytest.fixture
def measurement_dir(tmp_path):
    """
    Returns directory path where should be stored all results related
    to measurement. If 'measurement_dir' is provided by config then use it,
    otherwise new directory is generated.

    Returns:
        str: Path to measurement directory
    """
    if config.ENV_DATA.get('measurement_dir'):
        measurement_dir = config.ENV_DATA.get('measurement_dir')
        logger.info(
            f"Using measurement dir from configuration: {measurement_dir}"
        )
    else:
        measurement_dir = os.path.join(
            os.path.dirname(tmp_path),
            'measurement_results'
        )
    if not os.path.exists(measurement_dir):
        logger.info(
            f"Measurement dir {measurement_dir} doesn't exist. Creating it."
        )
        os.mkdir(measurement_dir)
    return measurement_dir


@pytest.fixture
def measure_stop_ceph_mgr(measurement_dir):
    """
    Downscales Ceph Manager deployment, measures the time when it was
    downscaled and monitors alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph Manager pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA['cluster_namespace']
    )
    mgr_deployments = oc.get(selector=constants.MGR_APP_LABEL)['items']
    mgr = mgr_deployments[0]['metadata']['name']

    def stop_mgr():
        """
        Downscale Ceph Manager deployment for 6 minutes. First 5 minutes
        the alert should be in 'Pending'.
        After 5 minutes it should be 'Firing'.
        This configuration of monitoring can be observed in ceph-mixins which
        are used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L25

        Returns:
            str: Name of downscaled deployment
        """
        # run_time of operation
        run_time = 60 * 6
        nonlocal oc
        nonlocal mgr
        logger.info(f"Downscaling deployment {mgr} to 0")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{mgr}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return oc.get(mgr)

    test_file = os.path.join(measurement_dir, 'measure_stop_ceph_mgr.json')
    measured_op = measure_operation(stop_mgr, test_file)
    logger.info(f"Upscaling deployment {mgr} back to 1")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mgr}")
    return measured_op


@pytest.fixture
def measure_stop_ceph_mon(measurement_dir):
    """
    Downscales Ceph Monitor deployment, measures the time when it was
    downscaled and monitors alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph Monitor pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA['cluster_namespace']
    )
    mon_deployments = oc.get(selector=constants.MON_APP_LABEL)['items']
    mons = [
        deployment['metadata']['name']
        for deployment in mon_deployments
    ]

    # get monitor deployments to stop, leave even number of monitors
    split_index = len(mons) // 2 if len(mons) > 3 else 2
    mons_to_stop = mons[split_index:]
    logger.info(f"Monitors to stop: {mons_to_stop}")
    logger.info(f"Monitors left to run: {mons[:split_index]}")

    def stop_mon():
        """
        Downscale Ceph Monitor deployments for 14 minutes. First 15 minutes
        the alert CephMonQuorumAtRisk should be in 'Pending'. After 15 minutes
        the alert turns into 'Firing' state.
        This configuration of monitoring can be observed in ceph-mixins which
        are used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L16
        `Firing` state shouldn't actually happen because monitor should be
        automatically redeployed shortly after 10 minutes.

        Returns:
            str: Names of downscaled deployments
        """
        # run_time of operation
        run_time = 60 * 14
        nonlocal oc
        nonlocal mons_to_stop
        for mon in mons_to_stop:
            logger.info(f"Downscaling deployment {mon} to 0")
            oc.exec_oc_cmd(f"scale --replicas=0 deployment/{mon}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return mons_to_stop

    test_file = os.path.join(measurement_dir, 'measure_stop_ceph_mon.json')
    measured_op = measure_operation(stop_mon, test_file)

    # get new list of monitors to make sure that new monitors were deployed
    mon_deployments = oc.get(selector=constants.MON_APP_LABEL)['items']
    mons = [
        deployment['metadata']['name']
        for deployment in mon_deployments
    ]

    # check that downscaled monitors are removed as OCS should redeploy them
    # but only when we are running this for the first time
    check_old_mons_deleted = all(mon not in mons for mon in mons_to_stop)
    if measured_op['first_run'] and not check_old_mons_deleted:
        for mon in mons_to_stop:
            logger.info(f"Upscaling deployment {mon} back to 1")
            oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mon}")
        msg = f"Downscaled monitors {mons_to_stop} were not replaced"
        assert check_old_mons_deleted, msg

    return measured_op


@pytest.fixture
def measure_stop_ceph_osd(measurement_dir):
    """
    Downscales Ceph osd deployment, measures the time when it was
    downscaled and alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph osd pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA.get('cluster_namespace')
    )
    osd_deployments = oc.get(selector=constants.OSD_APP_LABEL).get('items')
    osds = [
        deployment.get('metadata').get('name')
        for deployment in osd_deployments
    ]

    # get osd deployments to stop, leave even number of osd
    osd_to_stop = osds[-1]
    logger.info(f"osd disks to stop: {osd_to_stop}")
    logger.info(f"osd disks left to run: {osds[:-1]}")

    def stop_osd():
        """
        Downscale Ceph osd deployments for 11 minutes. First 1 minutes
        the alert CephOSDDiskNotResponding should be in 'Pending'.
        After 1 minute the alert turns into 'Firing' state.
        This configuration of osd can be observed in ceph-mixins which
        is used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L21
        There should be also CephClusterWarningState alert that takes 10
        minutest to be firing.

        Returns:
            str: Names of downscaled deployments
        """
        # run_time of operation
        run_time = 60 * 11
        nonlocal oc
        nonlocal osd_to_stop
        logger.info(f"Downscaling deployment {osd_to_stop} to 0")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{osd_to_stop}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return osd_to_stop

    test_file = os.path.join(measurement_dir, 'measure_stop_ceph_osd.json')
    measured_op = measure_operation(stop_osd, test_file)
    logger.info(f"Upscaling deployment {osd_to_stop} back to 1")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{osd_to_stop}")

    return measured_op


@pytest.fixture
def measure_corrupt_pg(measurement_dir):
    """
    Create Ceph pool and corrupt Placement Group on one of OSDs, measures the
    time when it was corrupted and records alerts that were triggered during
    this event.

    Returns:
        dict: Contains information about `start` and `stop` time for
        corrupting Ceph Placement Group
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA.get('cluster_namespace')
    )
    osd_deployments = oc.get(selector=constants.OSD_APP_LABEL).get('items')
    osd_deployment = osd_deployments[0].get('metadata').get('name')
    ct_pod = pod.get_ceph_tools_pod()
    pool_name = helpers.create_unique_resource_name('corrupted', 'pool')
    ct_pod.exec_ceph_cmd(
        f"ceph osd pool create {pool_name} 1 1"
    )
    logger.info('Setting osd noout flag')
    ct_pod.exec_ceph_cmd('ceph osd set noout')
    logger.info(f"Put object into {pool_name}")
    pool_object = 'test_object'
    ct_pod.exec_ceph_cmd(f"rados -p {pool_name} put {pool_object} /etc/passwd")
    logger.info(f"Looking for Placement Group with {pool_object} object")
    pg = ct_pod.exec_ceph_cmd(f"ceph osd map {pool_name} {pool_object}")['pgid']
    logger.info(f"Found Placement Group: {pg}")

    dummy_deployment, dummy_pod = helpers.create_dummy_osd(osd_deployment)

    def corrupt_pg():
        """
        Corrupt PG on one OSD in Ceph pool for 12 minutes and measure it.
        There should be only CephPGRepairTakingTooLong Pending alert as
        it takes 2 hours for it to become Firing.
        This configuration of alert can be observed in ceph-mixins which
        is used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L23
        There should be also CephClusterErrorState alert that takes 10
        minutest to start firing.

        Returns:
            str: Name of corrupted deployment
        """
        # run_time of operation
        run_time = 60 * 12
        nonlocal oc
        nonlocal pool_name
        nonlocal pool_object
        nonlocal dummy_pod
        nonlocal pg
        nonlocal osd_deployment
        nonlocal dummy_deployment

        logger.info(f"Corrupting {pg} PG on {osd_deployment}")
        dummy_pod.exec_sh_cmd_on_pod(
            f"ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-"
            f"{osd_deployment.split('-')[-1]} --pgid {pg} {pool_object} "
            f"set-bytes /etc/shadow --no-mon-config"
        )
        logger.info('Unsetting osd noout flag')
        ct_pod.exec_ceph_cmd('ceph osd unset noout')
        ct_pod.exec_ceph_cmd(f"ceph pg deep-scrub {pg}")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{dummy_deployment}")
        oc.exec_oc_cmd(f"scale --replicas=1 deployment/{osd_deployment}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return osd_deployment

    test_file = os.path.join(measurement_dir, 'measure_corrupt_pg.json')
    measured_op = measure_operation(corrupt_pg, test_file)
    logger.info(f"Deleting pool {pool_name}")
    ct_pod.exec_ceph_cmd(
        f"ceph osd pool delete {pool_name} {pool_name} "
        f"--yes-i-really-really-mean-it"
    )
    logger.info(f"Checking that pool {pool_name} is deleted")

    logger.info(f"Deleting deployment {dummy_deployment}")
    oc.delete(resource_name=dummy_deployment)

    return measured_op

#
# IO Workloads
#


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

    pvc_size = \
        fiojob.get_storageutilization_size(target_percentage, ceph_pool_name)

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
    write_timeout = fiojob.get_timeout(fio_min_mbps, pvc_size)

    test_file = os.path.join(measurement_dir, f"{fixture_name}.json")

    measured_op = workloadfixture.measure_operation(
        lambda: fiojob.write_data_via_fio(
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
        pvc_size_tmp = fiojob.get_storageutilization_size(
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
        fiojob.delete_fio_data(measured_op, fio_job_file, check_pvc_size)

    return measured_op


# Percentages used in fixtures below are based on needs of:
# - alerting tests, which needs to cover alerts for breaching 75% and 85%
#   utilization (see KNIP-635 and document attached there).
# - metrics tests (KNIP-634) which would like to check lower utilizations as
#   well


@pytest.fixture
def workload_storageutilization_05p_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path):
    target_percentage = 0.05
    fixture_name = "workload_storageutilization_05p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_50p_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.5
    fixture_name = "workload_storageutilization_50p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_checksum_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path):
    target_percentage = 0.10
    fixture_name = "workload_storageutilization_checksum_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        with_checksum=True)
    return measured_op


@pytest.fixture
def workload_storageutilization_85p_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.85
    fixture_name = "workload_storageutilization_85p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_95p_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.95
    fixture_name = "workload_storageutilization_95p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_05p_cephfs(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path):
    target_percentage = 0.05
    fixture_name = "workload_storageutilization_05p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_50p_cephfs(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.5
    fixture_name = "workload_storageutilization_50p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_85p_cephfs(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.85
    fixture_name = "workload_storageutilization_85p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_95p_cephfs(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.95
    fixture_name = "workload_storageutilization_95p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def measure_noobaa_exceed_bucket_quota(
    measurement_dir,
    request,
    mcg_obj,
    awscli_pod
):
    """
    Create NooBaa bucket, set its capacity quota to 2GB and fill it with data.

    Returns:
        dict: Contains information about `start` and `stop` time for
        corrupting Ceph Placement Group
    """
    bucket_name = create_unique_resource_name(
        resource_description='bucket',
        resource_type='s3'
    )
    bucket = S3Bucket(
        mcg_obj,
        bucket_name
    )
    mcg_obj.send_rpc_query(
        'bucket_api',
        'update_bucket',
        {
            'name': bucket_name,
            'quota': {
                'unit': 'GIGABYTE',
                'size': 2
            }
        }
    )

    def teardown():
        """
        Delete test bucket.
        """
        bucket.delete()

    request.addfinalizer(teardown)

    def exceed_bucket_quota():
        """
        Upload 5 files with 500MB size into bucket that has quota set to 2GB.

        Returns:
            str: Name of utilized bucket
        """
        nonlocal mcg_obj
        nonlocal bucket_name
        nonlocal awscli_pod
        # run_time of operation
        run_time = 60 * 11
        awscli_pod.exec_cmd_on_pod(
            'dd if=/dev/zero of=/tmp/testfile bs=1M count=500'
        )
        for i in range(1, 6):
            awscli_pod.exec_cmd_on_pod(
                helpers.craft_s3_command(
                    mcg_obj,
                    f"cp /tmp/testfile s3://{bucket_name}/testfile{i}"
                ),
                out_yaml_format=False,
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_endpoint
                ]
            )

        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return bucket_name

    test_file = os.path.join(
        measurement_dir,
        'measure_noobaa_exceed__bucket_quota.json'
    )
    measured_op = measure_operation(exceed_bucket_quota, test_file)
    logger.info(f"Deleting data from bucket {bucket_name}")
    for i in range(1, 6):
        awscli_pod.exec_cmd_on_pod(
            helpers.craft_s3_command(
                mcg_obj,
                f"rm s3://{bucket_name}/testfile{i}"
            ),
            out_yaml_format=False,
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_endpoint
            ]
        )
    return measured_op


@pytest.fixture
def workload_idle(measurement_dir):
    """
    This workload represents a relative long timeframe when nothing special is
    happening, for test cases checking default status of various components
    (eg. no error alert is reported out of sudden, ceph should be healthy ...).

    Besides sheer waiting, this workload also checks that the number of ceph
    components (OSD and MON only) is the same at start and end of this wait,
    and passess the numbers to the test. If the number changes, something not
    exactly expected was happening with the cluster (eg. some node got offline,
    or cluster was expanded, ...) which doesn't match the idea of idle waiting
    and *invalidates the expectations of this workload*. Running test cases
    which expects idle workload in such case would be misleading, so we fail
    the workload in such case.
    """
    def count_ceph_components():
        ct_pod = pod.get_ceph_tools_pod()
        ceph_osd_ls_list = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd ls")
        logger.debug(f"ceph osd ls output: {ceph_osd_ls_list}")
        # the "+ 1" is a WORKAROUND for a bug in exec_ceph_cmd()
        # https://github.com/red-hat-storage/ocs-ci/issues/1152
        osd_num = len(ceph_osd_ls_list) + 1
        mon_num = len(ct_pod.exec_ceph_cmd(ceph_cmd="ceph mon metadata"))
        logger.info(
            f"There are {osd_num} OSDs, {mon_num} MONs")
        return osd_num, mon_num

    def do_nothing():
        sleep_time = 60 * 15  # seconds
        logger.info(f"idle workload is about to sleep for {sleep_time} s")
        osd_num_1, mon_num_1 = count_ceph_components()
        time.sleep(sleep_time)
        osd_num_2, mon_num_2 = count_ceph_components()
        # If this fails, we are likely observing an infra error or unsolicited
        # interference with test cluster from the outside. It could also be a
        # product bug, but this is less likely. See also docstring of this
        # workload fixture.
        msg = (
            "Assumption that nothing serious is happening not met, "
            "number of selected ceph components should be the same")
        assert osd_num_1 == osd_num_2, msg
        assert mon_num_1 == mon_num_2, msg
        assert osd_num_1 >= 3, "OCS cluster should have at least 3 OSDs"
        result = {'osd_num': osd_num_1, 'mon_num': mon_num_1}
        return result

    test_file = os.path.join(measurement_dir, 'measure_workload_idle.json')
    measured_op = measure_operation(do_nothing, test_file)
    return measured_op
