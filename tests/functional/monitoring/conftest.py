# -*- coding: utf8 -*-

import logging
import os
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.fiojob import workload_fio_storageutilization
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_nodes,
    unschedule_nodes,
    schedule_nodes,
)
from ocs_ci.ocs import rados_utils
from ocs_ci.ocs.resources import deployment, pod, storageconsumer
from ocs_ci.ocs.resources.objectbucket import MCGCLIBucket
from ocs_ci.ocs.resources.pod import get_mon_pods, get_osd_pods
from ocs_ci.utility.kms import get_kms_endpoint, set_kms_endpoint
from ocs_ci.utility.pagerduty import get_pagerduty_service_id
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler, exec_cmd
from ocs_ci.utility.workloadfixture import measure_operation, is_measurement_done
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import create_unique_resource_name
import ocs_ci.ocs.exceptions


logger = logging.getLogger(__name__)


@pytest.fixture
def measure_stop_ceph_mgr(measurement_dir, threading_lock):
    """
    Downscales Ceph Manager deployment, measures the time when it was
    downscaled and monitors alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph Manager pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA["cluster_namespace"],
        threading_lock=threading_lock,
    )
    mgr_deployments = oc.get(selector=constants.MGR_APP_LABEL)["items"]
    mgr = mgr_deployments[0]["metadata"]["name"]

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

    test_file = os.path.join(measurement_dir, "measure_stop_ceph_mgr.json")
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        # It seems that it takes longer to propagate incidents to PagerDuty.
        # Adding 3 extra minutes
        measured_op = measure_operation(
            stop_mgr,
            test_file,
            minimal_time=60 * 9,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
    else:
        measured_op = measure_operation(
            stop_mgr, test_file, threading_lock=threading_lock
        )
    logger.info(f"Upscaling deployment {mgr} back to 1")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mgr}")

    # wait for ceph to return into HEALTH_OK state after mgr deployment
    # is returned back to normal
    ceph_health_check(tries=20, delay=15)

    return measured_op


@pytest.fixture
def create_mon_quorum_loss(create_mon_quorum_loss=False):
    """
    Number of mon to go down in the cluster, so that accordingly
    CephMonQuorumRisk or CephMonQuorumLost alerts are seen

    Args:
        mon_quorum_lost (bool): True, if mon quorum to be lost. False Otherwise.

    Returns:
        mon_quorum_lost (bool): True, if all mons down expect one mon
            so that mon quorum lost. Otherwise False

    """
    return create_mon_quorum_loss


@pytest.fixture
def measure_stop_ceph_mon(measurement_dir, create_mon_quorum_loss, threading_lock):
    """
    Downscales Ceph Monitor deployment, measures the time when it was
    downscaled and monitors alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph Monitor pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA["cluster_namespace"],
        threading_lock=threading_lock,
    )
    mon_deployments = oc.get(selector=constants.MON_APP_LABEL)["items"]
    mons = [deployment["metadata"]["name"] for deployment in mon_deployments]

    # get monitor deployments to stop,
    # if mon quorum to be lost split_index will be 1
    # else leave even number of monitors
    split_index = (
        1 if create_mon_quorum_loss else len(mons) // 2 if len(mons) > 3 else 2
    )
    mons_to_stop = mons[split_index:]
    logger.info(f"Monitors to stop: {mons_to_stop}")
    logger.info(f"Monitors left to run: {mons[:split_index]}")

    # run_time of operation
    run_time = 60 * 14

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
        nonlocal oc
        nonlocal mons_to_stop
        for mon in mons_to_stop:
            logger.info(f"Downscaling deployment {mon} to 0")
            oc.exec_oc_cmd(f"scale --replicas=0 deployment/{mon}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return mons_to_stop

    test_file = os.path.join(
        measurement_dir, f"measure_stop_ceph_mon_{split_index}.json"
    )
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        # It seems that it takes longer to propagate incidents to PagerDuty.
        # Adding 6 extra minutes so that alert is actually triggered and
        # unscheduling worker nodes so that monitor is not replaced
        worker_node_names = [
            node.name for node in get_nodes(node_type=constants.WORKER_MACHINE)
        ]
        unschedule_nodes(worker_node_names)
        measured_op = measure_operation(
            stop_mon,
            test_file,
            minimal_time=60 * 20,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
        schedule_nodes(worker_node_names)
    else:
        measured_op = measure_operation(
            stop_mon, test_file, threading_lock=threading_lock
        )

    # expected minimal downtime of a mon inflicted by this fixture
    measured_op["min_downtime"] = run_time - (60 * 2)

    # get new list of monitors to make sure that new monitors were deployed
    mon_deployments = oc.get(selector=constants.MON_APP_LABEL)["items"]
    mons = [deployment["metadata"]["name"] for deployment in mon_deployments]

    # check that downscaled monitors are removed as OCS should redeploy them
    # but only when we are running this for the first time
    check_old_mons_deleted = all(mon not in mons for mon in mons_to_stop)
    if measured_op["first_run"] and not check_old_mons_deleted:
        for mon in mons_to_stop:
            logger.info(f"Upscaling deployment {mon} back to 1")
            oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mon}")
        if (
            not split_index == 1
            and config.ENV_DATA["platform"].lower()
            not in constants.MANAGED_SERVICE_PLATFORMS
        ):
            msg = f"Downscaled monitors {mons_to_stop} were not replaced"
            assert check_old_mons_deleted, msg

    # wait for ceph to return into HEALTH_OK state after mon deployment
    # is returned back to normal
    ceph_health_check(tries=40, delay=15)

    return measured_op


@pytest.fixture
def measure_stop_ceph_osd(measurement_dir, threading_lock):
    """
    Downscales Ceph osd deployment, measures the time when it was
    downscaled and alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph osd pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA.get("cluster_namespace"),
        threading_lock=threading_lock,
    )
    osd_deployments = oc.get(selector=constants.OSD_APP_LABEL).get("items")
    osds = [deployment.get("metadata").get("name") for deployment in osd_deployments]

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
        run_time = 60 * 16
        nonlocal oc
        nonlocal osd_to_stop
        logger.info(f"Downscaling deployment {osd_to_stop} to 0")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{osd_to_stop}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return osd_to_stop

    test_file = os.path.join(measurement_dir, "measure_stop_ceph_osd.json")
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        # It seems that it takes longer to propagate incidents to PagerDuty.
        # Adding 3 extra minutes
        measured_op = measure_operation(
            stop_osd,
            test_file,
            minimal_time=60 * 19,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
    else:
        measured_op = measure_operation(
            stop_osd, test_file, threading_lock=threading_lock
        )
    logger.info(f"Upscaling deployment {osd_to_stop} back to 1")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{osd_to_stop}")

    # wait for ceph to return into HEALTH_OK state after osd deployment
    # is returned back to normal
    # The check is increased to cover for slow ops events in case of larger clusters
    # with uploaded data
    ceph_health_check(tries=40, delay=15)

    return measured_op


@pytest.fixture
def measure_corrupt_pg(request, measurement_dir, threading_lock):
    """
    Create Ceph pool and corrupt Placement Group on one of OSDs, measures the
    time when it was corrupted and records alerts that were triggered during
    this event.

    Returns:
        dict: Contains information about `start` and `stop` time for
            corrupting Ceph Placement Group
    """
    osd_deployment = deployment.get_osd_deployments()[0]
    original_deployment_revision = osd_deployment.revision
    ct_pod = pod.get_ceph_tools_pod()
    pool_name = helpers.create_unique_resource_name("corrupted", "pool")
    ct_pod.exec_ceph_cmd(f"ceph osd pool create {pool_name} 1 1")
    ct_pod.exec_ceph_cmd(f"ceph osd pool application enable {pool_name} rbd")

    def teardown():
        """
        Make sure that corrupted pool is deleted and ceph health is ok
        """
        nonlocal pool_name
        nonlocal osd_deployment
        nonlocal original_deployment_revision
        logger.info(f"Deleting pool {pool_name}")
        ct_pod.exec_ceph_cmd(
            f"ceph osd pool delete {pool_name} {pool_name} "
            f"--yes-i-really-really-mean-it"
        )
        logger.info("Unsetting osd noout flag")
        ct_pod.exec_ceph_cmd("ceph osd unset noout")
        logger.info("Unsetting osd noscrub flag")
        ct_pod.exec_ceph_cmd("ceph osd unset noscrub")
        logger.info("Unsetting osd nodeep-scrub flag")
        ct_pod.exec_ceph_cmd("ceph osd unset nodeep-scrub")
        logger.info(f"Checking that pool {pool_name} is deleted")
        logger.info(
            f"Restoring deployment {osd_deployment.name} "
            f"to its original revision: {original_deployment_revision}"
        )
        if original_deployment_revision:
            osd_deployment.set_revision(original_deployment_revision)
            # unset original_deployment_revision because revision number is deleted when used
            original_deployment_revision = False
        # wait for ceph to return into HEALTH_OK state after osd deployment
        # is returned back to normal
        ceph_health_check(tries=20, delay=15)

    request.addfinalizer(teardown)
    logger.info("Setting osd noout flag")
    ct_pod.exec_ceph_cmd("ceph osd set noout")
    logger.info(f"Put object into {pool_name}")
    pool_object = "test_object"
    ct_pod.exec_ceph_cmd(f"rados -p {pool_name} put {pool_object} /etc/passwd")
    logger.info(f"Corrupting pool {pool_name} on {osd_deployment.name}")
    rados_utils.corrupt_pg(osd_deployment, pool_name, pool_object)

    def wait_with_corrupted_pg():
        """
        PG on one OSD in Ceph pool should be corrupted at the time of execution
        of this function. Measure it for 14 minutes.
        There should be only CephPGRepairTakingTooLong Pending alert as
        it takes 2 hours for it to become Firing.
        This configuration of alert can be observed in ceph-mixins which
        is used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L23
        There should be also CephClusterErrorState alert that takes 10
        minutest to start firing.

        Returns:
            str: Name of corrupted pod
        """
        nonlocal osd_deployment
        # run_time of operation
        run_time = 60 * 14
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return osd_deployment.name

    test_file = os.path.join(measurement_dir, "measure_corrupt_pg.json")

    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        # It seems that it takes longer to propagate incidents to PagerDuty.
        # Adding 3 extra minutes
        measured_op = measure_operation(
            wait_with_corrupted_pg,
            test_file,
            minimal_time=60 * 17,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
    else:
        measured_op = measure_operation(
            wait_with_corrupted_pg, test_file, threading_lock=threading_lock
        )

    teardown()

    return measured_op


#
# IO Workloads
#

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
    tmp_path,
    threading_lock,
):
    fixture_name = "workload_storageutilization_05p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=0.05,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_50p_rbd(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    supported_configuration,
    threading_lock,
):
    fixture_name = "workload_storageutilization_50p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=0.5,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_checksum_rbd(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    threading_lock,
):
    fixture_name = "workload_storageutilization_checksum_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_size=10,
        with_checksum=True,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_85p_rbd(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    supported_configuration,
    threading_lock,
):
    fixture_name = "workload_storageutilization_85p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=0.85,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_97p_rbd(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    supported_configuration,
    threading_lock,
):
    fixture_name = "workload_storageutilization_97p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=0.97,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_05p_cephfs(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    threading_lock,
):
    fixture_name = "workload_storageutilization_05p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=0.05,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_50p_cephfs(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    supported_configuration,
    threading_lock,
):
    fixture_name = "workload_storageutilization_50p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=0.5,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_85p_cephfs(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    supported_configuration,
    threading_lock,
):
    fixture_name = "workload_storageutilization_85p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=0.85,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_97p_cephfs(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    supported_configuration,
    threading_lock,
):
    fixture_name = "workload_storageutilization_97p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=0.97,
        threading_lock=threading_lock,
    )
    return measured_op


# storage utilization of constant sizes


@pytest.fixture
def workload_storageutilization_10g_rbd(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    threading_lock,
):
    fixture_name = "workload_storageutilization_10G_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_size=10,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def workload_storageutilization_10g_cephfs(
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    threading_lock,
):
    fixture_name = "workload_storageutilization_10G_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_size=10,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture
def measure_noobaa_exceed_bucket_quota(
    measurement_dir, request, mcg_obj, awscli_pod, threading_lock
):
    """
    Create NooBaa bucket, set its capacity quota to 2GB and fill it with data.

    Returns:
        dict: Contains information about `start` and `stop` time for
        corrupting Ceph Placement Group
    """
    bucket_name = create_unique_resource_name(
        resource_description="bucket", resource_type="s3"
    )
    quota = "2Gi"
    bucket = MCGCLIBucket(bucket_name, mcg=mcg_obj, quota=quota)
    bucket_info = mcg_obj.get_bucket_info(bucket.name)
    logger.info(f"Bucket {bucket.name} storage: {bucket_info['storage']}")
    logger.info(f"Bucket {bucket.name} data: {bucket_info['data']}")

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
        run_time = 60 * 14
        awscli_pod.exec_cmd_on_pod("dd if=/dev/zero of=/tmp/testfile bs=1M count=500")
        for i in range(1, 6):
            awscli_pod.exec_cmd_on_pod(
                craft_s3_command(
                    f"cp /tmp/testfile s3://{bucket_name}/testfile{i}", mcg_obj
                ),
                out_yaml_format=False,
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_endpoint,
                ],
            )

        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return bucket_name

    test_file = os.path.join(
        measurement_dir, "measure_noobaa_exceed__bucket_quota.json"
    )
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        measured_op = measure_operation(
            exceed_bucket_quota,
            test_file,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
    else:
        measured_op = measure_operation(
            exceed_bucket_quota, test_file, threading_lock=threading_lock
        )

    bucket_info = mcg_obj.get_bucket_info(bucket.name)
    logger.info(f"Bucket {bucket.name} storage: {bucket_info['storage']}")
    logger.info(f"Bucket {bucket.name} data: {bucket_info['data']}")

    logger.info(f"Deleting data from bucket {bucket_name}")
    for i in range(1, 6):
        awscli_pod.exec_cmd_on_pod(
            craft_s3_command(f"rm s3://{bucket_name}/testfile{i}", mcg_obj),
            out_yaml_format=False,
            secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint],
        )
    return measured_op


@pytest.fixture
def workload_idle(measurement_dir, threading_lock):
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

    @retry(CommandFailed, text_in_exception="failed to get OSD and MON pods")
    def count_ceph_components():
        ceph_osd_ls_list = get_osd_pods()
        osd_num = len(ceph_osd_ls_list)
        mon_num = len(get_mon_pods())
        logger.info(f"There are {osd_num} OSDs, {mon_num} MONs")
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
            "number of selected ceph components should be the same"
        )
        assert osd_num_1 == osd_num_2, msg
        assert mon_num_1 == mon_num_2, msg
        assert osd_num_1 >= 3, "OCS cluster should have at least 3 OSDs"
        result = {"osd_num": osd_num_1, "mon_num": mon_num_1}
        return result

    test_file = os.path.join(measurement_dir, "measure_workload_idle.json")

    # if io_in_bg detected, request and wait for it's temporary shutdown
    # but only if the fixture will actually run and measure the workload
    restart_io_in_bg = False
    if not is_measurement_done(test_file) and config.RUN.get("io_in_bg"):
        logger.info("io_in_bg detected, trying to pause it via load_status")
        config.RUN["load_status"] = "to_be_paused"
        restart_io_in_bg = True
        timeout = 600
        sleep_time = 60
        ts = TimeoutSampler(timeout, sleep_time, config.RUN.get, "load_status")
        try:
            for load_status in ts:
                if load_status == "paused":
                    logger.info("io_in_bg seems paused now")
                    break
        except ocs_ci.ocs.exceptions.TimeoutExpiredError as ex:
            error_msg = (
                f"io_in_bf failed to stop after {timeout} timeout, "
                "bug in io_in_bf (of ocs-ci) prevents execution of "
                "test cases which uses this fixture, rerun the affected "
                "test cases in a dedicated run and consider ocs-ci fix"
            )
            logger.error(ex)
            logger.error(error_msg)
            raise Exception(error_msg)
    else:
        logger.debug("io_in_bg not detected, good")

    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        measured_op = measure_operation(
            do_nothing,
            test_file,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
    else:
        measured_op = measure_operation(
            do_nothing, test_file, threading_lock=threading_lock
        )
    if restart_io_in_bg:
        logger.info("reverting load_status to resume io_in_bg")
        config.RUN["load_status"] = "to_be_resumed"
    return measured_op


@pytest.fixture
def measure_stop_rgw(measurement_dir, request, rgw_deployments, threading_lock):
    """
    Downscales RGW deployments, measures the time when it was
    downscaled and monitors alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            RGW pods

    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA["cluster_namespace"],
        threading_lock=threading_lock,
    )

    def stop_rgw():
        """
        Downscale RGW interface deployments for 5 minutes.

        Returns:
            str: Name of downscaled deployment

        """
        # run_time of operation
        run_time = 60 * 5
        nonlocal oc
        nonlocal rgw_deployments
        for rgw_deployment in rgw_deployments:
            rgw = rgw_deployment["metadata"]["name"]
            logger.info(f"Downscaling deployment {rgw} to 0")
            oc.exec_oc_cmd(f"scale --replicas=0 deployment/{rgw}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return rgw_deployments

    test_file = os.path.join(measurement_dir, "measure_stop_rgw.json")
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        # It seems that it takes longer to propagate incidents to PagerDuty.
        # Adding 3 extra minutes
        measured_op = measure_operation(
            stop_rgw,
            test_file,
            minimal_time=60 * 8,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
    else:
        measured_op = measure_operation(
            stop_rgw, test_file, threading_lock=threading_lock
        )

    logger.info("Return RGW pods")
    for rgw_deployment in rgw_deployments:
        rgw = rgw_deployment["metadata"]["name"]
        logger.info(f"Upscaling deployment {rgw} to 1")
        oc.exec_oc_cmd(f"scale --replicas=1 deployment/{rgw}")

    return measured_op


@pytest.fixture
def measure_noobaa_ns_target_bucket_deleted(
    measurement_dir,
    request,
    bucket_factory,
    namespace_store_factory,
    cld_mgr,
    threading_lock,
):
    """
    Create Namespace bucket from 2 namespace resources. Delete target bucket
    used in one of the resources.

    Returns:
        dict: Contains information about `start` and `stop` time for deleting
            target bucket

    """
    logger.info("Create the namespace resources and verify health")
    nss_tup = ("oc", {"aws": [(2, "us-east-2")]})
    ns_stores = namespace_store_factory(*nss_tup)

    logger.info("Create the namespace bucket on top of the namespace resource")
    bucketclass_dict = {
        "interface": "OC",
        "namespace_policy_dict": {
            "type": "Multi",
            "namespacestores": ns_stores,
        },
    }
    ns_bucket = bucket_factory(
        amount=1,
        interface=bucketclass_dict["interface"],
        bucketclass=bucketclass_dict,
    )

    def delete_target_bucket():
        """
        Delete target bucket from NS store.

        Returns:
            str: Name of deleted target bucket

        """
        # run_time of operation
        run_time = 60 * 12
        nonlocal ns_stores
        nonlocal cld_mgr

        cld_mgr.aws_client.delete_uls(ns_stores[0].uls_name)
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return ns_stores[0].uls_name

    test_file = os.path.join(measurement_dir, "measure_delete_target_bucket.json")
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        measured_op = measure_operation(
            delete_target_bucket,
            test_file,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
    else:
        measured_op = measure_operation(
            delete_target_bucket, test_file, threading_lock=threading_lock
        )
    logger.info("Delete NS bucket, bucketclass and NS store so that alert is cleared")
    ns_bucket[0].delete()
    ns_bucket[0].bucketclass.delete()
    ns_stores[0].delete()
    return measured_op


@pytest.fixture
def measure_stop_worker_nodes(request, measurement_dir, nodes, threading_lock):
    """
    Stop worker nodes that doesn't contain RGW (so that alerts are triggered
    correctly), measure the time when it was stopped and monitors alerts that
    were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            worker node

    """
    mgr_pod = pod.get_mgr_pods()[0]
    mgr_node = pod.get_pod_node(mgr_pod)
    test_nodes = [
        worker_node
        for worker_node in get_nodes(node_type=constants.WORKER_MACHINE)
        if worker_node.name != mgr_node.name
    ]

    def stop_nodes():
        """
        Turn off test nodes for 5 minutes.

        Returns:
            list: Names of nodes that were turned down

        """
        # run_time of operation
        run_time = 60 * 5
        nonlocal test_nodes
        node_names = [node.name for node in test_nodes]
        logger.info(f"Turning off nodes {node_names}")
        nodes.stop_nodes(nodes=test_nodes)
        # Validate node reached NotReady state
        wait_for_nodes_status(node_names=node_names, status=constants.NODE_NOT_READY)
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return node_names

    def finalizer():
        nodes.restart_nodes_by_stop_and_start_teardown()
        assert ceph_health_check(), "Ceph cluster health is not OK"
        logger.info("Ceph cluster health is OK")

    request.addfinalizer(finalizer)

    test_file = os.path.join(measurement_dir, "measure_stop_nodes.json")
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        # It seems that it takes longer to propagate incidents to PagerDuty.
        # Adding 3 extra minutes
        measured_op = measure_operation(
            stop_nodes,
            test_file,
            minimal_time=60 * 8,
            pagerduty_service_ids=[get_pagerduty_service_id()],
            threading_lock=threading_lock,
        )
    else:
        measured_op = measure_operation(
            stop_nodes, test_file, threading_lock=threading_lock
        )
    logger.info("Turning on nodes")
    try:
        nodes.start_nodes(nodes=test_nodes)
    except CommandFailed:
        logger.warning(
            "Nodes were not found: they were probably recreated. Check ceph health below"
        )
    # Validate all nodes are in READY state and up
    retry((CommandFailed, ResourceWrongStatusException,), tries=60, delay=15,)(
        wait_for_nodes_status
    )(timeout=900)

    # wait for ceph to return into HEALTH_OK state after mgr deployment
    # is returned back to normal
    ceph_health_check(tries=20, delay=15)

    return measured_op


@pytest.fixture
def measure_rewrite_kms_endpoint(request, measurement_dir, threading_lock):
    """
    Change kms endpoint address to invalid value, measure the time when it was
    rewritten and alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for rewritting
            the endpont
    """
    original_endpoint = get_kms_endpoint()
    logger.debug(f"Original kms endpoint is {original_endpoint}")

    def change_kms_endpoint():
        """
        Change value of KMS configuration for 3 minutes.
        """
        # run_time of operation
        run_time = 60 * 3
        invalid_endpoint = original_endpoint[0:-1]
        logger.info(
            f"Changing value of kms endpoint in cluster configuration to {invalid_endpoint}"
        )
        set_kms_endpoint(invalid_endpoint)
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return

    def teardown():
        logger.info(f"Restoring KMS endpoint to {original_endpoint}")
        set_kms_endpoint(original_endpoint)
        logger.info("KMS endpoint restored")

    request.addfinalizer(teardown)

    test_file = os.path.join(measurement_dir, "measure_rewrite_kms_endpoint.json")
    measured_op = measure_operation(
        change_kms_endpoint, test_file, threading_lock=threading_lock
    )

    teardown()

    return measured_op


@pytest.fixture
def measure_change_client_ocs_version_and_stop_heartbeat(
    request, measurement_dir, threading_lock
):
    """
    Change ocs version of client to a different number, measure the time when it was
    rewritten and alerts that were triggered during this event. To achieve the change
    will be also stopped heartbeat cron job on the client to ensure that the version
    is not rewritten.

    Returns:
        dict: Contains information about `start` and `stop` time for rewritting
            the client version

    """

    current_version = storageconsumer.get_ocs_version()
    logger.info(f"Reported client version: {current_version}")
    original_cluster = config.cluster_ctx
    logger.info(f"Provider cluster key: {original_cluster}")
    logger.info("Switch to client cluster")
    config.switch_to_consumer()
    client_cluster = config.cluster_ctx
    logger.info(f"Client cluster key: {client_cluster}")
    cluster_id = exec_cmd(
        "oc get clusterversion version -o jsonpath='{.spec.clusterID}'"
    )
    client_name = f"storageconsumer-{cluster_id}"
    client = storageconsumer.StorageConsumer()

    def change_client_version():
        """
        Stop heartbeat and change value of ocs version in storage client resource
        for 3 minutes.

        """
        nonlocal client
        nonlocal original_cluster
        # run_time of operation
        run_time = 60 * 3
        client.stop_heartbeat()
        client.set_ocs_version("4.13.0")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        logger.info(f"Switch to original cluster ({original_cluster})")
        config.switch_ctx(original_cluster)
        return

    def teardown():
        nonlocal client
        nonlocal original_cluster
        nonlocal client_cluster
        logger.info(f"Switch to client cluster ({client_cluster})")
        config.switch_ctx(client_cluster)
        client.resume_heartbeat()
        logger.info(f"Switch to original cluster ({original_cluster})")
        config.switch_ctx(original_cluster)

    request.addfinalizer(teardown)

    test_file = os.path.join(measurement_dir, "measure_change_client_version.json")
    measured_op = measure_operation(
        change_client_version,
        test_file,
        threading_lock=threading_lock,
        metadata={"client_name": client_name},
    )

    teardown()

    return measured_op
