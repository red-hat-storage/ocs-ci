import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade, post_upgrade, aws_platform_required, bugzilla
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import BS_OPTIMAL
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from tests.manage.mcg.helpers import (
    retrieve_test_objects_to_pod, sync_object_directory
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

LOCAL_TESTOBJS_DIR_PATH = '/aws/original'
LOCAL_TEMP_PATH = '/aws/temp'
DOWNLOADED_OBJS = []


# TODO: move this to ocs_ci/ocs/resources/job.py when the file is created
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
    job_name = job.ocp.resource_name
    logger.info(f"Checking number of active pods for job {job_name}")

    def _retrieve_job_state():
        job_obj = job.ocp.get(resource_name=job_name, out_yaml_format=True)
        return job_obj.get('items')[0]['status']['active']

    try:
        for state in TimeoutSampler(
            timeout=timeout,
            sleep=3,
            func=_retrieve_job_state
        ):
            if state == desired_count:
                return True
            else:
                logger.debug(
                    f"Number of active pods for job {job_name}: {state}"
                )
    except TimeoutExpiredError:
        logger.error(
            f"Job {job_name} doesn't have correct number of active pods ({desired_count})"
        )
        return False


@aws_platform_required
@pre_upgrade
def test_fill_bucket(
    mcg_obj_session,
    awscli_pod_session,
    multiregion_mirror_setup_session
):
    """
    Test multi-region bucket creation using the S3 SDK. Fill the bucket for
    upgrade testing.
    """

    (
        bucket,
        backingstore1,
        backingstore2
    ) = multiregion_mirror_setup_session
    mcg_bucket_path = f's3://{bucket.name}'

    # Download test objects from the public bucket
    awscli_pod_session.exec_cmd_on_pod(
        command=f'mkdir {LOCAL_TESTOBJS_DIR_PATH}'
    )
    DOWNLOADED_OBJS = retrieve_test_objects_to_pod(
        awscli_pod_session, LOCAL_TESTOBJS_DIR_PATH
    )

    logger.info('Uploading all pod objects to MCG bucket')

    # Upload test objects to the NooBucket 3 times
    for i in range(3):
        sync_object_directory(
            awscli_pod_session,
            LOCAL_TESTOBJS_DIR_PATH,
            f'{mcg_bucket_path}/{i}/',
            mcg_obj_session
        )

    mcg_obj_session.check_if_mirroring_is_done(bucket.name)
    assert bucket.status == constants.STATUS_BOUND

    # Retrieve all objects from MCG bucket to result dir in Pod
    sync_object_directory(
        awscli_pod_session,
        mcg_bucket_path,
        LOCAL_TEMP_PATH,
        mcg_obj_session
    )

    assert bucket.status == constants.STATUS_BOUND

    # Checksum is compared between original and result object
    for obj in DOWNLOADED_OBJS:
        for i in range(3):
            assert mcg_obj_session.verify_s3_object_integrity(
                original_object_path=f'{LOCAL_TESTOBJS_DIR_PATH}/{obj}',
                result_object_path=f'{LOCAL_TEMP_PATH}/{i}/{obj}',
                awscli_pod=awscli_pod_session
            ), 'Checksum comparison between original and result object failed'
    assert bucket.status == constants.STATUS_BOUND


@aws_platform_required
@post_upgrade
@pytest.mark.polarion_id("OCS-2038")
def test_noobaa_postupgrade(
    mcg_obj_session,
    awscli_pod_session,
    multiregion_mirror_setup_session
):
    """
    Check bucket data and remove resources created in 'test_fill_bucket'.
    """

    (
        bucket,
        backingstore1,
        backingstore2
    ) = multiregion_mirror_setup_session
    mcg_bucket_path = f's3://{bucket.name}'

    # Checksum is compared between original and result object
    for obj in DOWNLOADED_OBJS:
        assert mcg_obj_session.verify_s3_object_integrity(
            original_object_path=f'{LOCAL_TESTOBJS_DIR_PATH}/{obj}',
            result_object_path=f'{LOCAL_TEMP_PATH}/{obj}',
            awscli_pod=awscli_pod_session
        ), 'Checksum comparision between original and result object failed'

    assert bucket.status == constants.STATUS_BOUND

    # Clean up the temp dir
    awscli_pod_session.exec_cmd_on_pod(
        command=f'sh -c \"rm -rf {LOCAL_TEMP_PATH}/*\"'
    )

    mcg_obj_session.check_backingstore_state(
        'backing-store-' + backingstore1['name'],
        BS_OPTIMAL,
        timeout=360
    )
    mcg_obj_session.check_backingstore_state(
        'backing-store-' + backingstore2['name'],
        BS_OPTIMAL,
        timeout=360
    )

    assert bucket.status == constants.STATUS_BOUND

    # Verify integrity of A
    # Retrieve all objects from MCG bucket to result dir in Pod
    sync_object_directory(
        awscli_pod_session,
        mcg_bucket_path,
        LOCAL_TEMP_PATH,
        mcg_obj_session
    )
    assert bucket.status == constants.STATUS_BOUND


@aws_platform_required
@bugzilla('1820974')
@pre_upgrade
def test_buckets_before_upgrade(upgrade_buckets, mcg_obj_session):
    """
    Test that all buckets in cluster are in OPTIMAL state before upgrade.
    """
    for bucket in mcg_obj_session.read_system().get('buckets'):
        assert bucket.get('mode') == BS_OPTIMAL


@aws_platform_required
@bugzilla('1820974')
@post_upgrade
@pytest.mark.polarion_id("OCS-2181")
def test_buckets_after_upgrade(upgrade_buckets, mcg_obj_session):
    """
    Test that all buckets in cluster are in OPTIMAL state after upgrade.
    """
    for bucket in mcg_obj_session.read_system().get('buckets'):
        assert bucket.get('mode') == BS_OPTIMAL


@pre_upgrade
def test_start_upgrade_mcg_io(mcg_workload_job):
    """
    Confirm that there is MCG workload job running before upgrade.
    """
    # wait a few seconds for fio job to start
    assert wait_for_active_pods(mcg_workload_job, 1, timeout=20)


@post_upgrade
@pytest.mark.polarion_id("OCS-2207")
def test_upgrade_mcg_io(mcg_workload_job):
    """
    Confirm that there is MCG workload job running after upgrade.
    """
    assert wait_for_active_pods(mcg_workload_job, 1)
