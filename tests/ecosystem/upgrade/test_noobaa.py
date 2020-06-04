import logging
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade, post_upgrade, aws_platform_required, bugzilla
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import BS_OPTIMAL
from tests.manage.mcg.helpers import (
    retrieve_test_objects_to_pod, sync_object_directory
)

logger = logging.getLogger(__name__)

LOCAL_TESTOBJS_DIR_PATH = '/aws/original'
LOCAL_TEMP_PATH = '/aws/temp'
DOWNLOADED_OBJS = []


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
    assert bucket.status == constants.STATUS_BOUND, (
        f"bucket {bucket.name} doesn't have {constants.STATUS_BOUND} "
        f"status, it has {bucket.status} status"
    )

    # Retrieve all objects from MCG bucket to result dir in Pod
    sync_object_directory(
        awscli_pod_session,
        mcg_bucket_path,
        LOCAL_TEMP_PATH,
        mcg_obj_session
    )

    assert bucket.status == constants.STATUS_BOUND, (
        f"bucket {bucket.name} doesn't have {constants.STATUS_BOUND} "
        f"status, it has {bucket.status} status"
    )

    # Checksum is compared between original and result object
    for obj in DOWNLOADED_OBJS:
        for i in range(3):
            assert mcg_obj_session.verify_s3_object_integrity(
                original_object_path=f'{LOCAL_TESTOBJS_DIR_PATH}/{obj}',
                result_object_path=f'{LOCAL_TEMP_PATH}/{i}/{obj}',
                awscli_pod=awscli_pod_session
            ), 'Checksum comparison between original and result object failed'
    assert bucket.status == constants.STATUS_BOUND, (
        f"bucket {bucket.name} doesn't have {constants.STATUS_BOUND} "
        f"status, it has {bucket.status} status"
    )


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
    assert bucket.status == constants.STATUS_BOUND


@pre_upgrade
def test_start_upgrade_mcg_io(mcg_workload_job):
    """
    Confirm that there is MCG workload job running before upgrade.
    """
    job_status = None
    job_name = mcg_workload_job.ocp.resource_name
    # wait a few seconds for fio job to start
    logger.info(f"Checking number of active pods for job {job_name}")
    for i in range(0, 5):
        job = mcg_workload_job.ocp.get(
            resource_name=job_name,
            out_yaml_format=True
        )
        # 'active' attribute holds information about how many pods are running
        job_status = job['items'][0]['status']['active']
        if job_status == 1:
            break
        time.sleep(3)
    assert job_status == 1, (
        f"Number of running pods for job {job_name} is not 1, "
        f"it is {job_status}"
    )


@post_upgrade
@pytest.mark.polarion_id("OCS-2207")
def test_upgrade_mcg_io(mcg_workload_job):
    """
    Confirm that there is MCG workload job running after upgrade.
    """
    job_name = mcg_workload_job.ocp.resource_name
    logger.info(f"Checking number of active pods for job {job_name}")
    job = mcg_workload_job.ocp.get(
        resource_name=job_name,
        out_yaml_format=True
    )
    # 'active' attribute holds information about how many pods are running
    job_status = job['items'][0]['status']['active']
    assert job_status == 1, (
        f"Number of running pods for job {job_name} is not 1, "
        f"it is {job_status}"
    )
