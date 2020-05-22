import logging
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade, post_upgrade, aws_platform_required, bugzilla
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import BS_OPTIMAL
from tests.manage.mcg.helpers import (
    retrieve_anon_s3_resource, sync_object_directory
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
    test_objects = retrieve_anon_s3_resource().Bucket(
        constants.TEST_FILES_BUCKET
    ).objects.all()

    for obj in test_objects:
        logger.info(f'Downloading {obj.key} from AWS test bucket')
        awscli_pod_session.exec_cmd_on_pod(
            command=(
                f'sh -c "wget -P {LOCAL_TESTOBJS_DIR_PATH} '
                f'https://{constants.TEST_FILES_BUCKET}.s3.amazonaws.com/{obj.key}"'
            )
        )
        DOWNLOADED_OBJS.append(f'{obj.key}')
        # Use 3x time more objects than there is in test objects pod
        for i in range(2):
            awscli_pod_session.exec_cmd_on_pod(
                command=f'sh -c "'
                        f'cp {LOCAL_TESTOBJS_DIR_PATH}/{obj.key} '
                        f'{LOCAL_TESTOBJS_DIR_PATH}/{obj.key}.{i}"'
            )
            DOWNLOADED_OBJS.append(f'{obj.key}.{i}')

    logger.info('Uploading all pod objects to MCG bucket')

    sync_object_directory(
        awscli_pod_session,
        's3://' + constants.TEST_FILES_BUCKET,
        LOCAL_TESTOBJS_DIR_PATH
    )

    # Upload test objects to the NooBucket
    sync_object_directory(
        awscli_pod_session,
        LOCAL_TESTOBJS_DIR_PATH,
        mcg_bucket_path,
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
        assert mcg_obj_session.verify_s3_object_integrity(
            original_object_path=f'{LOCAL_TESTOBJS_DIR_PATH}/{obj}',
            result_object_path=f'{LOCAL_TEMP_PATH}/{obj}',
            awscli_pod=awscli_pod_session
        ), 'Checksum comparision between original and result object failed'
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
    assert bucket.status == constants.STATUS_BOUND


@pre_upgrade
def test_start_upgrade_mcg_io(mcg_workload_job):
    """
    Confirm that there is MCG workload job running before upgrade.
    """
    job_status = None
    # wait a few seconds for fio job to start
    for i in range(0, 5):
        job = mcg_workload_job.ocp.get(
            resource_name=mcg_workload_job.ocp.resource_name,
            out_yaml_format=True
        )
        # 'active' attribute holds information about how many pods are running
        job_status = job['items'][0]['status']['active']
        if job_status == 1:
            break
        time.sleep(3)
    assert job_status == 1


@post_upgrade
@pytest.mark.polarion_id("OCS-2207")
def test_upgrade_mcg_io(mcg_workload_job):
    """
    Confirm that there is MCG workload job running after upgrade.
    """
    job = mcg_workload_job.ocp.get(
        resource_name=mcg_workload_job.ocp.resource_name,
        out_yaml_format=True
    )
    # 'active' attribute holds information about how many pods are running
    job_status = job['items'][0]['status']['active']
    assert job_status == 1
