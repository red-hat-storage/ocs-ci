import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers, pre_upgrade, post_upgrade,
    aws_platform_required, filter_insecure_request_warning
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import BS_AUTH_FAILED, BS_OPTIMAL
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from tests.manage.mcg.helpers import (
    retrieve_test_objects_to_pod, sync_object_directory
)

logger = logging.getLogger(__name__)

LOCAL_TESTOBJS_DIR_PATH = '/aws/original'
LOCAL_TEMP_PATH = '/aws/temp'

@aws_platform_required
@filter_insecure_request_warning
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
        bucket_name,
        backingstore1,
        backingstore2
    ) = multiregion_mirror_setup_session
    mcg_bucket_path = f's3://{bucket_name}'

    # Download test objects from the public bucket
    downloaded_objs = retrieve_test_objects_to_pod(
        awscli_pod_session,
        '/aws/original/'
    )

    logger.info(f'Uploading all pod objects to MCG bucket')

    sync_object_directory(
        awscli_pod_session, 's3://' + constants.TEST_FILES_BUCKET,
        LOCAL_TESTOBJS_DIR_PATH
    )

    # Upload test objects to the NooBucket
    sync_object_directory(
        awscli_pod_session,
        LOCAL_TESTOBJS_DIR_PATH,
        mcg_bucket_path,
        mcg_obj_session
    )

    mcg_obj_session.check_if_mirroring_is_done(bucket_name)

    # Bring bucket A down
    mcg_obj_session.toggle_aws_bucket_readwrite(backingstore1['name'])
    mcg_obj_session.check_backingstore_state(
        'backing-store-' + backingstore1['name'],
        BS_AUTH_FAILED
    )

    # Verify integrity of B
    # Retrieve all objects from MCG bucket to result dir in Pod
    sync_object_directory(
        awscli_pod_session,
        mcg_bucket_path,
        LOCAL_TEMP_PATH,
        mcg_obj_session
    )

    # Checksum is compared between original and result object
    for obj in downloaded_objs:
        assert mcg_obj_session.verify_s3_object_integrity(
            original_object_path=f'{LOCAL_TESTOBJS_DIR_PATH}/{obj}',
            result_object_path=f'{LOCAL_TEMP_PATH}/{obj}',
            awscli_pod=awscli_pod_session
        ), 'Checksum comparision between original and result object failed'


@aws_platform_required
@filter_insecure_request_warning
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
        bucket_name,
        backingstore1,
        backingstore2
    ) = multiregion_mirror_setup_session
    mcg_bucket_path = f's3://{bucket_name}'

    # Download test objects from the public bucket
    downloaded_objs = retrieve_test_objects_to_pod(
        awscli_pod_session,
        '/aws/original/'
    )

    # Checksum is compared between original and result object
    for obj in downloaded_objs:
        assert mcg_obj_session.verify_s3_object_integrity(
            original_object_path=f'{LOCAL_TESTOBJS_DIR_PATH}/{obj}',
            result_object_path=f'{LOCAL_TEMP_PATH}/{obj}',
            awscli_pod=awscli_pod_session
        ), 'Checksum comparision between original and result object failed'

    # Clean up the temp dir
    awscli_pod_session.exec_cmd_on_pod(
        command=f'sh -c \"rm -rf {LOCAL_TEMP_PATH}/*\"'
    )

    # Bring B down, bring A up
    logger.info('Blocking bucket B')
    mcg_obj_session.toggle_aws_bucket_readwrite(backingstore2['name'])
    logger.info('Freeing bucket A')
    mcg_obj_session.toggle_aws_bucket_readwrite(
        backingstore1['name'],
        block=False
    )
    mcg_obj_session.check_backingstore_state(
        'backing-store-' + backingstore1['name'],
        BS_OPTIMAL
    )
    mcg_obj_session.check_backingstore_state(
        'backing-store-' + backingstore2['name'],
        BS_AUTH_FAILED
    )

    # Verify integrity of A
    # Retrieve all objects from MCG bucket to result dir in Pod
    sync_object_directory(
        awscli_pod_session,
        mcg_bucket_path,
        LOCAL_TEMP_PATH,
        mcg_obj_session
    )

    # Checksum is compared between original and result object
    for obj in downloaded_objs:
        assert mcg_obj_session.verify_s3_object_integrity(
            original_object_path=f'{LOCAL_TESTOBJS_DIR_PATH}/{obj}',
            result_object_path=f'{LOCAL_TEMP_PATH}/{obj}',
            awscli_pod=awscli_pod_session
        ), 'Checksum comparision between original and result object failed'
    # Bring B up
    mcg_obj_session.toggle_aws_bucket_readwrite(
        backingstore2['name'],
        block=False
    )
    mcg_obj_session.check_backingstore_state(
        'backing-store-' + backingstore2['name'],
        BS_OPTIMAL
    )
