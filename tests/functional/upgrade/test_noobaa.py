import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade,
    post_upgrade,
    red_squad,
    skipif_aws_creds_are_missing,
    skipif_managed_service,
    mcg,
)
from ocs_ci.ocs.constants import BS_OPTIMAL
from ocs_ci.ocs.bucket_utils import (
    retrieve_test_objects_to_pod,
    sync_object_directory,
    verify_s3_object_integrity,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.mcg_workload import wait_for_active_pods
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)

LOCAL_TESTOBJS_DIR_PATH = "/aws/original"
LOCAL_TEMP_PATH = "/aws/temp"
DOWNLOADED_OBJS = []


@pytest.mark.skip(
    "Skipping due to noobaa-core-0 OOMKill - https://redhat.atlassian.net/browse/DFBUGS-6945"
)
@skipif_aws_creds_are_missing
@skipif_managed_service
@pre_upgrade
@mcg
@red_squad
def test_fill_bucket(
    mcg_obj_session, awscli_pod_session, multiregion_mirror_setup_session
):
    """
    Test multi-region bucket creation using the S3 SDK. Fill the bucket for
    upgrade testing.
    """
    global DOWNLOADED_OBJS

    (bucket, created_backingstores) = multiregion_mirror_setup_session

    mcg_bucket_path = f"s3://{bucket.name}"

    # Download test objects from the public bucket
    awscli_pod_session.exec_cmd_on_pod(command=f"mkdir {LOCAL_TESTOBJS_DIR_PATH}")
    DOWNLOADED_OBJS = retrieve_test_objects_to_pod(
        awscli_pod_session, LOCAL_TESTOBJS_DIR_PATH
    )
    assert DOWNLOADED_OBJS, "No objects downloaded in pre-upgrade phase"

    logger.info("Uploading all pod objects to MCG bucket")

    retry(CommandFailed, tries=3, delay=10)(sync_object_directory)(
        awscli_pod_session,
        LOCAL_TESTOBJS_DIR_PATH,
        mcg_bucket_path,
        mcg_obj_session,
    )

    mcg_obj_session.check_if_mirroring_is_done(bucket.name, timeout=900)
    bucket.verify_health()

    # Retrieve all objects from MCG bucket to result dir in Pod
    retry(CommandFailed, tries=3, delay=10)(sync_object_directory)(
        awscli_pod_session, mcg_bucket_path, LOCAL_TEMP_PATH, mcg_obj_session
    )

    bucket.verify_health()

    # Checksum is compared between original and result object
    for obj in DOWNLOADED_OBJS:
        assert verify_s3_object_integrity(
            original_object_path=f"{LOCAL_TESTOBJS_DIR_PATH}/{obj}",
            result_object_path=f"{LOCAL_TEMP_PATH}/{obj}",
            awscli_pod=awscli_pod_session,
        ), "Checksum comparison between original and result object failed"
    bucket.verify_health()


@pytest.mark.skip(
    "Skipping due to noobaa-core-0 OOMKill - https://redhat.atlassian.net/browse/DFBUGS-6945"
)
@skipif_aws_creds_are_missing
@skipif_managed_service
@post_upgrade
@pytest.mark.polarion_id("OCS-2038")
@mcg
@red_squad
def test_noobaa_postupgrade(
    mcg_obj_session, awscli_pod_session, multiregion_mirror_setup_session
):
    """
    Check bucket data and remove resources created in 'test_fill_bucket'.
    """

    (bucket, created_backingstores) = multiregion_mirror_setup_session
    backingstore1 = created_backingstores[0]
    backingstore2 = created_backingstores[1]
    mcg_bucket_path = f"s3://{bucket.name}"

    assert (
        DOWNLOADED_OBJS
    ), "No pre-upgrade objects available for post-upgrade integrity validation"
    # Checksum is compared between original and result object
    for obj in DOWNLOADED_OBJS:
        assert verify_s3_object_integrity(
            original_object_path=f"{LOCAL_TESTOBJS_DIR_PATH}/{obj}",
            result_object_path=f"{LOCAL_TEMP_PATH}/{obj}",
            awscli_pod=awscli_pod_session,
        ), "Checksum comparision between original and result object failed"

    bucket.verify_health(timeout=100)

    # Clean up the temp dir
    awscli_pod_session.exec_cmd_on_pod(command=f'sh -c "rm -rf {LOCAL_TEMP_PATH}/*"')

    mcg_obj_session.check_backingstore_state(
        "backing-store-" + backingstore1.name, BS_OPTIMAL, timeout=360
    )
    mcg_obj_session.check_backingstore_state(
        "backing-store-" + backingstore2.name, BS_OPTIMAL, timeout=360
    )

    bucket.verify_health()

    # Verify integrity of A
    # Retrieve all objects from MCG bucket to result dir in Pod
    retry(CommandFailed, tries=3, delay=10)(sync_object_directory)(
        awscli_pod_session, mcg_bucket_path, LOCAL_TEMP_PATH, mcg_obj_session
    )
    bucket.verify_health()


@skipif_managed_service
@pre_upgrade
@mcg
@red_squad
def test_buckets_before_upgrade(upgrade_buckets, mcg_obj_session):
    """
    Test that all buckets in cluster are in OPTIMAL state before upgrade.
    """
    for bucket in mcg_obj_session.read_system().get("buckets"):
        assert bucket.get("mode") == BS_OPTIMAL


@skipif_managed_service
@post_upgrade
@pytest.mark.polarion_id("OCS-2181")
@mcg
@red_squad
def test_buckets_after_upgrade(upgrade_buckets, mcg_obj_session):
    """
    Test that all buckets in cluster are in OPTIMAL state after upgrade.
    """
    for bucket in mcg_obj_session.read_system().get("buckets"):
        assert bucket.get("mode") == BS_OPTIMAL


@pre_upgrade
@skipif_managed_service
@mcg
@red_squad
def test_start_upgrade_mcg_io(mcg_workload_job):
    """
    Confirm that there is MCG workload job running before upgrade.
    """
    # wait a few seconds for fio job to start
    assert wait_for_active_pods(
        mcg_workload_job, 1, timeout=20
    ), f"Job {mcg_workload_job.name} doesn't have any running pod"


# TODO: Remove this skip once the feature is implemented
@pytest.mark.skip("Skipping until https://url.corp.redhat.com/c960ed4 is complete")
@post_upgrade
@skipif_managed_service
@pytest.mark.polarion_id("OCS-2207")
@mcg
@red_squad
def deprecated_test_upgrade_mcg_io(mcg_workload_job):
    """
    Confirm that there is MCG workload job running after upgrade.
    """

    assert wait_for_active_pods(
        mcg_workload_job, 1
    ), f"Job {mcg_workload_job.name} doesn't have any running pod"
