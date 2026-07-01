import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    MCGTest,
    ignore_leftover_label,
    mcg,
    red_squad,
    skipif_mcg_only,
    tier1,
    tier2,
    polarion_id,
    skipif_external_mode,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    rm_object_recursive,
    s3_get_object,
    s3_head_object,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.bucket_logging_manager import BucketLoggingManager
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs

logger = logging.getLogger(__name__)

RECONCILE_WAIT = 30


@red_squad
@mcg
@ignore_leftover_label(constants.CUSTOM_MCG_LABEL)
@skipif_external_mode
class TestBucketLogs(MCGTest):
    """
    Test the MCG bucket logs feature
    """

    @pytest.fixture(scope="class", autouse=True)
    def reduce_log_uploader_worker_interval(self, add_env_vars_to_noobaa_core_class):
        """
        Reduce the interval in which the bucket log uploader runs
        """
        new_interval_in_miliseconds = 60 * 1000
        add_env_vars_to_noobaa_core_class(
            [
                (
                    constants.BUCKET_LOG_UPLOADER_DELAY_PARAM,
                    new_interval_in_miliseconds,
                )
            ]
        )

    @pytest.mark.parametrize(
        argnames=["use_provided_logs_pvc"],
        argvalues=[
            pytest.param(False, marks=[tier1, polarion_id("OCS-6242")]),
            pytest.param(
                True,
                marks=[tier2, polarion_id("OCS-6243"), skipif_mcg_only],
            ),
        ],
        ids=[
            "default-logs-pvc",
            "provided-logs-pvc",
        ],
    )
    def test_guaranteed_bucket_logs_management(
        self,
        bucket_factory,
        mcg_obj_session,
        awscli_pod_session,
        use_provided_logs_pvc,
        pvc_factory,
    ):
        """
        Test setting up and removing the guaranteed
        bucket logs feature on MCG:

        1. Enable guaranteed bucket logs on top of the noobaa CR
        2. Validate that the noobaa CR has been updated
        3. Wait for the nb pods to have mounts to logs PVC
        4. Create two buckets: source bucket and logs bucket
        5. Apply bucket logging on top of the  source bucket
        6. Validate that the bucket logging configuration has been set
        7. Disable the bucket logging configuration
        8. Validate that the bucket logging configuration has been removed
        9. Disable guaranteed bucket logs on top of the noobaa CR
        10. Validate that the noobaa CR has been updated
        11. Wait for the nb pods to restart without the mounts
        12. Validate the logs PVC hasn't been deleted
        """
        logs_manager = BucketLoggingManager(mcg_obj_session, awscli_pod_session)

        provided_logs_pvc = None
        if use_provided_logs_pvc:
            clstr_proj_obj = OCP(namespace=config.ENV_DATA["cluster_namespace"])
            provided_logs_pvc = pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=clstr_proj_obj,
                size=20,
                access_mode=constants.ACCESS_MODE_RWX,
            )

            # Label the PVC and the PV to avoid false leftover errors
            provided_logs_pvc.add_label(constants.CUSTOM_MCG_LABEL)
            pv_ocp_obj = OCP(
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.PV,
            )
            pv_ocp_obj.add_label(
                resource_name=provided_logs_pvc.backed_pv,
                label=constants.CUSTOM_MCG_LABEL,
            )

        # 1. Enable guaranteed bucket logs on top of the noobaa CR
        logs_pvc_name = provided_logs_pvc.name if use_provided_logs_pvc else None
        logs_manager.enable_bucket_logging_on_cr(logs_pvc=logs_pvc_name)

        # 2. Validate that the noobaa CR has been updated
        cr_logging_config = logs_manager.get_logging_config_from_cr()
        assert cr_logging_config["loggingType"] == "guaranteed", (
            "Failed to enable guaranteed bucket logs - "
            f"get-logging-config returned {cr_logging_config}"
        )

        # 3. Wait for the nb pods to have mounts to logs PVC
        assert logs_manager.wait_for_logs_pvc_mount_status(
            mount_status_expected=True,
        ), "One of the noobaa pods failed to mount the logs PVC"

        # 4. Create two buckets: source bucket and logs bucket
        source_bucket, logs_bucket = (b.name for b in bucket_factory(amount=2))

        # 5. Apply bucket logging on top of the source bucket
        logs_manager.put_bucket_logging(source_bucket, logs_bucket)

        # 6. Validate that the bucket logging configuration has been set
        bucket_logging_config = logs_manager.get_bucket_logging(source_bucket)
        assert bucket_logging_config["LoggingEnabled"]["TargetBucket"] == logs_bucket, (
            f"Failed to set bucket logging on {source_bucket} - "
            f"get-logging-config returned {bucket_logging_config}"
        )

        # 7. Disable the bucket logging configuration
        logs_manager.remove_bucket_logging(source_bucket)

        # 8. Validate that the bucket logging configuration has been removed
        bucket_logging_config = logs_manager.get_bucket_logging(source_bucket)
        assert not bucket_logging_config, (
            f"Failed to remove bucket logging on {source_bucket} - "
            f"get-logging-config returned {bucket_logging_config}"
        )

        # 9. Disable guaranteed bucket logs on top of the noobaa CR
        logs_manager.disable_bucket_logging_on_cr()

        # 10. Validate that the noobaa CR has been updated
        cr_logging_config = logs_manager.get_logging_config_from_cr()
        assert not cr_logging_config, (
            "Failed to disable guaranteed bucket logs - "
            f"get-logging-config returned {cr_logging_config}"
        )

        # 11. Wait for the nb pods to restart without the mounts
        assert logs_manager.wait_for_logs_pvc_mount_status(
            mount_status_expected=False,
        ), "One of the noobaa pods failed to unmount the logs PVC"

        # 12. Validate that the logs PVC hasn't been deleted
        pvc_dicts = get_all_pvc_objs(namespace=config.ENV_DATA["cluster_namespace"])
        assert any(
            pvc.name == logs_manager.cur_logs_pvc for pvc in pvc_dicts
        ), f"The logs PVC {logs_manager.cur_logs_pvc} was deleted"

    @pytest.mark.parametrize(
        argnames=["use_provided_logs_pvc"],
        argvalues=[
            pytest.param(False, marks=[tier1, polarion_id("OCS-6244")]),
            pytest.param(
                True,
                marks=[tier2, polarion_id("OCS-6245"), skipif_mcg_only],
            ),
        ],
        ids=[
            "default-logs-pvc",
            "provided-logs-pvc",
        ],
    )
    def test_bucket_logs_integrity(
        self,
        mcg_obj_session,
        awscli_pod_session,
        bucket_factory,
        enable_guaranteed_bucket_logging,
        test_directory_setup,
        use_provided_logs_pvc,
    ):
        """
        Test that S3 operations are logged correctly
        when bucket logging is enabled:

        1. Create two buckets: source bucket and logs bucket
        2. Setup guaranteed bucket logging on the source bucket using the logs bucket
        3. Upload objects to the source bucket
        4. Get each object
        5. Head each object
        6. Delete the objects
        7. Wait for all expected operations to appear in the logs bucket

        Note that every operation should be logged twice with different op codes:
        The first should be the attempt of the operation with the 102 code,
        and the second should be the actual operation with regular code.
        """
        enable_guaranteed_bucket_logging(use_provided_logs_pvc)
        blm = BucketLoggingManager(mcg_obj_session, awscli_pod_session)

        # 1. Create two buckets: source bucket and logs bucket
        source_bucket, logs_bucket = (b.name for b in bucket_factory(amount=2))

        # 2. Setup guaranteed bucket logging on the source bucket using the logs bucket
        blm.put_bucket_logging(source_bucket, logs_bucket, verify=True)

        # 3. Upload objects to the source bucket
        obj_keys = write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=source_bucket,
            file_dir=test_directory_setup.origin_dir,
            amount=20,
            mcg_obj=mcg_obj_session,
        )

        # 4. Get each object
        for obj_key in obj_keys:
            s3_get_object(
                s3_obj=mcg_obj_session, bucketname=source_bucket, object_key=obj_key
            )

        # 5. Head each object
        for obj_key in obj_keys:
            s3_head_object(
                s3_obj=mcg_obj_session, bucketname=source_bucket, object_key=obj_key
            )

        # 6. Delete the objects
        rm_object_recursive(awscli_pod_session, source_bucket, mcg_obj_session)

        # 7. Wait for all expected operations to appear in the logs bucket
        expected_ops = []
        for obj_key in obj_keys:
            for op in ["PUT", "DELETE", "GET", "HEAD"]:
                expected_ops.append((op, f"/{source_bucket}/{obj_key}"))

        blm.await_and_verify_bucket_logs(
            logs_bucket, source_bucket, expected_ops, check_intent=True
        )
        logger.info("All the expected logs were found")
