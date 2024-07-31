import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    MCGTest,
    bugzilla,
    ignore_leftover_label,
    mcg,
    red_squad,
    skipif_mcg_only,
    tier1,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.bucket_logging_manager import BucketLoggingManager
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

RECONCILE_WAIT = 30


@red_squad
@mcg
@ignore_leftover_label(constants.NOOBAA_ENDPOINT_POD_LABEL)
class TestBucketLogs(MCGTest):
    """
    Test the MCG bucket logs feature
    """

    # Track created PVCs for cleanup
    created_pvcs = []

    @pytest.mark.fixture(scope="class", autouse=True)
    def cleanup(self, request, mcg_obj_session, awscli_pod_session):
        """
        Cleanup leftover resources
        """

        def finalizer():
            # Disable guaranteed bucket logs
            logs_manager = BucketLoggingManager(mcg_obj_session, awscli_pod_session)
            logs_manager.disable_bucket_logging_on_cr()

            # Wait for the nb pods to restart without the mounts
            sample = TimeoutSampler(
                timeout=120,
                sleep=15,
                func=logs_manager.check_if_nb_pods_mount_the_logs_pvc,
            )
            assert sample.wait_for_func_status(
                result=False
            ), "One of the noobaa pods failed to unmount the logs PVC"

            for pvc in self.created_pvcs:
                pvc.delete()

        request.addfinalizer(finalizer)

    @tier1
    @pytest.mark.parametrize(
        argnames=["use_provided_logs_pvc"],
        argvalues=[
            pytest.param(
                True,
                marks=[skipif_mcg_only],
            ),
            pytest.param(False, marks=[bugzilla("2302842")]),
        ],
        ids=[
            "provided-logs-pvc",
            "default-logs-pvc",
        ],
    )
    def test_guaranteed_bucket_logs_management(
        self,
        bucket_factory,
        mcg_obj_session,
        awscli_pod_session,
        use_provided_logs_pvc,
    ):
        """
        Test setting up, removing and updating the guaranteed
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
            provided_logs_pvc = helpers.create_pvc(
                sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
                size="20Gi",
                namespace=config.ENV_DATA["cluster_namespace"],
                access_mode=constants.ACCESS_MODE_RWX,
            )
            self.created_pvcs.append(provided_logs_pvc)
            provided_logs_pvc = provided_logs_pvc.name

        # 1. Enable guaranteed bucket logs on top of the noobaa CR
        logs_manager.enable_bucket_logging(provided_logs_pvc)

        # 2. Validate that the noobaa CR has been updated
        cr_logging_config = logs_manager.get_logging_config_from_cr()
        assert cr_logging_config["loggingType"] == "guaranteed", (
            "Failed to enable guaranteed bucket logs - "
            f"get-logging-config returned {cr_logging_config}"
        )

        # 3. Wait for the nb pods to have mounts to logs PVC
        sample = TimeoutSampler(
            timeout=120, sleep=15, func=logs_manager.check_if_nb_pods_mount_the_logs_pvc
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"The noobaa pods failed to mount the logs PVC in {sample.timeout} seconds"

        # 4. Create two buckets: source bucket and logs bucket
        source_bucket, logs_bucket = (b.name for b in bucket_factory(amount=2))

        # 5. Apply bucket logging on top of the source bucket
        logs_manager.set_logging_config_on_bucket(source_bucket, logs_bucket)

        # 6. Validate that the bucket logging configuration has been set
        bucket_logging_config = logs_manager.get_logging_config_from_bucket(
            source_bucket
        )
        assert bucket_logging_config["LoggingEnabled"]["TargetBucket"] == logs_bucket, (
            f"Failed to set bucket logging on {source_bucket} - "
            f"get-logging-config returned {bucket_logging_config}"
        )

        # 7. Disable the bucket logging configuration
        logs_manager.remove_logging_config_from_bucket(source_bucket)

        # 8. Validate that the bucket logging configuration has been removed
        bucket_logging_config = logs_manager.get_logging_config_from_bucket(
            source_bucket
        )
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
        sample = TimeoutSampler(
            timeout=120, sleep=15, func=logs_manager.check_if_nb_pods_mount_the_logs_pvc
        )
        assert sample.wait_for_func_status(
            result=False
        ), "One of the noobaa pods failed to unmount the logs PVC"

        # 12. Validate that the logs PVC hasn't been deleted
        pvc_dicts = get_all_pvc_objs(namespace=config.ENV_DATA["cluster_namespace"])
        assert any(
            pvc.name == logs_manager.cur_logs_pvc for pvc in pvc_dicts
        ), f"The logs PVC {logs_manager.cur_logs_pvc} was deleted"
