import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    magenta_squad,
    ignore_leftovers,
    encryption_at_rest_required,
    jira,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.keyrotation_helper import (
    validate_key_rotation_schedules,
    verify_new_key_after_rotation,
    OSDKeyrotation,
    KeyRotation,
)
from ocs_ci.helpers.osd_resize import basic_resize_osd, get_storage_size
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import warp

logger = logging.getLogger(__name__)


@magenta_squad
@system_test
@ignore_leftovers
@encryption_at_rest_required
@jira("DFBUGS-5769")
class TestKeyRotationWithClusterFull(E2ETest):
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()
        self.sanity_helpers.health_check()

    @pytest.fixture()
    def warps3(self, request):
        warps3 = warp.Warp()
        warps3.create_resource_warp(replicas=4, multi_client=True)

        def teardown():
            warps3.cleanup(multi_client=True)

        request.addfinalizer(teardown)
        return warps3

    def teardown(self):
        OSDKeyrotation().set_keyrotation_schedule("@weekly")

    def test_cluster_wide_encryption_key_rotation(
        self,
        bucket_factory_session,
        mcg_obj,
        mcg_obj_session,
        run_fio_till_cluster_full,
        noobaa_db_backup_and_recovery_locally,
        validate_noobaa_rebuild_system,
        validate_noobaa_db_backup_recovery_locally_system,
        warps3,
        setup_mcg_bg_features,
        multi_pvc_pod_lifecycle_factory,
        multi_pvc_clone_factory,
        multi_snapshot_factory,
        snapshot_restore_factory,
        project_factory,
    ):
        """
        1. Run entry criteria on the cluster initially to have required load on the cluster to perform system test.
        2. Function OSDKeyrotation().set_keyrotation_schedule is to set cluster wide keyrotation period to every 5 mins
        3. Using verify_new_key_after_rotation function to Capture the keys details and verify new keys
         after the scheduled time.
        4. Fill the cluster till the full ratio limits (85%) by running FIO from multiple pods and verify key rotation.
        5. Once the cluster reaches read-only state, resize the OSD using basic_resize_osd and verify key rotation.
        6. Run validate_noobaa_rebuild_system function to verify key rotation still works as it is afetr nobba rebuild.
        7. Run validate_noobaa_db_backup_recovery_locally_system and verify key rotation.

        """
        logger.test_step("Setup MCG entry criteria")
        setup_mcg_bg_features(
            num_of_buckets=10,
            object_amount=10,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            skip_any_provider=["azure"],
        )
        logger.info("MCG setup completed: 10 buckets with 10 objects each")

        logger.test_step("Setup CephFS entry criteria")
        pvc_obj, pod_obj = multi_pvc_pod_lifecycle_factory(
            measure=False, delete=False, num_of_pvcs=30
        )
        logger.info("CephFS setup completed: 30 PVCs created")
        # Commenting below code due to ocs-ci issue 11605
        # multi_pvc_clone_factory(pvc_obj=pvc_obj[:11])
        # snap_list = multi_snapshot_factory(pvc_obj=pvc_obj[:11])
        # for snapshot in snap_list:
        #     snapshot_restore_factory(snapshot_obj=snapshot)

        logger.test_step("Configure cluster-wide key rotation schedule")
        time_interval_to_rotate_key_in_minutes = str(5)
        tries = 10
        delays = int(time_interval_to_rotate_key_in_minutes) * 60 / tries
        schedule = f"*/{time_interval_to_rotate_key_in_minutes} * * * *"
        logger.info(
            f"Setting key rotation schedule to: {schedule} (every {time_interval_to_rotate_key_in_minutes} minutes)"
        )

        key_rotation = KeyRotation()
        if not key_rotation.is_keyrotation_enable():
            logger.info("Key rotation not enabled, enabling it now")
            key_rotation.enable_keyrotation()
        else:
            logger.info("Key rotation already enabled")

        OSDKeyrotation().set_keyrotation_schedule(schedule)
        logger.info(f"Key rotation schedule set to: {schedule}")

        logger.test_step("Verify initial key rotation schedule and new key generation")
        self.verify_key_rotation_time(schedule=schedule)
        logger.info(f"Verifying new key generation (tries={tries}, delay={delays}s)")
        verify_new_key_after_rotation(tries, delays)
        logger.info("Initial key rotation verified successfully")

        logger.test_step("Fill cluster to 85% capacity and verify key rotation")
        logger.info("Starting FIO workload to fill cluster to 85% capacity")
        run_fio_till_cluster_full()
        logger.info("Cluster filled to 85% capacity")

        logger.info("Verifying key rotation schedule unchanged after cluster fill")
        self.verify_key_rotation_time(schedule=schedule)

        logger.info("Verifying new key generation after cluster fill")
        verify_new_key_after_rotation(tries, delays)
        logger.info("Key rotation verified successfully after cluster fill to 85%")

        logger.test_step("Resize OSD and verify key rotation")
        storage_size = get_storage_size()
        logger.info(f"Performing OSD resize to storage size: {storage_size}")
        basic_resize_osd(storage_size)
        logger.info("OSD resize completed")

        logger.info("Verifying key rotation schedule unchanged after OSD resize")
        self.verify_key_rotation_time(schedule=schedule)

        logger.info("Verifying new key generation after OSD resize")
        verify_new_key_after_rotation(tries, delays)
        logger.info("Key rotation verified successfully after OSD resize")

        logger.test_step("Perform NooBaa rebuild and verify key rotation")
        logger.info("Initiating NooBaa rebuild process")
        validate_noobaa_rebuild_system(bucket_factory_session, mcg_obj_session)
        logger.info("NooBaa rebuild completed successfully")

        logger.info("Verifying key rotation schedule unchanged after NooBaa rebuild")
        self.verify_key_rotation_time(schedule=schedule)

        logger.info("Verifying new key generation after NooBaa rebuild")
        verify_new_key_after_rotation(tries, delays)
        logger.info("Key rotation verified successfully after NooBaa rebuild")

        logger.test_step("Perform NooBaa DB backup and recovery, verify key rotation")
        logger.info("Refreshing MCG S3 credentials after NooBaa rebuild")
        mcg_obj.update_s3_creds()
        mcg_obj_session.update_s3_creds()
        logger.info("MCG credentials refreshed")

        logger.info("Triggering NooBaa DB backup and recovery locally")
        validate_noobaa_db_backup_recovery_locally_system(
            bucket_factory_session,
            noobaa_db_backup_and_recovery_locally,
            warps3,
            mcg_obj_session,
        )
        logger.info("NooBaa DB backup and recovery completed successfully")

        logger.info(
            "Verifying key rotation schedule unchanged after NooBaa DB backup and recovery"
        )
        self.verify_key_rotation_time(schedule=schedule)

        logger.info("Verifying new key generation after NooBaa DB backup and recovery")
        verify_new_key_after_rotation(tries, delays)
        logger.info(
            "Key rotation verified successfully after NooBaa DB backup and recovery"
        )

        logger.info(
            "Test execution completed successfully for cluster-wide encryption key rotation"
        )

    def verify_key_rotation_time(self, schedule):
        """
        This function handles the exceptions raised by validate_key_rotation_schedules

        """
        logger.debug(f"Validating key rotation schedule: {schedule}")
        try:
            validate_key_rotation_schedules(schedule=schedule)
        except ValueError:
            logger.exception(
                f"Key rotation schedule validation failed for schedule '{schedule}'"
            )
            raise
        else:
            logger.info(f"Key rotation schedule validated successfully: {schedule}")
