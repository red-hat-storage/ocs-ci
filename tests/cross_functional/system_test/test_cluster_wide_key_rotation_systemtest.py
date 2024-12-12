import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    magenta_squad,
    ignore_leftovers,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.e2e_helpers import (
    Run_fio_till_cluster_full,
)
from ocs_ci.helpers.keyrotation_helper import (
    verify_key_rotation_time,
    verify_new_key_after_rotation,
    OSDKeyrotation,
)
from ocs_ci.helpers.osd_resize import basic_resize_osd, get_storage_size
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import warp

log = logging.getLogger(__name__)


@magenta_squad
@system_test
@ignore_leftovers
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

    def test_cluster_wide_encryption_key_rotation_system(
        self,
        teardown_project_factory,
        pvc_factory,
        pod_factory,
        bucket_factory_session,
        mcg_obj_session,
        noobaa_db_backup_and_recovery_locally,
        validate_noobaa_rebuild_system,
        validate_noobaa_db_backup_recovery_locally_system,
        warps3,
    ):
        """
        1. Function OSDKeyrotation().set_keyrotation_schedule is to set cluster wide keyrotation period to every 5 mins
        2. Using verify_new_key_after_rotation function to Capture the keys details and verify new keys
         after the scheduled time.
        3.  Set custom taints to all the worker nodes and make sure #1 and #2 still exists after rebooting the pods.
          a) Apply custom taint to all nodes
          b)  Add toleration in  storagecluster CR and odf-operator subscription.
          c) Verify toleration applied in ODF subscription and Storage Cluster CR are reflecting for other
            subscriptions ,Ceph and nooba components or not.
          d) Verify the pods in all nodes are running as per taints applied.
          e) Restart all ocs the pods on all nodes.

        4. Fill the cluster till the full ratio limits (85%) by running FIO from multiple pods and verify key rotation.
        5. Once the cluster reaches read-only state, resize the OSD using basic_resize_osd and verify key rotation.
        6. Run validate_noobaa_rebuild_system function to verify key rotation still works as it is afetr nobba rebuild.
        7. Run validate_noobaa_db_backup_recovery_locally_system and verify key rotation.

        """
        time_interval_to_rotate_key_in_minutes = str(5)
        tries = 10
        delays = int(time_interval_to_rotate_key_in_minutes) * 60 / tries
        schedule = f"*/{time_interval_to_rotate_key_in_minutes} * * * *"
        log.info("Setting the key rotation time by editing storage cluster")
        OSDKeyrotation().set_keyrotation_schedule(schedule)
        log.info("Verifying the key rotation time set properly or not")
        verify_key_rotation_time(schedule=schedule)
        log.info("Verifying the new key generated by comparing it with older key")
        verify_new_key_after_rotation(tries, delays)

        # TODO: Custom taints PR 9808 not yet merged. Will include that part once the merge completes.
        run_fio_obj = Run_fio_till_cluster_full()
        run_fio_obj.run_cluster_full_fio(
            teardown_project_factory, pvc_factory, pod_factory
        )
        log.info(
            "Verifying the key rotation time is still unchanged after 85% cluster full"
        )
        verify_key_rotation_time(schedule=schedule)
        log.info(
            "After cluster full 85%, verifying the new key generated by comparing it with older key"
        )
        verify_new_key_after_rotation(tries, delays)

        log.info("Performing OSD resize")
        basic_resize_osd(get_storage_size())
        log.info("After OSD resize, checking the key rotation time is unchanged")
        verify_key_rotation_time(schedule=schedule)
        log.info(
            "After OSD resize, verifying the new key generated by comparing it with older key"
        )
        verify_new_key_after_rotation(tries, delays)

        log.info("Triggering noobaa rebuild test")

        validate_noobaa_rebuild_system(bucket_factory_session, mcg_obj_session)
        log.info("After noobaa rebuild, checking the key rotation time is unchanged")
        verify_key_rotation_time(schedule=schedule)
        log.info(
            "After noobaa rebuild, verifying the new key generated by comparing it with older key"
        )
        verify_new_key_after_rotation(tries, delays)
        log.info("Starting noobaa rebuild cleanup activity")

        log.info("Triggering noobaa db backup and recovery locally")

        validate_noobaa_db_backup_recovery_locally_system(
            bucket_factory_session,
            noobaa_db_backup_and_recovery_locally,
            warps3,
            mcg_obj_session,
        )

        log.info(
            "After noobaa db backup and recovery, checking the key rotation time is unchanged"
        )
        verify_key_rotation_time(schedule=schedule)
        log.info(
            "After noobaa db backup and  recovery, verifying the new key generated by comparing it with older key"
        )
        verify_new_key_after_rotation(tries, delays)
        run_fio_obj.cleanup()
