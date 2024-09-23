import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    magenta_squad,
    ignore_leftovers,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.e2e_helpers import (
    validate_noobaa_rebuild_system,
    validate_noobaa_db_backup_recovery_locally_system,
    Run_fio_till_cluster_full,
)
from ocs_ci.helpers.keyrotation_helper import (
    verify_key_rotation_time,
    enable_key_rotation,
    set_key_rotation_time,
    verify_new_key_after_rotation,
)
from ocs_ci.helpers.osd_resize import basic_resize_osd, get_storage_size
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants, warp
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@magenta_squad
@system_test
@ignore_leftovers
class TestKeyRotationWithClusterFull(E2ETest):
    """
    1. Set cluster wide keyrotation period to every 5 mins by editing storage cluster and capture the keys details
     for OSDs, Noobaa.
    2.  Set custom taints to all the worker nodes and make sure #1 and #2 still exists after rebooting the pods.
      a) Apply custom taint to all nodes
      b)  Add toleration in  storagecluster CR and odf-operator subscription.
      c) Verify toleration applied in ODF subscription and Storage Cluster CR are reflecting for other subscriptions,
         Ceph and nooba components or not.
      d) Verify the pods in all nodes are running as per taints applied.
      e) Restart all ocs the pods on all nodes.

    3. Fill the cluster till the full ratio limits (85%) by running IO from multiple pods and verify key rotation.
    4. Once the cluster reaches read-only state, resize the OSD and verify key rotation.
    5. Run noobaa_rebuild test and verify key rotation still works as it is.
    6. Run noobaa-db backup and recovery locally. Verify key rotation.

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()

    def noobaa_rebuild_cleanup(self):
        """
        Cleanup function which clears all the noobaa rebuild entries.

        """
        # Get the deployment replica count
        deploy_obj = OCP(
            kind=constants.DEPLOYMENT,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        noobaa_deploy_obj = deploy_obj.get(
            resource_name=constants.NOOBAA_OPERATOR_DEPLOYMENT
        )
        if noobaa_deploy_obj["spec"]["replicas"] != 1:
            log.info(
                f"Scaling back {constants.NOOBAA_OPERATOR_DEPLOYMENT} deployment to replica: 1"
            )
            deploy_obj.exec_oc_cmd(
                f"scale deployment {constants.NOOBAA_OPERATOR_DEPLOYMENT} --replicas=1"
            )

    @pytest.fixture()
    def warps3(self, request):
        warps3 = warp.Warp()
        warps3.create_resource_warp(replicas=4, multi_client=True)

        def teardown():
            warps3.cleanup()

        request.addfinalizer(teardown)
        return warps3

    def test_cluster_wide_encryption_key_rotation_system(
        self,
        teardown_project_factory,
        pvc_factory,
        pod_factory,
        threading_lock,
        bucket_factory_session,
        mcg_obj_session,
        noobaa_db_backup_and_recovery_locally,
        warps3,
    ):
        time_interval_to_rotate_key_in_minutes = str(5)
        tries = 10
        delays = int(time_interval_to_rotate_key_in_minutes) * 60 / tries
        log.info("Enabling the key rotation if not done")
        enable_key_rotation()
        log.info("Setting the key rotation time by editing storage cluster")
        set_key_rotation_time(time_interval_to_rotate_key_in_minutes)
        schedule = f"*/{time_interval_to_rotate_key_in_minutes} * * * *"
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

        # TODO:OSD-resize feature dropped from 4.16 release. It is supported only from 4.17 onwards.
        log.info("Performing OSD resize")
        basic_resize_osd(get_storage_size())
        log.info("After OSD resize, checking the key rotation time is unchanged")
        verify_key_rotation_time(schedule=schedule)
        log.info(
            "After OSD resize, verifying the new key generated by comparing it with older key"
        )
        verify_new_key_after_rotation(tries, delays)

        run_fio_obj.cleanup()
        log.info("Triggering noobaa rebuild test")
        validate_noobaa_rebuild_system(self, bucket_factory_session, mcg_obj_session)
        log.info("After noobaa rebuild, checking the key rotation time is unchanged")
        verify_key_rotation_time(schedule=schedule)
        log.info(
            "After noobaa rebuild, verifying the new key generated by comparing it with older key"
        )
        verify_new_key_after_rotation(tries, delays)
        log.info("Starting noobaa rebuild cleanup activity")
        self.noobaa_rebuild_cleanup()

        log.info("Triggering noobaa db backup and recovery locally")
        validate_noobaa_db_backup_recovery_locally_system(
            self,
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
