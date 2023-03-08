import logging
import pytest

from ocs_ci.ocs import warp
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    E2ETest,
    tier3,
    skipif_managed_service,
    skipif_ocs_version,
    skipif_external_mode,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.resources.pod import (
    wait_for_storage_pods,
    get_noobaa_pods,
    get_pod_logs,
)

log = logging.getLogger(__name__)


@tier3
@ignore_leftovers
@pytest.mark.polarion_id("OCS-2605")
@pytest.mark.bugzilla("1924047")
@skipif_ocs_version("<4.6")
@skipif_managed_service
@skipif_external_mode
class TestNoobaaBackupAndRecovery(E2ETest):
    """
    Test to verify noobaa backup and recovery

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_noobaa_db_backup_and_recovery(
        self,
        pvc_factory,
        pod_factory,
        snapshot_factory,
        bucket_factory,
        rgw_bucket_factory,
        noobaa_db_backup_and_recovery,
    ):
        """
        Test case to verify noobaa backup and recovery

        1. Take snapshot db-noobaa-db-0 PVC and retore it to PVC
        2. Scale down the statefulset noobaa-db
        3. Get the yaml of the current PVC, db-noobaa-db-0 and
           change the parameter persistentVolumeReclaimPolicy to Retain for restored PVC
        4. Delete both PVCs, the PV for the original claim db-noobaa-db-0 will be removed.
           The PV for claim db-noobaa-db-0-snapshot-restore will move to ‘Released’
        5. Edit again restore PV and remove the claimRef section.
           The volume will transition to Available.
        6. Edit the yaml db-noobaa-db-0.yaml and change the setting volumeName to restored PVC.
        7. Scale up the stateful set again and the pod should be running

        """
        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)

        # Verify all storage pods are running
        wait_for_storage_pods()

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        # Deleting Resources
        self.sanity_helpers.delete_resources()

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=120)

    @pytest.fixture()
    def warps3(self, request):

        warps3 = warp.Warp()
        warps3.create_resource_warp(replicas=4, multi_client=True)

        def teardown():
            warps3.cleanup()

        request.addfinalizer(teardown)
        return warps3

    def test_noobaa_db_backup_recovery_locally(
        self,
        bucket_factory,
        noobaa_db_backup_and_recovery_locally,
        warps3,
        mcg_obj_session,
    ):
        bucket_name = bucket_factory()[0].name

        # noobaa_db_backup_and_recovery_locally()
        # time.sleep(10)
        warps3.run_benchmark(
            bucket_name=bucket_name,
            access_key=mcg_obj_session.access_key_id,
            secret_key=mcg_obj_session.access_key,
            duration="10m",
            concurrent=10,
            objects=100,
            obj_size="1MiB",
            validate=True,
            timeout=4000,
            multi_client=True,
            tls=True,
            debug=True,
            insecure=True,
        )
        log.info(mcg_obj_session.access_key_id)
        log.info(mcg_obj_session.access_key)

        search_string = (
            "AssertionError [ERR_ASSERTION]: _id must be unique. "
            "found 2 rows with _id=undefined in table bucketstats"
        )
        nb_pods = get_noobaa_pods()
        for pod in nb_pods:
            pod_logs = get_pod_logs(pod_name=pod.name)
            for line in pod_logs:
                assert (
                    search_string not in line
                ), f"[Error] {search_string} found in the noobaa pod logs"
        log.info(f"No {search_string} errors are found in the noobaa pod logs")
