import logging
import pytest

from ocs_ci.ocs import warp
from ocs_ci.framework.pytest_customization.marks import magenta_squad, mcg
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

logger = logging.getLogger(__name__)


@mcg
@magenta_squad
@tier3
@ignore_leftovers
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

    @pytest.mark.polarion_id("OCS-2605")
    @skipif_ocs_version("<4.6")
    def deprecated_test_noobaa_db_backup_and_recovery(
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
        logger.test_step("Backup and restore NooBaa DB using snapshot")
        logger.info("Starting NooBaa DB backup and recovery procedure via snapshot")
        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)
        logger.info("NooBaa DB backup and recovery completed successfully")

        logger.test_step("Verify all storage pods are running")
        logger.info("Waiting for all storage pods to reach Running state")
        wait_for_storage_pods()
        logger.info("All storage pods are running")

        logger.test_step("Create and delete resources to verify MCG functionality")
        logger.info(
            "Creating resources using sanity helpers (PVCs, pods, buckets, RGW buckets)"
        )
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        logger.info("Resources created successfully")
        logger.info("Deleting created resources to verify cleanup")
        self.sanity_helpers.delete_resources()
        logger.info("Resources deleted successfully")

        logger.test_step("Verify cluster health after recovery")
        logger.info("Running cluster health check (max 120 retries)")
        self.sanity_helpers.health_check(tries=120)
        logger.info("Cluster health check passed - NooBaa DB recovery verified")

    @pytest.fixture()
    def warps3(self, request):
        logger.info("Setting up warp S3 benchmarking fixture")
        warps3 = warp.Warp()
        logger.info("Creating warp resources: replicas=4, multi_client=True")
        warps3.create_resource_warp(replicas=4, multi_client=True)
        logger.info("Warp resources created successfully")

        def teardown():
            logger.info("Teardown: Cleaning up warp resources (multi_client=True)")
            warps3.cleanup(multi_client=True)
            logger.info("Teardown completed: Warp resources cleaned up")

        request.addfinalizer(teardown)
        return warps3

    @pytest.mark.polarion_id("OCS-4842")
    @skipif_ocs_version("<4.8")
    def test_noobaa_db_backup_recovery_locally(
        self,
        bucket_factory,
        noobaa_db_backup_and_recovery_locally,
        warps3,
        mcg_obj_session,
    ):
        """
        Test to verify Backup and Restore for Multicloud Object Gateway database locally
        Backup procedure:
            * Create a test bucket and write some data
            * Backup noobaa secrets to local folder OR store it in secret objects
            * Backup the PostgreSQL database and save it to a local folder
            * For testing, write new data to show a little data loss between backup and restore.

        Restore procedure:
            * Stop MCG reconciliation
            * Stop the NooBaa Service before restoring the NooBaa DB.
              There will be no object service after this point
            * Verify that all NooBaa components (except NooBaa DB) have 0 replicas
            * Login to the NooBaa DB pod and cleanup potential database clients to nbcore
            * Restore DB from a local folder
            * Delete current noobaa secrets and restore them from a local folder OR secrets objects.
            * Restore MCG reconciliation
            * Start the NooBaa service
            * Restart the NooBaa DB pod
            * Check that the old data exists, but not s3://testloss/
        Run multi client warp benchamrking to verify bug https://bugzilla.redhat.com/show_bug.cgi?id=2141035

        """
        logger.test_step("Create bucket for warp benchmarking")
        logger.info("Creating bucket for warp S3 benchmarking")
        bucket_name = bucket_factory()[0].name
        logger.info(f"Bucket created for warp: {bucket_name}")

        logger.test_step("Backup and restore NooBaa DB locally")
        logger.info("Starting NooBaa DB backup and recovery procedure (local backup)")
        noobaa_db_backup_and_recovery_locally()
        logger.info("NooBaa DB local backup and recovery completed successfully")

        logger.test_step("Run multi-client warp S3 benchmarking")
        logger.info(
            f"Starting warp benchmark: bucket={bucket_name}, duration=10m, "
            f"concurrent=10, objects=100, obj_size=1MiB, multi_client=True"
        )
        warps3.run_benchmark(
            bucket_name=bucket_name,
            access_key=mcg_obj_session.access_key_id,
            secret_key=mcg_obj_session.access_key,
            duration="10m",
            concurrent=10,
            objects=100,
            obj_size="1MiB",
            validate=True,
            timeout=6000,
            multi_client=True,
            tls=True,
            debug=True,
            insecure=True,
        )
        logger.info("Warp benchmark completed successfully")

        logger.test_step("Verify no assertion errors in NooBaa pod logs (BZ 2141035)")
        search_string = (
            "AssertionError [ERR_ASSERTION]: _id must be unique. "
            "found 2 rows with _id=undefined in table bucketstats"
        )
        logger.info(
            f"Checking NooBaa pod logs for assertion errors: '{search_string[:50]}...'"
        )
        nb_pods = get_noobaa_pods()
        logger.info(f"Found {len(nb_pods)} NooBaa pod(s) to check")
        for idx, pod in enumerate(nb_pods, 1):
            logger.debug(f"Checking pod {idx}/{len(nb_pods)}: {pod.name}")
            pod_logs = get_pod_logs(pod_name=pod.name)
            logger.debug(f"Retrieved {len(pod_logs)} log lines from pod {pod.name}")
            for line in pod_logs:
                logger.assertion(f"Verify no assertion errors in {pod.name} logs")
                assert (
                    search_string not in line
                ), f"[Error] {search_string} found in the noobaa pod logs"
        logger.info(
            f"No assertion errors found in any NooBaa pod logs ({len(nb_pods)} pods checked)"
        )
