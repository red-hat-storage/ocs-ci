import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    jira,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    tier4,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers.dr_helpers import (
    relocate,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    wait_for_mirroring_status_ok,
    get_volumesnapshots_in_namespace,
    wait_for_volumesnapshots_deletion,
    wait_for_drpc_deletion,
    cleanup_orphaned_volumesnapshots,
)

logger = logging.getLogger(__name__)


@green_squad
@tier4
@jira("DFBUGS-10229")
@skipif_ocs_version("<4.22")
class TestVolumeSnapshotsOrphanedAfterDRAndAppDeletion(ManageTest):
    """
    Test to verify that VolumeSnapshots are not orphaned on the primary cluster
    after a DR operation (Failover or Relocate) followed by application deletion.

    Bug: DFBUGS-10229
    When a CephFS-based workload undergoes a DR operation and is then deleted,
    VolumeSnapshots were left orphaned on the primary cluster. The fix ensures
    that VolumeSnapshots associated with PVCs are cleaned up during the
    UnprotectVolSyncPVC flow.
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, request, dr_workload):
        """
        Setup: Deploy a CephFS-based workload with DR protection.
        Teardown: Clean up any remaining resources.

        Args:
            request: pytest request object for finalizer registration
            dr_workload: Factory fixture that deploys DR-protected workloads

        Returns:
            None
        """
        self.namespace = None
        self.workload = None
        self.workload_deleted = False

        def finalizer():
            """
            Ensure workload is cleaned up and no orphaned resources remain.
            """
            if not self.workload_deleted and self.workload is not None:
                try:
                    logger.info("Finalizer: Cleaning up DR workload")
                    self.workload.delete()
                except Exception as ex:
                    logger.warning(f"Finalizer: Error cleaning up workload: {ex}")

            if self.namespace:
                cleanup_orphaned_volumesnapshots(self.namespace, cluster_indices=[0, 1])

        request.addfinalizer(finalizer)

        logger.test_step("Deploy CephFS-based workload with DR protection")
        self.workload = dr_workload(
            num_of_subscription=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )[0]
        self.namespace = self.workload.workload_namespace
        logger.info(f"Workload deployed in namespace: {self.namespace}")

    def test_volumesnapshots_cleaned_after_dr_and_app_deletion(
        self,
    ):
        """
        Test that VolumeSnapshots are properly cleaned up after a DR operation
        followed by application deletion.

        DFBUGS-10229: VolumeSnapshots were orphaned on the primary cluster after
        a DR operation (Failover or Relocate) on a CephFS-based workload followed
        by application deletion.

        Steps:
            1. Deploy a CephFS-based workload on cluster C1 with DR protection
            2. Verify workload is running and VolumeSnapshots exist
            3. Perform a Relocate DR operation from C1 to C2
            4. Wait for DR operation to complete and workload to be running on C2
            5. Verify VolumeSnapshots exist on C2 (new primary)
            6. Delete the application/workload
            7. Verify DRPC is fully deleted
            8. Verify no orphaned VolumeSnapshots remain on C2 (the primary cluster)
        """
        primary_cluster_name = get_current_primary_cluster_name(
            self.workload.workload_namespace, self.workload.workload_name
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            self.workload.workload_namespace, self.workload.workload_name
        )
        logger.info(
            f"Initial primary cluster: {primary_cluster_name}, "
            f"secondary cluster: {secondary_cluster_name}"
        )

        logger.test_step("Verify workload is running on the primary cluster")
        set_current_primary_cluster_context(
            self.workload.workload_namespace, self.workload.workload_name
        )
        wait_for_all_resources_creation(
            self.workload.workload_pvc_count,
            self.workload.workload_pod_count,
            self.workload.workload_namespace,
        )

        logger.test_step("Verify VolumeSnapshots exist on primary cluster before DR operation")
        initial_vs_names = get_volumesnapshots_in_namespace(self.namespace)
        logger.info(f"Found {len(initial_vs_names)} VolumeSnapshots on primary cluster before DR operation")

        logger.test_step("Wait for mirroring to be healthy before performing DR operation")
        wait_for_mirroring_status_ok(
            replaying_images=self.workload.workload_pvc_count
        )

        logger.test_step(
            f"Perform Relocate DR operation from {primary_cluster_name} to {secondary_cluster_name}"
        )
        relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=self.workload.workload_namespace,
            workload_instance=self.workload,
        )

        logger.test_step("Wait for workload to be running on the new primary cluster (C2)")
        set_current_primary_cluster_context(
            self.workload.workload_namespace, self.workload.workload_name
        )
        wait_for_all_resources_creation(
            self.workload.workload_pvc_count,
            self.workload.workload_pod_count,
            self.workload.workload_namespace,
        )
        new_primary_cluster_name = get_current_primary_cluster_name(
            self.workload.workload_namespace, self.workload.workload_name
        )
        logger.info(f"Workload is now running on new primary cluster: {new_primary_cluster_name}")

        logger.test_step("Verify old primary cluster resources are cleaned up")
        set_current_secondary_cluster_context(
            self.workload.workload_namespace, self.workload.workload_name
        )
        wait_for_all_resources_deletion(
            self.workload.workload_namespace,
        )

        logger.test_step("Verify VolumeSnapshots exist on new primary cluster (C2) after DR operation")
        set_current_primary_cluster_context(
            self.workload.workload_namespace, self.workload.workload_name
        )
        post_dr_vs_names = get_volumesnapshots_in_namespace(self.namespace)
        logger.info(
            f"Found {len(post_dr_vs_names)} VolumeSnapshots on new primary cluster (C2) after DR operation"
        )

        logger.test_step("Wait for mirroring to stabilize after DR operation")
        wait_for_mirroring_status_ok(
            replaying_images=self.workload.workload_pvc_count
        )

        logger.test_step("Delete the DR-protected application/workload")
        self.workload.delete()
        self.workload_deleted = True

        logger.test_step("Verify DRPC is fully deleted on the hub cluster")
        config.switch_to_acm_ctx()
        wait_for_drpc_deletion(namespace=self.namespace, timeout=300, sleep=15)

        logger.test_step(
            "Verify no orphaned VolumeSnapshots remain on the new primary cluster (C2) after app deletion"
        )
        set_current_primary_cluster_context(
            self.workload.workload_namespace, self.workload.workload_name
        )
        orphaned_snapshots = wait_for_volumesnapshots_deletion(
            namespace=self.namespace, timeout=300, sleep=20
        )

        logger.assertion(
            f"Check: expected orphaned VolumeSnapshots=0, actual={len(orphaned_snapshots)}"
        )
        assert len(orphaned_snapshots) == 0, (
            f"DFBUGS-10229: VolumeSnapshots are orphaned on the primary cluster (C2) after "
            f"DR operation and app deletion. Orphaned snapshots: {orphaned_snapshots}"
        )

        logger.test_step(
            "Verify no orphaned VolumeSnapshots remain on the old primary cluster (C1) as well"
        )
        set_current_secondary_cluster_context(
            self.workload.workload_namespace, self.workload.workload_name
        )
        secondary_vs_names = get_volumesnapshots_in_namespace(self.namespace)

        logger.assertion(
            f"Check: expected orphaned VolumeSnapshots on secondary=0, actual={len(secondary_vs_names)}"
        )
        assert len(secondary_vs_names) == 0, (
            f"DFBUGS-10229: VolumeSnapshots are orphaned on the secondary cluster (C1) after "
            f"DR operation and app deletion. Orphaned snapshots: {secondary_vs_names}"
        )

        logger.info(
            "DFBUGS-10229 verification passed: No orphaned VolumeSnapshots found on either cluster "
            "after DR operation and application deletion"
        )
