import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.helpers import helpers
from ocs_ci.helpers.odf_cephfs_snap import (
    create_provider_retain_cephfs_snapclass,
    delete_volumesnaps_volumesnapcontents,
    get_cephfs_snap_by_name,
    get_cephfs_snap_entries,
)
from ocs_ci.helpers.odf_cli import odf_cli_cephfs_snap_setup_helper
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pvc as pvc_resource
from ocs_ci.ocs.resources.storage_cluster import get_storage_client
from ocs_ci.utility.prometheus import (
    PrometheusAPI,
    wait_for_alert_cleared,
    wait_for_alert_firing,
)

log = logging.getLogger(__name__)


@green_squad
class TestCephFSOrphanedSnapshotAlert(ManageTest):
    """
    Verify that orphaning CephFS snapshots — by deleting their Kubernetes
    VolumeSnapshot/VolumeSnapshotContent objects while the Ceph-side
    snapshots are retained — triggers CephFSOrphanedSnapshot Prometheus
    alerts, and that the alerts clear after removing the orphans via the
    odf CLI.
    """

    retain_snapclass_name = "test-cephfs-retain-snapclass"

    @pytest.fixture(autouse=True)
    def setup(self, request, pvc_factory, threading_lock):
        """
        1. Switch active context to the client (consumer) cluster.
        2. On the provider cluster: create a VolumeSnapshotClass with
           deletionPolicy Retain and register it on the matching
           StorageConsumer so it propagates to the client.
        3. On the client cluster: wait for the snapclass to appear, then
           create a CephFS PVC and initialise the odf CLI runner.
        4. Initialise the Prometheus API client and an empty snapshot list.

        Note: in multicluster mode the consumer context is intentionally NOT
        restored to provider on teardown so that the pvc_factory finalizer
        runs on the correct (consumer) cluster. In standalone mode there is
        only one cluster so no context switching is needed.
        """
        if config.multicluster:
            config.switch_to_consumer()

        snapclass_teardown = create_provider_retain_cephfs_snapclass(
            snapclass_name=self.retain_snapclass_name,
            storage_client_name=get_storage_client().resource_name,
        )
        request.addfinalizer(snapclass_teardown)

        self.pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=1,
            status=constants.STATUS_BOUND,
        )

        self._snap_runner = odf_cli_cephfs_snap_setup_helper(
            storage_client=get_storage_client().resource_name
        )

        self.api = PrometheusAPI(
            threading_lock=threading_lock,
            cluster_context=config.RunWithFirstConsumerConfigContextIfAvailable,
        )

        self.snap_list_names = []

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Delete any leftover CephFS snapshots and their k8s objects after
        the test, keeping the active context on the consumer cluster so
        that the pvc_factory finalizer can reach the correct cluster.
        """

        def _cleanup_ceph_snaps():
            if not self.snap_list_names:
                return
            # Delete remaining k8s objects (e.g. bound group not cleaned up
            # during the test). With Retain policy, this does not remove the
            # Ceph-side snapshot; we handle that below via the odf CLI.
            for snap_data in self.snap_list_names:
                snap_obj = snap_data["snap_obj"]
                if snap_obj.ocp.is_exist(resource_name=snap_obj.name):
                    snapcontent_obj = helpers.get_snapshot_content_obj(snap_obj)
                    snap_obj.delete()
                    snap_obj.ocp.wait_for_delete(snap_obj.name)
                    if snapcontent_obj.ocp.is_exist(resource_name=snapcontent_obj.name):
                        snapcontent_obj.delete()
                        snapcontent_obj.ocp.wait_for_delete(snapcontent_obj.name)
            # Delete any remaining Ceph-side snapshots
            entries = get_cephfs_snap_entries(self._snap_runner)
            existing = {e["snapshot"] for e in entries}
            for snap_data in self.snap_list_names:
                ceph_name = snap_data["ceph_snap_name"]
                if ceph_name in existing:
                    log.info(
                        "Teardown: deleting leftover Ceph snapshot '%s'",
                        ceph_name,
                    )
                    self._snap_runner.delete(snap_data["subvolume"], ceph_name)

        request.addfinalizer(_cleanup_ceph_snaps)

    def create_retain_cephfs_snapshots(self, num_snapshots):
        """
        Create ``num_snapshots`` CephFS VolumeSnapshots with Retain policy
        and record each one in ``self.snap_list_names`` for teardown.

        For each snapshot:
        1. Create a k8s VolumeSnapshot and wait until it is ready.
        2. Identify the new Ceph-side snapshot entry from odf cephfs-snap
           ls and verify it is Bound.
        3. Verify the matching VolumeSnapshotContent exists.
        4. Append the snapshot data to ``self.snap_list_names``.

        Args:
            num_snapshots (int): Number of snapshots to create.
        """
        for i in range(num_snapshots):
            log.info(
                "Creating CephFS VolumeSnapshot %d/%d with Retain policy",
                i + 1,
                num_snapshots,
            )
            existing_ceph_names = {d["ceph_snap_name"] for d in self.snap_list_names}
            snap_name = helpers.create_unique_resource_name("test", "cephfs-snap")
            snap_obj = pvc_resource.create_pvc_snapshot(
                pvc_name=self.pvc_obj.name,
                snap_yaml=constants.CSI_CEPHFS_SNAPSHOT_YAML,
                snap_name=snap_name,
                namespace=self.pvc_obj.namespace,
                sc_name=self.retain_snapclass_name,
                wait=True,
                timeout=120,
            )

            snap_entries = get_cephfs_snap_entries(self._snap_runner)
            new_entries = [
                e for e in snap_entries if e["snapshot"] not in existing_ceph_names
            ]
            assert new_entries, (
                f"No new Ceph snapshot appeared after creating k8s snap "
                f"'{snap_name}'"
            )
            new_entry = new_entries[0]
            log.info("New Ceph snapshot entry: %s", new_entry)
            assert new_entry["state"] == constants.CEPHFS_SNAPSHOT_STATE_BOUND, (
                f"Expected state '{constants.CEPHFS_SNAPSHOT_STATE_BOUND}', "
                f"got '{new_entry['state']}'"
            )

            snapcontent_obj = helpers.get_snapshot_content_obj(snap_obj)
            assert (
                snapcontent_obj.name
            ), f"VolumeSnapshotContent not found for snap '{snap_name}'"
            log.info(
                "VolumeSnapshotContent for snap '%s': %s",
                snap_name,
                snapcontent_obj.name,
            )

            self.snap_list_names.append(
                {
                    "snap_obj": snap_obj,
                    "ceph_snap_name": new_entry["snapshot"],
                    "subvolume": new_entry["subvolume"],
                }
            )

    def delete_orphaned_cephfs_snapshots(self, snap_list_names):
        """
        Delete every orphaned Ceph-side snapshot in ``snap_list_names``
        via the odf CLI runner.

        Args:
            snap_list_names (list[dict]): Snapshot data as returned by
                :meth:`create_retain_cephfs_snapshots`.  Each dict must
                contain ``"subvolume"`` and ``"ceph_snap_name"`` keys.
        """
        for snap_data in snap_list_names:
            log.info(
                "Deleting orphaned Ceph snapshot '%s' from subvolume '%s'",
                snap_data["ceph_snap_name"],
                snap_data["subvolume"],
            )
            self._snap_runner.delete(
                snap_data["subvolume"], snap_data["ceph_snap_name"]
            )

    def verify_cephfs_snapshots_state(self, snap_list_names, expected_state):
        """
        Verify that every Ceph-side snapshot in ``snap_list_names`` is in
        ``expected_state`` according to odf cephfs-snap ls.

        Args:
            snap_list_names (list[dict]): Snapshot data as returned by
                :meth:`create_retain_cephfs_snapshots`.  Each dict must
                contain a ``"ceph_snap_name"`` key.
            expected_state (str): Expected state string, e.g.
                ``constants.CEPHFS_SNAPSHOT_STATE_ORPHANED`` or
                ``constants.CEPHFS_SNAPSHOT_STATE_BOUND``.

        Raises:
            AssertionError: If any snapshot is missing or not in the
                expected state.
        """
        snap_entries = get_cephfs_snap_entries(self._snap_runner)
        for snap_data in snap_list_names:
            ceph_name = snap_data["ceph_snap_name"]
            entry = get_cephfs_snap_by_name(snap_entries, ceph_name)
            assert entry["state"] == expected_state, (
                f"Expected state '{expected_state}' for snapshot "
                f"'{ceph_name}', got '{entry['state']}'"
            )
            log.info(
                "Snapshot '%s' is in '%s' state as expected",
                ceph_name,
                expected_state,
            )

    def verify_cephfs_snapshots_orphaned(self, snap_list_names):
        """
        Verify that every Ceph-side snapshot in ``snap_list_names`` has
        transitioned to the ``orphaned`` state in odf cephfs-snap ls.

        Args:
            snap_list_names (list[dict]): Snapshot data as returned by
                :meth:`create_retain_cephfs_snapshots`.  Each dict must
                contain a ``"ceph_snap_name"`` key.

        Raises:
            AssertionError: If any snapshot is missing or not orphaned.
        """
        self.verify_cephfs_snapshots_state(
            snap_list_names, constants.CEPHFS_SNAPSHOT_STATE_ORPHANED
        )

    def verify_cephfs_snapshots_bound(self, snap_list_names):
        """
        Verify that every Ceph-side snapshot in ``snap_list_names`` remains
        in the ``bound`` state in odf cephfs-snap ls.

        Args:
            snap_list_names (list[dict]): Snapshot data as returned by
                :meth:`create_retain_cephfs_snapshots`.  Each dict must
                contain a ``"ceph_snap_name"`` key.

        Raises:
            AssertionError: If any snapshot is missing or not in bound state.
        """
        self.verify_cephfs_snapshots_state(
            snap_list_names, constants.CEPHFS_SNAPSHOT_STATE_BOUND
        )

    @tier1
    def test_cephfs_orphaned_snapshot_alert(self):
        """
        Steps:
        1. Verify no CephFS snapshots exist.
        2. Create 7 VolumeSnapshots (3 to be orphaned, 4 to remain bound)
           using the Retain snapclass and wait until each is ready.
        3. List snapshots with odf cephfs-snap ls; verify each is present
           and in a Bound state.
        4. Verify both VolumeSnapshot and VolumeSnapshotContent exist in
           Kubernetes for each snapshot.
        5. Delete the VolumeSnapshots and VolumeSnapshotContents for the
           first 3 snapshots only. Their Ceph-side snapshots are retained
           due to the Retain policy, becoming orphaned.
        6. Verify the 3 deleted snapshots are orphaned and the remaining
           4 are still in Bound state.
        7. Wait for the CephFSOrphanedSnapshot Prometheus alert to fire
           (one alert per storage client, regardless of orphan count)
           and validate it.
        8. Delete the 3 orphaned snapshots via odf cephfs-snap delete.
        9. Verify only the 4 bound snapshots remain.
        10. Verify all alerts are cleared.
        """
        num_of_orphaned = 3
        num_of_bound = 4

        # Step 1: verify no snapshots exist
        log.info("Step 1: Verifying no CephFS snapshots exist")
        assert not get_cephfs_snap_entries(
            self._snap_runner
        ), "Expected no CephFS snapshots before the test"

        # Steps 2-4: create all snapshots with Retain policy,
        # verify Ceph state is Bound and k8s objects exist
        log.info(
            "Steps 2-4: Creating %d CephFS VolumeSnapshots with Retain "
            "policy and verifying Kubernetes objects",
            num_of_orphaned + num_of_bound,
        )
        self.create_retain_cephfs_snapshots(num_of_orphaned + num_of_bound)
        orphaned_snaps = self.snap_list_names[:num_of_orphaned]
        bound_snaps = self.snap_list_names[num_of_orphaned:]

        # Step 5: delete k8s objects for the orphaned group only;
        # Ceph-side snapshots are retained, becoming orphaned
        log.info(
            "Step 5: Deleting VolumeSnapshots and VolumeSnapshotContents "
            "for %d snapshot(s)",
            num_of_orphaned,
        )
        delete_volumesnaps_volumesnapcontents(orphaned_snaps)

        # Step 6: verify the two groups are in the expected states
        log.info(
            "Step 6: Verifying %d snapshot(s) are orphaned and "
            "%d snapshot(s) remain bound",
            num_of_orphaned,
            num_of_bound,
        )
        self.verify_cephfs_snapshots_orphaned(orphaned_snaps)
        self.verify_cephfs_snapshots_bound(bound_snaps)

        # Step 7: wait for the alert to fire and validate
        log.info("Step 7: Waiting for CephFSOrphanedSnapshot alert to fire")
        wait_for_alert_firing(
            self.api,
            constants.ALERT_CEPHFS_ORPHANED_SNAPSHOT,
            expected_severity="warning",
            expected_message_substr=constants.CEPHFS_SNAPSHOT_STATE_ORPHANED,
        )

        # Step 8: delete the orphaned snapshots via odf CLI
        log.info(
            "Step 8: Deleting %d orphaned snapshot(s) via odf CLI",
            num_of_orphaned,
        )
        self.delete_orphaned_cephfs_snapshots(orphaned_snaps)

        # Step 9: verify only the bound group remains
        log.info(
            "Step 9: Verifying %d bound snapshot(s) remain",
            num_of_bound,
        )
        snap_entries = get_cephfs_snap_entries(self._snap_runner)
        assert len(snap_entries) == num_of_bound, (
            f"Expected {num_of_bound} snapshot(s) after cleanup, "
            f"got {len(snap_entries)}"
        )
        self.verify_cephfs_snapshots_bound(bound_snaps)

        # Step 10: verify all alerts are cleared
        log.info("Step 10: Waiting for CephFSOrphanedSnapshot alerts to clear")
        wait_for_alert_cleared(self.api, constants.ALERT_CEPHFS_ORPHANED_SNAPSHOT)
