import logging
import random
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad, polarion_id
from ocs_ci.framework.testlib import (
    ManageTest,
    hci_provider_and_client_required,
    tier1,
    tier2,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import get_cephfs_subvolumegroup
from ocs_ci.helpers.odf_cephfs_snap import (
    verify_bound_snapshot_delete_rejected,
    create_provider_retain_cephfs_snapclass,
    delete_volumesnaps_volumesnapcontents,
    get_cephfs_snap_by_name,
    get_cephfs_snap_entries,
    wait_and_verify_snapshot_bound,
)
from ocs_ci.ocs.resources.storageconsumer import get_consumer_svg_on_provider
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

        storage_client = (
            get_storage_client().resource_name if config.multicluster else None
        )
        self._snap_runner = odf_cli_cephfs_snap_setup_helper(
            storage_client=storage_client
        )

        self.api = PrometheusAPI(
            threading_lock=threading_lock,
            cluster_context=config.RunWithFirstConsumerConfigContextIfAvailable,
        )

        self.provider_api = None
        if config.multicluster:
            self.provider_api = PrometheusAPI(
                threading_lock=threading_lock,
                cluster_context=config.RunWithProviderConfigContextIfAvailable,
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

    def _wait_for_orphaned_alert_firing(self):
        """
        Wait for the CephFSOrphanedSnapshot alert to fire.

        In multicluster mode both the provider and consumer Prometheus are
        checked.  In standalone mode the single cluster's Prometheus is used.
        """
        if self.provider_api:
            log.info("Waiting for CephFSOrphanedSnapshot alert to fire on provider")
            wait_for_alert_firing(
                self.provider_api,
                constants.ALERT_CEPHFS_ORPHANED_SNAPSHOT,
                expected_severity="warning",
                expected_message_substr=constants.CEPHFS_SNAPSHOT_STATE_ORPHANED,
            )
        log.info("Waiting for CephFSOrphanedSnapshot alert to fire on consumer")
        wait_for_alert_firing(
            self.api,
            constants.ALERT_CEPHFS_ORPHANED_SNAPSHOT,
            expected_severity="warning",
            expected_message_substr=constants.CEPHFS_SNAPSHOT_STATE_ORPHANED,
        )

    def _wait_for_orphaned_alert_cleared(self):
        """
        Wait for the CephFSOrphanedSnapshot alert to clear.

        In multicluster mode both the provider and consumer Prometheus are
        checked.  In standalone mode the single cluster's Prometheus is used.
        """
        if self.provider_api:
            log.info("Waiting for CephFSOrphanedSnapshot alerts to clear on provider")
            wait_for_alert_cleared(
                self.provider_api, constants.ALERT_CEPHFS_ORPHANED_SNAPSHOT
            )
        log.info("Waiting for CephFSOrphanedSnapshot alerts to clear on consumer")
        wait_for_alert_cleared(self.api, constants.ALERT_CEPHFS_ORPHANED_SNAPSHOT)

    def _resolve_svg(self, svg_param):
        """
        Resolve ``svg_param`` to an actual subvolume group name and set it
        on ``self._snap_runner``.

        Args:
            svg_param (str or None): ``"consumer_svg_on_provider"`` fetches
                the consumer's SVG from the provider cluster.
                ``"default_svg"`` uses the cluster's default subvolume group.
                ``None`` leaves the snap runner without an explicit ``--svg``
                flag (original behaviour).
        """
        if svg_param == "consumer_svg_on_provider":
            storage_client_name = get_storage_client().resource_name
            client_cluster_name = config.ENV_DATA.get("cluster_name")
            with config.RunWithProviderConfigContextIfAvailable():
                svg = get_consumer_svg_on_provider(
                    storage_client_name,
                    client_cluster_name=client_cluster_name,
                )
            log.info("Using consumer SVG on provider: %s", svg)
        elif svg_param == "default_svg":
            svg = get_cephfs_subvolumegroup()
            log.info("Using default CephFS subvolumegroup: %s", svg)
        else:
            svg = None
            log.info("Using no explicit --svg (odf-cli default)")
        self._snap_runner.svg = svg

    @tier1
    @pytest.mark.parametrize(
        "svg_param",
        [
            pytest.param(None, marks=polarion_id("OCS-7944")),
            pytest.param("default_svg", marks=polarion_id("OCS-7946")),
            pytest.param(
                "consumer_svg_on_provider",
                marks=[hci_provider_and_client_required, polarion_id("OCS-7947")],
            ),
        ],
        ids=["no_svg", "default_svg", "consumer_svg_on_provider"],
    )
    def test_cephfs_orphaned_snapshot_alert(self, svg_param):
        """
        Args:
            svg_param (str or None): Controls the ``--svg`` flag passed to
                odf-cli. ``"consumer_svg_on_provider"`` uses the consumer's
                SVG on the provider (provider-client only).
                ``"default_svg"`` uses the cluster's default subvolume group.
                ``None`` omits ``--svg`` (original behaviour).

        Steps:
        1. Resolve the ``--svg`` value from ``svg_param`` and update the
           snap runner.
        2. Verify no CephFS snapshots exist.
        3. Create N VolumeSnapshots (num_of_orphaned to be orphaned,
           num_of_bound to remain bound) using the Retain snapclass and
           wait until each is ready.
        4. List snapshots with odf cephfs-snap ls; verify each is present
           and in a Bound state.
        5. Verify both VolumeSnapshot and VolumeSnapshotContent exist in
           Kubernetes for each snapshot.
        6. Delete the VolumeSnapshots and VolumeSnapshotContents for the
           first num_of_orphaned snapshots only. Their Ceph-side snapshots
           are retained due to the Retain policy, becoming orphaned.
        7. Verify the num_of_orphaned deleted snapshots are orphaned and
           the remaining num_of_bound are still in Bound state.
        8. Wait for the CephFSOrphanedSnapshot Prometheus alert to fire
           (one alert per storage client, regardless of orphan count)
           and validate it.
        9. Delete the num_of_orphaned orphaned snapshots via odf cephfs-snap
           delete.
        10. Verify only the num_of_bound bound snapshots remain.
        11. Verify all alerts are cleared.
        """
        seed = int(time.time())
        random.seed(seed)
        log.info("Random seed for this test run: %d", seed)
        num_of_orphaned = random.randint(1, 4)
        num_of_bound = random.randint(4, 7)

        log.test_step("Resolve odf-cli --svg from svg_param=%s", svg_param)
        self._resolve_svg(svg_param)

        log.test_step("Verify no CephFS snapshots exist")
        assert not get_cephfs_snap_entries(
            self._snap_runner
        ), "Expected no CephFS snapshots before the test"

        log.test_step(
            "Create %d CephFS VolumeSnapshots with Retain "
            "policy and verify Kubernetes objects",
            num_of_orphaned + num_of_bound,
        )
        self.create_retain_cephfs_snapshots(num_of_orphaned + num_of_bound)
        orphaned_snaps = self.snap_list_names[:num_of_orphaned]
        bound_snaps = self.snap_list_names[num_of_orphaned:]

        log.test_step(
            "Delete VolumeSnapshots and VolumeSnapshotContents " "for %d snapshot(s)",
            num_of_orphaned,
        )
        delete_volumesnaps_volumesnapcontents(orphaned_snaps)

        log.test_step(
            "Verify %d snapshot(s) are orphaned and %d snapshot(s) remain bound",
            num_of_orphaned,
            num_of_bound,
        )
        self.verify_cephfs_snapshots_orphaned(orphaned_snaps)
        self.verify_cephfs_snapshots_bound(bound_snaps)

        log.test_step("Wait for CephFSOrphanedSnapshot alert to fire")
        self._wait_for_orphaned_alert_firing()

        log.test_step(
            "Delete %d orphaned snapshot(s) via odf CLI",
            num_of_orphaned,
        )
        self.delete_orphaned_cephfs_snapshots(orphaned_snaps)

        log.test_step("Verify %d bound snapshot(s) remain", num_of_bound)
        snap_entries = get_cephfs_snap_entries(self._snap_runner)
        assert len(snap_entries) == num_of_bound, (
            f"Expected {num_of_bound} snapshot(s) after cleanup, "
            f"got {len(snap_entries)}"
        )
        self.verify_cephfs_snapshots_bound(bound_snaps)

        log.test_step("Wait for CephFSOrphanedSnapshot alerts to clear")
        self._wait_for_orphaned_alert_cleared()

    @tier2
    @polarion_id("OCS-8027")
    def test_cephfs_bound_snapshot_delete_rejected(self):
        """
        Verify that attempting to delete a Bound CephFS snapshot via the
        odf CLI is rejected with an error and the snapshot is preserved.

        Steps:
        1. Resolve the odf-cli --svg value (no explicit svg).
        2. Verify no CephFS snapshots exist.
        3. Create snapshots with Retain policy: 1 to be orphaned,
           2 to remain bound.
        4. Delete VolumeSnapshot/VolumeSnapshotContent for the orphaned
           snapshot so it becomes orphaned; bound snapshots remain bound.
        5. Verify the orphaned snapshot is orphaned and the bound
           snapshots remain bound.
        6. Attempt to delete a bound snapshot via odf cephfs-snap delete.
        7. Verify the delete operation fails (CommandFailed is raised)
           and a non-empty error message is returned.
        8. Verify the targeted bound snapshot still exists in Bound state.
        9. List all CephFS snapshots and confirm the bound snapshot is
           present and unchanged.
        """
        num_of_orphaned = 1
        num_of_bound = 2

        log.test_step("Resolve odf-cli --svg (no explicit svg)")
        self._resolve_svg(None)

        log.test_step("Verify no CephFS snapshots exist")
        assert not get_cephfs_snap_entries(
            self._snap_runner
        ), "Expected no CephFS snapshots before the test"

        log.test_step(
            "Create %d CephFS VolumeSnapshots with Retain policy",
            num_of_orphaned + num_of_bound,
        )
        self.create_retain_cephfs_snapshots(num_of_orphaned + num_of_bound)
        orphaned_snaps = self.snap_list_names[:num_of_orphaned]
        bound_snaps = self.snap_list_names[num_of_orphaned:]

        log.test_step(
            "Delete VolumeSnapshot/VolumeSnapshotContent for "
            "%d orphaned snapshot(s)",
            num_of_orphaned,
        )
        delete_volumesnaps_volumesnapcontents(orphaned_snaps)

        log.test_step(
            "Verify %d snapshot(s) are orphaned and " "%d snapshot(s) remain bound",
            num_of_orphaned,
            num_of_bound,
        )
        self.verify_cephfs_snapshots_orphaned(orphaned_snaps)
        self.verify_cephfs_snapshots_bound(bound_snaps)

        target_snap = bound_snaps[0]
        log.test_step(
            "Attempt to delete bound snapshot '%s' via odf CLI",
            target_snap["ceph_snap_name"],
        )
        verify_bound_snapshot_delete_rejected(self._snap_runner, target_snap)

        log.test_step(
            "Verify bound snapshot '%s' still exists in Bound state",
            target_snap["ceph_snap_name"],
        )
        wait_and_verify_snapshot_bound(self._snap_runner, target_snap)

        log.test_step("List CephFS snapshots and confirm bound snapshot is unchanged")
        snap_entries = get_cephfs_snap_entries(self._snap_runner)
        bound_names = {
            e["snapshot"]
            for e in snap_entries
            if e["state"] == constants.CEPHFS_SNAPSHOT_STATE_BOUND
        }
        assert target_snap["ceph_snap_name"] in bound_names, (
            f"Bound snapshot '{target_snap['ceph_snap_name']}' not "
            f"found in snapshot list after failed delete attempt"
        )
        log.info(
            "Confirmed: bound snapshot '%s' is unchanged after "
            "failed delete attempt",
            target_snap["ceph_snap_name"],
        )
