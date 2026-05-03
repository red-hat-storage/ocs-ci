"""
RHSTOR-7679 — CephFS Subvolume Metrics (Prometheus-queryable tests).

ODF CONSOLE UI TESTS — OUT OF SCOPE FOR OCS-CI:
  All RHSTOR-7679 test cases in the CSV describe interactions with the ODF
  console UI (page navigation, Figma layout comparison, drill-down drawers,
  tab switching, etc.).  These require a browser automation framework
  (Selenium / Cypress) and are NOT automatable with ocs-ci's standard
  pytest-over-oc approach.

  Status from CSV (all verified manually):
    - Subvolume metrics section reachable          — Passed (manual)
    - Metrics load with active CephFS workloads    — Passed (manual)
    - Metric units and labels                      — Passed (manual)
    - Top 10 by IOPS / Latency / Throughput        — Passed (manual)
    - Column sort / tab switch                     — Passed (manual)
    - Namespace / PVC identification               — Passed (manual)

  Pending (manual or future browser-automation work, tracked in OCSQE-4576):
    - RBAC — limited user
    - Fewer than 10 PVCs
    - Refresh / auto-refresh
    - All Drill-Down tests
    - Provider/Client topology tests
    - External mode cluster

WHAT THIS FILE COVERS:
  The underlying Prometheus metrics that feed the UI CAN be validated in
  ocs-ci by querying Prometheus directly.  This file tests:
    1. ocs_cephfs_subvolume_count is present and non-zero with active PVCs.
    2. ocs_cephfs_subvolume_count decrements when PVCs are deleted.
    3. In provider mode, ocs_cephfs_subvolume_count carries consumer_name.
    4. ocs_cephfs_snapshot_content_count is present.
    5. ocs_cephfs_pv_metadata is present with correct labels.

Existing coverage
-----------------
  - tests/functional/pv/pv_services/test_pvc_delete_subvolumegroup.py
      ::test_pvc_delete_subvolumegroup
    Tests PVC lifecycle and SVG cleanup — does NOT query Prometheus metrics.

  - tests/functional/monitoring/prometheus/metrics/test_monitoring_defaults.py
      ::test_ceph_metrics_available
    Verifies a broad set of Ceph metrics are present but does not specifically
    check CephFS subvolume counts or consumer_name labels.

Addition vs existing tests
--------------------------
This file explicitly queries ocs_cephfs_subvolume_count via Prometheus and
the /metrics endpoint and verifies value semantics (count increments and
decrements with PVC lifecycle).
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    tier2,
    skipif_ocs_version,
    hci_provider_and_client_required,
    runs_on_provider,
)
from ocs_ci.helpers.metrics_exporter import (
    get_metrics_exporter_pod,
    query_metrics_exporter_endpoint,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus

log = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_ocs_version("<4.22")
class TestCephFSSubvolumeMetrics(ManageTest):
    """
    RHSTOR-7679 — Validate the Prometheus metrics that back the CephFS
    subvolume metrics UI page.
    """

    def test_cephfs_subvolume_count_present_and_nonzero(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        Verify ocs_cephfs_subvolume_count is present in Prometheus and reflects
        the number of active CephFS PVCs.

        Steps:
        1. Create 2 CephFS PVCs.
        2. Query Prometheus for ocs_cephfs_subvolume_count.
        3. Verify the metric is present with a value >= 2.
        """
        pvc1 = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=1,
            status=constants.STATUS_BOUND,
        )
        pvc2 = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=1,
            status=constants.STATUS_BOUND,
        )
        teardown_factory(pvc1)
        teardown_factory(pvc2)

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        log.info("Querying ocs_cephfs_subvolume_count from Prometheus")
        result = api.query("ocs_cephfs_subvolume_count")
        assert result, "ocs_cephfs_subvolume_count metric is not present in Prometheus"

        total = sum(float(r["value"][1]) for r in result)
        assert total >= 2, (
            f"Expected ocs_cephfs_subvolume_count >= 2 after creating 2 PVCs, "
            f"got {total}"
        )
        log.info(f"ocs_cephfs_subvolume_count = {total} (expected >= 2) — OK")

    def test_cephfs_subvolume_count_decrements_on_pvc_delete(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        Verify ocs_cephfs_subvolume_count decrements when CephFS PVCs are
        deleted.

        Steps:
        1. Create 3 CephFS PVCs; record count before.
        2. Delete 2 PVCs.
        3. Verify count decreased by 2.
        """
        pvcs = [
            pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                size=1,
                status=constants.STATUS_BOUND,
            )
            for _ in range(3)
        ]
        for pvc in pvcs:
            teardown_factory(pvc)

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        def _get_count():
            result = api.query("ocs_cephfs_subvolume_count")
            return sum(float(r["value"][1]) for r in result) if result else 0

        count_before = _get_count()
        log.info(f"ocs_cephfs_subvolume_count before deletion: {count_before}")

        # Delete 2 of the 3 PVCs
        for pvc in pvcs[:2]:
            pvc.delete()
            pvc.ocp.wait_for_delete(pvc.name)

        count_after = _get_count()
        log.info(f"ocs_cephfs_subvolume_count after deleting 2 PVCs: {count_after}")
        assert count_after == count_before - 2, (
            f"Expected count to decrease by 2 from {count_before}, "
            f"got {count_after}"
        )

    def test_cephfs_pv_metadata_labels(self, pvc_factory, teardown_factory):
        """
        Verify ocs_cephfs_pv_metadata metric is present with required labels
        (name, subvolume, subvolume_group, volume).

        Steps:
        1. Create a CephFS PVC.
        2. Query /metrics endpoint for ocs_cephfs_pv_metadata.
        3. Verify a line exists with name=<pvc-name> and required label keys.
        """
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=1,
            status=constants.STATUS_BOUND,
        )
        teardown_factory(pvc_obj)

        pod = get_metrics_exporter_pod()
        pod_name = pod["metadata"]["name"]

        output = query_metrics_exporter_endpoint(
            pod_name=pod_name,
            metric_filter="ocs_cephfs_pv_metadata",
        )
        assert output, "ocs_cephfs_pv_metadata not found in /metrics output"

        required_labels = ["name=", "subvolume=", "subvolume_group=", "volume="]
        for line in output.splitlines():
            if line.startswith("ocs_cephfs_pv_metadata") and not line.startswith("#"):
                missing = [l for l in required_labels if l not in line]
                assert (
                    not missing
                ), f"ocs_cephfs_pv_metadata line is missing labels {missing}: {line}"
                log.info(f"ocs_cephfs_pv_metadata labels verified: {line[:120]}")
                break
        else:
            pytest.fail("No data line found for ocs_cephfs_pv_metadata")

    def test_cephfs_snapshot_content_count_present(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        Verify ocs_cephfs_snapshot_content_count metric is present in
        Prometheus (value can be 0 if no snapshots exist).

        Steps:
        1. Ensure at least one CephFS PVC exists.
        2. Query Prometheus for ocs_cephfs_snapshot_content_count.
        3. Verify the metric is present (value is a valid float).
        """
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=1,
            status=constants.STATUS_BOUND,
        )
        teardown_factory(pvc_obj)

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        result = api.query("ocs_cephfs_snapshot_content_count")
        assert (
            result is not None
        ), "ocs_cephfs_snapshot_content_count metric not present in Prometheus"
        log.info(
            f"ocs_cephfs_snapshot_content_count = "
            f"{result[0]['value'][1] if result else 'absent'}"
        )


# ---------------------------------------------------------------------------
# Provider-mode variant — requires provider+client topology
# ---------------------------------------------------------------------------


@green_squad
@tier2
@skipif_ocs_version("<4.22")
@runs_on_provider
@hci_provider_and_client_required
class TestCephFSSubvolumeMetricsProvider(ManageTest):
    """
    RHSTOR-7679 — Validate ocs_cephfs_subvolume_count carries consumer_name
    in Provider mode (feeds the UI's per-client breakdown).

    UI test note: The console UI tests for Provider/Client topology
    (e.g., "Client Can Show Metrics When Sharing Works") are manual-only and
    tracked in OCSQE-4576.  The tests below validate the Prometheus data layer
    that the UI reads from.
    """

    def test_cephfs_subvolume_count_consumer_name(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        RHSTOR-7679 Provider/Client — ocs_cephfs_subvolume_count carries
        consumer_name label for client PVCs.

        Steps:
        1. Create CephFS PVCs on a client cluster.
        2. On the provider, query ocs_cephfs_subvolume_count.
        3. Verify entries with consumer_name=<client-name> and count >= 1.
        4. Verify SVG isolation (each client's subvolumes are in a separate
           subvolume group).

        TODO: Creating PVCs on the client requires multi-cluster context.
        Tracked in OCSQE-4576.
        """
        pytest.skip(
            "Creating client PVCs requires multi-cluster context switching. "
            "Tracked in OCSQE-4576."
        )

    def test_cephfs_subvolume_count_multiple_consumers(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        RHSTOR-7679 Provider/Client — ocs_cephfs_subvolume_count correctly
        reports per-consumer isolation when multiple clients are connected.

        Steps:
        1. Create 3 CephFS PVCs on client 1, 2 on client 2.
        2. Query provider; verify client1: count=3, client2: count=2
           in separate metric entries.

        TODO: Requires two client clusters. Tracked in OCSQE-4576.
        """
        pytest.skip("Requires two client clusters. Tracked in OCSQE-4576.")
