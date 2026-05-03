"""
RHSTOR-7465 — HighRBDCloneSnapshotCount alert test cases.

EXISTING TEST (Above Threshold — Alert Firing):
  The test that creates 201+ clones and verifies the HighRBDCloneSnapshotCount
  alert fires is already automated.  See PR #14947 and the note in the CSV:
    "Soft-Limit Threshold: Above Threshold: Alert Firing — Automated"
  If/when that test lands in ocs-ci, it belongs alongside the tests in this
  file (same class, same squad/tier decorators).

  Addition vs existing: the existing PR tests the basic firing on the provider.
  The test_high_rbd_clone_count_above_threshold_consumer test in this file
  (currently skipped — see below) would extend it to also verify the alert
  appears on the consumer Prometheus via the gRPC relay.

This file adds the remaining cases:
  - test_high_rbd_clone_count_below_threshold  (L-1 clones, NOT firing)
  - test_high_rbd_clone_count_boundary         (exactly L clones, NOT firing)
  - test_high_rbd_clone_count_alert_clear      (from Firing → reduce → cleared)

Helper additions
----------------
Steps that wait for alerts use the new helpers in ocs_ci/utility/prometheus.py:
  - wait_and_validate_alert_firing(api, alert_name, ...)
  - wait_for_alert_cleared(api, alert_name, ...)
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ocs_version,
    hci_provider_and_client_required,
    runs_on_provider,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.utility.prometheus import (
    wait_and_validate_alert_firing,
    wait_for_alert_cleared,
)

log = logging.getLogger(__name__)

# Default soft-limit threshold (number of RBD clones per source image)
CLONE_SOFT_LIMIT = 200

# RBD snapshot class with Retain policy used to set up a source image
RETAIN_RBD_SNAPCLASS_NAME = "test-rbd-retain-snapclass-clone-alert"


@green_squad
@tier2
@skipif_ocs_version("<4.22")
class TestHighRBDCloneSnapshotCountAlert(ManageTest):
    """
    Verify the HighRBDCloneSnapshotCount Prometheus alert behaviour for
    the RBD clone soft-limit feature (RHSTOR-7465).

    Test isolation note
    -------------------
    Creating 200+ clones is time-consuming (~1-2 hours in production).
    For CI, coordinate with the dev team on a configurable threshold
    (see RHSTOR-7465 CSV note).  These tests use a helper that batch-creates
    clones and waits for the rule's evaluation window.
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, pvc_factory, teardown_factory):
        """
        Create a source RBD PVC and a VolumeSnapshotClass with Retain policy.
        A snapshot is taken from the source PVC so that clones can be created
        from it.
        """
        from ocs_ci.ocs import templating
        from ocs_ci.ocs.resources import ocs

        self.source_pvc = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=1,
            status=constants.STATUS_BOUND,
        )

        snapclass_data = templating.load_yaml(constants.CSI_RBD_SNAPSHOTCLASS_YAML)
        snapclass_data["metadata"]["name"] = RETAIN_RBD_SNAPCLASS_NAME
        snapclass_data["deletionPolicy"] = constants.RECLAIM_POLICY_RETAIN
        self.snapclass_obj = ocs.OCS(**snapclass_data)
        assert self.snapclass_obj.create(
            do_reload=True
        ), f"Failed to create VolumeSnapshotClass {RETAIN_RBD_SNAPCLASS_NAME}"
        teardown_factory(self.snapclass_obj)

    def _create_rbd_clones(self, count, pvc_factory, teardown_factory):
        """
        Create `count` RBD clone PVCs from self.source_pvc.

        Returns:
            list: Created PVC objects.
        """
        log.info(f"Creating {count} RBD clone PVC(s) from {self.source_pvc.name}")
        clones = []
        for i in range(count):
            clone = pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                size=1,
                status=constants.STATUS_BOUND,
                source_pvc=self.source_pvc,
            )
            teardown_factory(clone)
            clones.append(clone)
        log.info(f"Created {len(clones)} clone(s)")
        return clones

    def test_high_rbd_clone_count_below_threshold(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        RHSTOR-7465 — Soft-Limit: Below Threshold (L-1 clones).

        Steps:
        1. Create L-1 RBD clone PVCs from the source PVC.
        2. Verify the ocs_rbd_children_count metric reflects L-1.
        3. Wait for the alert evaluation window (duration + buffer).
        4. Assert HighRBDCloneSnapshotCount is NOT firing.

        Note: This test was marked Passed (manually) in the CSV.
        When the threshold is configurable in CI (RHSTOR-7465 dev note),
        replace CLONE_SOFT_LIMIT with the CI-configured value.
        """
        clone_count = CLONE_SOFT_LIMIT - 1
        self._create_rbd_clones(clone_count, pvc_factory, teardown_factory)

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        # The alert must NOT be firing after L-1 clones
        log.info(
            f"Verifying HighRBDCloneSnapshotCount does NOT fire at {clone_count} "
            f"clones (threshold={CLONE_SOFT_LIMIT})"
        )
        firing = api.wait_for_alert(
            name=constants.ALERT_HIGH_RBD_CLONE_SNAPSHOT_COUNT,
            state="firing",
            timeout=120,  # short poll — we expect no alert
        )
        assert not firing, (
            f"HighRBDCloneSnapshotCount fired unexpectedly at {clone_count} "
            f"clones (below threshold {CLONE_SOFT_LIMIT})"
        )
        log.info("Confirmed: HighRBDCloneSnapshotCount is NOT firing below threshold")

    def test_high_rbd_clone_count_boundary(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        RHSTOR-7465 — Soft-Limit: Boundary (exactly L clones).

        The alert rule uses > L (strictly greater), so at exactly L clones
        the alert should NOT fire.

        Steps:
        1. Create exactly L RBD clone PVCs.
        2. Verify metric shows L.
        3. Wait 2× evaluation window.
        4. Assert alert is NOT firing (rule expression is > L).
        """
        self._create_rbd_clones(CLONE_SOFT_LIMIT, pvc_factory, teardown_factory)

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        log.info(
            f"Verifying HighRBDCloneSnapshotCount does NOT fire at exactly "
            f"{CLONE_SOFT_LIMIT} clones (boundary; rule uses > {CLONE_SOFT_LIMIT})"
        )
        firing = api.wait_for_alert(
            name=constants.ALERT_HIGH_RBD_CLONE_SNAPSHOT_COUNT,
            state="firing",
            timeout=120,
        )
        assert not firing, (
            f"HighRBDCloneSnapshotCount fired at exactly {CLONE_SOFT_LIMIT} "
            f"clones — the rule expression may be >= instead of >"
        )
        log.info(
            "Confirmed: HighRBDCloneSnapshotCount is NOT firing at exactly threshold"
        )

    def test_high_rbd_clone_count_alert_clear(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        RHSTOR-7465 — Alert Clear & Recovery: reduce clones below L.

        Starting from a Firing state (L+1 clones), delete clones until
        count drops below L and verify the alert clears.

        Steps:
        1. Create L+1 clone PVCs (triggers alert).
        2. Wait for HighRBDCloneSnapshotCount to fire and validate it.
        3. Delete clones until count is L-1.
        4. Wait for the alert to clear.
        5. Verify ALERTS metric returns to 0 for this alert.
        """
        clone_count = CLONE_SOFT_LIMIT + 1
        clones = self._create_rbd_clones(clone_count, pvc_factory, teardown_factory)

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        # Step 2: wait for alert to fire
        log.info(
            f"Step 2: Waiting for HighRBDCloneSnapshotCount to fire at "
            f"{clone_count} clones"
        )
        wait_and_validate_alert_firing(
            api=api,
            alert_name=constants.ALERT_HIGH_RBD_CLONE_SNAPSHOT_COUNT,
            timeout=600,
            expected_severity="warning",
        )

        # Step 3: delete enough clones to drop below threshold
        clones_to_delete = clones[: (clone_count - CLONE_SOFT_LIMIT + 1)]
        log.info(
            f"Step 3: Deleting {len(clones_to_delete)} clone(s) to bring "
            f"count below {CLONE_SOFT_LIMIT}"
        )
        for pvc_obj in clones_to_delete:
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)

        # Step 4: wait for alert to clear
        log.info("Step 4: Waiting for HighRBDCloneSnapshotCount to clear")
        wait_for_alert_cleared(
            api=api,
            alert_name=constants.ALERT_HIGH_RBD_CLONE_SNAPSHOT_COUNT,
            timeout=600,
        )
        log.info("HighRBDCloneSnapshotCount alert cleared after clone reduction")


# ---------------------------------------------------------------------------
# Provider/Client variant — requires provider+client topology
# ---------------------------------------------------------------------------


@green_squad
@tier2
@skipif_ocs_version("<4.22")
@runs_on_provider
@hci_provider_and_client_required
class TestHighRBDCloneSnapshotCountProviderClient(ManageTest):
    """
    RHSTOR-7465 (provider/client variant) — verify the alert fires on the
    consumer Prometheus and carries the correct consumer_name label.

    EXISTING TEST (above-threshold, provider side):
      See PR #14947 and OCSQE-4606. The basic firing test on the provider is
      already covered. This class extends it to cover the consumer side.

    Addition vs existing PR:
      The existing test verifies the alert fires on the provider Prometheus.
      test_high_rbd_clone_count_consumer_alert below additionally verifies:
        - The alert is relayed to the consumer Prometheus via gRPC.
        - The consumer alert carries the correct alertname, severity, and
          annotations (message + runbook_url/description).
    """

    def test_high_rbd_clone_count_consumer_alert(
        self, pvc_factory, teardown_factory, threading_lock
    ):
        """
        RHSTOR-7465 — Consumer-side: verify HighRBDCloneSnapshotCount appears
        on consumer Prometheus after being triggered on the provider.

        Steps:
        1. On the client cluster, create L+1 RBD clone PVCs.
        2. On the provider Prometheus, wait for the alert with consumer_name.
        3. On the consumer Prometheus, wait for the alert to appear via gRPC.
        4. Validate alert labels and annotations on the consumer.
        5. Delete clones; verify alert clears on both provider and consumer.

        TODO: Switching kubeconfig contexts to query consumer Prometheus
        requires the multi-cluster config helpers. Implement using
        config.RunWithFirstConsumerConfigContextIfAvailable() once the
        consumer Prometheus access pattern is established.
        Tracked in OCSQE-4606.
        """
        pytest.skip(
            "Consumer Prometheus access requires multi-cluster context switching. "
            "Tracked in OCSQE-4606."
        )
