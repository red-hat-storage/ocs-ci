import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    jira,
    skipif_ocs_version,
    mdr,
)
from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.helpers.dr_helpers import (
    get_current_primary_cluster_name,
    wait_for_drpc_phase,
    wait_for_first_kube_object_protection,
    monitor_drpc_protected_condition_stability,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)

# How long to observe the DRPC status for stability (seconds)
OBSERVATION_WINDOW = 600
# Polling interval for DRPC status checks (seconds)
POLL_INTERVAL = 30
# Maximum allowed transitions between Protecting and Healthy during observation
MAX_ALLOWED_TRANSITIONS = 2


@mdr
@green_squad
@tier4
@jira("DFBUGS-8924")
@skipif_ocs_version("<4.22")
class TestDiscoveredAppsCaptureIntervalStability(ManageTest):
    """
    Test to verify that discovered apps with captureInterval of 5 minutes
    do not display inconsistent behavior (DFBUGS-8924).

    The bug caused DRPC to juggle between 'Protecting' and 'Healthy' states
    frequently, showing BSL errors and kube object protect errors when
    captureInterval was set to 5 minutes. The fix ensures that during an
    active capture cycle, the ClusterDataProtected condition is set to a
    transitional 'Uploading' state instead of falsely reporting
    'KubeObjectsCaptureNotStarted'.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, request, discovered_apps_dr_workload):
        """
        Setup fixture to deploy a discovered app workload with kube object
        protection enabled and register cleanup.

        The captureInterval is derived from the DRPolicy schedulingInterval.
        The discovered_apps_dr_workload fixture deploys a BusyboxDiscoveredApps
        workload with kube object protection enabled (kubeobject=1).

        Args:
            request: pytest request object for finalizer registration.
            discovered_apps_dr_workload: Factory fixture for discovered app
                DR workload deployment.

        Returns:
            None. Sets self.workload and self.drpc_name on the instance.
        """
        logger.test_step("Deploy discovered app workload with kube object protection")
        self.workload = discovered_apps_dr_workload(kubeobject=1)
        self.namespace = self.workload.workload_namespace
        self.drpc_name = self.workload.drpc_name

        logger.info(
            f"Deployed discovered app workload: drpc_name={self.drpc_name}, "
            f"namespace={self.namespace}"
        )

        def finalizer():
            """Clean up the DR workload resources."""
            logger.info(f"Cleaning up DR workload {self.drpc_name}")
            self.workload.delete()

        request.addfinalizer(finalizer)

    def test_discovered_app_captureinterval_stability(self):
        """
        Verify that a discovered app with kube object protection remains
        stable and does not oscillate between Protecting and Healthy states.

        Bug: DFBUGS-8924
        When captureInterval was set to 5 minutes, the DRPC would juggle
        between 'Protecting' and 'Healthy' states, showing BSL errors and
        kube object protect errors. The fix ensures that during capture,
        the condition reports 'Uploading' instead of 'CaptureNotStarted'.

        Steps:
            1. Deploy a discovered app with kube object protection.
            2. Wait for the DRPC to reach initial 'Deployed' phase.
            3. Wait for the first successful kube object protection cycle.
            4. Monitor DRPC Protected condition over an observation window.
            5. Verify no BSL or KubeObjectsCaptureNotStarted errors appear.
            6. Verify the number of state transitions is within acceptable
               bounds (no oscillation).
            7. Verify VRG ClusterDataProtected condition does not show
               'KubeObjectsCaptureNotStarted' persistently.
        """
        primary_cluster_name = get_current_primary_cluster_name(
            self.workload.workload_namespace,
            self.workload.discovered_apps_placement_name,
        )
        logger.info(f"Primary cluster: {primary_cluster_name}")

        logger.test_step("Wait for DRPC to reach Deployed phase")

        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=self.drpc_name,
        )

        wait_for_drpc_phase(drpc_obj, phase="Deployed", timeout=300, sleep=15)

        logger.test_step("Wait for first successful kube object protection cycle")
        protection_time = wait_for_first_kube_object_protection(
            drpc_obj, timeout=600, sleep=20
        )

        logger.assertion(
            f"expected=first kube object protection seen, "
            f"actual={protection_time is not None}"
        )
        assert protection_time is not None, (
            f"DRPC {self.drpc_name} never completed first kube object protection "
            f"within timeout"
        )

        logger.test_step(
            f"Monitor DRPC Protected condition stability over {OBSERVATION_WINDOW}s "
            f"observation window"
        )
        metrics = monitor_drpc_protected_condition_stability(
            drpc_obj,
            observation_window=OBSERVATION_WINDOW,
            poll_interval=POLL_INTERVAL,
        )

        transitions = metrics["transitions"]
        bsl_errors_seen = metrics["bsl_errors"]
        capture_not_started_count = metrics["capture_not_started_count"]
        uploading_seen = metrics["uploading_seen"]
        final_protected = metrics["final_protected"]

        logger.test_step("Verify DRPC did not oscillate between Protecting and Healthy")
        logger.assertion(
            f"expected=transitions <= {MAX_ALLOWED_TRANSITIONS}, "
            f"actual=transitions={transitions}"
        )
        assert transitions <= MAX_ALLOWED_TRANSITIONS, (
            f"DRPC {self.drpc_name} oscillated between Protecting and Healthy "
            f"{transitions} times during {OBSERVATION_WINDOW}s observation window. "
            f"Maximum allowed transitions: {MAX_ALLOWED_TRANSITIONS}. "
            f"This indicates the bug DFBUGS-8924 may not be fixed."
        )

        logger.test_step("Verify no persistent BSL errors occurred")
        max_bsl_errors = 2
        logger.assertion(
            f"expected=bsl_errors <= {max_bsl_errors}, "
            f"actual=bsl_errors={len(bsl_errors_seen)}"
        )
        assert len(bsl_errors_seen) <= max_bsl_errors, (
            f"DRPC {self.drpc_name} showed {len(bsl_errors_seen)} BSL errors "
            f"during {OBSERVATION_WINDOW}s observation window. "
            f"BSL errors: {bsl_errors_seen}. "
            f"This indicates the bug DFBUGS-8924 may not be fixed."
        )

        logger.test_step(
            "Verify KubeObjectsCaptureNotStarted does not appear persistently"
        )
        max_capture_not_started = 3
        logger.assertion(
            f"expected=capture_not_started <= {max_capture_not_started}, "
            f"actual=capture_not_started={capture_not_started_count}"
        )
        assert capture_not_started_count <= max_capture_not_started, (
            f"DRPC {self.drpc_name} showed 'KubeObjectsCaptureNotStarted' "
            f"{capture_not_started_count} times during {OBSERVATION_WINDOW}s "
            f"observation window. Maximum allowed: {max_capture_not_started}. "
            f"The fix should report 'Uploading' or 'capture in-progress' instead."
        )

        logger.test_step("Verify DRPC ends in a healthy Protected state")
        logger.assertion(
            f"expected=Protected condition status='True', "
            f"actual={final_protected}"
        )
        assert final_protected is not None, (
            f"DRPC {self.drpc_name} does not have a Protected condition in its status"
        )
        assert final_protected.get("status") == "True", (
            f"DRPC {self.drpc_name} Protected condition is not 'True' at end of "
            f"observation. Status: {final_protected.get('status')}, "
            f"Reason: {final_protected.get('reason')}, "
            f"Message: {final_protected.get('message')}"
        )

        logger.info(
            f"DRPC {self.drpc_name} remained stable. "
            f"Total transitions: {transitions}, BSL errors: {len(bsl_errors_seen)}, "
            f"CaptureNotStarted occurrences: {capture_not_started_count}, "
            f"Uploading state seen: {uploading_seen}"
        )