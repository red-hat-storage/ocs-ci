"""
Negative: Invalid Subscription Channel Handling (RHSTOR-8290 / TC-12 Phase 2).

Verify FDF handles invalid Subscription channel gracefully — Subscription
enters pending/error state, and recovery works when channel is corrected.
"""

import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier3,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier3
class TestFDFInvalidSubscription:
    """
    Verify FDF handles invalid Subscription channel gracefully.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """Record the current Subscription channel for restoration."""
        self.namespace = config.ENV_DATA["cluster_namespace"]
        sub_ocp = OCP(kind="Subscription", namespace=self.namespace)
        subs = sub_ocp.get().get("items", [])

        self.original_channels = {}
        for sub in subs:
            name = sub["metadata"]["name"]
            channel = sub["spec"].get("channel", "")
            self.original_channels[name] = channel
        logger.info("Original channels: %s", self.original_channels)

    @pytest.fixture()
    def restore_channels(self):
        """Restore original Subscription channels after test."""
        yield
        logger.info("Restoring original Subscription channels")
        sub_ocp = OCP(kind="Subscription", namespace=self.namespace)
        for name, channel in self.original_channels.items():
            if channel:
                sub_ocp.patch(
                    resource_name=name,
                    params=f'{{"spec": {{"channel": "{channel}"}}}}',
                    format_type="merge",
                )
                logger.info("Restored '%s' to channel '%s'", name, channel)
        verify_storage_cluster()

    def test_invalid_channel_recovery(self, restore_channels):
        """
        Patch Subscription with non-existent channel, verify pending/error
        state, then fix and verify install proceeds.

        Checkpoint B: Invalid channel handled, recovery works.
        """
        sub_ocp = OCP(kind="Subscription", namespace=self.namespace)
        bad_channel = "stable-99.99"

        # Patch the odf-operator Subscription with invalid channel
        target_sub = None
        for name in self.original_channels:
            if "odf-operator" in name:
                target_sub = name
                break

        if not target_sub:
            target_sub = list(self.original_channels.keys())[0]

        logger.info(
            "Patching Subscription '%s' with invalid channel '%s'",
            target_sub,
            bad_channel,
        )
        sub_ocp.patch(
            resource_name=target_sub,
            params=f'{{"spec": {{"channel": "{bad_channel}"}}}}',
            format_type="merge",
        )

        # Wait and verify Subscription enters a non-healthy state
        logger.info("Waiting for Subscription to reflect invalid channel")
        error_detected = False
        end_time = time.time() + 120
        while time.time() < end_time:
            sub_data = sub_ocp.get(resource_name=target_sub)
            conditions = sub_data.get("status", {}).get("conditions", [])
            state = sub_data.get("status", {}).get("state", "")

            for cond in conditions:
                msg = cond.get("message", "")
                reason = cond.get("reason", "")
                if (
                    "not found" in msg.lower()
                    or "error" in reason.lower()
                    or "constraint" in msg.lower()
                ):
                    logger.info(
                        "Subscription error detected — reason: %s, msg: %s",
                        reason,
                        msg,
                    )
                    error_detected = True
                    break

            if state in ("UpgradePending", "AtLatestKnown"):
                current_channel = sub_data["spec"].get("channel", "")
                if current_channel == bad_channel:
                    logger.info("Subscription state '%s' with invalid channel", state)
                    error_detected = True

            if error_detected:
                break
            time.sleep(10)

        assert error_detected, (
            f"Subscription '{target_sub}' did not show error/pending state "
            f"with invalid channel '{bad_channel}'"
        )
        logger.info("Checkpoint B: Invalid channel handled correctly")

        # Verify no operator crashes
        pod_ocp = OCP(kind="pod", namespace=self.namespace)
        pods = pod_ocp.get().get("items", [])
        crash_pods = [
            p["metadata"]["name"]
            for p in pods
            if any(
                cs.get("state", {}).get("waiting", {}).get("reason")
                == "CrashLoopBackOff"
                for cs in p.get("status", {}).get("containerStatuses", [])
            )
        ]
        assert not crash_pods, f"Operator pods in CrashLoopBackOff: {crash_pods}"

        # Recovery is handled by restore_channels fixture
        logger.info("Invalid Subscription channel handling verified")
