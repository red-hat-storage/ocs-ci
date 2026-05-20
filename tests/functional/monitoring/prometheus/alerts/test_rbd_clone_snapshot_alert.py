# -*- coding: utf8 -*-
"""
Test case TC-9.1: Verify HighRBDCloneSnapshotCount alert fires when CSI clones
exceed the clone soft limit of 200 (ocs_rbd_children_count > 200).

Supports both internal mode (single cluster) and provider mode (multi-cluster).
In provider mode, additionally validates the consumer_name label on the alert.
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    tier4c,
    runs_on_provider,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus


log = logging.getLogger(__name__)


@blue_squad
@tier4c
@runs_on_provider
@skipif_ocs_version("<4.18")
@pytest.mark.polarion_id("OCS-XXXX")
def test_high_rbd_clone_snapshot_count_alert(
    measure_create_high_rbd_clone_snapshot_count, threading_lock
):
    """
    Test that HighRBDCloneSnapshotCount alert fires when CSI clones exceed
    the clone soft limit of 200 (ocs_rbd_children_count > 200). Creates
    201 CSI clones from a single RBD PVC to trigger the alert.

    In provider mode, clones are created on the consumer cluster and the
    alert is validated on the provider with consumer_name label matching
    the StorageConsumer. In internal mode, resources are created locally
    and the alert is validated without consumer_name check.

    Verifies the alert clears after the clones are deleted in both modes.

    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    alerts = measure_create_high_rbd_clone_snapshot_count.get("prometheus_alerts")
    metadata = measure_create_high_rbd_clone_snapshot_count.get("metadata")
    client_name = metadata.get("client_name")
    is_provider_mode = metadata.get("is_provider_mode")

    log.info(
        f"Checking {constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT} alert "
        f"(provider_mode={is_provider_mode})"
    )

    target_alerts = [
        alert
        for alert in alerts
        if alert.get("labels", {}).get("alertname")
        == constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT
        and alert.get("state") == "firing"
    ]

    assert target_alerts, (
        f"Expected {constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT} alert to be "
        f"firing but no such alert was found in collected alerts"
    )
    log.info(
        f"Found {len(target_alerts)} firing "
        f"{constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT} alert(s)"
    )

    for alert in target_alerts:
        severity = alert.get("labels", {}).get("severity")
        assert (
            severity == "warning"
        ), f"Expected severity 'warning' but got '{severity}'"

    if is_provider_mode:
        log.info(f"Provider mode: validating consumer_name={client_name}")
        for alert in target_alerts:
            alert_consumer_name = alert.get("labels", {}).get("consumer_name")
            assert alert_consumer_name, (
                f"{constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT} alert is missing "
                f"consumer_name label in provider mode. "
                f"Alert labels: {alert.get('labels')}"
            )
            assert alert_consumer_name == client_name, (
                f"consumer_name label mismatch: got '{alert_consumer_name}', "
                f"expected '{client_name}'"
            )
    else:
        log.info("Internal mode: skipping consumer_name validation")

    log.info(
        f"{constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT} alert validated, "
        "checking alert is cleared"
    )

    api.check_alert_cleared(
        label=constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT,
        measure_end_time=measure_create_high_rbd_clone_snapshot_count.get("stop"),
        time_min=600,
    )
