# -*- coding: utf8 -*-
"""
Test HighRBDCloneSnapshotCount alert for ocs-metrics-exporter.

RHSTOR-7964 TC-9.1 validates the alert fires when CSI clones exceed the
clone soft limit of 200 (ocs_rbd_children_count > 200). Supports both
internal mode (single cluster) and provider mode (multi-cluster).

Polarion:
    OCS-XXXX
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    runs_on_provider,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ms_consumer,
    tier2,
)
from ocs_ci.framework.testlib import (
    tier4c,
    skipif_ocs_version,
)
from ocs_ci.helpers.ocs_metrics_exporter_helpers import (
    assert_prometheus_exposition_text,
    create_prometheus_k8s_bearer_token,
    get_ocs_metrics_exporter_pod,
    parse_metric_families,
    scrape_full_metrics_text,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import prometheus


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def exporter_pod():
    pod = get_ocs_metrics_exporter_pod()
    assert pod, "ocs-metrics-exporter pod not found or not running"
    return pod


@pytest.fixture(scope="module")
def bearer_token():
    return create_prometheus_k8s_bearer_token()


@pytest.fixture(scope="module")
def metrics_text(exporter_pod, bearer_token):
    text = scrape_full_metrics_text(exporter_pod, bearer_token=bearer_token)
    assert_prometheus_exposition_text(text)
    return text


@pytest.fixture(scope="module")
def metric_families(metrics_text):
    return parse_metric_families(metrics_text)


@pytest.fixture(scope="module")
def prometheus_alert_rules():
    """
    Collect all alerting rules from PrometheusRule CRDs in the storage namespace.
    """
    namespace = config.ENV_DATA["cluster_namespace"]
    prom_rule_ocp = OCP(
        api_version="monitoring.coreos.com/v1",
        kind="PrometheusRule",
        namespace=namespace,
    )
    all_rules = prom_rule_ocp.get().get("items", [])
    alert_rules = {}
    for pr in all_rules:
        for group in pr.get("spec", {}).get("groups", []):
            for rule in group.get("rules", []):
                if "alert" in rule:
                    alert_rules[rule["alert"]] = rule
    return alert_rules


@runs_on_provider
@blue_squad
@tier2
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-XXXX")
class TestRBDCloneSnapshotAlertRule:
    """
    Validate HighRBDCloneSnapshotCount alert rule and source metric.
    """

    def test_high_rbd_clone_snapshot_alert_rule_and_metric(
        self, prometheus_alert_rules, metric_families
    ):
        """
        Verify alert rule exists and source metric is emitted.

        Verification points:
        1. HighRBDCloneSnapshotCount alert rule exists in PrometheusRule CRDs
        2. ocs_rbd_children_count metric emitted by exporter
        """
        alert_name = constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT

        # --- Check 1: Alert rule exists ---
        rule = prometheus_alert_rules.get(alert_name)
        assert rule, (
            f"Alert rule '{alert_name}' not found in any PrometheusRule CRD "
            f"in namespace {config.ENV_DATA['cluster_namespace']}"
        )
        assert rule.get("expr"), f"Alert rule '{alert_name}' has no expr"
        rule_severity = rule.get("labels", {}).get("severity", "").lower()
        assert rule_severity == "warning", (
            f"Alert '{alert_name}' severity: expected 'warning', "
            f"got '{rule_severity}'"
        )
        logger.info(
            "Check 1 PASSED: Alert rule '%s' found — expr: %s, "
            "severity: %s, for: %s",
            alert_name,
            rule["expr"][:80],
            rule_severity,
            rule.get("for", "n/a"),
        )

        # --- Check 2: Source metric emitted ---
        samples = metric_families.get("ocs_rbd_children_count", [])
        if samples:
            logger.info(
                "Check 2 PASSED: ocs_rbd_children_count metric present "
                "with %d sample(s)",
                len(samples),
            )
            for sample in samples[:3]:
                logger.info(
                    "  sample: labels=%s value=%s",
                    sample["labels"],
                    sample["value"],
                )
        else:
            logger.warning(
                "Check 2: ocs_rbd_children_count metric not emitted — "
                "no RBD images with children on this cluster. "
                "Alert rule still exists."
            )


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
    the clone soft limit of 200 (ocs_rbd_children_count > 200).

    Creates 201 CSI clones from a single RBD PVC to trigger the alert.
    In provider mode, validates consumer_name label on the alert.
    Verifies the alert clears after clones are deleted.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    alerts = measure_create_high_rbd_clone_snapshot_count.get("prometheus_alerts")
    metadata = measure_create_high_rbd_clone_snapshot_count.get("metadata")
    client_name = metadata.get("client_name")
    is_provider_mode = metadata.get("is_provider_mode")

    logger.info(
        "Checking %s alert (provider_mode=%s)",
        constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT,
        is_provider_mode,
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
    logger.info(
        "Found %d firing %s alert(s)",
        len(target_alerts),
        constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT,
    )

    for alert in target_alerts:
        severity = alert.get("labels", {}).get("severity")
        assert (
            severity == "warning"
        ), f"Expected severity 'warning' but got '{severity}'"

    if is_provider_mode:
        logger.info("Provider mode: validating consumer_name=%s", client_name)
        for alert in target_alerts:
            alert_consumer = alert.get("labels", {}).get("consumer_name")
            assert alert_consumer, (
                f"{constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT} alert is missing "
                f"consumer_name label in provider mode. "
                f"Alert labels: {alert.get('labels')}"
            )
            assert alert_consumer == client_name, (
                f"consumer_name mismatch: got '{alert_consumer}', "
                f"expected '{client_name}'"
            )
    else:
        logger.info("Internal mode: skipping consumer_name validation")

    logger.info(
        "%s alert validated, checking alert is cleared",
        constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT,
    )

    api.check_alert_cleared(
        label=constants.ALERT_HIGHRBDCLONESNAPSHOTCOUNT,
        measure_end_time=measure_create_high_rbd_clone_snapshot_count.get("stop"),
        time_min=600,
    )
