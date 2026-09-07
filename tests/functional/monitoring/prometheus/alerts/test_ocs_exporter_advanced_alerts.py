# -*- coding: utf8 -*-
"""
Test advanced alert rule definitions and source metrics for ocs-metrics-exporter.

RHSTOR-7964 TC-020 through TC-023 validates that PrometheusRule CRDs contain
the correct alert definitions for the rearchitected exporter, and that the
source metrics feeding these alerts are emitted.

Polarion:
    OCS-6020
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
from ocs_ci.helpers.ocs_metrics_exporter_helpers import (
    assert_prometheus_exposition_text,
    create_prometheus_k8s_bearer_token,
    get_ocs_metrics_exporter_pod,
    parse_metric_families,
    scrape_full_metrics_text,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

EXPECTED_ALERTS = [
    {
        "name": constants.ALERT_ODF_PERSISTENT_VOLUME_MIRROR_STATUS,
        "source_metric": "ocs_pool_mirroring_status",
        "severity": "warning",
    },
    {
        "name": constants.ALERT_CEPHFS_STALE_SUBVOLUME,
        "source_metric": "ocs_cephfs_subvolume_count",
        "severity": "warning",
    },
    {
        "name": constants.ALERT_ODF_RBD_CLIENT_BLOCKED,
        "source_metric": "ocs_rbd_client_blocklisted",
        "severity": "warning",
    },
]


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
    logger.info(
        "Found %d alerting rules across %d PrometheusRule CRDs",
        len(alert_rules),
        len(all_rules),
    )
    return alert_rules


@runs_on_provider
@blue_squad
@tier2
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-6020")
class TestAdvancedAlertValidation:
    """
    Validate advanced alert rule definitions and their source metrics.
    """

    def test_advanced_alert_rules_and_metric_sources(
        self, prometheus_alert_rules, metric_families
    ):
        """
        Single-flow test covering alert rules, source metrics, and labels.

        Verification points:
        1. PrometheusRule CRDs contain expected alert definitions
        2. Exporter emits the source metrics feeding each alert
        3. Source metric label structure validation
        """
        # --- Check 1: Alert rules exist in PrometheusRule CRDs ---
        for alert_def in EXPECTED_ALERTS:
            alert_name = alert_def["name"]
            rule = prometheus_alert_rules.get(alert_name)
            assert rule, (
                f"Alert rule '{alert_name}' not found in any PrometheusRule CRD "
                f"in namespace {config.ENV_DATA['cluster_namespace']}"
            )
            assert rule.get("expr"), f"Alert rule '{alert_name}' has no expr defined"
            rule_severity = rule.get("labels", {}).get("severity", "").lower()
            assert rule_severity == alert_def["severity"], (
                f"Alert '{alert_name}' severity mismatch: "
                f"expected '{alert_def['severity']}', got '{rule_severity}'"
            )
            logger.info(
                "Check 1: Alert rule '%s' found — expr: %s, severity: %s, for: %s",
                alert_name,
                rule["expr"][:80],
                rule_severity,
                rule.get("for", "n/a"),
            )

        logger.info(
            "Check 1 PASSED: all %d alert rules found in PrometheusRule CRDs",
            len(EXPECTED_ALERTS),
        )

        # --- Check 2: Source metrics emitted by exporter ---
        metrics_found = 0
        for alert_def in EXPECTED_ALERTS:
            metric_name = alert_def["source_metric"]
            samples = metric_families.get(metric_name, [])
            if samples:
                metrics_found += 1
                logger.info(
                    "Check 2: Metric '%s' present with %d sample(s) "
                    "(source for alert '%s')",
                    metric_name,
                    len(samples),
                    alert_def["name"],
                )
            else:
                logger.warning(
                    "Check 2: Metric '%s' not emitted — feature may not be "
                    "active (e.g., no mirroring configured, no subvolumes, "
                    "no blocklist entries). Alert '%s' rule still exists.",
                    metric_name,
                    alert_def["name"],
                )

        assert metrics_found > 0, (
            "None of the expected source metrics were found in exporter output: "
            + ", ".join(a["source_metric"] for a in EXPECTED_ALERTS)
        )
        logger.info(
            "Check 2 PASSED: %d/%d source metrics emitted by exporter",
            metrics_found,
            len(EXPECTED_ALERTS),
        )

        # --- Check 3: Metric label structure validation ---
        is_provider = config.ENV_DATA.get("cluster_type", "").lower() == "provider"
        for alert_def in EXPECTED_ALERTS:
            metric_name = alert_def["source_metric"]
            samples = metric_families.get(metric_name, [])
            if not samples:
                continue
            if is_provider:
                samples_with_consumer = [
                    s for s in samples if s["labels"].get("consumer_name")
                ]
                assert samples_with_consumer, (
                    f"Provider mode: metric '{metric_name}' has no samples with "
                    f"'consumer_name' label"
                )
                logger.info(
                    "Check 3: Metric '%s' has consumer_name on %d/%d samples "
                    "(provider mode)",
                    metric_name,
                    len(samples_with_consumer),
                    len(samples),
                )
            else:
                for sample in samples:
                    consumer = sample["labels"].get("consumer_name", "")
                    if consumer and consumer != "internal":
                        logger.warning(
                            "Check 3: Metric '%s' has unexpected consumer_name "
                            "'%s' on internal mode cluster",
                            metric_name,
                            consumer,
                        )
                logger.info(
                    "Check 3: Metric '%s' label structure valid (internal mode)",
                    metric_name,
                )

        logger.info("Check 3 PASSED: metric label structure validated")
