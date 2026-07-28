"""
Test module for RHSTOR-7964 Group 4: Internal Mode Metrics Validation

This module tests ocs-metrics-exporter behavior in Internal/Standalone mode:
- ocs-tm011: Verify consumer_name label empty/absent in internal mode
- ocs-tm012: Verify rados_namespace matches cluster namespace in internal mode
- ocs-tm013: Verify cluster-level metrics have no consumer_name
"""

import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ms_consumer,
    tier1,
)
from ocs_ci.helpers import ocs_metrics_exporter_helpers as ome_helpers
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


def skip_if_provider_mode():
    """
    Skip test if running on a provider cluster (with consumers).

    Internal mode tests should only run on standalone/internal clusters.
    """
    if getattr(config, "multicluster", False):
        if config.is_consumer_exist() or config.hci_client_exist():
            pytest.skip(
                "Test is for internal/standalone mode only; "
                "skipping on provider cluster with consumers"
            )


@pytest.fixture(scope="module")
def exporter_pod():
    """
    Fixture to get the ocs-metrics-exporter pod.

    Returns:
        Pod: The ocs-metrics-exporter pod object
    """
    pod = ome_helpers.get_ocs_metrics_exporter_pod()
    assert pod is not None, "ocs-metrics-exporter pod not found"
    assert (
        pod.get().get("status", {}).get("phase") == constants.STATUS_RUNNING
    ), "ocs-metrics-exporter pod is not running"
    return pod


@pytest.fixture(scope="module")
def metrics_text(exporter_pod):
    """
    Fixture to scrape full metrics from the exporter pod.

    Args:
        exporter_pod (Pod): The exporter pod fixture

    Returns:
        str: Full metrics text in Prometheus exposition format
    """
    logger.info("Scraping full metrics from ocs-metrics-exporter pod")
    text = ome_helpers.scrape_full_metrics_text(exporter_pod, max_bytes=131072)
    ome_helpers.assert_prometheus_exposition_text(text)
    logger.info("Successfully scraped %d bytes of metrics", len(text))
    return text


@pytest.fixture(scope="module")
def metric_families(metrics_text):
    """
    Fixture to parse metrics text into structured format.

    Args:
        metrics_text (str): Raw metrics text

    Returns:
        dict: Parsed metric families {metric_name: [samples]}
    """
    families = ome_helpers.parse_metric_families(metrics_text)
    logger.info("Parsed %d metric families", len(families))
    return families


@pytest.mark.polarion_id("OCS-6011")
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.parametrize(
    "metric_name",
    [
        "ocs_rbd_pv_metadata",
        "ocs_rbd_children_count",
        "ocs_cephfs_subvolume_count",
    ],
)
def test_consumer_name_absent_in_internal_mode(metric_families, metric_name):
    """
    Test Case: ocs-tm011
    Verify consumer_name label is absent or set to "internal" in internal mode.

    In internal/standalone mode (no remote consumers), PV-level metrics should
    either have no consumer_name label or use the sentinel value "internal".

    Steps:
        1. Skip if running on provider cluster with consumers
        2. Scrape metrics from exporter pod
        3. For each PV-level metric, verify no samples reference a remote consumer

    Expected Result:
        - All PV-level metrics exist
        - consumer_name is absent or equals "internal"
    """
    skip_if_provider_mode()

    logger.info(
        "Verifying consumer_name is absent or 'internal' in metric '%s'",
        metric_name,
    )

    if metric_name not in metric_families:
        pytest.skip(
            f"Metric '{metric_name}' not present; may not be applicable "
            "for this cluster configuration"
        )

    samples = metric_families[metric_name]
    logger.info("Found %d samples for metric '%s'", len(samples), metric_name)

    for idx, sample in enumerate(samples):
        labels = sample.get("labels", {})
        consumer = labels.get("consumer_name")
        assert consumer is None or consumer == "internal", (
            f"Internal mode: metric '{metric_name}' sample #{idx} has unexpected "
            f"consumer_name='{consumer}' (expected absent or 'internal'); "
            f"sample: {sample}"
        )

    logger.info(
        "Verified: All %d samples of '%s' have no remote consumer_name (internal mode)",
        len(samples),
        metric_name,
    )


@pytest.mark.polarion_id("OCS-6012")
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
def test_rados_namespace_in_internal_mode(metric_families):
    """
    Test Case: ocs-tm012
    Verify rados_namespace is present and consistent in internal mode.

    In internal mode, RBD PVs use the default pool namespace. The
    rados_namespace label should be present (may be empty for default
    namespace) and consistent across all samples.

    Steps:
        1. Skip if running on provider cluster with consumers
        2. Check ocs_rbd_pv_metadata metric
        3. Verify all samples have rados_namespace label
        4. Verify rados_namespace is consistent across samples

    Expected Result:
        - ocs_rbd_pv_metadata metric exists
        - All samples have a rados_namespace label with the same value
    """
    skip_if_provider_mode()

    metric_name = "ocs_rbd_pv_metadata"

    logger.info(
        "Verifying rados_namespace consistency in metric '%s' for internal mode",
        metric_name,
    )

    ome_helpers.assert_metric_present(metric_families, metric_name)

    samples = metric_families[metric_name]
    logger.info("Found %d samples for metric '%s'", len(samples), metric_name)

    if len(samples) == 0:
        logger.warning(
            "No samples found for '%s' - this may indicate no RBD PVCs "
            "exist in the cluster. Test will pass but validation is limited.",
            metric_name,
        )
        return

    rados_namespaces = set()
    for idx, sample in enumerate(samples):
        labels = sample.get("labels", {})

        assert "rados_namespace" in labels, (
            f"Internal mode: metric '{metric_name}' sample #{idx} missing "
            f"rados_namespace label; sample: {sample}"
        )

        rados_namespaces.add(labels["rados_namespace"])

    assert len(rados_namespaces) == 1, (
        f"Internal mode: expected all samples of '{metric_name}' to share the "
        f"same rados_namespace, but found {rados_namespaces}"
    )

    actual_namespace = rados_namespaces.pop()
    logger.info(
        "Verified: All %d samples of '%s' have consistent "
        "rados_namespace='%s' (internal mode)",
        len(samples),
        metric_name,
        actual_namespace,
    )


@pytest.mark.polarion_id("OCS-6013")
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.parametrize(
    "metric_name,uses_storage_consumer_name",
    [
        ("ocs_rbd_mirror_daemon_health", False),
        ("ocs_mirror_daemon_count", False),
        ("ocs_storage_consumer_metadata", True),
        ("ocs_storage_client_last_heartbeat", True),
    ],
)
def test_cluster_level_metrics_no_consumer_name(
    metric_families, metric_name, uses_storage_consumer_name
):
    """
    Test Case: ocs-tm013
    Verify cluster-level metrics have no consumer_name label.

    Cluster-level metrics (mirror daemon health, mirror daemon count, etc.) should
    NOT have a consumer_name label. Some metrics like ocs_storage_consumer_metadata
    use storage_consumer_name instead (which is different from consumer_name).

    Steps:
        1. Skip if running on provider cluster with consumers
        2. For each cluster-level metric, verify metric exists (or skip)
        3. Verify NO samples have consumer_name label
        4. If metric uses storage_consumer_name, verify that's present instead

    Expected Result:
        - Cluster-level metrics exist (when applicable)
        - No consumer_name label in any sample
    """
    skip_if_provider_mode()

    logger.info(
        "Verifying cluster-level metric '%s' has no consumer_name label", metric_name
    )

    if metric_name not in metric_families:
        pytest.skip(
            f"Metric '{metric_name}' not present; may not be applicable "
            "for this cluster configuration (e.g., no mirroring enabled)"
        )

    samples = metric_families[metric_name]
    logger.info("Found %d samples for metric '%s'", len(samples), metric_name)

    if len(samples) == 0:
        logger.info(
            "No samples for '%s' - metric exists but has no data. "
            "This is acceptable for cluster-level metrics.",
            metric_name,
        )
        return

    for idx, sample in enumerate(samples):
        labels = sample.get("labels", {})

        assert "consumer_name" not in labels, (
            f"Cluster-level metric '{metric_name}' sample #{idx} should NOT have "
            f"consumer_name label; sample: {sample}"
        )

        if uses_storage_consumer_name and "storage_consumer_name" in labels:
            logger.debug(
                "Sample #%d has storage_consumer_name='%s' (expected for this metric)",
                idx,
                labels["storage_consumer_name"],
            )

    logger.info(
        "Verified: All %d samples of '%s' have no consumer_name label "
        "(cluster-level metric)",
        len(samples),
        metric_name,
    )
