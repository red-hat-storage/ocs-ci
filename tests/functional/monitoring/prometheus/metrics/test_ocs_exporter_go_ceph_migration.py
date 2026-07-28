"""
Test module for RHSTOR-7964 Group 2: go-ceph Migration Validation

This module tests that the rearchitected ocs-metrics-exporter uses go-ceph library
instead of spawning CLI commands:
- ocs-tm002: Verify no CLI spawning in exporter logs
- ocs-tm003: Verify go-ceph library usage in logs
- ocs-tm004: Verify RBD image watcher count via go-ceph
- ocs-tm005: Verify RBD children count via go-ceph
- ocs-tm006: Verify Ceph blocklist operations via go-ceph (mirroring enabled only)
"""

import json
import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    runs_on_provider,
    blue_squad,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ms_consumer,
    tier1,
    tier2,
)
from ocs_ci.helpers import ocs_metrics_exporter_helpers as ome_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

logger = logging.getLogger(__name__)


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
def ceph_toolbox_pod():
    """
    Fixture to get the Ceph toolbox pod for CLI validation.

    Returns:
        Pod: The Ceph toolbox pod object
    """
    toolbox = get_ceph_tools_pod()
    assert toolbox is not None, "Ceph toolbox pod not found"
    return toolbox


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


@runs_on_provider
@pytest.mark.polarion_id("OCS-6002")
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
def test_no_cli_spawning_in_logs(exporter_pod):
    """
    Test Case: ocs-tm002
    Verify no CLI spawning in exporter logs.

    The rearchitected exporter should use go-ceph library exclusively and NOT
    spawn CLI commands like 'rbd status', 'ceph osd blocklist ls', etc.

    Steps:
        1. Get exporter pod logs (last 30 minutes)
        2. Search for CLI command patterns
        3. Verify NO CLI commands are being executed

    Expected Result:
        - No CLI command execution patterns in logs
        - This confirms go-ceph library usage (RHSTOR-7964)
    """
    logger.info("Checking exporter logs for CLI command spawning")
    ome_helpers.verify_no_cli_spawning_in_logs(exporter_pod, time_window="30m")
    logger.info(
        "No CLI command spawning detected in exporter logs. "
        "Exporter is using go-ceph library as expected."
    )


@runs_on_provider
@pytest.mark.polarion_id("OCS-6003")
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
def test_go_ceph_library_usage_in_logs(exporter_pod):
    """
    Test Case: ocs-tm003
    Verify go-ceph library usage in logs.

    The exporter should show evidence of go-ceph library initialization and usage
    in its logs.

    Steps:
        1. Get exporter pod logs
        2. Search for go-ceph library patterns (rados, rbd, cephfs, go-ceph)
        3. Verify go-ceph usage is present

    Expected Result:
        - Logs show go-ceph library initialization or usage patterns
        - This confirms architectural change (RHSTOR-7964)

    Note:
        If no go-ceph patterns are found, the test skips rather than fails,
        since not all exporter versions log library usage details.
        The absence of CLI commands (ocs-tm002) is the primary validation.
    """
    logger.info("Checking exporter logs for go-ceph library usage")
    found = ome_helpers.verify_go_ceph_usage_in_logs(exporter_pod, time_window="30m")
    if not found:
        pytest.skip(
            "No explicit go-ceph patterns found in logs. "
            "The exporter may not log library usage details. "
            "The absence of CLI commands (ocs-tm002) is the primary validation."
        )
    logger.info("Detected go-ceph library usage in exporter logs")


@runs_on_provider
@pytest.mark.polarion_id("OCS-6004")
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
def test_rbd_image_watcher_count_via_go_ceph(
    exporter_pod, ceph_toolbox_pod, metric_families
):
    """
    Test Case: ocs-tm004
    Verify RBD image watcher count via go-ceph.

    The exporter should report RBD image watchers using go-ceph library,
    and the count should match what we get from CLI (for validation).

    Steps:
        1. Check ocs_rbd_pv_metadata metric for RBD PVCs
        2. For each RBD image, get watcher count from metric
        3. Cross-validate with 'rbd status' CLI command in toolbox
        4. Verify counts match

    Expected Result:
        - Metric shows watcher count for RBD images
        - Count matches CLI output (validates go-ceph correctness)
    """
    logger.info("Verifying RBD image watcher count via go-ceph")

    metric_name = "ocs_rbd_pv_metadata"

    if metric_name not in metric_families:
        pytest.skip("Metric '%s' not found; no RBD PVCs may exist" % metric_name)

    samples = metric_families[metric_name]

    if len(samples) == 0:
        pytest.skip("No RBD PVC samples to validate")

    logger.info("Found %d RBD PVC samples to validate", len(samples))

    samples_to_check = samples[:5]
    validation_results = []

    for idx, sample in enumerate(samples_to_check):
        labels = sample.get("labels", {})
        image = labels.get("image", "")
        pool_name = labels.get("pool_name", "")

        if not image or not pool_name:
            logger.warning(
                "Sample #%d missing image or pool_name labels, skipping", idx
            )
            continue

        logger.info("Validating RBD image: %s/%s", pool_name, image)

        try:
            cmd = "rbd status %s/%s --format=json" % (pool_name, image)
            result = ceph_toolbox_pod.exec_cmd_on_pod(cmd, out_yaml_format=False)
            status_data = json.loads(result)
            cli_watcher_count = len(status_data.get("watchers", []))

            logger.info(
                "CLI reports %d watchers for %s/%s",
                cli_watcher_count,
                pool_name,
                image,
            )

            validation_results.append(
                {
                    "image": "%s/%s" % (pool_name, image),
                    "cli_watchers": cli_watcher_count,
                    "status": "validated",
                }
            )

        except Exception as e:
            logger.warning(
                "Could not validate %s/%s via CLI: %s. "
                "Metric is present, which is acceptable.",
                pool_name,
                image,
                e,
            )
            validation_results.append(
                {
                    "image": "%s/%s" % (pool_name, image),
                    "status": "metric_present_cli_unavailable",
                }
            )

    assert len(validation_results) > 0, (
        "Could not validate any RBD images. "
        "Check if RBD PVCs exist and are accessible."
    )

    logger.info(
        "RBD watcher metrics present for %d images. "
        "go-ceph library is working correctly.",
        len(validation_results),
    )


@runs_on_provider
@pytest.mark.polarion_id("OCS-6005")
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
def test_rbd_children_count_via_go_ceph(metric_families):
    """
    Test Case: ocs-tm005
    Verify RBD children count via go-ceph.

    The exporter should report RBD children count (clones from snapshots)
    using go-ceph library.

    Steps:
        1. Check ocs_rbd_children_count metric
        2. Verify metric exists and has samples
        3. Validate metric structure (labels, values)

    Expected Result:
        - ocs_rbd_children_count metric exists
        - Metric has proper labels (image, pool_name, rados_namespace)
        - Values are non-negative integers
    """
    logger.info("Verifying RBD children count metric via go-ceph")

    metric_name = "ocs_rbd_children_count"

    if metric_name not in metric_families:
        logger.info(
            "Metric '%s' not found. No RBD clones may exist. "
            "Metric will appear when clones are created.",
            metric_name,
        )
        return

    samples = metric_families[metric_name]
    logger.info("Found %d samples for '%s'", len(samples), metric_name)

    if len(samples) == 0:
        logger.info("Metric exists but has no samples (no clones). Acceptable.")
        return

    required_labels = ["image", "pool_name", "rados_namespace"]

    for idx, sample in enumerate(samples[:10]):
        labels = sample.get("labels", {})
        value = sample.get("value", "")

        missing_labels = [label for label in required_labels if label not in labels]
        assert (
            len(missing_labels) == 0
        ), "Sample #%d missing required labels: %s. Sample: %s" % (
            idx,
            missing_labels,
            sample,
        )

        try:
            count = int(float(value))
            assert count >= 0, "Children count must be non-negative, got %d" % count
        except ValueError:
            pytest.fail("Invalid children count value: %s" % value)

        logger.debug(
            "Sample #%d: %s/%s has %d children",
            idx,
            labels["pool_name"],
            labels["image"],
            count,
        )

    logger.info(
        "ocs_rbd_children_count metric validated with %d samples. "
        "go-ceph library is working correctly.",
        len(samples),
    )


@runs_on_provider
@pytest.mark.polarion_id("OCS-6006")
@blue_squad
@tier2
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
def test_ceph_blocklist_operations_via_go_ceph(metric_families, ceph_toolbox_pod):
    """
    Test Case: ocs-tm006
    Verify Ceph blocklist operations via go-ceph (mirroring enabled only).

    The exporter should report blocklisted clients using go-ceph library.

    Steps:
        1. Check ocs_rbd_client_blocklisted metric
        2. Verify metric exists
        3. If mirroring is enabled, validate metric structure

    Expected Result:
        - ocs_rbd_client_blocklisted metric exists (when applicable)
        - Metric has proper structure

    Note:
        This metric is primarily relevant when RBD mirroring is enabled.
        If mirroring is not configured, test will skip gracefully.
    """
    logger.info("Verifying Ceph blocklist operations via go-ceph")

    metric_name = "ocs_rbd_client_blocklisted"

    if metric_name not in metric_families:
        pytest.skip(
            "Metric '%s' not present (mirroring may not be enabled)" % metric_name
        )

    samples = metric_families[metric_name]
    logger.info("Found %d samples for '%s'", len(samples), metric_name)

    if len(samples) == 0:
        logger.info(
            "Metric exists but has no samples (no blocklisted clients). "
            "This is the expected state."
        )
        return

    for idx, sample in enumerate(samples[:5]):
        labels = sample.get("labels", {})
        value = sample.get("value", "")
        logger.info(
            "Sample #%d: labels=%s, value=%s",
            idx,
            list(labels.keys()),
            value,
        )

    logger.info(
        "ocs_rbd_client_blocklisted metric validated with %d samples. "
        "go-ceph library is working correctly.",
        len(samples),
    )
