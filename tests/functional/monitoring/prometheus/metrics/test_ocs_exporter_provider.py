# -*- coding: utf8 -*-
"""
Provider-mode metric label validation with consumer_name.
per-client metrics carry consumer_name label
on the provider cluster when remote consumers are onboarded.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    runs_on_provider,
    skipif_external_mode,
    skipif_hci_client,
    skipif_mcg_only,
    skipif_ms_consumer,
)
from ocs_ci.framework.testlib import skipif_managed_service, tier1
from ocs_ci.helpers import ocs_metrics_exporter_helpers as ome_helpers


logger = logging.getLogger(__name__)


def _get_provider_exporter_families():
    """
    Scrape /metrics from ocs-metrics-exporter on provider, return parsed families.
    """
    namespace = config.ENV_DATA["cluster_namespace"]
    pod = ome_helpers.get_ocs_metrics_exporter_pod(namespace)
    if pod is None:
        pytest.skip("ocs-metrics-exporter not deployed on provider")
    body = ome_helpers.scrape_full_metrics_text(pod)
    ome_helpers.assert_prometheus_exposition_text(body)
    return ome_helpers.parse_metric_families(body)


@blue_squad
@tier1
@skipif_managed_service
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@skipif_hci_client
@runs_on_provider
@pytest.mark.parametrize(
    "metric_name",
    [
        pytest.param("ocs_rbd_pv_metadata", marks=pytest.mark.polarion_id("ocs-tm014")),
        pytest.param(
            "ocs_rbd_children_count", marks=pytest.mark.polarion_id("ocs-tm015")
        ),
        pytest.param(
            "ocs_rbd_mirror_image_state", marks=pytest.mark.polarion_id("ocs-tm016")
        ),
        pytest.param(
            "ocs_pool_mirroring_image_health",
            marks=pytest.mark.polarion_id("ocs-tm017"),
        ),
        pytest.param(
            "ocs_pool_mirroring_status", marks=pytest.mark.polarion_id("ocs-tm017")
        ),
        pytest.param(
            "ocs_rbd_client_blocklisted", marks=pytest.mark.polarion_id("ocs-tm023")
        ),
    ],
)
def test_consumer_name_label_on_provider_metric(metric_name):
    """
    TC-7.1 through TC-7.5 consolidated: Verify per-client metrics carry consumer_name
    label on the provider cluster.

    Each metric is expected to have consumer_name populated for remote client workloads.
    Metrics that require mirroring or specific setup are skipped if not present.

    Polarion:
        ocs-tm014, ocs-tm015, ocs-tm016, ocs-tm017, ocs-tm023
    """
    ome_helpers.skip_if_no_provider_with_consumers()
    families = _get_provider_exporter_families()
    if metric_name not in families:
        pytest.skip(
            f"{metric_name} not present in /metrics "
            "(may need mirroring, clones, or blocklist setup)"
        )
    ome_helpers.assert_metric_has_consumer_name(families, metric_name)

    consumer_names = ome_helpers.get_consumer_names_from_metrics(families, metric_name)
    logger.info(
        "metric %s: consumer_name values found: %s",
        metric_name,
        consumer_names,
    )
    assert len(consumer_names) >= 1, (
        f"expected at least one consumer_name value for {metric_name}; "
        f"got {consumer_names}"
    )
