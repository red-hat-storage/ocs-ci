# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries directly without a workload, to
check that OCS Monitoring is configured and available as expected for RGW.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    skipif_managed_service,
    runs_on_provider,
    skipif_ocs_version,
    tier4c,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import metrics
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


@blue_squad
@skipif_ocs_version("<4.6")
@tier4c
@pytest.mark.polarion_id("OCS-2385")
@skipif_managed_service
@runs_on_provider
def test_ceph_rgw_metrics_after_metrics_exporter_respin(
    rgw_deployments, threading_lock
):
    """
    RGW metrics should be provided via OCP Prometheus even after
    ocs-metrics-exporter pod is respinned.

    """
    logger.info("Respin ocs-metrics-exporter pod")
    pod_obj = ocp.OCP(
        kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
    )
    metrics_pods = pod_obj.get(selector="app.kubernetes.io/name=ocs-metrics-exporter")[
        "items"
    ]
    assert len(metrics_pods) == 1
    metrics_pod_data = metrics_pods[0]
    metrics_pod = OCS(**metrics_pod_data)
    metrics_pod.delete(force=True)

    logger.info("Wait for ocs-metrics-exporter pod to come up")
    assert pod_obj.wait_for_resource(
        condition="Running",
        selector="app.kubernetes.io/name=ocs-metrics-exporter",
        resource_count=1,
        timeout=600,
    )

    logger.info("Collect RGW metrics")
    prometheus = PrometheusAPI(threading_lock=threading_lock)
    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus, metrics.ceph_rgw_metrics
    )
    msg = (
        "OCS Monitoring should provide some value(s) for tested rgw metrics, "
        "so that the list of metrics without results is empty."
    )
    assert list_of_metrics_without_results == [], msg
