# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries directly without a workload, to
check that OCS Monitoring is configured and available as expected for RGW.
"""

import logging

import pytest

from ocs_ci.framework.testlib import skipif_ocs_version, tier1, tier4, tier4a
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs import metrics
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


@skipif_ocs_version("<4.6")
@tier4
@tier4a
@pytest.mark.polarion_id("OCS-2385")
def test_ceph_rgw_metrics_after_metrics_exporter_respin(rgw_deployments):
    """
    RGW metrics should be provided via OCP Prometheus even after
    ocs-metrics-exporter pod is respinned.

    """
    logger.info("Respin ocs-metrics-exporter pod")
    pod_obj = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
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
    prometheus = PrometheusAPI()
    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus, metrics.ceph_rgw_metrics
    )
    msg = (
        "OCS Monitoring should provide some value(s) for tested rgw metrics, "
        "so that the list of metrics without results is empty."
    )
    assert list_of_metrics_without_results == [], msg


@tier1
@pytest.mark.post_ocp_upgrade
@pytest.mark.run(order=1)
@pytest.mark.polarion_id("OCS-2584")
def verify_rgw_pods_restart_count(verify_rgw_restart_count_session):
    """
    This test starting session scope fixture, that verify RGW pod restarts count
    """
    logger.info("Starting RGW pod restart counts")
