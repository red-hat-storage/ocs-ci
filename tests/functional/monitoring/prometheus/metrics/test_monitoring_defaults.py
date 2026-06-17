# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries directly without a workload, to
check that OCS Monitoring is configured and available as expected.
"""

import logging
import ipaddress

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    metrics_for_external_mode_required,
    blue_squad,
    skipif_mcg_only,
    runs_on_provider,
    provider_client_platform_required,
    provider_mode,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs import metrics
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.prometheus import PrometheusAPI, check_query_range_result_enum
from ocs_ci.helpers.helpers import storagecluster_independent_check
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service


logger = logging.getLogger(__name__)


@blue_squad
@tier1
@pytest.mark.post_ocp_upgrade
@pytest.mark.first
@pytest.mark.polarion_id("OCS-1261")
@skipif_managed_service
@runs_on_provider
def test_monitoring_enabled(threading_lock):
    """
    OCS Monitoring is enabled after OCS installation (which is why this test
    has a post deployment marker) by asking for values of one ceph and one
    noobaa related metrics.
    """
    logger.info("Starting test: Verify OCS monitoring is enabled and provides metrics")
    prometheus = PrometheusAPI(threading_lock=threading_lock)

    if (
        storagecluster_independent_check()
        and float(config.ENV_DATA["ocs_version"]) < 4.6
    ):
        logger.info(
            f"Skipping ceph metrics because it is not enabled for external "
            f"mode for OCS {float(config.ENV_DATA['ocs_version'])}"
        )

    else:
        logger.test_step("Query and validate ceph_pool_stored metric")
        result = prometheus.query("ceph_pool_stored")
        logger.info(f"Received {len(result)} results for ceph_pool_stored metric")
        logger.assertion(f"Ceph metric results: expected>0, actual={len(result)}")
        assert len(result) > 0, "No values received for ceph_pool_stored query"

        logger.info("Validating metric values are non-negative integers")
        for metric in result:
            _, value = metric["value"]
            logger.debug(f"Pool metric value: {value}")
            logger.assertion(f"Pool bytes value: expected>=0, actual={int(value)}")
            assert (
                int(value) >= 0
            ), "number of bytes in a pool isn't a positive integer or zero"

        logger.test_step(
            "Verify ceph_pool_stored result count matches actual pool count"
        )
        ct_pod = pod.get_ceph_tools_pod()
        ceph_pools = ct_pod.exec_ceph_cmd("ceph osd pool ls")
        logger.info(f"Ceph pools: {len(ceph_pools)}, Metric results: {len(result)}")
        logger.assertion(
            f"Pool count validation: expected={len(ceph_pools)}, actual={len(result)}"
        )
        assert len(result) == len(
            ceph_pools
        ), "Metric result count doesn't match pool count"

    logger.test_step("Query and validate NooBaa_bucket_status metric")
    result = prometheus.query("NooBaa_bucket_status")
    logger.info(f"Received {len(result)} results for NooBaa_bucket_status metric")
    logger.assertion(f"NooBaa metric results: expected>0, actual={len(result)}")
    assert len(result) > 0, "No values received for NooBaa_bucket_status query"

    logger.info("Validating NooBaa metric values are non-negative integers")
    for metric in result:
        _, value = metric["value"]
        logger.debug(f"Bucket status value: {value}")
        logger.assertion(f"Bucket status value: expected>=0, actual={int(value)}")
        assert int(value) >= 0, "bucket status isn't a positive integer or zero"

    logger.info("Test passed: OCS monitoring is enabled and providing valid metrics")


@provider_mode
@blue_squad
@tier1
@pytest.mark.polarion_id("OCS-1265")
@skipif_managed_service
@runs_on_provider
def test_ceph_mgr_dashboard_not_deployed():
    """
    Check that `ceph mgr dashboard`_ is not deployed after installation of OCS
    (this is upstream rook feature not supported in downstream OCS).

    .. _`ceph mgr dashboard`: https://rook.io/docs/rook/v1.0/ceph-dashboard.html
    """
    logger.info("Starting test: Verify ceph mgr dashboard is not deployed")

    logger.test_step("Verify no ceph mgr dashboard pods are deployed")
    ocp_pod = ocp.OCP(
        kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
    )
    ocs_pods = ocp_pod.get()["items"]
    logger.info(f"Checking {len(ocs_pods)} OCS pods for dashboard references")

    dashboard_pods_found = []
    for pod_item in ocs_pods:
        assert pod_item["kind"] == constants.POD
        pod_name = pod_item["metadata"]["name"]
        if "dashboard" in pod_name or "ceph-mgr-dashboard" in pod_name:
            dashboard_pods_found.append(pod_name)

    logger.assertion(
        f"Dashboard pods found: expected=0, actual={len(dashboard_pods_found)}"
    )
    assert len(dashboard_pods_found) == 0, (
        f"ceph mgr dashboard pods should not be deployed as part of OCS. "
        f"Found: {dashboard_pods_found}"
    )
    logger.info("No dashboard pods found in OCS namespace")

    logger.test_step("Verify no ceph mgr dashboard routes are deployed")
    ocp_route = ocp.OCP(kind=constants.ROUTE)
    all_routes = ocp_route.get(all_namespaces=True)["items"]
    logger.info(f"Checking {len(all_routes)} routes across all namespaces")

    dashboard_routes_found = []
    for route in all_routes:
        assert route["kind"] == constants.ROUTE
        route_name = route["metadata"]["name"]
        if "ceph-mgr-dashboard" in route_name:
            dashboard_routes_found.append(route_name)

    logger.assertion(
        f"Dashboard routes found: expected=0, actual={len(dashboard_routes_found)}"
    )
    assert len(dashboard_routes_found) == 0, (
        f"ceph mgr dashboard routes should not be deployed as part of OCS. "
        f"Found: {dashboard_routes_found}"
    )
    logger.info("No dashboard routes found")

    logger.info("Test passed: Ceph mgr dashboard is not deployed")


@skipif_external_mode
@skipif_mcg_only
@blue_squad
@skipif_ocs_version("<4.6")
@metrics_for_external_mode_required
@tier1
@pytest.mark.polarion_id("OCS-1267")
@skipif_managed_service
@runs_on_provider
def test_ceph_rbd_metrics_available(threading_lock):
    """
    Ceph RBD metrics should be provided via OCP Prometheus as well.
    See also: https://ceph.com/rbd/new-in-nautilus-rbd-performance-monitoring/
    """
    logger.info("Starting test: Verify Ceph RBD metrics are available")

    logger.test_step("Check all expected Ceph RBD metrics are available in Prometheus")
    prometheus = PrometheusAPI(threading_lock=threading_lock)
    logger.info(f"Checking {len(metrics.ceph_rbd_metrics)} RBD metrics")

    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus, metrics.ceph_rbd_metrics
    )

    logger.assertion(
        f"Missing RBD metrics: expected=0, actual={len(list_of_metrics_without_results)}"
    )
    if list_of_metrics_without_results:
        logger.error(f"Missing metrics: {list_of_metrics_without_results}")

    assert list_of_metrics_without_results == [], (
        "OCS Monitoring should provide some value(s) for tested rbd metrics, "
        f"but the following metrics are missing: {list_of_metrics_without_results}"
    )

    logger.info("Test passed: All Ceph RBD metrics are available")


@skipif_external_mode
@provider_mode
@skipif_mcg_only
@blue_squad
@tier1
@metrics_for_external_mode_required
@pytest.mark.polarion_id("OCS-1268")
@skipif_managed_service
@runs_on_provider
def test_ceph_metrics_available(threading_lock):
    """
    Ceph metrics as listed in KNIP-634 should be provided via OCP Prometheus.

    Ceph Object Gateway https://docs.ceph.com/docs/master/radosgw/ is
    deployed on on-prem platforms only (such as VMWare - see BZ 1763150),
    so this test case ignores failures for ceph_rgw_* and ceph_objecter_*
    metrics when running on cloud platforms (such as AWS).

    Since ODF 4.9 only subset of all ceph metrics ``ceph_metrics_healthy`` will
    be always available, as noted in BZ 2028649.
    """
    logger.info("Starting test: Verify Ceph metrics are available")
    current_platform = config.ENV_DATA["platform"].lower()
    logger.info(f"Platform: {current_platform}")

    logger.test_step("Check all expected Ceph metrics are available in Prometheus")
    prometheus = PrometheusAPI(threading_lock=threading_lock)
    logger.info(f"Checking {len(metrics.ceph_metrics_healthy)} Ceph metrics")

    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus,
        metrics.ceph_metrics_healthy,
        current_platform=current_platform,
    )

    logger.assertion(
        f"Missing Ceph metrics: expected=0, actual={len(list_of_metrics_without_results)}"
    )
    if list_of_metrics_without_results:
        logger.error(f"Missing metrics: {list_of_metrics_without_results}")

    assert list_of_metrics_without_results == [], (
        "OCS Monitoring should provide some value(s) for all tested metrics, "
        f"but the following metrics are missing: {list_of_metrics_without_results}"
    )

    logger.info("Test passed: All expected Ceph metrics are available")


@skipif_external_mode
@skipif_mcg_only
@blue_squad
@tier1
@metrics_for_external_mode_required
@pytest.mark.post_ocp_upgrade
@pytest.mark.polarion_id("OCS-1302")
@skipif_managed_service
@runs_on_provider
def test_monitoring_reporting_ok_when_idle(workload_idle, threading_lock):
    """
    When nothing is happening, OCP Prometheus reports OCS status as OK.

    If this test case fails, the status is either reported wrong or the
    cluster is in a broken state. Either way, a failure here is not good.
    """
    logger.info(
        "Starting test: Verify monitoring reports OK status when cluster is idle"
    )
    logger.info(f"Idle period: {workload_idle['start']} to {workload_idle['stop']}")
    prometheus = PrometheusAPI(threading_lock=threading_lock)

    logger.test_step("Validate ceph_health_status metric reports healthy (value=0)")
    health_result = prometheus.query_range(
        query="ceph_health_status",
        start=workload_idle["start"],
        end=workload_idle["stop"],
        step=15,
    )
    health_validation = check_query_range_result_enum(
        result=health_result, good_values=[0], bad_values=[1], exp_metric_num=1
    )
    health_msg = "ceph_health_status {} report 0 (health ok) as expected"
    if health_validation:
        health_msg = health_msg.format("does")
        logger.info(health_msg)
    else:
        health_msg = health_msg.format("should")
        logger.error(health_msg)

    logger.test_step("Validate ceph_mon_quorum_status metric shows healthy quorum")
    mon_result = prometheus.query_range(
        query="ceph_mon_quorum_status",
        start=workload_idle["start"],
        end=workload_idle["stop"],
        step=15,
    )
    mon_validation = check_query_range_result_enum(
        result=mon_result,
        good_values=[1],
        bad_values=[0],
        exp_metric_num=workload_idle["result"]["mon_num"],
    )
    mon_msg = "ceph_mon_quorum_status {} indicate no problems with quorum"
    if mon_validation:
        mon_msg = mon_msg.format("does")
        logger.info(mon_msg)
    else:
        mon_msg = mon_msg.format("should")
        logger.error(mon_msg)

    logger.test_step(
        "Validate ceph_osd_up and ceph_osd_in metrics show all OSDs healthy"
    )
    osd_validations = []
    for metric in ("ceph_osd_up", "ceph_osd_in"):
        logger.debug(f"Checking {metric} metric")
        osd_result = prometheus.query_range(
            query=metric,
            start=workload_idle["start"],
            end=workload_idle["stop"],
            step=15,
        )
        osd_validation = check_query_range_result_enum(
            result=osd_result,
            good_values=[1],
            bad_values=[0],
            exp_metric_num=workload_idle["result"]["osd_num"],
        )
        osd_validations.append(osd_validation)
        osd_msg = "{} metric {} indicate no problems with OSDs"
        if osd_validation:
            osd_msg = osd_msg.format(metric, "does")
            logger.info(osd_msg)
        else:
            osd_msg = osd_msg.format(metric, "should")
            logger.error(osd_msg)

    # Validate all metrics after logging
    logger.assertion(
        f"Ceph health status validation: expected=True, actual={health_validation}"
    )
    assert health_validation, health_msg

    logger.assertion(
        f"MON quorum status validation: expected=True, actual={mon_validation}"
    )
    assert mon_validation, mon_msg

    logger.assertion(
        f"OSD status validation: expected=all_True, actual={all(osd_validations)}, "
        f"validations={osd_validations}"
    )
    osds_msg = "ceph_osd_{up,in} metrics should indicate no OSD issues"
    assert all(osd_validations), osds_msg

    logger.info("Test passed: All monitoring metrics report OK status when idle")


@provider_mode
@blue_squad
@tier1
@runs_on_provider
@provider_client_platform_required
@pytest.mark.polarion_id("OCS-5204")
def test_provider_metrics_available(threading_lock):
    """
    Metrics added in provider-client mode should be provided via OCP Prometheus on provider.
    """
    logger.info("Starting test: Verify provider-client metrics are available")

    logger.test_step("Check all expected provider metrics are available in Prometheus")
    prometheus = PrometheusAPI(threading_lock=threading_lock)
    logger.info(f"Checking {len(metrics.provider_metrics)} provider metrics")

    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus, metrics.provider_metrics
    )

    logger.assertion(
        f"Missing provider metrics: expected=0, actual={len(list_of_metrics_without_results)}"
    )
    if list_of_metrics_without_results:
        logger.error(f"Missing metrics: {list_of_metrics_without_results}")

    assert list_of_metrics_without_results == [], (
        "OCS Monitoring should provide some value(s) for tested provider metrics, "
        f"but the following metrics are missing: {list_of_metrics_without_results}"
    )

    logger.info("Test passed: All provider metrics are available")


@blue_squad
@tier1
@skipif_external_mode
@pytest.mark.polarion_id("OCS-6796")
def test_monitoring_ip_connectivity(threading_lock):
    """
    Procedure:
    1. Retrieves the IPv4/6 addresses of rook-ceph-exporter pods.
    2. Logs into the prometheus-k8s pod in the openshift-monitoring namespace.
    3. Checks connectivity using curl to fetch metrics from each exporter's /metrics endpoint.
    4. Asserts that the expected Ceph metric is present in the response.

    """
    logger.info("Starting test: Verify monitoring IP connectivity to Ceph exporters")

    logger.test_step("Get rook-ceph-exporter pod IP addresses")
    exporter_pods = pod.get_pods_having_label(constants.EXPORTER_APP_LABEL)
    ip_addresses = [pod_obj["status"]["podIP"] for pod_obj in exporter_pods]
    logger.info(f"Found {len(ip_addresses)} exporter pods with IPs: {ip_addresses}")

    logger.test_step("Locate prometheus-k8s pod in monitoring namespace")
    pod_obj_list = pod.get_all_pods(
        namespace=defaults.OCS_MONITORING_NAMESPACE, selector_label=["prometheus"]
    )
    prometheus_pod_obj = None
    for pod_obj in pod_obj_list:
        if "prometheus-k8s" in pod_obj.name:
            prometheus_pod_obj = pod_obj
            break

    logger.assertion(
        f"Prometheus pod found: expected=True, actual={prometheus_pod_obj is not None}"
    )
    assert (
        prometheus_pod_obj is not None
    ), "Prometheus pod not found in the monitoring namespace"
    logger.info(f"Found Prometheus pod: {prometheus_pod_obj.name}")

    logger.test_step("Verify connectivity and metrics from each exporter pod")
    failed_ips = []
    for ip_address in ip_addresses:
        formatted_ip = (
            f"[{ip_address}]"
            if ipaddress.ip_address(ip_address).version == 6
            else ip_address
        )
        logger.debug(f"Testing connectivity to {formatted_ip}:9926")

        cmd = (
            f"oc rsh -n {defaults.OCS_MONITORING_NAMESPACE} {prometheus_pod_obj.name} "
            f"curl -vv http://{formatted_ip}:9926/metrics"
        )
        out = run_cmd(cmd=cmd)

        if "ceph_AsyncMessenger_Worker_msgr_connection" not in out:
            failed_ips.append(ip_address)
            logger.error(
                f"Expected Ceph metric not found in output for IP {ip_address}"
            )
        else:
            logger.debug(f"Successfully retrieved metrics from {ip_address}")

    logger.assertion(
        f"Exporter connectivity validation: expected=0 failed, actual={len(failed_ips)} failed"
    )
    assert len(failed_ips) == 0, (
        f"Expected Ceph metric 'ceph_AsyncMessenger_Worker_msgr_connection' not found "
        f"in output for {len(failed_ips)} IP(s): {failed_ips}"
    )

    logger.info("Test passed: All exporter pods are accessible and providing metrics")
