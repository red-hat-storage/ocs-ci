# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries directly without a workload, to
check that OCS Monitoring is configured and available as expected.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import metrics_for_external_mode_required
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs import metrics
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.prometheus import PrometheusAPI, check_query_range_result
from tests.helpers import storagecluster_independent_check


logger = logging.getLogger(__name__)


@tier1
@pytest.mark.post_ocp_upgrade
@pytest.mark.first
@pytest.mark.polarion_id("OCS-1261")
def test_monitoring_enabled():
    """
    OCS Monitoring is enabled after OCS installation (which is why this test
    has a post deployment marker) by asking for values of one ceph and one
    noobaa related metrics.
    """
    prometheus = PrometheusAPI()

    if (
        storagecluster_independent_check()
        and float(config.ENV_DATA['ocs_version']) < 4.6
    ):
        logger.info(
            f"Skipping ceph metrics because it is not enabled for external "
            f"mode for OCS {float(config.ENV_DATA['ocs_version'])}"
        )

    else:
        # ask for values of ceph_pool_stored metric
        logger.info("Checking that ceph data are provided in OCS monitoring")
        result = prometheus.query('ceph_pool_stored')
        msg = "check that we actually received some values for a ceph query"
        assert len(result) > 0, msg
        for metric in result:
            _, value = metric['value']
            assert_msg = "number of bytes in a pool isn't a positive integer or zero"
            assert int(value) >= 0, assert_msg
        # additional check that values makes at least some sense
        logger.info(
            "Checking that size of ceph_pool_stored result matches number of pools")
        ct_pod = pod.get_ceph_tools_pod()
        ceph_pools = ct_pod.exec_ceph_cmd("ceph osd pool ls")
        assert len(result) == len(ceph_pools)

    # again for a noobaa metric
    logger.info("Checking that MCG/NooBaa data are provided in OCS monitoring")
    result = prometheus.query('NooBaa_bucket_status')
    msg = "check that we actually received some values for a MCG/NooBaa query"
    assert len(result) > 0, msg
    for metric in result:
        _, value = metric['value']
        assert int(value) >= 0, "bucket status isn't a positive integer or zero"


@tier1
@pytest.mark.polarion_id("OCS-1265")
def test_ceph_mgr_dashboard_not_deployed():
    """
    Check that `ceph mgr dashboard`_ is not deployed after installation of OCS
    (this is upstream rook feature not supported in downstream OCS).

    .. _`ceph mgr dashboard`: https://rook.io/docs/rook/v1.0/ceph-dashboard.html
    """
    logger.info("Checking that there is no ceph mgr dashboard pod deployed")
    ocp_pod = ocp.OCP(
        kind=constants.POD,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    # if there is no "items" in the reply, OCS is very broken
    ocs_pods = ocp_pod.get()['items']
    for pod_item in ocs_pods:
        # just making the assumptions explicit
        assert pod_item['kind'] == constants.POD
        pod_name = pod_item['metadata']['name']
        msg = "ceph mgr dashboard should not be deployed as part of OCS"
        assert "dashboard" not in pod_name, msg
        assert "ceph-mgr-dashboard" not in pod_name, msg

    logger.info("Checking that there is no ceph mgr dashboard route")
    ocp_route = ocp.OCP(kind=constants.ROUTE)
    for route in ocp_route.get(all_namespaces=True)['items']:
        # just making the assumptions explicit
        assert route['kind'] == constants.ROUTE
        route_name = route['metadata']['name']
        msg = "ceph mgr dashboard route should not be deployed as part of OCS"
        assert "ceph-mgr-dashboard" not in route_name, msg


@skipif_ocs_version('<4.6')
@metrics_for_external_mode_required
@tier1
@pytest.mark.bugzilla("1779336")
@pytest.mark.polarion_id("OCS-1267")
def test_ceph_rbd_metrics_available():
    """
    Ceph RBD metrics should be provided via OCP Prometheus as well.
    See also: https://ceph.com/rbd/new-in-nautilus-rbd-performance-monitoring/
    """
    prometheus = PrometheusAPI()
    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus, metrics.ceph_rbd_metrics)
    msg = (
        "OCS Monitoring should provide some value(s) for tested rbd metrics, "
        "so that the list of metrics without results is empty.")
    assert list_of_metrics_without_results == [], msg


@tier1
@metrics_for_external_mode_required
@pytest.mark.polarion_id("OCS-1268")
def test_ceph_metrics_available():
    """
    Ceph metrics as listed in KNIP-634 should be provided via OCP Prometheus.

    Ceph Object Gateway https://docs.ceph.com/docs/master/radosgw/ is
    deployed on on-prem platforms only (such as VMWare - see BZ 1763150),
    so this test case ignores failures for ceph_rgw_* and ceph_objecter_*
    metrics when running on cloud platforms (such as AWS).
    """
    prometheus = PrometheusAPI()
    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus,
        metrics.ceph_metrics,
        current_platform=config.ENV_DATA['platform'].lower())
    msg = (
        "OCS Monitoring should provide some value(s) for all tested metrics, "
        "so that the list of metrics without results is empty.")
    assert list_of_metrics_without_results == [], msg


@tier1
@metrics_for_external_mode_required
@pytest.mark.post_ocp_upgrade
@pytest.mark.polarion_id("OCS-1302")
def test_monitoring_reporting_ok_when_idle(workload_idle):
    """
    When nothing is happening, OCP Prometheus reports OCS status as OK.

    If this test case fails, the status is either reported wrong or the
    cluster is in a broken state. Either way, a failure here is not good.
    """
    prometheus = PrometheusAPI()

    health_result = prometheus.query_range(
        query='ceph_health_status',
        start=workload_idle['start'],
        end=workload_idle['stop'],
        step=15)
    health_validation = check_query_range_result(
        result=health_result,
        good_values=[0],
        bad_values=[1],
        exp_metric_num=1)
    health_msg = "ceph_health_status {} report 0 (health ok) as expected"
    if health_validation:
        health_msg = health_msg.format('does')
        logger.info(health_msg)
    else:
        health_msg = health_msg.format('should')
        logger.error(health_msg)

    mon_result = prometheus.query_range(
        query='ceph_mon_quorum_status',
        start=workload_idle['start'],
        end=workload_idle['stop'],
        step=15)
    mon_validation = check_query_range_result(
        result=mon_result,
        good_values=[1],
        bad_values=[0],
        exp_metric_num=workload_idle['result']['mon_num'])
    mon_msg = "ceph_mon_quorum_status {} indicate no problems with quorum"
    if mon_validation:
        mon_msg = mon_msg.format('does')
        logger.info(mon_msg)
    else:
        mon_msg = mon_msg.format('should')
        logger.error(mon_msg)

    osd_validations = []
    for metric in ("ceph_osd_up", "ceph_osd_in"):
        osd_result = prometheus.query_range(
            query=metric,
            start=workload_idle['start'],
            end=workload_idle['stop'],
            step=15)
        osd_validation = check_query_range_result(
            result=osd_result,
            good_values=[1],
            bad_values=[0],
            exp_metric_num=workload_idle['result']['osd_num'])
        osd_validations.append(osd_validation)
        osd_msg = "{} metric {} indicate no problems with OSDs"
        if osd_validation:
            osd_msg = osd_msg.format(metric, 'does')
            logger.info(osd_msg)
        else:
            osd_msg = osd_msg.format(metric, 'should')
            logger.error(osd_msg)

    # after logging everything properly, make the test fail if necessary
    # see ERRORs reported in the test log for details
    assert health_validation, health_msg
    assert mon_validation, mon_msg
    osds_msg = "ceph_osd_{up,in} metrics should indicate no OSD issues"
    assert all(osd_validations), osds_msg
