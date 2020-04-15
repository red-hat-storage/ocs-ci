# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries using negative workloads.
"""

import logging

import pytest

from ocs_ci.framework.testlib import tier3
from ocs_ci.utility.prometheus import PrometheusAPI, check_query_range_result


logger = logging.getLogger(__name__)


@tier3
@pytest.mark.polarion_id("OCS-1306")
def test_monitoring_shows_mon_down(measure_stop_ceph_mon):
    """
    Make sure simple problems with MON daemons are reported via OCP Prometheus.
    """
    prometheus = PrometheusAPI()
    # time (in seconds) for monitoring to notice the change
    expected_delay = 60

    affected_mons = measure_stop_ceph_mon['result']
    # we asked to stop just a single mon ... make this assumption explicit
    assert len(affected_mons) == 1
    affected_mon = affected_mons[0]
    # translate this into ceph daemon name
    ceph_daemon = "mon.{}".format(affected_mon[len('rook-ceph-mon-'):])
    logger.info(
        f"affected mon was {affected_mon}, aka {ceph_daemon} ceph daemon")

    logger.info("let's check that ceph health was affected")
    health_result = prometheus.query_range(
        query='ceph_health_status',
        start=measure_stop_ceph_mon['start'],
        end=measure_stop_ceph_mon['stop'],
        step=15)
    health_validation = check_query_range_result(
        result=health_result,
        good_values=[1],
        bad_values=[0],
        exp_metric_num=1,
        exp_delay=expected_delay)
    health_msg = "health status should be affected by missing mon"
    assert health_validation, health_msg

    logger.info("let's check that mon quorum status value was affected")
    mon_result = prometheus.query_range(
        query='ceph_mon_quorum_status{ceph_daemon="%s"}' % ceph_daemon,
        start=measure_stop_ceph_mon['start'],
        end=measure_stop_ceph_mon['stop'],
        step=15)
    mon_validation = check_query_range_result(
        result=mon_result,
        good_values=[0],
        bad_values=[1],
        exp_metric_num=1,
        exp_delay=expected_delay)
    mon_msg = "ceph_mon_quorum_status value should be affected by missing mon"
    assert mon_validation, mon_msg


@tier3
@pytest.mark.polarion_id("OCS-1307")
def test_monitoring_shows_osd_down(measure_stop_ceph_osd):
    """
    Make sure simple problems with OSD daemons are reported via OCP Prometheus.
    """
    prometheus = PrometheusAPI()
    # time (in seconds) for monitoring to notice the change
    expected_delay = 60

    affected_osd = measure_stop_ceph_osd['result']
    # translate this into ceph daemon name
    ceph_daemon = "osd.{}".format(int(affected_osd[len('rook-ceph-osd-'):]))
    logger.info(
        f"affected osd was {affected_osd}, aka {ceph_daemon} ceph daemon")

    logger.info("let's check that ceph health was affected")
    health_result = prometheus.query_range(
        query='ceph_health_status',
        start=measure_stop_ceph_osd['start'],
        end=measure_stop_ceph_osd['stop'],
        step=15)
    health_validation = check_query_range_result(
        result=health_result,
        good_values=[1],
        bad_values=[0],
        exp_metric_num=1,
        exp_delay=expected_delay)
    health_msg = "health status should be affected by missing osd"
    assert health_validation, health_msg

    logger.info("let's check that osd up value was affected")
    osd_up_result = prometheus.query_range(
        query='ceph_osd_up{ceph_daemon="%s"}' % ceph_daemon,
        start=measure_stop_ceph_osd['start'],
        end=measure_stop_ceph_osd['stop'],
        step=15)
    osd_up_validation = check_query_range_result(
        result=osd_up_result,
        good_values=[0],
        bad_values=[1],
        exp_metric_num=1,
        exp_delay=expected_delay)
    osd_up_msg = "ceph_osd_up value should be affected by missing osd"
    assert osd_up_validation, osd_up_msg

    logger.info("let's check that osd in value was not affected")
    # osd in value is not affected because we just stopped the osd, we
    # haven't removed it from the luster
    osd_in_result = prometheus.query_range(
        query='ceph_osd_in{ceph_daemon="%s"}' % ceph_daemon,
        start=measure_stop_ceph_osd['start'],
        end=measure_stop_ceph_osd['stop'],
        step=15)
    osd_in_validation = check_query_range_result(
        result=osd_in_result,
        good_values=[1],
        bad_values=[0],
        exp_metric_num=1)
    osd_in_msg = "ceph_osd_in value should not be affected by missing osd"
    assert osd_in_validation, osd_in_msg
