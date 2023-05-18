# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries using negative workloads.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    bugzilla,
    tier3,
    skipif_managed_service,
    skipif_external_mode,
)
from ocs_ci.ocs import metrics
from ocs_ci.utility.prometheus import PrometheusAPI, check_query_range_result_enum


logger = logging.getLogger(__name__)


@tier3
@pytest.mark.polarion_id("OCS-1306")
@skipif_managed_service
@skipif_external_mode
def test_monitoring_shows_mon_down(measure_stop_ceph_mon):
    """
    Make sure simple problems with MON daemons are reported via OCP Prometheus.
    """
    prometheus = PrometheusAPI()
    # time (in seconds) for monitoring to notice the change
    expected_delay = 60
    # query resolution step used in this test case (number of seconds)
    query_step = 15

    affected_mons = measure_stop_ceph_mon["result"]
    # we asked to stop just a single mon ... make this assumption explicit
    assert len(affected_mons) == 1
    affected_mon = affected_mons[0]
    # translate this into ceph daemon name
    ceph_daemon = "mon.{}".format(affected_mon[len("rook-ceph-mon-") :])
    logger.info(f"affected mon was {affected_mon}, aka {ceph_daemon} ceph daemon")

    logger.info("let's check that ceph health was affected")
    health_result = prometheus.query_range(
        query="ceph_health_status",
        start=measure_stop_ceph_mon["start"],
        end=measure_stop_ceph_mon["stop"],
        step=query_step,
    )
    health_validation = check_query_range_result_enum(
        result=health_result,
        good_values=[1],
        bad_values=[0],
        exp_metric_num=1,
        exp_good_time=measure_stop_ceph_mon["min_downtime"],
        exp_delay=expected_delay,
    )
    health_msg = "health status should be affected by missing mon"

    logger.info("let's check that mon quorum status value was affected")
    mon_result = prometheus.query_range(
        query='ceph_mon_quorum_status{ceph_daemon="%s"}' % ceph_daemon,
        start=measure_stop_ceph_mon["start"],
        end=measure_stop_ceph_mon["stop"],
        step=query_step,
        validate=False,
    )
    mon_validation = check_query_range_result_enum(
        result=mon_result,
        good_values=[0],
        bad_values=[1],
        exp_metric_num=1,
        exp_good_time=measure_stop_ceph_mon["min_downtime"],
        exp_delay=expected_delay,
    )
    mon_msg = "ceph_mon_quorum_status value should be affected by missing mon"

    # checking validation results when both queries are performed makes sure
    # that there is evidence for both mon and health queries in the test case
    # logs in case of an assert failure
    assert health_validation, health_msg
    assert mon_validation, mon_msg

    # since we don't do strict result validation in the previous query, we
    # are going to check the min. expected size of the reply explicitly, taking
    # into account the min. expected downtime of the affected ceph mon
    assert len(mon_result) == 1, "there should be one metric for one mon"
    min_mon_samples = measure_stop_ceph_mon["min_downtime"] / query_step
    mon_sample_size = len(mon_result[0]["values"])
    assert mon_sample_size >= min_mon_samples


@tier3
@pytest.mark.polarion_id("OCS-1307")
@skipif_managed_service
@skipif_external_mode
def test_monitoring_shows_osd_down(measure_stop_ceph_osd):
    """
    Make sure simple problems with OSD daemons are reported via OCP Prometheus.
    """
    prometheus = PrometheusAPI()
    # time (in seconds) for monitoring to notice the change
    expected_delay = 60

    affected_osd = measure_stop_ceph_osd["result"]
    # translate this into ceph daemon name
    ceph_daemon = "osd.{}".format(int(affected_osd[len("rook-ceph-osd-") :]))
    logger.info(f"affected osd was {affected_osd}, aka {ceph_daemon} ceph daemon")

    logger.info("let's check that ceph health was affected")
    health_result = prometheus.query_range(
        query="ceph_health_status",
        start=measure_stop_ceph_osd["start"],
        end=measure_stop_ceph_osd["stop"],
        step=15,
    )
    health_validation = check_query_range_result_enum(
        result=health_result,
        good_values=[1],
        bad_values=[0],
        exp_metric_num=1,
        exp_delay=expected_delay,
    )
    health_msg = "health status should be affected by missing osd"

    logger.info("let's check that osd up value was affected")
    osd_up_result = prometheus.query_range(
        query='ceph_osd_up{ceph_daemon="%s"}' % ceph_daemon,
        start=measure_stop_ceph_osd["start"],
        end=measure_stop_ceph_osd["stop"],
        step=15,
    )
    osd_up_validation = check_query_range_result_enum(
        result=osd_up_result,
        good_values=[0],
        bad_values=[1],
        exp_metric_num=1,
        exp_delay=expected_delay,
    )
    osd_up_msg = "ceph_osd_up value should be affected by missing osd"

    logger.info("let's check that osd in value was not affected")
    # osd in value is not affected because we just stopped the osd, we
    # haven't removed it from the luster
    osd_in_result = prometheus.query_range(
        query='ceph_osd_in{ceph_daemon="%s"}' % ceph_daemon,
        start=measure_stop_ceph_osd["start"],
        end=measure_stop_ceph_osd["stop"],
        step=15,
    )
    osd_in_validation = check_query_range_result_enum(
        result=osd_in_result, good_values=[1], bad_values=[0], exp_metric_num=1
    )
    osd_in_msg = "ceph_osd_in value should not be affected by missing osd"

    # checking validation results when all queries are performed makes sure
    # that there is evidence for all queries in the test case logs in case of
    # an assert failure
    assert health_validation, health_msg
    assert osd_up_validation, osd_up_msg
    assert osd_in_validation, osd_in_msg


@tier3
@bugzilla("2203795")
@pytest.mark.polarion_id("OCS-2734")
@skipif_managed_service
def test_ceph_metrics_presence_when_osd_down(measure_stop_ceph_osd):
    """
    Since ODF 4.9 ceph metrics covering disruptions will be available only
    when there are some disruptions to report, as noted in BZ 2028649.

    This test case covers this behaviour for one stopped/disabled OSD.
    """
    prometheus = PrometheusAPI()
    metrics_expected = list(metrics.ceph_metrics_healthy)
    # metrics which should be present with one OSD down
    for mtr in ("ceph_pg_degraded", "ceph_pg_undersized"):
        assert mtr in metrics.ceph_metrics, "test code needs to be updated"
        # make sure the test code is consistent with metrics module
        metrics_expected.append(mtr)
    # metrics which should not be present with one OSD down
    for mtr in ["ceph_pg_clean"]:
        assert mtr in metrics.ceph_metrics, "test code needs to be updated"
        metrics_expected.remove(mtr)
    metrics_without_results = metrics.get_missing_metrics(
        prometheus,
        metrics_expected,
        current_platform=config.ENV_DATA["platform"].lower(),
        start=measure_stop_ceph_osd["start"],
        stop=measure_stop_ceph_osd["stop"],
    )
    msg = (
        "Prometheus should provide some value(s) for all tested metrics, "
        "so that the list of metrics without results is empty."
    )
    assert metrics_without_results == [], msg
