# -*- coding: utf8 -*-

import pytest

from ocs_ci.utility.prometheus import check_query_range_result


@pytest.fixture
def query_range_result_ok():
    """
    Simpified example of data produced by ``PrometheusAPI.query_range()`` with
    ``ceph_mon_quorum_status`` query, which performs Prometheus `instant
    query`_.

    .. _`instant query`: https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
    """
    query_range_data = [
        {
            "metric": {
                "__name__": "ceph_mon_quorum_status",
                "ceph_daemon": "mon.a",
                "endpoint": "http-metrics",
                "instance": "10.131.0.36:9283",
                "job": "rook-ceph-mgr",
                "namespace": "openshift-storage",
                "pod": "rook-ceph-mgr-a-66df496d9d-snssn",
                "service": "rook-ceph-mgr",
            },
            "values": [
                [1585652658.918, "1"],
                [1585652673.918, "1"],
                [1585652688.918, "1"],
                [1585652703.918, "1"],
                [1585652718.918, "1"],
                [1585652733.918, "1"],
                [1585652748.918, "1"],
                [1585652763.918, "1"],
                [1585652778.918, "1"],
                [1585652793.918, "1"],
                [1585652808.918, "1"],
                [1585652823.918, "1"],
                [1585652838.918, "1"],
                [1585652853.918, "1"],
                [1585652868.918, "1"],
                [1585652883.918, "1"],
            ],
        },
        {
            "metric": {
                "__name__": "ceph_mon_quorum_status",
                "ceph_daemon": "mon.b",
                "endpoint": "http-metrics",
                "instance": "10.131.0.36:9283",
                "job": "rook-ceph-mgr",
                "namespace": "openshift-storage",
                "pod": "rook-ceph-mgr-a-66df496d9d-snssn",
                "service": "rook-ceph-mgr",
            },
            "values": [
                [1585652658.918, "1"],
                [1585652673.918, "1"],
                [1585652688.918, "1"],
                [1585652703.918, "1"],
                [1585652718.918, "1"],
                [1585652733.918, "1"],
                [1585652748.918, "1"],
                [1585652763.918, "1"],
                [1585652778.918, "1"],
                [1585652793.918, "1"],
                [1585652808.918, "1"],
                [1585652823.918, "1"],
                [1585652838.918, "1"],
                [1585652853.918, "1"],
                [1585652868.918, "1"],
                [1585652883.918, "1"],
            ],
        },
    ]
    return query_range_data


@pytest.fixture
def query_range_result_single_error(query_range_result_ok):
    """
    Range query data with one bad value.
    """
    query_range_data = query_range_result_ok
    value_tuple = query_range_data[1]['values'][6]
    value_tuple[1] = "0"
    return query_range_data


@pytest.fixture
def query_range_result_delay_60s(query_range_result_ok):
    """
    Range query data with bad values in first 4 values (first 60s of the
    measurement).
    """
    query_range_data = query_range_result_ok
    # insert bad value "0" for first 60s in both metrics
    for i in range(2):
        for j in range(4):
            value_tuple = query_range_data[i]['values'][j]
            value_tuple[1] = "0"
    return query_range_data


@pytest.fixture
def query_range_result_bad_last_90s(query_range_result_ok):
    """
    Range query data with bad values in last 6 values (last 90s of the
    measurement).
    """
    query_range_data = query_range_result_ok
    # insert bad value "0" for last 90s in both metrics
    for i in range(2):
        for j in range(10, 16):
            value_tuple = query_range_data[i]['values'][j]
            value_tuple[1] = "0"
    return query_range_data


def test_check_query_range_result_null():
    """
    The function does't throw any exception and returns true when executed
    with empty arguments.
    """
    assert check_query_range_result({}, [])


def test_check_query_range_result_simple(query_range_result_ok):
    """
    The function validates query_range_result_ok data assuming 1 is a good
    value.
    """
    assert check_query_range_result(query_range_result_ok, good_values=[1])


def test_check_query_range_result_simple_fail(query_range_result_ok):
    """
    Assuming 0 is a good value, the validation should fail.
    """
    result = check_query_range_result(query_range_result_ok, good_values=[0])
    assert not result


def test_check_query_range_result_single_error(query_range_result_single_error):
    """
    The function finds single error in query_range_result_single_error data,
    assuming 1 is a good value.
    """
    result1 = check_query_range_result(
        query_range_result_single_error,
        good_values=[1])
    assert not result1
    result2 = check_query_range_result(
        query_range_result_single_error,
        good_values=[1, 0])
    assert result2, "assuming both 1 and 0 are good values, check should pass"


def test_check_query_range_result_exp_metric_num(query_range_result_ok):
    """
    Check that exp_metric_num is checked as expected when specified.
    """
    result1 = check_query_range_result(
        query_range_result_ok,
        good_values=[1],
        exp_metric_num=2)
    assert result1, "check should pass when exp_metric_num matches the data"
    result2 = check_query_range_result(
        query_range_result_ok,
        good_values=[1],
        exp_metric_num=3)
    assert not result2, "check should fail when exp_metric_num doesn't match"


def test_check_query_range_result_exp_delay(query_range_result_delay_60s):
    """
    Check that exp_metric_num is taken into account, so that initial bad values
    are ignored.
    """
    result1 = check_query_range_result(
        query_range_result_delay_60s,
        good_values=[1],
        bad_values=[0])
    assert not result1, "without specifying exp_delay validation should fail"
    result2 = check_query_range_result(
        query_range_result_delay_60s,
        good_values=[1],
        bad_values=[0],
        exp_delay=60)
    assert result2, "taking exp_delay into account, validation should pass"


def test_check_query_range_result_exp_good_time(query_range_result_bad_last_90s):
    """
    Check that exp_good_time is taken into account, so that initial bad values
    are ignored if appear after the good time passess.
    """
    result1 = check_query_range_result(
        query_range_result_bad_last_90s,
        good_values=[1],
        bad_values=[0])
    assert not result1, "without exp_good_time validation should fail"
    result2 = check_query_range_result(
        query_range_result_bad_last_90s,
        good_values=[1],
        bad_values=[0],
        exp_good_time=150)
    assert result2, "taking exp_good_time into account, validation should pass"
