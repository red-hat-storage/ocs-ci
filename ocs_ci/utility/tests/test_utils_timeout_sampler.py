# -*- coding: utf8 -*-

import logging
import time

import pytest

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler, TimeoutIterator


@pytest.mark.parametrize("timeout_cls", [TimeoutSampler, TimeoutIterator])
def test_ts_null(timeout_cls):
    """
    Creating TimeoutSampler without any parameters should fail on TypeError.
    """
    with pytest.raises(TypeError):
        timeout_cls()


@pytest.mark.parametrize("timeout_cls", [TimeoutSampler, TimeoutIterator])
def test_ts_simple_usecase(timeout_cls):
    """
    Iterate over results of a simple TimeoutSampler instance and check that
    expected number of iterations happened and that TimeoutExpiredError was
    raised in the end.
    """
    timeout = 4
    sleep_time = 1
    func = lambda: 1  # noqa: E731
    results = []
    with pytest.raises(TimeoutExpiredError):
        for result in timeout_cls(timeout, sleep_time, func):
            results.append(result)
    assert results == [1, 1, 1, 1]


def test_ts_simple_logging(caplog, capsys):
    """
    For a simple usecase, check that TimeoutSampler logging works.
    """
    timeout = 3
    sleep_time = 1
    func = lambda: print("i")  # noqa: E731
    caplog.set_level(logging.DEBUG)
    with pytest.raises(TimeoutExpiredError):
        for _ in TimeoutSampler(timeout, sleep_time, func):
            pass
    # check stdout output from the func
    captured = capsys.readouterr()
    assert captured.out == "i\n" * timeout
    # check that log contains entries about ts iteration
    sleep_msg = f"Going to sleep for {sleep_time} seconds before next iteration"
    for rec in caplog.records:
        assert rec.getMessage() == sleep_msg
    assert len(caplog.records) == timeout


def test_ts_one_iteration():
    """
    When timeout == sleep_time, one iteration should happen.
    """
    timeout = 1
    sleep_time = 1
    func = lambda: 1  # noqa: E731
    results = []
    with pytest.raises(TimeoutExpiredError):
        for result in TimeoutSampler(timeout, sleep_time, func):
            results.append(result)
    assert results == [1]


def test_ts_one_iteration_big_sleep_time():
    """
    When timeout < sleep_time, TimeoutSampler object init fails on
    ValueError exception (as given timeout can't be quaranteed).
    """
    timeout = 1
    sleep_time = 2
    func = lambda: 1  # noqa: E731
    with pytest.raises(ValueError):
        TimeoutSampler(timeout, sleep_time, func)


def test_ts_one_iteration_big_func_runtime():
    """
    When timeout < runtime of 1st iteration, one iteration should happen.
    TimeoutSampler won't kill the function while it's running over the overall
    timeout.
    """
    timeout = 1
    sleep_time = 1

    def func():
        time.sleep(2)
        return 1

    results = []
    with pytest.raises(TimeoutExpiredError):
        for result in TimeoutSampler(timeout, sleep_time, func):
            results.append(result)
    assert results == [1]


def test_ts_func_exception(caplog):
    """
    Check that TimeoutSampler handles exception raised during iteration.
    INFO-level logs are rate-limited, but DEBUG-level logs capture all exceptions.
    """
    timeout = 2
    sleep_time = 1

    def func():
        raise Exception("oh no")

    results = []
    caplog.set_level(logging.DEBUG)
    with pytest.raises(TimeoutExpiredError):
        for result in TimeoutSampler(timeout, sleep_time, func):
            results.append(result)
    assert results == []
    # Check that exceptions were logged at DEBUG level for each iteration
    # Filter records to only include DEBUG-level exception messages
    debug_exception_records = [
        rec
        for rec in caplog.records
        if rec.levelno == logging.DEBUG
        and "Exception raised during iteration attempt" in rec.getMessage()
    ]
    # Each failed attempt should have a DEBUG log entry
    assert len(debug_exception_records) == timeout

    # Also verify that INFO-level rate-limited message was logged at least once
    info_exception_records = [
        rec
        for rec in caplog.records
        if rec.levelno == logging.INFO and "TimeoutSampler attempt" in rec.getMessage()
    ]
    assert len(info_exception_records) >= 1
    for rec in info_exception_records:
        assert "for function 'func' failed" in rec.getMessage()


def test_ti_func_values():
    """
    Iterate over results of a simple TimeoutIterator instance when function
    args and kwargs are specified.
    """
    timeout = 1
    sleep_time = 1

    def func(a, b, c=None):
        if c is None:
            return 0
        else:
            return a + b

    ti1 = TimeoutIterator(
        timeout, sleep_time, func=func, func_args=[1], func_kwargs={"b": 2, "c": 3}
    )
    results1 = []
    with pytest.raises(TimeoutExpiredError):
        for result in ti1:
            results1.append(result)
    assert results1 == [3]

    ti2 = TimeoutIterator(timeout, sleep_time, func, func_args=[1, 2])
    results2 = []
    with pytest.raises(TimeoutExpiredError):
        for result in ti2:
            results2.append(result)
    assert results2 == [0]


def test_ts_wait_for_value_positive():
    """
    Check that wait_for_value() function waits for func to return given value
    as expected.
    """
    timeout = 10
    sleep_time = 1
    func_state = []

    def func():
        func_state.append(0)
        return len(func_state)

    ts = TimeoutSampler(timeout, sleep_time, func)
    start = time.time()
    ts.wait_for_func_value(3)
    end = time.time()
    assert 2 < (end - start) < 3


def test_ts_wait_for_value_negative(caplog):
    """
    Check that when wait_for_value() fails to see expected return value of
    given func within given timeout, exception is raised and the problem
    logged.
    """
    timeout = 3
    sleep_time = 2
    func = lambda: 1  # noqa: E731
    ts = TimeoutSampler(timeout, sleep_time, func)
    caplog.set_level(logging.ERROR)
    with pytest.raises(TimeoutExpiredError):
        ts.wait_for_func_value(2)
    # check that the problem was logged properly
    assert len(caplog.records) == 1
    for rec in caplog.records:
        log_msg = rec.getMessage()
        assert "function <lambda> failed" in log_msg
        assert "failed to return expected value 2" in log_msg
        assert "during 3 second timeout" in log_msg
