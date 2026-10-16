# -*- coding: utf8 -*-

import functools
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
        # iteration-level details belong to DEBUG (see docs/logging_guide.md)
        assert rec.levelno == logging.DEBUG
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
        msg = rec.getMessage()
        assert "for function 'func' failed" in msg
        # the rate-limited INFO message names the exception type but not its
        # (possibly sensitive) message text, which stays at DEBUG level
        assert "failed with Exception" in msg
        assert "oh no" not in msg


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
        assert "last sampled value: 1" in log_msg


def test_ts_func_without_name():
    """
    Check that TimeoutSampler handles func objects without __name__ attribute
    (e.g. functools.partial): the timeout message is still assembled and
    failing attempts are logged instead of crashing with AttributeError
    inside the exception handler.
    """

    def func(a):
        raise ValueError("nope")

    ts = TimeoutSampler(1, 1, functools.partial(func, 1))
    # the call string (and so the timeout message) was built despite the
    # missing __name__ attribute
    assert len(ts.timeout_exc_args) == 2
    with pytest.raises(TimeoutExpiredError):
        for _ in ts:
            pass


def test_ts_timeout_chained_to_last_exception():
    """
    When sampling times out and the most recent attempt raised an exception,
    the TimeoutExpiredError is chained to it (__cause__), so the root cause
    shows up directly in the resulting traceback.
    """

    def func():
        raise ValueError("persistent failure")

    with pytest.raises(TimeoutExpiredError) as excinfo:
        for _ in TimeoutSampler(1, 1, func):
            pass
    assert isinstance(excinfo.value.__cause__, ValueError)
    assert str(excinfo.value.__cause__) == "persistent failure"


def test_ts_timeout_not_chained_without_func_exception():
    """
    When func never raised, the TimeoutExpiredError has no __cause__.
    """
    func = lambda: 1  # noqa: E731
    with pytest.raises(TimeoutExpiredError) as excinfo:
        for _ in TimeoutSampler(1, 1, func):
            pass
    assert excinfo.value.__cause__ is None


def test_ts_post_mortem_attributes_last_attempt_ok():
    """
    attempt/last_result/last_exception attributes are available for
    post-mortem inspection: when the most recent attempt succeeded,
    last_exception is None and last_result holds its return value.
    """
    func_state = []

    def func():
        func_state.append(0)
        if len(func_state) == 1:
            raise ValueError("transient")
        return len(func_state)

    ts = TimeoutSampler(2, 1, func)
    with pytest.raises(TimeoutExpiredError):
        for _ in ts:
            pass
    assert ts.attempt == 2
    assert ts.last_result == 2
    assert ts.last_exception is None


def test_ts_post_mortem_attributes_last_attempt_failed():
    """
    When the most recent attempt raised, last_exception holds the exception
    while last_result still holds the value of the last successful attempt,
    and the timeout exception is chained to last_exception.
    """
    outcomes = [1, ValueError("boom")]

    def func():
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    ts = TimeoutSampler(2, 1, func)
    with pytest.raises(TimeoutExpiredError) as excinfo:
        for _ in ts:
            pass
    assert ts.attempt == 2
    assert ts.last_result == 1
    assert isinstance(ts.last_exception, ValueError)
    assert excinfo.value.__cause__ is ts.last_exception


def test_ts_wait_for_value_negative_complex_value(caplog):
    """
    Complex sampled values (e.g. dicts) are reported in the error log only by
    their type, so large or potentially sensitive content is not dumped into
    the log.
    """
    func = lambda: {"password": "secret123"}  # noqa: E731  # pragma: allowlist secret
    ts = TimeoutSampler(1, 1, func)
    caplog.set_level(logging.ERROR)
    with pytest.raises(TimeoutExpiredError):
        ts.wait_for_func_value(True)
    assert len(caplog.records) == 1
    log_msg = caplog.records[0].getMessage()
    assert "last sampled value: <dict>" in log_msg
    assert "secret123" not in log_msg


def test_ts_wait_for_value_negative_never_succeeded(caplog):
    """
    When func never returns a value (every attempt raises), the error log
    reports "<no successful sample>" instead of the ambiguous "None", so a
    func that never ran to completion is distinguishable from one that
    genuinely returned None.
    """

    def func():
        raise ValueError("always fails")

    ts = TimeoutSampler(1, 1, func)
    caplog.set_level(logging.ERROR)
    with pytest.raises(TimeoutExpiredError):
        ts.wait_for_func_value(1)
    log_msg = caplog.records[0].getMessage()
    assert "last sampled value: <no successful sample>" in log_msg
    # the terminal ERROR log still surfaces the last failure reason
    assert "last attempt raised ValueError: always fails" in log_msg


def test_ts_progress_heartbeat(caplog):
    """
    Long-running samplers log a rate-limited INFO-level progress heartbeat
    (at most once per progress_log_interval seconds), regardless of how many
    sampling iterations happen within that window.
    """
    func = lambda: 1  # noqa: E731
    # sleep (0.25s) is much shorter than the heartbeat interval (1s), so many
    # iterations happen per interval -- this exercises the rate-limit ceiling
    # (a per-iteration log would produce ~8 heartbeats, not <= 2).
    ts = TimeoutSampler(2, 0.25, func)
    ts.progress_log_interval = 1
    caplog.set_level(logging.INFO)
    iterations = 0
    with pytest.raises(TimeoutExpiredError):
        for _ in ts:
            iterations += 1
    heartbeats = [
        r for r in caplog.records if "Still waiting for results of" in r.getMessage()
    ]
    # far more iterations than heartbeats proves the heartbeat is rate-limited
    # rather than logged on every iteration
    assert iterations >= 4
    assert 1 <= len(heartbeats) <= 2
    for rec in heartbeats:
        assert rec.levelno == logging.INFO
        assert "Still waiting for results of <lambda>" in rec.getMessage()
        assert "of 2s timeout" in rec.getMessage()


def test_ts_no_progress_heartbeat_for_short_wait(caplog):
    """
    Samplers which time out sooner than progress_log_interval do not log any
    progress messages (so short waits stay quiet on the INFO level).
    """
    caplog.set_level(logging.DEBUG)
    with pytest.raises(TimeoutExpiredError):
        for _ in TimeoutSampler(2, 1, lambda: 1):
            pass
    assert not [r for r in caplog.records if "Still waiting" in r.getMessage()]


def test_ts_float_sleep_logged_correctly(caplog):
    """
    Float sleep intervals are rendered correctly in the sleep log message
    (%d used to truncate 0.5 to 0).
    """
    caplog.set_level(logging.DEBUG)
    with pytest.raises(TimeoutExpiredError):
        for _ in TimeoutSampler(1, 0.5, lambda: 1):
            pass
    sleep_records = [r for r in caplog.records if "Going to sleep" in r.getMessage()]
    assert sleep_records
    for rec in sleep_records:
        assert "0.5 seconds" in rec.getMessage()
