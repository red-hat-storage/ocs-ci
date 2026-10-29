import pytest

from ocs_ci.helpers import ceph_mon_healthcheck as healthcheck
from ocs_ci.ocs.exceptions import TimeoutExpiredError


@pytest.mark.parametrize(
    "samples, expected_error",
    [
        ([(0, {"a", "b"}), (40, {"a", "b", "c"})], AssertionError),
        ([(0, {"a", "b"}), (90, {"a", "b"}), (110, {"a", "b", "c"})], None),
        (
            [(0, {"a", "b"}), (90, {"a", "b"}), (210, {"a", "b", "c"})],
            TimeoutExpiredError,
        ),
    ],
)
def test_mon_failover_timing(monkeypatch, samples, expected_error):
    clock = [0]
    current_ids = [set()]

    def sampler(timeout, sleep, func):
        for elapsed, mon_ids in samples:
            clock[0] = elapsed
            current_ids[0] = mon_ids
            yield func()

    monkeypatch.setattr(healthcheck.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(healthcheck, "get_mon_ids", lambda: current_ids[0])
    monkeypatch.setattr(healthcheck, "TimeoutSampler", sampler)

    if expected_error:
        with pytest.raises(expected_error):
            healthcheck.wait_for_mon_failover({"a", "b"}, timeout=200, min_wait=80)
    else:
        assert (
            healthcheck.wait_for_mon_failover({"a", "b"}, timeout=200, min_wait=80)
            == "c"
        )


def test_mon_failover_does_not_assume_when_between_samples(monkeypatch):
    clock = [0]

    def sampler(timeout, sleep, func):
        yield func()
        clock[0] = 90
        yield func()

    mon_ids = iter([{"a", "b"}, {"a", "b", "c"}])
    monkeypatch.setattr(healthcheck.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(healthcheck, "get_mon_ids", lambda: next(mon_ids))
    monkeypatch.setattr(healthcheck, "TimeoutSampler", sampler)

    with pytest.raises(AssertionError, match="minimum wait"):
        healthcheck.wait_for_mon_failover({"a", "b"}, timeout=200, min_wait=80)
