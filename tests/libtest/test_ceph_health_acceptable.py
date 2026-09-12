"""Unit tests for resiliency Ceph health acceptance (HEALTH_WARN vs HEALTH_ERR)."""

import pytest

from ocs_ci.framework.testlib import libtest
from ocs_ci.resiliency.resiliency_tools import is_ceph_health_acceptable


@libtest
@pytest.mark.parametrize(
    "health_output",
    [
        "HEALTH_OK",
        "HEALTH_WARN",
        "HEALTH_WARN Degraded data redundancy: 19239/348156 objects degraded "
        "(5.526%), 70 pgs degraded",
        "HEALTH_WARN 1/3 mons down, quorum b,c",
        "HEALTH_WARN noout flag(s) set",
        "HEALTH_WARN Reduced data availability: 2 pgs peering",
    ],
)
def test_resiliency_accepts_ok_and_warn_health(health_output):
    """HEALTH_OK and HEALTH_WARN (degraded/recovery) must not fail resiliency."""
    assert is_ceph_health_acceptable(health_output) is True


@libtest
@pytest.mark.parametrize(
    "health_output",
    [
        "HEALTH_ERR",
        "HEALTH_ERR 1 filesystem is degraded",
        "HEALTH_ERR 3 osds down",
        "",
        None,
    ],
)
def test_resiliency_fails_only_on_error_health(health_output):
    """Only HEALTH_ERR (and missing status) is treated as a resiliency failure."""
    assert is_ceph_health_acceptable(health_output) is False
