import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    blue_squad,
    provider_mode,
    runs_on_provider,
)
from ocs_ci.ocs.ocp import OCP
import ocs_ci.utility.prometheus
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.monitoring import validate_no_prometheus_rule_failures

log = logging.getLogger(__name__)


@blue_squad
@runs_on_provider
def test_alerting_works(threading_lock):
    """
    If alerting works then there is at least one alert.
    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts_response = prometheus.get(
        "alerts", payload={"silenced": False, "inhibited": False}
    )
    assert alerts_response.ok is True
    alerts = alerts_response.json()["data"]["alerts"]
    log.info(f"Prometheus Alerts: {alerts}")
    assert len(alerts) > 0


@provider_mode
@blue_squad
@pytest.mark.polarion_id("OCS-2503")
@tier1
@runs_on_provider
def test_prometheus_rule_failures(threading_lock):
    """
    There should be no PrometheusRuleFailures alert when OCS is configured.
    This test is extended to check for many-to-many matching errors in Prometheus logs (more in DFBUGS-2571).
    If such error message found PrometheusRuleFailures alert must fire as well.
    """
    no_prometheus_rule_failures = False
    for no_prometheus_rule_failures in TimeoutSampler(
        timeout=120,
        sleep=30,
        func=validate_no_prometheus_rule_failures,
        func_args=threading_lock,
    ):
        if no_prometheus_rule_failures:
            break
    assert no_prometheus_rule_failures, "Not all prometheus rule checks passed"


def setup_module(module):
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()


def teardown_module(module):
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
