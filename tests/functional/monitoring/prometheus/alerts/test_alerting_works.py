import logging

from flaky import flaky
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    blue_squad,
    provider_mode,
)
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
import ocs_ci.utility.prometheus
from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.ocs.utils import get_pod_name_by_pattern

log = logging.getLogger(__name__)


@blue_squad
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
@flaky(max_runs=3)
def test_prometheus_rule_failures(threading_lock):
    """
    There should be no PrometheusRuleFailures alert when OCS is configured.
    This test is extended to check for many-to-many matching errors in Prometheus logs (more in DFBUGS-2571).
    If such error message found PrometheusRuleFailures alert must fire as well.
    """
    # any check with state False will fail the test
    test_results = {}

    prometheus = ocs_ci.utility.prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts_response = prometheus.get(
        "alerts", payload={"silenced": False, "inhibited": False}
    )
    test_results["alert-msg-ok-check"] = alerts_response.ok is True
    alerts = alerts_response.json()["data"]["alerts"]
    log.info(f"Prometheus Alerts: {alerts}")
    test_results[f"{constants.ALERT_PROMETHEUSRULEFAILURES}-present-check"] = (
        constants.ALERT_PROMETHEUSRULEFAILURES
        not in [alert["labels"]["alertname"] for alert in alerts]
    )
    prometheus_pods = get_pod_name_by_pattern(
        defaults.PROMETHEUS_ROUTE, constants.MONITORING_NAMESPACE
    )
    for pod_name in prometheus_pods:
        log.info(f"Checking logs of pod {pod_name}")
        pod_logs = get_pod_logs(
            pod_name=pod_name,
            namespace=constants.MONITORING_NAMESPACE,
        ).splitlines()
        pod_logs.reverse()
        for log_line in pod_logs:
            if "many-to-many matching not allowed" in log_line.lower():
                test_results[f"many-to-many-error-present-{pod_name}-check"] = False
                break
        else:
            test_results[f"many-to-many-error-present-{pod_name}-check"] = True

    assert all(test_results.values()), f"One or more checks failed: {test_results}"


def setup_module(module):
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()


def teardown_module(module):
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
