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
from ocs_ci.utility.utils import TimeoutIterator
from ocs_ci.ocs.monitoring import validate_no_prometheus_rule_failures

logger = logging.getLogger(__name__)


@blue_squad
@runs_on_provider
def test_alerting_works(threading_lock):
    """
    If alerting works then there is at least one alert.
    """
    logger.info("Starting test: Verify Prometheus alerting is working")

    logger.test_step("Query Prometheus for active alerts")
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts_response = prometheus.get(
        "alerts", payload={"silenced": False, "inhibited": False}
    )

    logger.assertion(
        f"Prometheus API response: expected=ok, actual={'ok' if alerts_response.ok else 'failed'}"
    )
    assert alerts_response.ok is True, "Prometheus API request failed"

    logger.test_step("Verify at least one alert exists")
    alerts = alerts_response.json()["data"]["alerts"]
    logger.info(f"Number of alerts found: {len(alerts)}")
    logger.debug(f"Prometheus Alerts: {alerts}")

    logger.assertion(f"Alert count: expected>0, actual={len(alerts)}")
    assert len(alerts) > 0, "No alerts found - alerting may not be working"

    logger.info("Test passed: Prometheus alerting is functioning correctly")


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
    logger.info("Starting test: Verify no Prometheus rule failures")

    logger.test_step(
        "Poll for Prometheus rule validation (timeout: 120s, interval: 30s)"
    )
    no_prometheus_rule_failures = False
    iteration = 0
    for no_prometheus_rule_failures in TimeoutIterator(
        timeout=120,
        sleep=30,
        func=validate_no_prometheus_rule_failures,
        func_kwargs={"threading_lock": threading_lock},
    ):
        iteration += 1
        logger.debug(
            f"Validation iteration {iteration}: "
            f"no_failures={no_prometheus_rule_failures}"
        )
        if no_prometheus_rule_failures:
            logger.info(
                f"All Prometheus rules validated successfully after iteration {iteration}"
            )
            break

    logger.assertion(
        f"Prometheus rule validation: expected=True, actual={no_prometheus_rule_failures}"
    )
    assert no_prometheus_rule_failures, "Not all prometheus rule checks passed"

    logger.info("Test passed: No Prometheus rule failures detected")


def setup_module(module):
    logger.info("Setting up module: Storing original user for cleanup")
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()
    logger.info(f"Original user stored: {module.original_user}")


def teardown_module(module):
    logger.info("Tearing down module: Restoring original user")
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
    logger.info(f"Restored user: {module.original_user}")
