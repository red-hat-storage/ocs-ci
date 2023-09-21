import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import bugzilla, tier1, blue_squad
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
import ocs_ci.utility.prometheus


log = logging.getLogger(__name__)


@blue_squad
def test_alerting_works():
    """
    If alerting works then there is at least one alert.
    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    alerts_response = prometheus.get(
        "alerts", payload={"silenced": False, "inhibited": False}
    )
    assert alerts_response.ok is True
    alerts = alerts_response.json()["data"]["alerts"]
    log.info(f"Prometheus Alerts: {alerts}")
    assert len(alerts) > 0


@blue_squad
@pytest.mark.polarion_id("OCS-2503")
@bugzilla("1897674")
@tier1
def test_prometheus_rule_failures():
    """
    There should be no PrometheusRuleFailures alert when OCS is configured.
    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    alerts_response = prometheus.get(
        "alerts", payload={"silenced": False, "inhibited": False}
    )
    assert alerts_response.ok is True
    alerts = alerts_response.json()["data"]["alerts"]
    log.info(f"Prometheus Alerts: {alerts}")
    assert constants.ALERT_PROMETHEUSRULEFAILURES not in [
        alert["labels"]["alertname"] for alert in alerts
    ]


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
