import logging
import pytest

import ocs_ci.utility.prometheus


log = logging.getLogger(__name__)

def test_alerting_works():
    """
    If alerting works then there is at least one alert.
    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    alerts_response = prometheus.alerts()
    assert alerts_response.ok == True
    alerts = alerts_response.json()['data']['alerts']
    log.info(f"Prometheus Alerts: {alerts}")
    assert len(alerts) > 0
