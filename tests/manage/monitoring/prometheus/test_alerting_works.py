import logging

import ocs_ci.utility.prometheus
from ocs_ci.ocs.ocp import OCP


log = logging.getLogger(__name__)


def test_alerting_works():
    """
    If alerting works then there is at least one alert.
    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    alerts_response = prometheus.get('alerts', payload={
        'silenced': False,
        'inhibited': False
    })
    assert alerts_response.ok is True
    alerts = alerts_response.json()['data']['alerts']
    log.info(f"Prometheus Alerts: {alerts}")
    assert len(alerts) > 0


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
