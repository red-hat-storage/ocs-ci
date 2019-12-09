import base64
import logging
import os
import requests
import tempfile
import time
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(name=__file__)


def check_alert_list(
    label,
    msg,
    alerts,
    states,
    severity="warning",
    ignore_more_occurences=False
):
    """
    Check list of alerts that there are alerts with requested label and
    message for each provided state. If some alert is missing then this check
    fails.

    Args:
        label (str): Alert label
        msg (str): Alert message
        alerts (list): List of alerts to check
        states (list): List of states to check, order is important
        ignore_more_occurences (bool): If true then there is checkced only
            occurence of alert with requested label, message and state but
            it is not checked if there is more of occurences than one.
    """

    target_alerts = [
        alert
        for alert
        in alerts
        if alert.get('labels').get('alertname') == label
    ]

    logger.info(f"Checking properties of found {label} alerts")
    if ignore_more_occurences:
        for state in states:
            delete = False
            for key, alert in reversed(list(enumerate(target_alerts))):
                if alert.get('state') == state:
                    if delete:
                        d_msg = f"Ignoring {alert} as alert already appeared."
                        logger.debug(d_msg)
                        target_alerts.pop(key)
                    else:
                        delete = True
    assert_msg = (
        f"Incorrect number of {label} alerts ({len(target_alerts)} "
        f"instead of {len(states)} with states: {states})."
        f"\nAlerts: {target_alerts}"
    )
    assert len(target_alerts) == len(states), assert_msg

    for key, state in enumerate(states):

        assert_msg = 'Alert message for alert {label} is not correct'
        assert target_alerts[key]['annotations']['message'] == msg, assert_msg

        assert_msg = f"Alert {label} doesn't have {severity} severity"
        assert target_alerts[key]['annotations']['severity_level'] == severity, assert_msg

        assert_msg = f"Alert {label} is not in {state} state"
        assert target_alerts[key]['state'] == state, assert_msg

    logger.info(f"Alerts were triggered correctly during utilization")


class PrometheusAPI(object):
    """
    This is wrapper class for Prometheus API.
    """

    _token = None
    _user = None
    _password = None
    _endpoint = None
    _cacert = None

    def __init__(self, user=None, password=None):
        """
        Constructor for PrometheusAPI class.

        Args:
            user (str): OpenShift username used to connect to API
        """
        self._user = user or config.RUN['username']
        if not password:
            filename = os.path.join(
                config.ENV_DATA['cluster_path'],
                config.RUN['password_location']
            )
            with open(filename) as f:
                password = f.read()
        self._password = password
        self.refresh_connection()
        self.generate_cert()

    def refresh_connection(self):
        """
        Login into OCP, refresh endpoint and token.
        """
        ocp = OCP(
            kind=constants.ROUTE,
            namespace=defaults.OCS_MONITORING_NAMESPACE
        )
        assert ocp.login(self._user, self._password), 'Login to OCP failed'
        self._token = ocp.get_user_token()
        route_obj = ocp.get(
            resource_name=defaults.PROMETHEUS_ROUTE
        )
        self._endpoint = 'https://' + route_obj['spec']['host']

    def generate_cert(self):
        """
        Generate CA certificate from kubeconfig for API.

        TODO: find proper way how to generate/load cert files.
        """
        kubeconfig_path = os.path.join(
            config.ENV_DATA['cluster_path'],
            config.RUN['kubeconfig_location']
        )
        with open(kubeconfig_path, "r") as f:
            kubeconfig = yaml.load(f, yaml.Loader)
        cert_file = tempfile.NamedTemporaryFile(delete=False)
        cert_file.write(
            base64.b64decode(
                kubeconfig['clusters'][0]['cluster']['certificate-authority-data']
            )
        )
        cert_file.close()
        self._cacert = cert_file.name
        logger.info(f"Generated CA certification file: {self._cacert}")

    def get(self, resource, payload=None):
        """
        Get alerts from Prometheus API.

        Args:
            resource (str): Represents part of uri that specifies given
                resource
            payload (dict): Provide parameters to GET API call.
                e.g. for `alerts` resource this can be
                {'silenced': False, 'inhibited': False}

        Returns:
            dict: Response from Prometheus alerts api
        """
        pattern = f"/api/v1/{resource}"
        headers = {'Authorization': f"Bearer {self._token}"}

        logger.debug(f"GET {self._endpoint + pattern}")
        logger.debug(f"headers={headers}")
        logger.debug(f"verify={self._cacert}")
        logger.debug(f"params={payload}")

        response = requests.get(
            self._endpoint + pattern,
            headers=headers,
            verify=self._cacert,
            params=payload
        )
        return response

    def wait_for_alert(self, name, state=None, timeout=1200, sleep=5):
        """
        Search for alerts that have requested name and state.

        Args:
            name (str): Alert name
            state (str): Alert state, if provided then there are searched
                alerts with provided state. If not provided then alerts are
                searched for absence of the alert. Loop that looks for alerts
                is broken when there are no alerts returned from API. This
                is done because API is not returning any alerts that are not
                in pending or firing state
            timeout (int): Number of seconds for how long the alert should
                be searched
            sleep (int): Number of seconds to sleep in between alert search

        Returns:
            list: List of alert records
        """
        while timeout > 0:
            alerts_response = self.get(
                'alerts',
                payload={
                    'silenced': False,
                    'inhibited': False,
                }
            )
            msg = f"Request {alerts_response.request.url} failed"
            assert alerts_response.ok, msg
            if state:
                alerts = [
                    alert
                    for alert
                    in alerts_response.json().get('data').get('alerts')
                    if alert.get('labels').get('alertname') == name
                    and alert.get('state') == state
                ]
                logger.info(f"Checking for {name} alerts with state {state}... "
                            f"{len(alerts)} found")
                if len(alerts) > 0:
                    break
            else:
                # search for missing alerts, search is completed when
                # there are no alerts with given name
                alerts = [
                    alert
                    for alert
                    in alerts_response.json().get('data').get('alerts')
                    if alert.get('labels').get('alertname') == name
                ]
                logger.info(f"Checking for {name} alerts. There should be no alerts ... "
                            f"{len(alerts)} found")
                if len(alerts) == 0:
                    break
            time.sleep(sleep)
            timeout -= sleep
        return alerts

    def check_alert_cleared(self, label, measure_end_time, time_min=120):
        """
        Check that all alerts with provided label are cleared.

        Args:
            label (str): Alerts label
            measure_end_time (int): Timestamp of measurement end
            time_min (int): Number of seconds to wait for alert to be cleared
                since measurement end
        """
        time_actual = time.time()
        time_wait = int(
            (measure_end_time + time_min) - time_actual
        )
        if time_wait > 0:
            logger.info(f"Waiting for approximately {time_wait} seconds for alerts "
                        f"to be cleared ({time_min} seconds since measurement end)")
        else:
            time_wait = 1
        cleared_alerts = self.wait_for_alert(
            name=label,
            state=None,
            timeout=time_wait
        )
        logger.info(f"Cleared alerts: {cleared_alerts}")
        assert len(cleared_alerts) == 0, f"{label} alerts were not cleared"
        logger.info(f"{label} alerts were cleared")
