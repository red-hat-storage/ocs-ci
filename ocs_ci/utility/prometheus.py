import errno
import logging
import os
import requests

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(name=__file__)


class PrometheusAPI(object):
    """
    This is wrapper class for Prometheus API.
    """

    _token = None
    _user = None
    _password = None
    _endpoint = None

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
            if os.path.exists(filename):
                with open(filename) as f:
                    password = f.read()
            else:
                raise FileNotFoundError(
                    errno.ENOENT,
                    os.strerror(errno.ENOENT),
                    filename
                )
        self._password = password
        self.refresh_connection()

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

    def alerts(self):
        """
        Get alerts from Prometheus API.

        Returns:
            dict: Response from Prometheus alerts api
        """
        pattern = '/api/v1/alerts'
        headers = {'Authorization': f"Bearer {self._token}"}
        response = requests.get(
            self._endpoint + pattern,
            headers=headers,
            verify=False
        )
        return response
