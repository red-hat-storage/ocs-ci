import base64
import logging
import os
import requests
import tempfile
import yaml

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
                resource.
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
