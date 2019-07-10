import base64
import errno
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

    def alerts(self, silenced=False, inhibited=False):
        """
        Get alerts from Prometheus API.

        Args:
            silenced (bool): If alerts that are silenced should be searched.
                If `None` is provided then the flag is not set.
            inhibited (bool): If alerts that are inhibited should be searched.
                If `None` is provided then the flag is not set.

        Returns:
            dict: Response from Prometheus alerts api
        """
        pattern = '/api/v1/alerts'
        headers = {'Authorization': f"Bearer {self._token}"}
        payload = {}
        if silenced is not None:
            payload['silenced'] = silenced
        if inhibited is not None:
            payload['inhibited'] = silenced

        logger.info(f"GET {self._endpoint + pattern}")
        logger.info(f"headers={headers}")
        logger.info(f"verify={self._cacert}")
        logger.info(f"params={payload}")

        response = requests.get(
            self._endpoint + pattern,
            headers=headers,
            verify=self._cacert,
            params=payload
        )
        return response
