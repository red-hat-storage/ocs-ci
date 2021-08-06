import logging
import os
import requests
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(name=__file__)


def set_pagerduty_integration_secret(integration_key):
    """
    Update ocs-converged-pagerduty secret. This is valid only on ODF Managed Service.

    Args:
        integration_key (str): Integration key taken from PagerDuty Prometheus integration

    """
    logger.info("Setting up PagerSuty integration")
    kubeconfig = os.getenv("KUBECONFIG")
    cmd = (
        f"oc create secret generic {constants.PAGERDUTY_SECRET} "
        f"--from--literal=PAGERDUTY_KEY={integration_key} -n openshift-storage "
        f"--kubeconfig {kubeconfig} --dry-run -o yaml"
    )
    secret_data = exec_cmd(cmd, secrets=[integration_key]).stdout
    with tempfile.NamedTemporaryFile(
        prefix=f"{constants.PAGERDUTY_SECRET}_"
    ) as secret_file:
        secret_file.write(secret_data)
        secret_file.flush()
        exec_cmd(f"oc apply --kubeconfig {kubeconfig} -f {secret_file.name}")


class PagerDutyAPI(object):
    """
    This is wrapper class for PagerDuty API:
    https://developer.pagerduty.com/api-reference/

    In order to use the API, there must be set api_key. That is taken from
    AUTH/pagerduty/api_key in config.

    """

    _token = None
    _endpoint = "https://api.pagerduty.com"

    def __init__(self, token=None):
        """
        Constructor for PagerDutyAPI class.
        """
        self._token = token or config.AUTH["pagerduty"]["api_key"]
        self.set_pagerduty_secret

    def get(self, resource):
        """
        Get resources from PagerDuty API.

        Args:
            resource (str): Represents part of uri that specifies given
                resource

        Returns:
            dict: Response from Prometheus alerts api

        """
        pattern = f"/{resource}"
        headers = {
            "Authorization": f"Token {self._token}",
            "Accept": "application/vnd.pagerduty+json;version=2",
        }

        logger.debug(f"GET {self._endpoint + pattern}")
        logger.debug(f"headers={headers}")

        response = requests.get(
            self._endpoint + pattern,
            headers=headers,
            verify=False,
        )
        return response
