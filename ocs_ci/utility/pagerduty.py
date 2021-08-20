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
    logger.info("New integration key was set.")


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

    def get(self, resource, payload=None):
        """
        Get resources from PagerDuty API.

        Args:
            resource (str): Represents part of uri that specifies given
                resource
            payload (dict): Provide parameters to GET API call.
                e.g. for `incidents` resource this can be
                {"service_ids[]": <id>, "since": "2021-07-15T00:00:00Z", "time_zone": "UTC"}

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
        logger.debug(f"params={payload}")

        response = requests.get(
            self._endpoint + pattern,
            headers=headers,
            verify=False,
            params=payload,
        )
        return response

    def create(self, resource, payload=None):
        """
        Post resources from PagerDuty API.

        Args:
            resource (str): Represents part of uri that specifies given
                resource
            payload (dict): Provide parameters to POST API call.

        Returns:
            dict: Response from Prometheus alerts api

        """
        pattern = f"/{resource}"
        headers = {
            "Authorization": f"Token {self._token}",
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Content-Type": "application/json",
        }

        logger.debug(f"POST {self._endpoint + pattern}")
        logger.debug(f"headers={headers}")
        logger.debug(f"params={payload}")

        response = requests.post(
            self._endpoint + pattern,
            headers=headers,
            verify=False,
            params=payload,
        )
        return response

    def get_default_escalation_policy_id(self):
        """
        Get default account escalation policy from PagerDuty API.

        Returns:
            str: Escalation policy id

        """
        default = None
        policies = self.get("escalation_policies").json()
        for policy in policies["escalation_policies"]:
            if policy["name"] == "Default":
                default = policy["id"]
        if not default:
            logger.warning("PagerDuty default escalation policy was not found")
        return default

    def get_vendor_id(self, name):
        """
        Get id of vendor with provided name from PagerDuty API.

        Args:
            name (str): Vendor name

        Returns:
            str: Vendor id

        """
        vendor_id = None
        vendors = self.get("vendors").json()
        for vendor in vendors["vendors"]:
            if vendor["name"] == name:
                vendor_id = vendor["id"]
        if not vendor_id:
            logger.warning(f"PagerDuty vendor {name} was not found")
        return vendor_id

    def get_service_dict(self):
        """
        Get a structure prepared to be a payload for service creation via API.

        Returns:
            dict: Structure containing all data required to by PagerDuty API
                in order to create a service

        """
        cluster_name = config.ENV_DATA["cluster_name"]
        default_policy = self.get_default_escelation_policy_id()
        return {
            "service": {
                "type": "service",
                "name": cluster_name,
                "description": f"Service for cluster {cluster_name}",
                "status": "active",
                "escalation_policy": {
                    "id": default_policy,
                    "type": "escalation_policy_reference",
                },
                "alert_creation": "create_alerts_and_incidents",
            }
        }

    def get_integration_dict(self, vendor_name):
        """
        Get a structure prepared to be a payload for integration creation via API.

        Args:
            vendor_name (str): Name of vendor that is used in integration

        Returns:
            dict: Structure containing all data required to by PagerDuty API
                in order to create an integration

        """

        vendor_id = self.get_vendor_id("Prometheus")
        return {
            "integration": {
                "type": "generic_events_api_inbound_integration",
                "name": "Prometheus",
                "vendor": {"type": "vendor_reference", "id": vendor_id},
            }
        }
