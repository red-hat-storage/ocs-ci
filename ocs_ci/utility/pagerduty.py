import logging
import os
import requests
import tempfile
import time

from ocs_ci.framework import config
from ocs_ci.ocs import managedservice
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(name=__file__)


def set_pagerduty_integration_secret(integration_key):
    """
    Update ocs-converged-pagerduty secret. This is valid only on ODF Managed Service.
    ocs-converged-pagerduty secret is expected to be present prior to the update.

    Args:
        integration_key (str): Integration key taken from PagerDuty Prometheus integration

    """
    logger.info("Setting up PagerDuty integration")
    kubeconfig = os.getenv("KUBECONFIG")
    ns_name = config.ENV_DATA["service_namespace"]
    cmd = (
        f"oc create secret generic {managedservice.get_pagerduty_secret_name()} "
        f"--from-literal=PAGERDUTY_KEY={integration_key} -n {ns_name} "
        f"--kubeconfig {kubeconfig} --dry-run=client -o yaml"
    )
    secret_data = exec_cmd(
        cmd,
        secrets=[
            integration_key,
            managedservice.get_pagerduty_secret_name(),
        ],
    ).stdout
    with tempfile.NamedTemporaryFile(
        prefix=f"{managedservice.get_pagerduty_secret_name()}_"
    ) as secret_file:
        secret_file.write(secret_data)
        secret_file.flush()
        exec_cmd(f"oc apply --kubeconfig {kubeconfig} -f {secret_file.name}")
    logger.info("New integration key was set.")


def check_incident_list(summary, urgency, incidents, status="triggered"):
    """
    Check list of incidents that there are incidents with requested label
    in summary and specific urgency. If some incident is missing then this check
    fails.

    Args:
        summary (str): String that is part of incident summary
        urgency (str): Incident urgency
        incidents (list): List of incidents to check
        status (str): Incident status

    Returns:
        list: List of incidents that match search requirements

    """
    logger.info(f"Looking for incidents with summary {summary} and urgency {urgency}")
    target_incidents = [
        incident
        for incident in incidents
        if (
            summary in incident.get("summary")
            and incident.get("urgency") == urgency
            and incident.get("status") == status
        )
    ]
    if target_incidents:
        logger.info(f"Incidents with summary {summary} were found: {target_incidents}")
    else:
        logger.info(
            f"No incidents with summary {summary} and urgency {urgency} were found"
        )
    return target_incidents


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
            dict: Response from PagerDuty api

        """
        pattern = f"/{resource}"
        headers = {
            "Authorization": f"Token token={self._token}",
            "Accept": "application/vnd.pagerduty+json;version=2",
        }

        logger.debug(f"GET {self._endpoint + pattern}")
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
            dict: Response from PagerDuty api

        """
        pattern = f"/{resource}"
        headers = {
            "Authorization": f"Token token={self._token}",
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Content-Type": "application/json",
        }

        logger.debug(f"POST {self._endpoint + pattern}")
        logger.debug(f"json={payload}")

        response = requests.post(
            self._endpoint + pattern,
            headers=headers,
            verify=False,
            json=payload,
        )
        return response

    def delete(self, resource):
        """
        Delete resource from PagerDuty API.

        Args:
            resource (str): Represents part of uri that specifies given
                resource

        Returns:
            dict: Response from PagerDuty api

        """
        pattern = f"/{resource}"
        headers = {
            "Authorization": f"Token token={self._token}",
            "Accept": "application/vnd.pagerduty+json;version=2",
        }

        logger.debug(f"GET {self._endpoint + pattern}")

        response = requests.delete(
            self._endpoint + pattern,
            headers=headers,
            verify=False,
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
        offset = 0
        more = True
        while more:
            payload = {"limit": 100, "offset": offset}
            vendors = self.get("vendors", payload=payload).json()
            for vendor in vendors["vendors"]:
                if vendor["name"] == name:
                    vendor_id = vendor["id"]
                    break
            if vendors["more"]:
                offset = int(vendors["offset"]) + 100
            else:
                more = False
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
        # timestamp is added to service name to ensure unique name of service
        timestamp = time.time()
        default_policy = self.get_default_escalation_policy_id()
        return {
            "service": {
                "type": "service",
                "name": f"{cluster_name}_{int(timestamp)}",
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

    def wait_for_incident_cleared(
        self, summary, timeout=1200, sleep=5, pagerduty_service_ids=None
    ):
        """
        Search for incident to be cleared.

        Args:
            summary (str): Incident summary
            sleep (int): Number of seconds to sleep in between incidents search
            pagerduty_service_ids (list): service ids used in incidents get query

        Returns:
            list: List of incident records

        """
        while timeout > 0:
            incidents_response = self.get(
                "incidents", payload={"service_ids[]": pagerduty_service_ids}
            )
            msg = f"Request {incidents_response.request.url} failed"
            assert incidents_response.ok, msg
            incidents = [
                incident
                for incident in incidents_response.json().get("incidents")
                if summary in incident.get("summary")
                and incident.get("status") != "resolved"
            ]
            logger.info(
                f"Checking for {summary} incidents. There should be no incidents ... "
                f"{len(incidents)} found"
            )
            if len(incidents) == 0:
                break
            time.sleep(sleep)
            timeout -= sleep
        return incidents

    def check_incident_cleared(
        self, summary, measure_end_time, time_min=420, pagerduty_service_ids=None
    ):
        """
        Check that all incidents with provided summary are cleared.

        Args:
            summary (str): Incident summary
            measure_end_time (int): Timestamp of measurement end
            time_min (int): Number of seconds to wait for incidents to be cleared
                since measurement end
            pagerduty_service_ids (list): service ids used in incidents get query

        """
        time_actual = time.time()
        time_wait = int((measure_end_time + time_min) - time_actual)
        if time_wait > 0:
            logger.info(
                f"Waiting for approximately {time_wait} seconds for incidents "
                f"to be cleared ({time_min} seconds since measurement end)"
            )
        else:
            time_wait = 1
        cleared_incidents = self.wait_for_incident_cleared(
            summary=summary,
            timeout=time_wait,
            pagerduty_service_ids=pagerduty_service_ids,
        )
        logger.info(f"Cleared incidents: {cleared_incidents}")
        if len(cleared_incidents) != 0:
            raise UnexpectedBehaviour(f"{summary} incidents were not cleared")
        else:
            logger.info(f"{summary} incidents were cleared")
