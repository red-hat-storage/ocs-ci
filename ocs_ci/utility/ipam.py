"""
This module will interact with IPAM server
"""

import logging
import os
import requests

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import IPAMReleaseUpdateFailed, IPAMAssignUpdateFailed
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


class IPAM(object):
    """
    IPAM class
    """

    def __init__(self, appiapp, ipam=None, token=None):
        """
        Initialize required variables

        Args:
            apiapp (str): App ID
            ipam (str): IPAM server name or IP
            token (str): Authentication token

        """
        self.apiapp = appiapp
        self.ipam = ipam or config.ENV_DATA["ipam"]
        self.token = token or config.ENV_DATA["ipam_token"]

    def assign_ip(self, host, subnet):
        """
        Reserve IP in IPAM server for a given host

        Args:
            host (str): hostname to reserve IP in IPAM server
            subnet (str): subnet to reserve IP

        Returns:
            str: Reserved IP

        Raises:
            HTTPError: in case of HTTP error
            ConnectionError: in case of Connection Error
            Timeout: in case of Timeout
            RequestException: Any Exception from requests
            IPAMAssignUpdateFailed: if it fails to assign IP

        """
        endpoint = os.path.join("http://", self.ipam, "api/getFreeIP.php?")
        payload = {
            "apiapp": self.apiapp,
            "apitoken": self.token,
            "subnet": subnet,
            "host": host,
        }

        try:
            res = requests.post(endpoint, data=payload)
            res.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP Error: {http_err}")
            raise
        except requests.exceptions.ConnectionError as conn_err:
            logger.error(f"Error Connecting: {conn_err}")
            raise
        except requests.exceptions.Timeout as timeout_err:
            logger.error(f"Timeout Error: {timeout_err}")
            raise
        except requests.exceptions.RequestException as err:
            logger.error(f"Unexpected Error: {err}")
            raise

        if "Error" in res.text:
            logger.error(f"Error in assigning IP to host. Error: {res.text}")
            raise IPAMAssignUpdateFailed(f"Failed to assign IP to {host}")
        else:
            logger.info(f"Successfully assigned IP to {host}")
            return res.text

    def assign_ips(self, hosts, subnet):
        """
        Assign IPs to the hosts

        Args:
            hosts (list): List of hosts to reserve IP in IPAM server
            subnet (str): subnet to reserve IPs

        Returns:
            list: List of Reserved IP's

        """
        return [self.assign_ip(host, subnet) for host in hosts]

    @retry(IPAMReleaseUpdateFailed, tries=5, delay=3, backoff=1)
    def release_ip(self, hostname):
        """
        Release IP from IPAM server

        Args:
            hostname (str): Hostname to release IP

        Raises:
            IPAMReleaseUpdateFailed: If it fails to release IP from IPAM server

        """
        # release the IP
        endpoint = os.path.join("http://", self.ipam, "api/removeHost.php?")
        payload = {"apiapp": self.apiapp, "apitoken": self.token, "host": hostname}
        res = requests.post(endpoint, data=payload)
        if res.status_code == "200":
            logger.info(f"Successfully released {hostname} IP from IPAM server")
        else:
            raise IPAMReleaseUpdateFailed(
                f"Failed to release {hostname} IP from IPAM server"
            )

    def release_ips(self, hosts):
        """
        Releases host IP's from IPAM server

        Args:
            hosts (list): List of host names to release IP's

        """
        for host in hosts:
            self.release_ip(host)
