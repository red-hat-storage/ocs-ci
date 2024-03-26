# -*- coding: utf8 -*-
"""
This module is for interaction with Assisted Installer API
"""

import logging
import os
import requests
import time
import re
from urllib.parse import urljoin

from ocs_ci.framework import config

from ocs_ci.ocs.exceptions import OpenShiftAPIResponseException

logger = logging.getLogger(__name__)


class OpenShiftAPI(object):
    """
    Common class for interaction with various OpenShift APIs
    """

    def __init__(self, environment="production"):
        """
        Initialize required variables

        Args:
            environment (str): Environment defined in configuration (default: production)

        """
        # TODO: check required configuration
        self.env = environment

        self._token = None
        self._token_exp = 0

        # Load configuration
        # SSO URL for generating API token
        self.sso_url = config.AUTH["assisted_installer"][self.env]["sso_url"]
        # Offline token used for generating API token
        self.offline_token = config.AUTH["assisted_installer"][self.env][
            "offline_token"
        ]
        # Assisted Installer Console API url
        self.api_host_url = config.AUTH["assisted_installer"][self.env]["api_url"]

    @property
    def token(self):
        """
        Property for obtaining API token (based on OFFLINE_TOKEN)
        """
        if time.time() > (self._token_exp - 60):
            logger.debug("Refreshing API token")
            full_sso_url = urljoin(
                self.sso_url,
                "/auth/realms/redhat-external/protocol/openid-connect/token",
            )
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            }
            data = {
                "grant_type": "refresh_token",
                "client_id": "cloud-services",
                "refresh_token": self.offline_token,
            }
            now = time.time()
            resp = requests.post(full_sso_url, data=data, headers=headers)
            resp_json = resp.json()
            self._token = resp_json["access_token"]
            self._token_exp = now + resp_json["expires_in"]
            logger.debug(
                f"API token refreshed (expires in {self._token_exp - time.time(): .0f} seconds)"
            )
        else:
            logger.debug(
                f"API token is valid (expires in {self._token_exp - time.time(): .0f} seconds)"
            )

        return self._token

    @property
    def api_base_url(self):
        """
        Prepare and return base URL for API
        """
        return urljoin(self.api_host_url, f"api/{self.api_section}/{self.api_version}/")

    @property
    def headers(self):
        """
        prepare headers for API requests
        """
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        return headers

    def get_api_endpoint_url(self, endpoint=""):
        """
        prepare and return full api endpoint URL

        Args:
            endpoint (str): the final part of the endpoint URL

        Returns:
            str: full API endpoint URL
        """
        return urljoin(self.api_base_url, endpoint)

    def get_request(self, endpoint, params=None, json=True, ignore_failure=False):
        """
        send GET request to the API endpoint

        Args:
            endpoint (str): the final part of the endpoint URL
            params (dict): optional parameters encoded to the request URL
            json (bool): controls if the response should be decoded as json data
            ignore_failure (bool): controls how to deal with failed request (if raise exception or not)

        Returns:
            Response or dict: the response from the server (if json == True, parsed as json)
        """
        url = self.get_api_endpoint_url(endpoint)
        logger.debug(f"Sending GET request to '{url}' with parameters: {params}")
        resp = requests.get(url, params=params, headers=self.headers)
        logger.debug(f"Response: {resp.status_code} {resp.reason}")
        if not ignore_failure and not resp.ok:
            raise OpenShiftAPIResponseException(resp)
        if json:
            logger.debug(f"Response JSON: {resp.json()}")
            return resp.json()
        return resp

    def post_request(self, endpoint, data=None, json=True, ignore_failure=False):
        """
        send POST request to the API endpoint

        Args:
            endpoint (str): the final part of the endpoint URL
            data (dict): optional data send to the request URL
            json (bool): controls if the response should be decoded as json data
            ignore_failure (bool): controls how to deal with failed request (if raise exception or not)

        Returns:
            Response or dict: the response from the server (if json == True, parsed as json)
        """
        url = self.get_api_endpoint_url(endpoint)
        logger.debug(f"Sending POST request to '{url}' with data: {data}")
        resp = requests.post(url, json=data, headers=self.headers)
        logger.debug(f"Response: {resp.status_code} {resp.reason}")
        if not ignore_failure and not resp.ok:
            raise OpenShiftAPIResponseException(resp)
        if json:
            logger.debug(f"Response JSON: {resp.json()}")
            return resp.json()
        return resp

    def patch_request(self, endpoint, data=None, json=True, ignore_failure=False):
        """
        send PATCH request to the API endpoint

        Args:
            endpoint (str): the final part of the endpoint URL
            data (dict): optional data send to the request URL
            json (bool): controls if the response should be decoded as json data
            ignore_failure (bool): controls how to deal with failed request (if raise exception or not)

        Returns:
            Response or dict: the response from the server (if json == True, parsed as json)
        """
        url = self.get_api_endpoint_url(endpoint)
        logger.debug(f"Sending PATCH request to '{url}' with data: {data}")
        resp = requests.patch(url, json=data, headers=self.headers)
        logger.debug(f"Response: {resp.status_code} {resp.reason}")
        if not ignore_failure and not resp.ok:
            raise OpenShiftAPIResponseException(resp)
        if json:
            logger.debug(f"Response JSON: {resp.json()}")
            return resp.json()
        return resp

    def delete_request(self, endpoint, json=True, ignore_failure=False):
        """
        send DELETE request to the API endpoint

        Args:
            endpoint (str): the final part of the endpoint URL
            json (bool): controls if the response should be decoded as json data
            ignore_failure (bool): controls how to deal with failed request (if raise exception or not)

        Returns:
            Response or dict: the response from the server (if json == True, parsed as json)
        """
        url = self.get_api_endpoint_url(endpoint)
        logger.debug(f"Sending DELETE request to '{url}'")
        resp = requests.delete(url, headers=self.headers)
        logger.debug(f"Response: {resp.status_code} {resp.reason}")
        if not ignore_failure and not resp.ok:
            raise OpenShiftAPIResponseException(resp)
        if json:
            logger.debug(f"Response JSON: {resp.json()}")
            return resp.json()
        return resp


class AccountsMgmtAPI(OpenShiftAPI):
    """
    Class for interaction with Accounts mgmt API
    """

    def __init__(self, environment="production"):
        """
        Initialize required variables

        Args:
            environment (str): Assisted Installer environment defined in configuration (default: production)

        """
        self.api_version = "v1"
        self.api_section = "accounts_mgmt"
        super().__init__(environment=environment)

    def get_pull_secret_for_current_user(self):
        """
        Get pull-secret for the current user

        Returns:
            dict: content of the current user's pull-secret
        """
        return self.post_request("access_token")


class AssistedInstallerAPI(OpenShiftAPI):
    """
    Class for interaction with Assisted Installer API
    """

    def __init__(self, environment="production"):
        """
        Initialize required variables

        Args:
            environment (str): Assisted Installer environment defined in configuration (default: production)

        """
        self.api_version = "v2"
        self.api_section = "assisted-install"
        super().__init__(environment=environment)

    def get_component_versions(self):
        """
        Get component versions. Could be used to verify access.

        Returns:
            dict: the versions
        """
        return self.get_request("component-versions")

    def get_clusters(self):
        """
        Get list of clusters.

        Returns:
            dict: the clusters configuration
        """
        return self.get_request("clusters")

    def get_cluster(self, cluster_id):
        """
        Get information about one cluster.

        Args:
            cluster_id (str): cluster ID

        Returns:
            dict: the cluster configuration
        """
        return self.get_request(f"clusters/{cluster_id}")

    def create_cluster(self, data):
        """
        Create (register) new cluster in Assisted Installer console

        Args:
            data (dict): cluster configuration based on
                https://api.openshift.com/?urls.primaryName=assisted-service%20service#/installer/v2RegisterCluster

        """
        return self.post_request("clusters", data)

    def install_cluster(self, cluster_id):
        """
        Launch installation of the OCP cluster.

        Args:
            cluster_id (str): cluster ID

        Returns:
            dict: the cluster configuration
        """
        return self.post_request(f"clusters/{cluster_id}/actions/install")

    def get_cluster_hosts(self, cluster_id):
        """
        Get information about hosts connected to cluster.

        Args:
            cluster_id (str): cluster ID

        Returns:
            dict: the hosts configuration
        """
        return self.get_request(f"clusters/{cluster_id}/hosts")

    def get_cluster_host(self, cluster_id, host_id):
        """
        Get information about host connected to cluster.

        Args:
            cluster_id (str): cluster ID
            host_id (str): host ID

        Returns:
            dict: the host configuration
        """
        return self.get_request(f"clusters/{cluster_id}/hosts/{host_id}")

    def get_cluster_admin_credentials(self, cluster_id):
        """
        Get admin credentials and console URL.

        Args:
            cluster_id (str): cluster ID

        Returns:
            dict: the credentials and console URL
        """
        return self.get_request(f"clusters/{cluster_id}/credentials")

    def get_cluster_kubeconfig(self, cluster_id):
        """
        Get kubeconfig

        Args:
            cluster_id (str): cluster ID

        Returns:
            str: the kubeconfig content
        """
        params = {
            "file_name": "kubeconfig",
        }
        resp = self.get_request(
            f"clusters/{cluster_id}/downloads/credentials", params=params, json=False
        )
        return resp.text

    def download_cluster_logs(self, cluster_id, log_dir):
        """
        Get cluster logs

        Args:
            cluster_id (str): cluster ID
            log_dir (str): destination directory, where to place the logs

        Returns:
            str: the path of the downloaded file

        """
        params = {
            "logs_type": "all",
        }
        resp = self.get_request(
            f"clusters/{cluster_id}/logs", params=params, json=False
        )
        filenames = re.findall(
            "filename=(.+)", resp.headers.get("content-disposition", "")
        )
        if filenames:
            filename = filenames[0].strip("\"'")
        else:
            filename = f"logs_{cluster_id}.tar"
        log_dir = os.path.expanduser(log_dir)
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, filename), "wb") as fd:
            fd.write(resp.content)
        return os.path.join(log_dir, filename)

    def download_cluster_file(self, cluster_id, dest_dir, file_name="metadata.json"):
        """
        Download cluster related files

        Args:
            cluster_id (str): cluster ID
            dest_dir (str): destination directory, where to place the file
            file_name (str): file to download [metadata.json, bootstrap.ign, master.ign, worker.ign,
                install-config.yaml, custom_manifests.json, custom_manifests.yaml] (default: metadata.json)

        """
        params = {
            "file_name": file_name,
        }
        resp = self.get_request(
            f"clusters/{cluster_id}/downloads/files", params=params, json=False
        )
        filenames = re.findall(
            "filename=(.+)", resp.headers.get("content-disposition", "")
        )
        if filenames:
            filename = filenames[0].strip("\"'")
        else:
            filename = file_name
        dest_dir = os.path.expanduser(dest_dir)
        os.makedirs(dest_dir, exist_ok=True)
        with open(os.path.join(dest_dir, filename), "wb") as fd:
            fd.write(resp.content)

    def delete_cluster(self, cluster_id):
        """
        Delete cluster

        Args:
            cluster_id (str): cluster ID
        """
        resp = self.delete_request(f"clusters/{cluster_id}", json=False)
        return resp.ok

    def get_infra_envs(self):
        """
        Get list of infra_envs.

        Returns:
            dict: the infra_envs configuration
        """
        return self.get_request("infra-envs")

    def get_infra_env(self, infra_env_id):
        """
        Get information about one infra_env.

        Args:
            infra_env_id (str): Infra environment ID

        Returns:
            dict: the infra_env configuration
        """
        return self.get_request(f"infra-envs/{infra_env_id}")

    def create_infra_env(self, data):
        """
        Create (register) new Infrastructure Environment in Assisted Installer console

        Args:
            data (dict): Infrastructure environment configuration based on
                https://api.openshift.com/?urls.primaryName=assisted-service%20service#/installer/RegisterInfraEnv

        """
        return self.post_request("infra-envs", data)

    def get_infra_env_hosts(self, infra_env_id):
        """
        Get information about hosts from infra_env.

        Args:
            infra_env_id (str): Infra environment ID

        Returns:
            dict: the hosts configuration
        """
        return self.get_request(f"infra-envs/{infra_env_id}/hosts")

    def get_infra_env_host(self, infra_env_id, host_id):
        """
        Get information about host from infra_env.

        Args:
            infra_env_id (str): Infra environment ID
            host_id (str): host ID

        Returns:
            dict: the host configuration
        """
        return self.get_request(f"infra-envs/{infra_env_id}/hosts/{host_id}")

    def update_infra_env_host(self, infra_env_id, host_id, update_data):
        """
        Update host configuration

        Args:
            infra_env_id (str): Infra environment ID
            host_id (str): Host ID
            update_data (dict): the data to be updated
        """
        data = update_data
        return self.patch_request(
            f"infra-envs/{infra_env_id}/hosts/{host_id}", data=data
        )

    def download_infra_file(
        self,
        infra_env_id,
        dest_dir,
        file_name,
        ipxe_script_type=None,
        discovery_iso_type=None,
    ):
        """
        Download Infrastructure Environment related file

        Args:
            infra_env_id (str): Infra environment ID
            dest_dir (str): destination directory, where to place the file
            file_name (str): file to download [discovery.ign, ipxe-script, static-network-config]
            ipxe_script_type (str): None or specify the script type to be served for iPXE
                ['discovery-image-always', 'boot-order-control']
            discovery_iso_type (str): None or overrides the ISO type for the disovery ignition
                ['full-iso', 'minimal-iso']

        Returns:
            str: the path of the downloaded file

        """
        params = {
            "file_name": file_name,
        }
        if ipxe_script_type:
            params["ipxe_script_type"] = ipxe_script_type
        if discovery_iso_type:
            params["discovery_iso_type"] = discovery_iso_type
        resp = self.get_request(
            f"infra-envs/{infra_env_id}/downloads/files", params=params, json=False
        )
        filenames = re.findall(
            "filename=(.+)", resp.headers.get("content-disposition", "")
        )
        if filenames:
            filename = filenames[0].strip("\"'")
        else:
            filename = file_name
        dest_dir = os.path.expanduser(dest_dir)
        os.makedirs(dest_dir, exist_ok=True)
        with open(os.path.join(dest_dir, filename), "wb") as fd:
            fd.write(resp.content)
        return os.path.join(dest_dir, filename)

    def get_discovery_iso_url(self, infra_env_id):
        """
        Get Assisted Installer discovery iso url

        Args:
            infra_env_id (str): Infra environment ID
        """
        resp = self.get_request(f"infra-envs/{infra_env_id}/downloads/image-url")
        return resp["url"]

    def delete_infra_env(self, infra_env_id):
        """
        Delete Infrastructure Environment

        Args:
            infra_env_id (str): Infra environment ID
        """
        resp = self.delete_request(f"infra-envs/{infra_env_id}", json=False)
        return resp.ok
