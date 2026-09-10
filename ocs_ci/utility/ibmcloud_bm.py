# -*- coding: utf8 -*-
"""
Module for interactions with IBM Cloud Cluster.

"""

import json
import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import exec_cmd, run_cmd  # IgnoreDeprecation
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


class IBMCloudBM(object):
    """
    Wrapper for IBM Cloud with Bare metal machines
    """

    def __init__(self, region=None):
        """
        Constructor for IBM Cloud Bare Metal machines

        Args:
            region (str): The region of the IBM Cloud Bare Metal machines

        """
        ibm_config = config.AUTH.get("ibmcloud", {})
        if not ibm_config:
            raise ValueError(
                "IBM Cloud Bare Metal is only supported on cluster which has IBM cloud details in AUTH section"
            )
        self.api_key = ibm_config["api_key"]
        self.account_id = ibm_config.get("account_id")
        self.region = region or config.ENV_DATA.get("region")

    def login(self):
        """
        Login to IBM Cloud account
        """
        login_cmd = f"ibmcloud login --apikey {self.api_key} -c {self.account_id} -r {self.region}"
        logger.info("Logging to IBM cloud")
        run_cmd(login_cmd, secrets=[self.api_key])
        logger.info("Successfully logged in to IBM cloud")

    @retry(
        CommandFailed,
        tries=3,
        delay=20,
        backoff=1,
    )
    def run_ibmcloud_bm_cmd(
        self, cmd, secrets=None, timeout=600, ignore_error=False, **kwargs
    ):
        """
        Wrapper function for `run_cmd` which if needed will perform IBM Cloud login
        command before running the ibmcloud bare metal command. In the case run_cmd will fail
        because the IBM cloud got disconnected, it will login and re-try.

        Args:
            cmd (str): command to run
            secrets (list): A list of secrets to be masked with asterisks
                This kwarg is popped in order to not interfere with
                subprocess.run(``**kwargs``)
            timeout (int): Timeout for the command, defaults to 600 seconds.
            ignore_error (bool): True if ignore non zero return code and do not
                raise the exception.
        """
        basic_cmd = "ibmcloud sl hardware "
        cmd = basic_cmd + cmd

        try:
            return run_cmd(cmd, secrets, timeout, ignore_error, **kwargs)
        except CommandFailed as ex:
            login_error_messages = [
                "Failed to get",
                "Access Denied",
                "Please login",
                "token is expired",
            ]
            # Check if we need to re-login to IBM Cloud account
            if any([error_msg in str(ex) for error_msg in login_error_messages]):
                self.login()
                wait_time_after_login = 10
                logger.info(f"Wait {wait_time_after_login} seconds before proceeding")
                time.sleep(wait_time_after_login)
                return run_cmd(cmd, secrets, timeout, ignore_error, **kwargs)
            else:
                raise ex

    def get_all_machines(self):
        """
        Get all the machines in the IBMCloud Bare metal machines

        Returns:
            list: List of dictionaries. List of all the machines in the IBMCloud Bare metal machines

        """
        cmd = "list --output json"
        machine_list = json.loads(self.run_ibmcloud_bm_cmd(cmd))
        return machine_list

    def get_machines_by_names(self, machine_names):
        """
        Get the machines in the IBMCloud Bare metal machines that have the given machine names

        Args:
            machine_names (list): The list of the machine names to search for.

        Returns:
            Get the machines in the IBMCloud Bare metal machines that have the given machine names

        """
        machine_list = self.get_all_machines()
        return [m for m in machine_list if m["hostname"] in machine_names]

    def stop_machines(self, machines):
        """
        Stop the IBMCloud Bare metal machines

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects to stop

        """
        for m in machines:
            logger.info(f"Powering off the machine with ip {m['primaryIpAddress']}")
            cmd = f"power-off {m['id']} -f"
            self.run_ibmcloud_bm_cmd(cmd)

    def start_machines(self, machines):
        """
        Start the IBMCloud Bare metal machines

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects to start

        """
        for m in machines:
            logger.info(f"Powering on the machine with ip {m['primaryIpAddress']}")
            cmd = f"power-on {m['id']}"
            self.run_ibmcloud_bm_cmd(cmd)

    def restart_machines(self, machines, force=False):
        """
        Reboot the IBMCloud Bare metal machines

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects to restart
            force (bool): If False, will perform a soft reboot. Otherwise, if True, will perform a hard reboot

        """
        reboot_type = "hard" if force else "soft"
        for m in machines:
            logger.info(f"Reboot the machine with the ip {m['primaryIpAddress']}")
            cmd = f"reboot {m['id']} -f --{reboot_type}"
            self.run_ibmcloud_bm_cmd(cmd)

    def restart_machines_by_stop_and_start(self, machines):
        """
        Restart the IBMCloud Bare metal machines by stop and start

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects to restart

        """
        self.stop_machines(machines)
        self.start_machines(machines)

    def get_machines_that_are_not_off_from_sensor_data(self, machines):
        """
        Retrieve a server’s hardware state via its internal sensors and check if RPM Sensor data is present.
        Data from RPM sensors won't be available if the machine is OFF. To be used only as an alternate method.
        A command resulting in the power status of the machine is not available. This method is not recommended to
        identify the machines that are ON and functional

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects

        Returns:
            list: List of machines which are not off
        """
        machines_not_off = []
        for m in machines:
            # Use json output because without json the output will have the title "RPM Sensor"
            cmd = f"sensor {m['id']} --output json"
            if "RPM Sensor" in self.run_ibmcloud_bm_cmd(cmd):
                machines_not_off.append(m)

        return machines_not_off


class IBMCloudVPCBM(IBMCloudBM):
    """
    Wrapper for IBM Cloud VPC Bare Metal servers

    Manages VPC-based bare metal servers using the IBM Cloud VPC API.
    Used for reserved pool deployments where servers are started/stopped
    between cluster deployments.
    """

    def __init__(self, region=None, pool_tag="odf-qe-reserved-pool"):
        """
        Constructor for IBM Cloud VPC Bare Metal servers

        Args:
            region (str): The region of the IBM Cloud VPC (defaults to us-south)
            pool_tag (str): Tag to identify reserved pool servers
        """
        super().__init__(region=region or config.ENV_DATA.get("region", "us-south"))
        self.pool_tag = pool_tag
        self.api_version = "2024-06-01"
        self.generation = "2"
        self.api_base = f"https://{self.region}.iaas.cloud.ibm.com/v1"
        self._token = None
        self._token_expiry = 0

        # Auto-login to IBM Cloud CLI for CIS DNS operations
        self.login()

    def _get_token(self):
        """
        Get IAM access token for API calls

        Returns:
            str: IAM access token

        Raises:
            CommandFailed: If token acquisition fails
        """
        if self._token and time.time() < self._token_expiry:
            return self._token

        logger.info("Getting IAM token for IBM Cloud VPC API")

        # Use requests library instead of subprocess curl
        try:
            response = requests.post(
                "https://iam.cloud.ibm.com/identity/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
                    "apikey": self.api_key,
                },
                timeout=30,
            )
            response.raise_for_status()
            token_data = response.json()

            self._token = token_data.get("access_token")
            if not self._token:
                raise CommandFailed("Failed to get IAM token from response")

            # Token expires in ~1 hour, cache for 50 minutes
            self._token_expiry = time.time() + 3000
            logger.info("Successfully obtained IAM token")
            return self._token

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get IAM token: {e}")
            raise CommandFailed(f"IAM token acquisition failed: {e}")

    @staticmethod
    def _create_cis_a_record(record_name, ip_address, cis_instance, domain_id):
        """
        Create a single CIS DNS A record (idempotent)

        Helper method to eliminate duplication in create_cis_dns_records().
        If record already exists with matching IP, returns existing record ID.
        If record exists with different IP, updates it.

        Args:
            record_name (str): DNS record name (e.g., "api.cluster-name")
            ip_address (str): IP address to point to
            cis_instance (str): CIS instance name
            domain_id (str): CIS domain ID

        Returns:
            str: DNS record ID

        Raises:
            CommandFailed: If DNS record creation/update fails
        """
        logger.info(
            f"Creating DNS record: {record_name}.ibmcloud2.qe.rh-ocs.com -> {ip_address}"
        )

        # Check if record already exists
        list_cmd = (
            f"ibmcloud cis dns-records {domain_id} "
            f"--instance {cis_instance} --per-page 1000 --output json"
        )
        try:
            result = exec_cmd(list_cmd).stdout.decode()
            records = json.loads(result)
            for record in records:
                # Match by name (substring for FQDN)
                if f"{record_name}." in record.get("name", ""):
                    record_id = record["id"]
                    existing_ip = record.get("content")
                    raise CommandFailed(
                        f"DNS record '{record_name}' already exists (ID: {record_id}, IP: {existing_ip}). "
                        f"Run destroy to clean up resources from previous deployment before retrying."
                    )
        except CommandFailed:
            # Re-raise CommandFailed (from existing record check above)
            raise
        except Exception as e:
            logger.warning(
                f"Failed to check for existing DNS record: {e}, proceeding to create"
            )

        # Create new record
        cmd = [
            "ibmcloud",
            "cis",
            "dns-record-create",
            domain_id,
            "--instance",
            cis_instance,
            "--type",
            "A",
            "--name",
            record_name,
            "--content",
            ip_address,
            "--ttl",
            "300",
            "--output",
            "json",
        ]

        cmd_str = " ".join(cmd)
        result = exec_cmd(cmd_str).stdout.decode()
        record = json.loads(result)
        record_id = record.get("id")
        logger.info(f"DNS record created: {record_id}")
        return record_id

    @staticmethod
    def create_cis_dns_records(
        cluster_name, api_vip, ingress_vip, cis_instance=None, domain_id=None
    ):
        """
        Create DNS records in IBM Cloud Internet Services (CIS) for API, API-INT, and Ingress VIPs.

        Creates three DNS records required for OpenShift Assisted Installer:
        - api.<cluster>.<domain> -> API VIP
        - api-int.<cluster>.<domain> -> API VIP (internal API)
        - *.apps.<cluster>.<domain> -> Ingress VIP

        Args:
            cluster_name (str): OpenShift cluster name
            api_vip (str): API load balancer IP
            ingress_vip (str): Ingress load balancer IP
            cis_instance (str): CIS instance name (default: from constants.IBM_CIS_INSTANCE)
            domain_id (str): CIS domain ID for ibmcloud2.qe.rh-ocs.com (default: from constants.IBM_CIS_DOMAIN_ID)

        Returns:
            dict: Dictionary with API, API-INT, and Ingress DNS record IDs
        """
        # Use constants if not provided
        if cis_instance is None:
            cis_instance = constants.IBM_CIS_INSTANCE
        if domain_id is None:
            domain_id = constants.IBM_CIS_DOMAIN_ID
        logger.info(f"Creating CIS DNS records for cluster {cluster_name}")

        # Create all three DNS records using helper method
        api_record_id = IBMCloudVPCBM._create_cis_a_record(
            f"api.{cluster_name}", api_vip, cis_instance, domain_id
        )
        api_int_record_id = IBMCloudVPCBM._create_cis_a_record(
            f"api-int.{cluster_name}", api_vip, cis_instance, domain_id
        )
        ingress_record_id = IBMCloudVPCBM._create_cis_a_record(
            f"*.apps.{cluster_name}", ingress_vip, cis_instance, domain_id
        )

        # Wait a bit for DNS propagation
        logger.info("Waiting 10 seconds for DNS propagation...")
        time.sleep(10)

        return {
            "api_record_id": api_record_id,
            "api_int_record_id": api_int_record_id,
            "ingress_record_id": ingress_record_id,
        }

    @staticmethod
    def delete_cis_dns_records(cluster_name, cis_instance=None, domain_id=None):
        """
        Delete DNS records in IBM Cloud Internet Services (CIS) for a cluster.

        Deletes all DNS records matching the cluster name pattern:
        - api.<cluster>.<domain>
        - api-int.<cluster>.<domain>
        - *.apps.<cluster>.<domain>

        Args:
            cluster_name (str): OpenShift cluster name
            cis_instance (str): CIS instance name (default: from constants.IBM_CIS_INSTANCE)
            domain_id (str): CIS domain ID (default: from constants.IBM_CIS_DOMAIN_ID)
        """
        # Use constants if not provided
        if cis_instance is None:
            cis_instance = constants.IBM_CIS_INSTANCE
        if domain_id is None:
            domain_id = constants.IBM_CIS_DOMAIN_ID

        logger.info(f"Deleting CIS DNS records for cluster {cluster_name}")

        # Get all DNS records with pagination (--per-page 1000)
        list_cmd = (
            f"ibmcloud cis dns-records {domain_id} "
            f"--instance {cis_instance} --per-page 1000 --output json"
        )
        try:
            result = exec_cmd(list_cmd).stdout.decode()
            records = json.loads(result)
        except Exception as e:
            logger.warning(f"Failed to list DNS records: {e}")
            return

        # Delete records matching cluster name (substring match for FQDNs)
        deleted_count = 0
        for record in records:
            record_name = record.get("name", "")
            # Match FQDNs containing cluster pattern (e.g., api.cluster.ibmcloud2.qe.rh-ocs.com)
            # Use substring match: ".{cluster_name}." appears in the FQDN
            if f".{cluster_name}." in record_name:
                record_id = record["id"]
                logger.info(f"Deleting DNS record: {record_name}")
                delete_cmd = (
                    f"ibmcloud cis dns-record-delete {domain_id} {record_id} "
                    f"--instance {cis_instance}"
                )
                try:
                    exec_cmd(delete_cmd)
                    deleted_count += 1
                    logger.info(f"Successfully deleted DNS record: {record_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete DNS record {record_name}: {e}")

        logger.info(f"Deleted {deleted_count} DNS records for cluster {cluster_name}")

    @retry(CommandFailed, tries=3, delay=20, backoff=1)
    def _api_call(self, method, endpoint, data=None):
        """
        Make an API call to IBM Cloud VPC API using requests library

        Args:
            method (str): HTTP method (GET, POST, PUT, DELETE, PATCH)
            endpoint (str): API endpoint path
            data (dict): Request body data (will be JSON-encoded)

        Returns:
            dict: Parsed JSON response

        Raises:
            CommandFailed: If API call fails
        """
        token = self._get_token()
        # Add version and generation parameters to endpoint
        separator = "&" if "?" in endpoint else "?"
        url = f"{self.api_base}{endpoint}{separator}version={self.api_version}&generation={self.generation}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Setup retry strategy for transient failures
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)

        try:
            logger.debug(f"API call: {method} {url}")
            # Don't log request body (may contain sensitive data like user_data)

            response = session.request(
                method=method, url=url, headers=headers, json=data, timeout=90
            )

            logger.debug(f"Response status: {response.status_code}")
            response.raise_for_status()

            # Parse response
            if response.content:
                parsed = response.json()
                logger.debug(
                    f"Parsed response type: {type(parsed)}, "
                    f"keys: {parsed.keys() if isinstance(parsed, dict) else 'N/A'}"
                )

                # Check for API errors in response body
                if isinstance(parsed, dict) and "errors" in parsed:
                    error_msg = parsed["errors"][0].get("message", "Unknown error")
                    raise CommandFailed(f"IBM Cloud API error: {error_msg}")

                return parsed

            logger.warning(f"Empty result from API call: {method} {endpoint}")
            return {}

        except requests.exceptions.HTTPError as e:
            logger.error(f"API call failed: {e}")
            if e.response is not None:
                logger.error(f"Response: {e.response.text}")

            # Check for auth errors that need token refresh
            if e.response and e.response.status_code in [401, 403]:
                self._token = None
                self._token_expiry = 0

            raise CommandFailed(
                f"IBM Cloud API error: {e.response.text if e.response else str(e)}"
            )

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise CommandFailed(f"IBM Cloud API request failed: {e}")

        finally:
            session.close()

    def _paginated_list(self, endpoint, result_key):
        """
        Make paginated API call, returning all results

        Args:
            endpoint (str): API endpoint path (e.g., "/bare_metal_servers")
            result_key (str): Key in response containing results (e.g., "bare_metal_servers")

        Returns:
            list: All results across all pages
        """
        results = []
        start = None

        while True:
            # Build URL with pagination params
            separator = "&" if "?" in endpoint else "?"
            page_endpoint = f"{endpoint}{separator}limit=100"
            if start:
                page_endpoint = f"{page_endpoint}&start={start}"

            response = self._api_call("GET", page_endpoint)

            # Extract results for this page
            batch = response.get(result_key, [])
            results.extend(batch)

            # Check for next page
            next_link = response.get("next", {}).get("href")
            if not next_link:
                break

            # Extract 'start' token from next URL
            import urllib.parse

            parsed = urllib.parse.urlparse(next_link)
            query_params = urllib.parse.parse_qs(parsed.query)
            start = query_params.get("start", [None])[0]

            if not start:
                break

        logger.debug(f"Fetched {len(results)} total items from {endpoint} across pages")
        return results

    def get_reserved_pool_servers(self, pool_tag=None):
        """
        Get all servers in the reserved pool

        Args:
            pool_tag (str): Tag to filter servers (defaults to instance pool_tag)

        Returns:
            list: List of server dictionaries with id, name, status, tags

        Raises:
            CommandFailed: If API call fails
        """
        tag = pool_tag or self.pool_tag
        logger.info(f"Getting reserved pool servers with tag: {tag}")

        # Use pagination to get all servers (may be >100 in large accounts)
        all_servers = self._paginated_list("/bare_metal_servers", "bare_metal_servers")

        pool_servers = [
            server for server in all_servers if tag in server.get("tags", [])
        ]

        logger.info(f"Found {len(pool_servers)} reserved pool servers")
        return pool_servers

    def get_server_status(self, server_id):
        """
        Get status of a specific server

        Args:
            server_id (str): Server ID

        Returns:
            dict: Server details including status

        Raises:
            CommandFailed: If API call fails
        """
        return self._api_call("GET", f"/bare_metal_servers/{server_id}")

    def start_server(self, server_id):
        """
        Start a stopped server

        Args:
            server_id (str): Server ID to start

        Raises:
            CommandFailed: If start operation fails
        """
        server = self.get_server_status(server_id)
        server_name = server.get("name", server_id)

        logger.info(f"Starting server {server_name} ({server_id})")
        self._api_call("POST", f"/bare_metal_servers/{server_id}/start")

    def stop_server(self, server_id, stop_type="soft"):
        """
        Stop a running server

        Args:
            server_id (str): Server ID to stop
            stop_type (str): Type of stop - 'soft' or 'hard'

        Raises:
            CommandFailed: If stop operation fails
        """
        server = self.get_server_status(server_id)
        server_name = server.get("name", server_id)

        logger.info(f"Stopping server {server_name} ({server_id}) - {stop_type} stop")
        self._api_call(
            "POST", f"/bare_metal_servers/{server_id}/stop", {"type": stop_type}
        )

    def get_ipxe_image_id(self):
        """
        Get iPXE image ID for VPC BM server reinitialization

        Uses hardcoded IBM Cloud public iPXE image ID to avoid pagination issues.
        Image: ibm-ipxe-20240326-amd64-1 (stable public image)

        Previous implementation queried all public images (1,190+ images) but only
        received the first page (50 results) due to API pagination, missing the iPXE
        image which was beyond page 1. Since the image ID is stable, hardcoding is
        more reliable.

        Returns:
            str: iPXE image ID

        Raises:
            CommandFailed: If image verification fails
        """
        # Check if already cached
        if hasattr(self, "_ipxe_image_id") and self._ipxe_image_id:
            return self._ipxe_image_id

        # IBM Cloud public iPXE image (stable ID)
        IPXE_IMAGE_ID = "r006-bfef819d-11af-4252-9bd3-bae1d9dd8e1d"

        logger.info(f"Using IBM Cloud public iPXE image: {IPXE_IMAGE_ID}")

        # Verify image exists and is available
        try:
            image = self._api_call("GET", f"/images/{IPXE_IMAGE_ID}")
            if image.get("status") != "available":
                raise CommandFailed(
                    f"iPXE image {IPXE_IMAGE_ID} is not available (status: {image.get('status')})"
                )

            logger.info(
                f"Verified iPXE image: {image.get('name')} (status: {image.get('status')})"
            )

            # Cache for session
            self._ipxe_image_id = IPXE_IMAGE_ID
            return self._ipxe_image_id

        except Exception as e:
            logger.error(f"Failed to verify iPXE image {IPXE_IMAGE_ID}: {e}")
            raise CommandFailed(f"iPXE image not available: {e}")

    def get_ssh_keys_for_server(self, server_id):
        """
        Get SSH key IDs configured for a server

        Args:
            server_id (str): Server ID

        Returns:
            list: List of SSH key ID strings

        Raises:
            CommandFailed: If API call fails
        """
        logger.info(f"Getting SSH keys for server {server_id}")
        init_config = self._api_call(
            "GET", f"/bare_metal_servers/{server_id}/initialization"
        )
        keys = init_config.get("keys", [])
        key_ids = [key["id"] for key in keys]
        logger.info(f"Found {len(key_ids)} SSH keys")
        return key_ids

    def wait_for_server_status(self, server_id, expected_status, timeout=600):
        """
        Wait for server to reach expected status

        Args:
            server_id (str): Server ID
            expected_status (str): Expected status (stopped, running)
            timeout (int): Timeout in seconds (default: 600s / 10 minutes)

        Raises:
            CommandFailed: If timeout reached
        """
        logger.info(
            f"Waiting for server to reach status: {expected_status} (timeout: {timeout}s)"
        )
        start_time = time.time()
        last_status = None

        while time.time() - start_time < timeout:
            server = self.get_server_status(server_id)
            current_status = server.get("status")

            # Log status changes
            if current_status != last_status:
                elapsed = int(time.time() - start_time)
                logger.info(f"Server status: {current_status} (elapsed: {elapsed}s)")
                last_status = current_status

            if current_status == expected_status:
                logger.info(f"Server reached status: {expected_status}")
                return
            time.sleep(10)

        # Log final status on timeout
        final_server = self.get_server_status(server_id)
        final_status = final_server.get("status")
        logger.error(
            f"Timeout: server stuck in '{final_status}' state after {timeout}s"
        )
        raise CommandFailed(
            f"Timeout waiting for server to reach {expected_status} (stuck in: {final_status})"
        )

    def generate_rhcos_ipxe_script(self, kernel_url, initrd_url, ai_kernel_args):
        """
        Generate inline iPXE script for Assisted Installer discovery boot

        Uses Jinja2 template from ocs_ci/templates/ipxe/rhcos-boot.ipxe.j2
        The script uses DHCP for network configuration (both iPXE and initramfs phases).
        This inline script is passed directly to the IBM Cloud API as user_data.

        For Assisted Installer: boots discovery image from AI service URLs,
        server registers with AI service. NO helper node needed.

        Args:
            kernel_url (str): Full URL to kernel from AI iPXE script
            initrd_url (str): Full URL to initrd from AI iPXE script
            ai_kernel_args (str): Kernel arguments from AI-generated iPXE script

        Returns:
            str: iPXE script content (suitable for inline use as user_data)
        """
        from ocs_ci.utility import templating

        # Use AI's kernel args as-is (includes all ignition params)
        # Prepare template data with full URLs
        template_data = {
            "kernel_url": kernel_url,
            "initrd_url": initrd_url,
            "kernel_args": ai_kernel_args,
        }

        # Render template
        _templating = templating.Templating()
        ipxe_script = _templating.render_template(
            "ipxe/rhcos-boot.ipxe.j2", template_data
        )

        return ipxe_script

    def reinitialize_with_ipxe(
        self, server_id, script_content, ssh_key_ids=None, wait_for_stop=True
    ):
        """
        Reinitialize server with iPXE image and inline boot script

        Uses inline iPXE script content passed directly as user_data to IBM Cloud API.
        This eliminates the need to upload scripts to helper node (images still on helper).

        Args:
            server_id (str): Server ID
            script_content (str): iPXE script content (inline, not URL)
            ssh_key_ids (list): SSH key IDs (optional, uses server's current keys)
            wait_for_stop (bool): Wait for server to stop first

        Raises:
            CommandFailed: If reinitialize operation fails
        """
        server = self.get_server_status(server_id)
        server_name = server.get("name", server_id)
        current_status = server.get("status")

        logger.info(
            f"Reinitializing {server_name} with iPXE boot (current status: {current_status})"
        )

        # Stop server if not already stopped
        if current_status and current_status != "stopped":
            logger.info(
                f"Server status is '{current_status}', stopping server with soft stop"
            )
            self.stop_server(server_id, stop_type="soft")
            if wait_for_stop:
                try:
                    # Try soft stop first with 5-minute timeout
                    self.wait_for_server_status(server_id, "stopped", 300)
                except CommandFailed as e:
                    logger.warning(f"Soft stop failed or timed out: {e}")
                    logger.info("Attempting hard stop (forceful power-off)")
                    self.stop_server(server_id, stop_type="hard")
                    # Wait for hard stop with 5-minute timeout
                    self.wait_for_server_status(server_id, "stopped", 300)

        # Get SSH keys if not provided
        if not ssh_key_ids:
            ssh_key_ids = self.get_ssh_keys_for_server(server_id)

        # Get latest iPXE image
        ipxe_image_id = self.get_ipxe_image_id()

        # Reinitialize with iPXE using inline script content
        # IBM Cloud API accepts the script content directly as user_data
        request_data = {
            "image": {"id": ipxe_image_id},
            "keys": [{"id": key_id} for key_id in ssh_key_ids],
            "user_data": script_content,
        }

        logger.info(f"Sending reinitialize request with iPXE image: {ipxe_image_id}")
        logger.info(f"Using inline iPXE script ({len(script_content)} bytes)")
        self._api_call(
            "PUT", f"/bare_metal_servers/{server_id}/initialization", request_data
        )
        logger.info(
            f"{server_name} reinitialized successfully - will boot with inline iPXE script"
        )

        # Wait for server to boot from iPXE and reach running state (15 min timeout)
        # Server reaches API "running" status in ~11 mins but continues internal configuration
        logger.info(
            f"Waiting for {server_name} to boot and reach running state (this may take up to 15 minutes)..."
        )
        self.wait_for_server_status(server_id, "running", timeout=900)

    def add_alb_pool_members(self, lb_id, server_ips, ports):
        """
        Add bare metal servers as members to existing ALB pools

        This is called after servers have been started/reinitialized.

        Args:
            lb_id (str): Load balancer ID
            server_ips (list): List of server IP addresses
            ports (list): List of ports to add members for (e.g., [6443, 22623] for API)

        Raises:
            CommandFailed: If member addition fails
        """
        logger.info(f"Adding {len(server_ips)} servers as members to ALB {lb_id} pools")

        # Get all pools for this ALB
        pools_response = self._api_call("GET", f"/load_balancers/{lb_id}/pools")
        pools = pools_response.get("pools", [])

        for port in ports:
            # Find the pool for this port
            pool = None
            for p in pools:
                if str(port) in p.get("name", ""):
                    pool = p
                    break

            if not pool:
                logger.warning(
                    f"Pool for port {port} not found in ALB {lb_id}, skipping"
                )
                continue

            pool_id = pool["id"]
            logger.info(f"Adding members to pool {pool_id} (port {port})")

            # Add each server as a member
            for ip in server_ips:
                try:
                    member_data = {"port": port, "target": {"address": ip}}
                    self._api_call(
                        "POST",
                        f"/load_balancers/{lb_id}/pools/{pool_id}/members",
                        member_data,
                    )
                    logger.info(f"Added server {ip} to pool {pool_id}")

                    # Wait for ALB to finish updating before adding next member
                    logger.info(
                        f"Waiting for ALB {lb_id} to finish updating after member addition"
                    )
                    self.wait_for_alb_ready(lb_id, timeout=120)
                except Exception as e:
                    logger.error(f"Failed to add server {ip} to pool {pool_id}: {e}")
                    raise

        logger.info(f"Successfully added all members to ALB {lb_id}")

    def create_alb(self, lb_name, subnet_id, ports, server_ips=None):
        """
        Create Application Load Balancer with pools and listeners

        Generic method for creating ALBs with configurable ports.
        Used for both API/MCS (ports 6443, 22623) and Ingress (ports 80, 443).

        Args:
            lb_name (str): Load balancer name (e.g., "cluster-api-lb", "cluster-ingress-lb")
            subnet_id (str): VPC subnet ID for ALB
            ports (list): List of ports to create pools/listeners for
            server_ips (list): Optional list of server IPs for backend pool (not used in current impl)

        Returns:
            tuple: (alb_private_ip, alb_id)

        Raises:
            CommandFailed: If ALB creation fails
        """
        logger.info(f"Creating ALB: {lb_name} with ports {ports}")

        # Check for existing ALB with same name (use pagination)
        existing_lbs = self._paginated_list("/load_balancers", "load_balancers")
        for lb in existing_lbs:
            if lb["name"] == lb_name:
                raise CommandFailed(
                    f"ALB '{lb_name}' already exists (ID: {lb['id']}). "
                    f"Run destroy to clean up resources from previous deployment before retrying."
                )

        lb_data = {
            "name": lb_name,
            "is_public": False,
            "subnets": [{"id": subnet_id}],
            # Note: Application Load Balancers do not use a profile parameter
            # (unlike Network Load Balancers which require profile: network-fixed)
            "resource_group": {
                "id": self._get_resource_group_id_from_subnet(subnet_id)
            },
        }

        lb_result = self._api_call("POST", "/load_balancers", lb_data)
        lb_id = lb_result["id"]

        # Wait for ALB to be ready and get its private IP (10 min timeout for slow provisioning)
        lb_details = self.wait_for_alb_ready(lb_id, timeout=600)
        lb_ip = lb_details["private_ips"][0]["address"]

        # Create backend pools and listeners for each port
        for port in ports:
            pool_data = {
                "algorithm": "round_robin",
                "health_monitor": {
                    "delay": 5,
                    "max_retries": 2,
                    "timeout": 2,
                    "type": "tcp",
                    "port": port,
                },
                "protocol": "tcp",
                "name": f"{lb_name}-pool-{port}",
            }
            pool_result = self._api_call(
                "POST", f"/load_balancers/{lb_id}/pools", pool_data
            )
            pool_id = pool_result["id"]

            # Wait for pool to become active before creating listener
            self.wait_for_pool_ready(lb_id, pool_id)

            # NOTE: Members are NOT added here because servers are stopped at this point.
            # Members will be added later via add_alb_pool_members() after servers start.

            # Create listener
            listener_data = {
                "port": port,
                "protocol": "tcp",
                "default_pool": {"id": pool_id},
            }
            self._api_call("POST", f"/load_balancers/{lb_id}/listeners", listener_data)

            # Wait for ALB to finish updating before creating next pool
            # This prevents UPDATE_PENDING errors when creating subsequent pools
            logger.info(
                f"Waiting for ALB {lb_id} to finish updating after listener creation"
            )
            self.wait_for_alb_ready(lb_id, timeout=120)

        logger.info(
            f"ALB created: {lb_ip} (ID: {lb_id}) - members will be added after servers start"
        )
        return lb_ip, lb_id

    def create_alb_for_api(self, cluster_name, subnet_id, server_ips):
        """
        Create Application Load Balancer for OpenShift API/MCS

        Convenience wrapper around create_alb() for API load balancer.

        Args:
            cluster_name (str): Cluster name for naming/tagging
            subnet_id (str): VPC subnet ID for ALB
            server_ips (list): List of server IP addresses for backend pool

        Returns:
            tuple: (alb_private_ip, alb_id)
        """
        lb_name = f"{cluster_name}-api-lb"
        return self.create_alb(lb_name, subnet_id, [6443, 22623], server_ips)

    def create_alb_for_ingress(self, cluster_name, subnet_id, server_ips):
        """
        Create Application Load Balancer for OpenShift Ingress

        Convenience wrapper around create_alb() for Ingress load balancer.

        Args:
            cluster_name (str): Cluster name for naming/tagging
            subnet_id (str): VPC subnet ID for ALB
            server_ips (list): List of server IP addresses for backend pool

        Returns:
            tuple: (alb_private_ip, alb_id)
        """
        lb_name = f"{cluster_name}-ingress-lb"
        return self.create_alb(lb_name, subnet_id, [80, 443], server_ips)

    def wait_for_alb_ready(self, lb_id, timeout=300):
        """
        Wait for ALB to reach active state and return its details

        Args:
            lb_id (str): Load balancer ID
            timeout (int): Timeout in seconds

        Returns:
            dict: ALB details including private IPs
        """
        logger.info(f"Waiting for ALB {lb_id} to become active")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                lb = self._api_call("GET", f"/load_balancers/{lb_id}")
            except requests.exceptions.HTTPError as e:
                if "404" in str(e):
                    raise CommandFailed(f"ALB {lb_id} not found")
                raise
            status = lb.get("provisioning_status")
            if status == "active":
                logger.info(f"ALB {lb_id} is active")
                return lb
            logger.debug(f"ALB status: {status}")
            time.sleep(10)
        raise CommandFailed(f"Timeout waiting for ALB {lb_id} to become active")

    def wait_for_pool_ready(self, lb_id, pool_id, timeout=120):
        """
        Wait for ALB pool to reach active provisioning status

        Args:
            lb_id (str): Load balancer ID
            pool_id (str): Pool ID
            timeout (int): Timeout in seconds (default 120)

        Raises:
            CommandFailed: If pool doesn't become active within timeout
        """
        logger.info(f"Waiting for pool {pool_id} to become active")
        start_time = time.time()
        while time.time() - start_time < timeout:
            pool = self._api_call("GET", f"/load_balancers/{lb_id}/pools/{pool_id}")
            status = pool.get("provisioning_status")
            if status == "active":
                logger.info(f"Pool {pool_id} is active")
                return
            logger.debug(f"Pool status: {status}")
            time.sleep(5)
        raise CommandFailed(f"Timeout waiting for pool {pool_id} to become active")

    def get_alb_id_by_name(self, lb_name):
        """
        Find ALB ID by name

        Args:
            lb_name (str): Load balancer name

        Returns:
            str or None: ALB ID if found, None otherwise
        """
        logger.info(f"Looking up ALB by name: {lb_name}")
        existing_lbs = self._paginated_list("/load_balancers", "load_balancers")
        for lb in existing_lbs:
            if lb["name"] == lb_name:
                logger.info(f"Found ALB {lb_name} with ID: {lb['id']}")
                return lb["id"]
        logger.warning(f"ALB {lb_name} not found")
        return None

    def delete_alb(self, lb_id, verify_ownership=True):
        """
        Delete Application Load Balancer

        Args:
            lb_id (str): Load balancer ID
            verify_ownership (bool): Verify ALB belongs to expected cluster

        Raises:
            ValueError: If ownership verification fails
        """
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        if verify_ownership:
            # Get ALB details to verify ownership using direct requests.get
            url = f"{self.api_base}/load_balancers/{lb_id}?version={self.api_version}&generation={self.generation}"
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 404:
                logger.info(f"ALB {lb_id} not found (already deleted) - skipping")
                return
            response.raise_for_status()
            alb = response.json()

            # Verify cluster name is in ALB name (REQUIRED safety check)
            cluster_name = config.ENV_DATA.get("cluster_name")
            alb_name = alb.get("name", "")
            if cluster_name:
                if cluster_name not in alb_name:
                    raise ValueError(
                        f"ALB name '{alb_name}' doesn't contain cluster name '{cluster_name}'. "
                        f"Refusing to delete for safety (possible cross-cluster deletion)."
                    )
            else:
                logger.warning(
                    "No cluster_name configured - skipping name-based ownership check"
                )

            # Verify resource group if configured (additional safety check)
            expected_rg = config.ENV_DATA.get("resource_group_id")
            if expected_rg:
                actual_rg = alb.get("resource_group", {}).get("id")
                if actual_rg != expected_rg:
                    raise ValueError(
                        f"ALB {lb_id} belongs to resource group {actual_rg}, "
                        f"expected {expected_rg}. Refusing deletion for safety."
                    )

        logger.info(f"Deleting ALB: {lb_id}")
        try:
            # Use direct requests.delete for explicit auditability
            url = f"{self.api_base}/load_balancers/{lb_id}?version={self.api_version}&generation={self.generation}"
            response = requests.delete(url, headers=headers, timeout=30)
            response.raise_for_status()
            logger.info(f"ALB {lb_id} deletion initiated")

            # Wait for deletion to complete
            logger.info("Waiting for ALB deletion to complete...")
            time.sleep(30)  # Initial wait for deletion to start

            # Poll until ALB is gone (max 5 minutes)
            for _ in range(30):
                try:
                    url = (
                        f"{self.api_base}/load_balancers/{lb_id}"
                        f"?version={self.api_version}&generation={self.generation}"
                    )
                    response = requests.get(url, headers=headers, timeout=30)
                    response.raise_for_status()
                    time.sleep(10)
                except requests.exceptions.HTTPError:
                    # ALB not found = deletion complete
                    logger.info(f"ALB {lb_id} deleted successfully")
                    return

            logger.warning(f"ALB {lb_id} deletion timed out (may still be deleting)")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Failed to delete ALB {lb_id}: {e}")
        except Exception as e:
            logger.warning(f"Failed to delete ALB {lb_id}: {e}")

    def _get_resource_group_id_from_subnet(self, subnet_id):
        """
        Get resource group ID from subnet details

        Args:
            subnet_id (str): VPC subnet ID

        Returns:
            str: Resource group ID
        """
        result = self._api_call("GET", f"/subnets/{subnet_id}")
        rg_id = result.get("resource_group", {}).get("id")
        if not rg_id:
            raise CommandFailed(f"No resource group found for subnet {subnet_id}")
        return rg_id
