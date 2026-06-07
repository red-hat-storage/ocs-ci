import json
import logging
from pathlib import Path
from paramiko import SSHClient, AutoAddPolicy

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import genereate_cred_file_rack
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


class IBMHCI(object):
    """
    Wrapper for IBM HCI Baremetal operations including power management
    """

    def __init__(self):
        """
        Initialize the variables required and load rack details
        """
        from ocs_ci.framework import config

        # SSH credentials for rack access
        self.rack_ssh_username = config.AUTH["ibm_hci"]["rack_ssh_username"]
        self.rack_ssh_password = config.AUTH["ibm_hci"]["rack_ssh_password"]

        # Get cluster name and construct file path
        cluster_name = config.ENV_DATA.get("cluster_name")
        self.rack_file_path = Path(f"{constants.IBM_HCI_RACK_DIR}/{cluster_name}.json")

        # Generate rack details if file doesn't exist
        # Note: genereate_cred_file_rack() will create the directory if needed
        if not self.rack_file_path.exists():
            log.info(
                f"Rack details file not found at {self.rack_file_path}, generating..."
            )
            genereate_cred_file_rack()

        # Load rack details
        self.rack_details = self._load_rack_details()

    def _load_rack_details(self):
        """
        Load rack details from cluster-specific JSON file

        Returns:
            dict: Rack details dictionary
        """
        try:
            with open(self.rack_file_path, "r") as f:
                rack_data = json.load(f)
                log.info(f"Successfully loaded rack details from {self.rack_file_path}")
                return rack_data
        except FileNotFoundError:
            log.error(f"Rack details file not found at {self.rack_file_path}")
            return {}
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse rack details JSON: {e}")
            return {}
        except Exception as e:
            log.error(f"Failed to load rack details: {e}")
            return {}

    def _get_node_details_by_name(self, node_name):
        """
        Get node details from rack_details based on node name

        Args:
            node_name (str): Name of the node

        Returns:
            tuple: (rack_serial, node_data, rack_ip) or (None, None, None) if not found
        """
        # Get node labels to find rack serial
        node_obj = OCP(kind=constants.NODE, resource_name=node_name)
        node_data = node_obj.get()

        if not node_data:
            log.error(f"Node {node_name} not found in cluster")
            return None, None, None

        # Handle case where get() returns a list
        if isinstance(node_data, list):
            if len(node_data) == 0:
                log.error(f"Node {node_name} not found in cluster")
                return None, None, None
            node_data = node_data[0]

        # Get rack label from node
        labels = node_data.get("metadata", {}).get("labels", {})
        rack_label = labels.get("isf.ibm.com/rack")

        if not rack_label:
            log.error(f"Node {node_name} does not have rack label")
            return None, None, None

        # Find node in rack details
        rack_serial = rack_label.lower()
        if rack_serial not in self.rack_details:
            log.error(f"Rack {rack_serial} not found in rack details")
            return None, None, None

        rack_info = self.rack_details[rack_serial]
        rack_ip = rack_info.get("rackInfo", {}).get("rackIP")

        # Find the specific node by matching node name with OCPRole
        # Node names format: "control-1-ru2.f51l039.abc.xyz.pqr.com"
        # OCPRole format: "control-1-ru2"
        node_short_name = node_name.split(".")[0]

        # Use exact match to avoid false positives (e.g., "worker-10" matching "worker-1")
        for ocp_role, node_info in rack_info.get("nodes", {}).items():
            if node_short_name == ocp_role:
                return rack_serial, node_info, rack_ip

        log.error(
            f"Node {node_name} (short name: {node_short_name}) not found in rack {rack_serial} details. "
            f"Available nodes: {list(rack_info.get('nodes', {}).keys())}"
        )
        return None, None, None

    def _execute_ssh_command(self, rack_ip, username, password, command):
        """
        Execute command on rack via SSH

        Args:
            rack_ip (str): IP address of the rack
            username (str): SSH username
            password (str): SSH password
            command (str): Command to execute

        Returns:
            tuple: (stdout, stderr, exit_code)
        """
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())

        try:
            ssh.connect(rack_ip, username=username, password=password, timeout=30)
            stdin, stdout, stderr = ssh.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            stdout_str = stdout.read().decode("utf-8")
            stderr_str = stderr.read().decode("utf-8")

            return stdout_str, stderr_str, exit_code
        except Exception as e:
            log.error(f"SSH command failed: {e}")
            raise
        finally:
            ssh.close()

    def _ensure_ipmitool_installed(self, rack_ip):
        """
        Ensure ipmitool is installed on the rack

        Args:
            rack_ip (str): IP address of the rack

        Returns:
            bool: True if ipmitool is available, False otherwise
        """

        # Check if ipmitool is installed
        check_cmd = "which ipmitool"
        log.info(f"Checking if ipmitool is installed on rack {rack_ip}")

        try:
            stdout, stderr, exit_code = self._execute_ssh_command(
                rack_ip, self.rack_ssh_username, self.rack_ssh_password, check_cmd
            )

            if exit_code == 0:
                log.info(f"ipmitool is already installed on rack {rack_ip}")
                return True

            # ipmitool not found, try to install it
            log.info(f"ipmitool not found on rack {rack_ip}, attempting to install...")

            # Try yum first (RHEL/CentOS)
            install_cmd = "yum install -y ipmitool"
            log.info(f"Installing ipmitool: {install_cmd}")

            stdout, stderr, exit_code = self._execute_ssh_command(
                rack_ip, self.rack_ssh_username, self.rack_ssh_password, install_cmd
            )

            if exit_code == 0:
                log.info(f"Successfully installed ipmitool on rack {rack_ip}")
                return True

            # If yum failed, try dnf (newer RHEL/Fedora)
            install_cmd = "dnf install -y ipmitool"
            log.info(f"Trying dnf: {install_cmd}")

            stdout, stderr, exit_code = self._execute_ssh_command(
                rack_ip, self.rack_ssh_username, self.rack_ssh_password, install_cmd
            )

            if exit_code == 0:
                log.info(f"Successfully installed ipmitool on rack {rack_ip}")
                return True

            log.error(f"Failed to install ipmitool on rack {rack_ip}")
            return False

        except Exception as e:
            log.error(f"Error checking/installing ipmitool: {e}")
            return False

    def _format_ip_for_command(self, ip_address):
        """
        Format IP address for use in commands. IPv6 addresses need to be wrapped in brackets.

        Args:
            ip_address (str): IP address (IPv4 or IPv6)

        Returns:
            str: Formatted IP address
        """
        # Check if it's an IPv6 address (contains colons)
        if ":" in ip_address:
            # Wrap IPv6 in brackets if not already wrapped
            if not ip_address.startswith("["):
                return f"[{ip_address}]"
        return ip_address

    def _power_operation_ipmi_ssh(
        self, rack_ip, node_username, node_password, node_ip, operation, force=False
    ):
        """
        Execute power operation using ipmitool via SSH to rack

        Args:
            rack_ip (str): IP address of the rack
            node_username (str): IPMI username for the node
            node_password (str): IPMI password for the node
            node_ip (str): IP address of the node BMC (IPv4 or IPv6)
            operation (str): Power operation (on, off, cycle, reset, status)
            force (bool): Force operation (for off/reset)

        Returns:
            str: Power state ("on", "off") if operation is "status"
            bool: True if successful, False otherwise for other operations
        """
        # Ensure ipmitool is installed
        if not self._ensure_ipmitool_installed(rack_ip):
            log.error(f"Cannot proceed without ipmitool on rack {rack_ip}")
            return False

        # Format IP address (wrap IPv6 in brackets)
        formatted_ip = self._format_ip_for_command(node_ip)

        # Map operations to IPMI commands
        ipmi_ops = {
            "on": "power on",
            "off": "power soft" if not force else "power off",
            "cycle": "power cycle",
            "reset": "power reset",
            "status": "power status",
        }

        if operation not in ipmi_ops:
            log.error(f"Invalid operation: {operation}")
            return False

        ipmi_cmd = ipmi_ops[operation]
        command = f"ipmitool -I lanplus -H {formatted_ip} -U {node_username} -P {node_password} {ipmi_cmd}"

        log.info(f"Executing IPMI command via SSH for {operation} on node {node_ip}")
        log.info(
            f"IPMI Command: ipmitool -I lanplus -H {formatted_ip} -U <REDACTED> -P <REDACTED> {ipmi_cmd}"
        )
        try:
            stdout, stderr, exit_code = self._execute_ssh_command(
                rack_ip, self.rack_ssh_username, self.rack_ssh_password, command
            )

            if exit_code == 0:
                log.info(f"IPMI {operation} successful: {stdout}")
                # For status operation, parse and return the actual state
                if operation == "status":
                    # IPMI output format: "Chassis Power is on" or "Chassis Power is off"
                    stdout_lower = stdout.lower()
                    if "on" in stdout_lower:
                        return "on"
                    elif "off" in stdout_lower:
                        return "off"
                    else:
                        log.warning(f"Could not parse power state from: {stdout}")
                        return None
                return True
            else:
                log.error(
                    f"IPMI {operation} failed with exit code {exit_code}: {stderr}"
                )
                return False if operation != "status" else None
        except Exception as e:
            log.error(f"IPMI operation via SSH failed: {e}")
            return False if operation != "status" else None

    def _power_operation_ipmi(
        self, rack_ip, node_username, node_password, node_ip, operation, force=False
    ):
        """
        Execute power operation using IPMI (for Lenovo nodes only)

        Args:
            rack_ip (str): IP address of the rack
            node_username (str): IPMI username for the node
            node_password (str): IPMI password for the node
            node_ip (str): IP address of the node BMC
            operation (str): Power operation (on, off, cycle, reset, status)
            force (bool): Force operation (for off/reset)

        Returns:
            str: Power state ("on", "off") if operation is "status"
            bool: True if successful, False otherwise for other operations
        """
        log.info("Using IPMI protocol for Lenovo node")
        return self._power_operation_ipmi_ssh(
            rack_ip, node_username, node_password, node_ip, operation, force
        )

    def _power_operation_redfish(
        self, rack_ip, node_username, node_password, node_ip, operation, force=False
    ):
        """
        Execute power operation using Redfish (for Dell nodes)

        Args:
            rack_ip (str): IP address of the rack
            node_username (str): Redfish username for the node
            node_password (str): Redfish password for the node
            node_ip (str): IP address of the node BMC (IPv4 or IPv6)
            operation (str): Power operation (on, off, cycle, reset, status)
            force (bool): Force operation (for off/reset)

        Returns:
            str: Power state ("on", "off") if operation is "status"
            bool: True if successful, False otherwise for other operations
        """
        # Format IP address (wrap IPv6 in brackets for URL)
        formatted_ip = self._format_ip_for_command(node_ip)

        # Map operations to Redfish reset types
        redfish_ops = {
            "on": "On",
            "off": "GracefulShutdown" if not force else "ForceOff",
            "cycle": "PowerCycle",
            "reset": "GracefulRestart" if not force else "ForceRestart",
            "status": "status",
        }

        if operation not in redfish_ops:
            log.error(f"Invalid operation: {operation}")
            return False

        # First, discover the Systems URI
        log.info(f"Discovering Redfish Systems URI for node {node_ip}")
        discover_cmd = f"curl -k -sS -f -u {node_username}:{node_password} https://{formatted_ip}/redfish/v1/Systems"
        log.info(
            f"Redfish Discovery Command: curl -k -sS -f -u <REDACTED>:<REDACTED> "
            f"https://{formatted_ip}/redfish/v1/Systems"
        )

        try:
            stdout, stderr, exit_code = self._execute_ssh_command(
                rack_ip, self.rack_ssh_username, self.rack_ssh_password, discover_cmd
            )

            if exit_code != 0:
                log.error(f"Failed to discover Systems URI: {stderr}")
                return False

            # Parse the response to get the first system member URI
            import json

            try:
                systems_data = json.loads(stdout)
                members = systems_data.get("Members", [])
                if not members:
                    log.error("No system members found in Redfish response")
                    return False

                # Get the first system's @odata.id
                system_uri = members[0].get("@odata.id", "")
                if not system_uri:
                    log.error("No @odata.id found in system member")
                    return False

                log.info(f"Found system URI: {system_uri}")

            except (json.JSONDecodeError, KeyError, IndexError) as e:
                log.error(f"Failed to parse Systems response: {e}")
                log.debug(f"Response was: {stdout}")
                return False

            # Now execute the actual operation
            if operation == "status":
                # Get power status
                command = (
                    f"curl -k -sS -f -u {node_username}:{node_password} "
                    f"https://{formatted_ip}{system_uri} | grep -i powerstate"
                )
                redacted_command = (
                    f"curl -k -sS -f -u <REDACTED>:<REDACTED> "
                    f"https://{formatted_ip}{system_uri} | grep -i powerstate"
                )
            else:
                reset_type = redfish_ops[operation]
                command = (
                    f"curl -k -sS -f -u {node_username}:{node_password} -X POST "
                    f"https://{formatted_ip}{system_uri}/Actions/ComputerSystem.Reset "
                    f"-H 'Content-Type: application/json' "
                    f'-d \'{{"ResetType": "{reset_type}"}}\''
                )
                redacted_command = (
                    f"curl -k -sS -f -u <REDACTED>:<REDACTED> -X POST "
                    f"https://{formatted_ip}{system_uri}/Actions/ComputerSystem.Reset "
                    f"-H 'Content-Type: application/json' "
                    f'-d \'{{"ResetType": "{reset_type}"}}\''
                )

            log.info(f"Executing Redfish command for {operation} on node {node_ip}")
            log.info(f"Redfish Command: {redacted_command}")

            stdout, stderr, exit_code = self._execute_ssh_command(
                rack_ip, self.rack_ssh_username, self.rack_ssh_password, command
            )

            # Rely on exit_code for success determination (curl -f fails on HTTP errors)
            if exit_code == 0:
                log.info(f"Redfish {operation} successful: {stdout}")
                # For status operation, parse and return the actual state
                if operation == "status":
                    # Redfish output format: "PowerState": "On" or "PowerState": "Off"
                    stdout_lower = stdout.lower()
                    if '"on"' in stdout_lower or "poweron" in stdout_lower:
                        return "on"
                    elif '"off"' in stdout_lower or "poweroff" in stdout_lower:
                        return "off"
                    else:
                        log.warning(f"Could not parse power state from: {stdout}")
                        return None
                return True
            else:
                log.error(
                    f"Redfish {operation} failed (exit code: {exit_code}): {stderr}"
                )
                return False if operation != "status" else None
        except Exception as e:
            log.error(f"Redfish operation failed: {e}")
            return False if operation != "status" else None

    def _detach_baremetalhost(self, node_name):
        """
        Detach BareMetalHost from management to prevent auto-recovery
        Uses the baremetalhost.metal3.io/detached annotation

        Args:
            node_name (str): Name of the node

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            log.info(f"Detaching BareMetalHost for node {node_name}")

            # Extract short node name (without domain)
            node_short_name = node_name.split(".")[0]

            # Check if BareMetalHost exists
            bmh_obj = OCP(
                kind="BareMetalHost",
                namespace="openshift-machine-api",
                resource_name=node_short_name,
            )

            if not bmh_obj.is_exist(resource_name=node_short_name):
                log.warning(f"BareMetalHost {node_short_name} not found, skipping")
                return True

            # Add detached annotation to prevent auto-recovery
            annotate_cmd = (
                f"annotate baremetalhost {node_short_name} "
                f"-n openshift-machine-api "
                f"baremetalhost.metal3.io/detached='' --overwrite"
            )

            bmh_obj.exec_oc_cmd(annotate_cmd)
            log.info(f"Successfully detached BareMetalHost {node_short_name}")
            return True

        except Exception as e:
            log.error(f"Failed to detach BareMetalHost for {node_name}: {e}")
            return False

    def _attach_baremetalhost(self, node_name):
        """
        Re-attach BareMetalHost to management after power on
        Removes the baremetalhost.metal3.io/detached annotation

        Args:
            node_name (str): Name of the node

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            log.info(f"Re-attaching BareMetalHost for node {node_name}")

            # Extract short node name (without domain)
            node_short_name = node_name.split(".")[0]

            # Check if BareMetalHost exists
            bmh_obj = OCP(
                kind="BareMetalHost",
                namespace="openshift-machine-api",
                resource_name=node_short_name,
            )

            if not bmh_obj.is_exist(resource_name=node_short_name):
                log.warning(f"BareMetalHost {node_short_name} not found, skipping")
                return True

            # Remove detached annotation to restore management
            annotate_cmd = (
                f"annotate baremetalhost {node_short_name} "
                f"-n openshift-machine-api "
                f"baremetalhost.metal3.io/detached- --overwrite"
            )

            bmh_obj.exec_oc_cmd(annotate_cmd)
            log.info(f"Successfully re-attached BareMetalHost {node_short_name}")
            return True

        except Exception as e:
            log.error(f"Failed to re-attach BareMetalHost for {node_name}: {e}")
            return False

    def power_operation(self, node_name, operation, force=False, wait=False):
        """
        Perform power operation on a node

        Args:
            node_name (str): Name of the node
            operation (str): Power operation (on, off, cycle, reset, status)
            force (bool): Force operation (for off/reset operations)
            wait (bool): Wait for operation to complete

        Returns:
            str: Power state ("on", "off") if operation is "status"
            bool: True if successful, False otherwise for other operations
        """
        log.info(
            f"Performing {operation} operation on node {node_name} (force={force}, wait={wait})"
        )

        # Track if we detached the BareMetalHost so we can re-attach in finally block
        detached_bmh = False
        result = False if operation != "status" else None

        try:
            # Get node details first to validate before detaching
            rack_serial, node_info, rack_ip = self._get_node_details_by_name(node_name)

            if not node_info or not rack_ip:
                log.error(f"Failed to get node details for {node_name}")
                return False

            ipv6 = node_info.get("ipv6")
            ipv4 = node_info.get("ipv4")
            manufacturer = node_info.get("manufacturer", "").lower()
            username = node_info.get("username")
            password = node_info.get("password")

            if not all([username, password]):
                log.error(f"Missing required credentials for node {node_name}")
                return False

            if not ipv6 and not ipv4:
                log.error(
                    f"No IP address (IPv6 or IPv4) available for node {node_name}"
                )
                return False

            # Detach BareMetalHost AFTER validation, before power off/cycle/reset
            if operation in ["off", "cycle", "reset"]:
                log.info("Detaching BareMetalHost to prevent auto-recovery")
                self._detach_baremetalhost(node_name)
                detached_bmh = True

            # Try IPv6 first, then IPv4 as fallback
            ip_addresses = []
            if ipv6:
                ip_addresses.append(("IPv6", ipv6))
            if ipv4:
                ip_addresses.append(("IPv4", ipv4))

            last_error = None
            for ip_type, node_ip in ip_addresses:
                try:
                    log.info(
                        f"Attempting {operation} using {ip_type} address: {node_ip}"
                    )

                    # Determine which protocol to use based on manufacturer
                    if "lenovo" in manufacturer:
                        log.info(f"Using IPMI for Lenovo node {node_name}")
                        result = self._power_operation_ipmi(
                            rack_ip, username, password, node_ip, operation, force
                        )
                    elif "dell" in manufacturer:
                        log.info(f"Using Redfish for Dell node {node_name}")
                        result = self._power_operation_redfish(
                            rack_ip, username, password, node_ip, operation, force
                        )
                    else:
                        log.error(f"Unsupported manufacturer: {manufacturer}")
                        return False

                    # If operation succeeded, break out of retry loop
                    if result or (operation == "status" and result is not None):
                        log.info(f"Operation {operation} succeeded using {ip_type}")
                        break
                    else:
                        log.warning(
                            f"Operation {operation} failed with {ip_type}, will try next IP if available"
                        )
                        last_error = f"Operation failed with {ip_type}"

                except Exception as e:
                    log.warning(f"Exception during {operation} with {ip_type}: {e}")
                    last_error = str(e)
                    # Continue to next IP address
                    continue
            else:
                # If we exhausted all IP addresses without success
                error_msg = (
                    f"Failed to perform {operation} on node {node_name} "
                    f"with all available IP addresses. Last error: {last_error}"
                )
                log.error(error_msg)
                raise RuntimeError(error_msg)

            if wait and result and operation in ["off", "on", "cycle", "reset"]:
                import time

                log.info(f"Waiting for {operation} operation to complete...")
                time.sleep(30)  # Wait for operation to take effect

            return result

        finally:
            # Always re-attach BareMetalHost if we detached it, regardless of success/failure
            # This prevents leaving hosts in detached state
            if detached_bmh:
                log.info(
                    "Re-attaching BareMetalHost to restore normal management "
                    f"(operation: {operation}, result: {result})"
                )
                self._attach_baremetalhost(node_name)

    def power_on(self, node_name, wait=False):
        """
        Power on a node

        Args:
            node_name (str): Name of the node
            wait (bool): Wait for operation to complete
        """
        return self.power_operation(node_name, "on", wait=wait)

    def power_off(self, node_name, force=False, wait=False):
        """
        Power off a node

        Args:
            node_name (str): Name of the node
            force (bool): Force power off
            wait (bool): Wait for operation to complete
        """
        return self.power_operation(node_name, "off", force=force, wait=wait)

    def power_cycle(self, node_name, wait=False):
        """
        Power cycle a node

        Args:
            node_name (str): Name of the node
            wait (bool): Wait for operation to complete
        """
        return self.power_operation(node_name, "cycle", wait=wait)

    def power_reset(self, node_name, force=False, wait=False):
        """
        Power reset a node

        Args:
            node_name (str): Name of the node
            force (bool): Force reset
            wait (bool): Wait for operation to complete
        """
        return self.power_operation(node_name, "reset", force=force, wait=wait)

    def power_status(self, node_name):
        """
        Get power status of a node

        Args:
            node_name (str): Name of the node

        Returns:
            str: Power state ("on", "off") or None if failed
        """
        return self.power_operation(node_name, "status")

    def power_status_direct(self, node_name):
        """
        Get power status of a node directly using rack details without querying Kubernetes API

        This method is useful when the cluster API is unavailable.

        Args:
            node_name (str): Full node name (e.g., "control-1-ru2.f51l039.fusion.tadn.ibm.com")

        Returns:
            str: Power status ("on", "off", etc.) or None if failed
        """
        # Parse node name to extract rack serial and node role
        parts = node_name.split(".")
        if len(parts) < 2:
            log.error(f"Invalid node name format: {node_name}")
            return None

        node_role = parts[0]
        rack_serial = parts[1]

        # Get rack data
        if rack_serial not in self.rack_details:
            log.error(f"Rack {rack_serial} not found in rack details")
            return None

        rack_data = self.rack_details[rack_serial]
        nodes_dict = rack_data.get("nodes", {})

        if node_role not in nodes_dict:
            log.error(f"Node {node_role} not found in rack {rack_serial}")
            return None

        node_info = nodes_dict[node_role]

        # Get required information
        ipv6 = node_info.get("ipv6")
        ipv4 = node_info.get("ipv4")
        manufacturer = node_info.get("manufacturer", "").lower()
        rack_info = rack_data.get("rackInfo", {})
        rack_ip = rack_info.get("rackIP")

        if not rack_ip:
            log.error(f"Missing rack IP for node {node_name}")
            return None

        if not ipv6 and not ipv4:
            log.error(f"No IP address (IPv6 or IPv4) available for node {node_name}")
            return None

        # Get credentials
        node_username = node_info.get("username", "USERID")
        node_password = node_info.get("password", "PASSW0RD")

        log.info(f"Checking power status of node {node_name} directly via IPMI/Redfish")

        # Try IPv6 first, then IPv4 as fallback
        ip_addresses = []
        if ipv6:
            ip_addresses.append(("IPv6", ipv6))
        if ipv4:
            ip_addresses.append(("IPv4", ipv4))

        last_error = None
        for ip_type, node_ip in ip_addresses:
            try:
                log.info(
                    f"Attempting power status check using {ip_type} address: {node_ip}"
                )

                # Execute power status based on manufacturer
                if "lenovo" in manufacturer:
                    result = self._power_operation_ipmi(
                        rack_ip,
                        node_username,
                        node_password,
                        node_ip,
                        "status",
                        force=False,
                    )
                elif "dell" in manufacturer:
                    result = self._power_operation_redfish(
                        rack_ip,
                        node_username,
                        node_password,
                        node_ip,
                        "status",
                        force=False,
                    )
                else:
                    log.error(f"Unsupported manufacturer: {manufacturer}")
                    return None

                # If we got a valid result, return it
                if result is not None:
                    log.info(f"Power status check succeeded using {ip_type}")
                    return result
                else:
                    log.warning(
                        f"Power status check failed with {ip_type}, will try next IP if available"
                    )
                    last_error = f"Status check failed with {ip_type}"

            except Exception as e:
                log.warning(f"Exception during power status check with {ip_type}: {e}")
                last_error = str(e)
                continue

        # If we exhausted all IP addresses without success
        error_msg = (
            f"Failed to get power status for node {node_name} "
            f"with all available IP addresses. Last error: {last_error}"
        )
        log.error(error_msg)
        raise RuntimeError(error_msg)

    def power_on_direct(self, node_name):
        """
        Power on a node directly using rack details without querying Kubernetes API

        This method is useful when the cluster API is unavailable.
        It extracts node information directly from rack_details.

        Args:
            node_name (str): Full node name (e.g., "control-1-ru2.f51l039.fusion.tadn.ibm.com")

        Returns:
            bool: True if successful, False otherwise
        """
        # Parse node name to extract rack serial and node role
        # Format: <role>.<rack_serial>.fusion.tadn.ibm.com
        parts = node_name.split(".")
        if len(parts) < 2:
            log.error(f"Invalid node name format: {node_name}")
            return False

        node_role = parts[0]  # e.g., "control-1-ru2"
        rack_serial = parts[1]  # e.g., "f51l039"

        # Get rack data
        if rack_serial not in self.rack_details:
            log.error(f"Rack {rack_serial} not found in rack details")
            return False

        rack_data = self.rack_details[rack_serial]
        nodes_dict = rack_data.get("nodes", {})

        if node_role not in nodes_dict:
            log.error(f"Node {node_role} not found in rack {rack_serial}")
            return False

        node_info = nodes_dict[node_role]

        # Get required information
        ipv6 = node_info.get("ipv6")
        ipv4 = node_info.get("ipv4")
        manufacturer = node_info.get("manufacturer", "").lower()
        rack_info = rack_data.get("rackInfo", {})
        rack_ip = rack_info.get("rackIP")

        if not rack_ip:
            log.error(f"Missing rack IP for node {node_name}")
            return False

        if not ipv6 and not ipv4:
            log.error(f"No IP address (IPv6 or IPv4) available for node {node_name}")
            return False

        # Get credentials from node_info
        node_username = node_info.get("username", "USERID")
        node_password = node_info.get("password", "PASSW0RD")

        log.info(f"Powering on node {node_name} directly via IPMI/Redfish")
        log.info(f"Rack IP: {rack_ip}, Manufacturer: {manufacturer}")

        # Try IPv6 first, then IPv4 as fallback
        ip_addresses = []
        if ipv6:
            ip_addresses.append(("IPv6", ipv6))
        if ipv4:
            ip_addresses.append(("IPv4", ipv4))

        last_error = None
        for ip_type, node_ip in ip_addresses:
            try:
                log.info(f"Attempting power on using {ip_type} address: {node_ip}")

                # Execute power on based on manufacturer
                if "lenovo" in manufacturer:
                    result = self._power_operation_ipmi(
                        rack_ip,
                        node_username,
                        node_password,
                        node_ip,
                        "on",
                        force=False,
                    )
                elif "dell" in manufacturer:
                    result = self._power_operation_redfish(
                        rack_ip,
                        node_username,
                        node_password,
                        node_ip,
                        "on",
                        force=False,
                    )
                else:
                    log.error(f"Unsupported manufacturer: {manufacturer}")
                    return False

                # If operation succeeded, return True
                if result:
                    log.info(f"Power on succeeded using {ip_type}")
                    return True
                else:
                    log.warning(
                        f"Power on failed with {ip_type}, will try next IP if available"
                    )
                    last_error = f"Power on failed with {ip_type}"

            except Exception as e:
                log.warning(f"Exception during power on with {ip_type}: {e}")
                last_error = str(e)
                continue

        # If we exhausted all IP addresses without success
        error_msg = (
            f"Failed to power on node {node_name} "
            f"with all available IP addresses. Last error: {last_error}"
        )
        log.error(error_msg)
        raise RuntimeError(error_msg)
