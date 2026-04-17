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
        self.rack_ssh_username = (config.AUTH["ibm_hci"]["rack_ssh_username"],)
        self.rack_ssh_password = (config.AUTH["ibm_hci"]["rack_ssh_password"],)

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
        node_short_name = node_name.split(".")[0]
        for ocp_role, node_info in rack_info.get("nodes", {}).items():
            if node_short_name.startswith(ocp_role):
                return rack_serial, node_info, rack_ip

        log.error(f"Node {node_name} not found in rack {rack_serial} details")
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

    def _power_operation_ipmi_ssh(
        self, rack_ip, node_username, node_password, node_ip, operation, force=False
    ):
        """
        Execute power operation using ipmitool via SSH to rack

        Args:
            rack_ip (str): IP address of the rack
            node_username (str): IPMI username for the node
            node_password (str): IPMI password for the node
            node_ip (str): IP address of the node BMC
            operation (str): Power operation (on, off, cycle, reset, status)
            force (bool): Force operation (for off/reset)

        Returns:
            bool: True if successful, False otherwise
        """
        # Ensure ipmitool is installed
        if not self._ensure_ipmitool_installed(rack_ip):
            log.error(f"Cannot proceed without ipmitool on rack {rack_ip}")
            return False

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
        command = f"ipmitool -I lanplus -H {node_ip} -U {node_username} -P {node_password} {ipmi_cmd}"

        log.info(f"Executing IPMI command via SSH for {operation} on node {node_ip}")
        log.info(f"IPMI Command: {command}")
        try:
            stdout, stderr, exit_code = self._execute_ssh_command(
                rack_ip, self.rack_ssh_username, self.rack_ssh_password, command
            )

            if exit_code == 0:
                log.info(f"IPMI {operation} successful: {stdout}")
                return True
            else:
                log.error(
                    f"IPMI {operation} failed with exit code {exit_code}: {stderr}"
                )
                return False
        except Exception as e:
            log.error(f"IPMI operation via SSH failed: {e}")
            return False

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
            bool: True if successful, False otherwise
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
            node_ip (str): IP address of the node BMC
            operation (str): Power operation (on, off, cycle, reset, status)
            force (bool): Force operation (for off/reset)

        Returns:
            bool: True if successful, False otherwise
        """
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
        discover_cmd = f"curl -k -s -u {node_username}:{node_password} https://{node_ip}/redfish/v1/Systems"
        log.info(f"Redfish Discovery Command: {discover_cmd}")

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
                    f"curl -k -s -u {node_username}:{node_password} "
                    f"https://{node_ip}{system_uri} | grep -i powerstate"
                )
            else:
                reset_type = redfish_ops[operation]
                command = (
                    f"curl -k -s -u {node_username}:{node_password} -X POST "
                    f"https://{node_ip}{system_uri}/Actions/ComputerSystem.Reset "
                    f"-H 'Content-Type: application/json' "
                    f'-d \'{{"ResetType": "{reset_type}"}}\''
                )

            log.info(f"Executing Redfish command for {operation} on node {node_ip}")
            log.info(f"Redfish Command: {command}")

            stdout, stderr, exit_code = self._execute_ssh_command(
                rack_ip, self.rack_ssh_username, self.rack_ssh_password, command
            )

            if exit_code == 0 or "success" in stdout.lower():
                log.info(f"Redfish {operation} successful: {stdout}")
                return True
            else:
                log.error(f"Redfish {operation} failed: {stderr}")
                return False
        except Exception as e:
            log.error(f"Redfish operation failed: {e}")
            return False

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
            bool: True if successful, False otherwise
        """
        log.info(
            f"Performing {operation} operation on node {node_name} (force={force}, wait={wait})"
        )

        # Detach BareMetalHost before power off/cycle/reset to prevent auto-recovery
        if operation in ["off", "cycle", "reset"]:
            log.info("Detaching BareMetalHost to prevent auto-recovery")
            self._detach_baremetalhost(node_name)

        # Get node details
        rack_serial, node_info, rack_ip = self._get_node_details_by_name(node_name)

        if not node_info or not rack_ip:
            log.error(f"Failed to get node details for {node_name}")
            return False

        node_ip = node_info.get("ipv4")
        manufacturer = node_info.get("manufacturer", "").lower()
        username = node_info.get("username")
        password = node_info.get("password")

        if not all([node_ip, username, password]):
            log.error(f"Missing required credentials for node {node_name}")
            return False

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

        if wait and result and operation in ["off", "on", "cycle", "reset"]:
            import time

            log.info(f"Waiting for {operation} operation to complete...")
            time.sleep(30)  # Wait for operation to take effect

        # Re-attach BareMetalHost after power on to restore normal management
        if operation == "on" and result:
            log.info("Re-attaching BareMetalHost to restore normal management")
            self._attach_baremetalhost(node_name)

        return result

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
        """Get power status of a node"""
        return self.power_operation(node_name, "status")
