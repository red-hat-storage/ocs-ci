"""
Base class for NFS tests that require out-of-cluster client connection.

This module provides a base test class with common functionality for NFS tests
that need to connect to an external NFS client VM for testing NFS exports.
"""

import ipaddress
import logging
import socket
import time

from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.exceptions import CommandFailed, ConfigurationError
from ocs_ci.utility import nfs_utils
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd

log = logging.getLogger(__name__)


class NFSClientTestBase(ManageTest):
    """
    Base class for NFS tests that need out-of-cluster client connection.

    This class provides common functionality for:
    - Establishing SSH connection to NFS client VM
    - Handling connection failures with automatic VM reboot
    - Mounting NFS exports with retry logic
    - Hostname resolution and /etc/hosts management

    Test classes should inherit from this base class and will have access to:
    - self.con: Property that returns an active connection to the NFS client VM
    - self.get_nfs_client_connection(): Method to create a new connection
    - self._mount_nfs_with_retry(): Method to mount NFS with retry logic
    """

    # If the connection to the NFS Client VM fails, it's possible that
    # the NFS Client VM might not be healthy, so rebooting it and re-trying
    @property
    @retry((TimeoutError, socket.gaierror), tries=3, delay=60, backoff=1)
    def con(self):
        """
        Create connection to NFS Client VM, if not accessible, try to restart it.

        This property provides a cached connection to the NFS client VM. If the
        connection fails, it will attempt to reboot the VM using OpenStack CLI
        (if configured) and retry the connection.

        Returns:
            Connection: Active SSH connection to the NFS client VM

        Raises:
            ConfigurationError: If VM is not accessible and reboot config is missing
            TimeoutError: If connection cannot be established after retries
            socket.gaierror: If hostname resolution fails
        """
        if (
            not hasattr(self, "__nfs_client_connection")
            or not self.__nfs_client_connection
        ):
            try:
                self.__nfs_client_connection = self.get_nfs_client_connection(
                    re_try=False
                )
            except (TimeoutError, socket.gaierror):
                nfs_client_vm_cloud = config.ENV_DATA.get("nfs_client_vm_cloud")
                nfs_client_vm_name = config.ENV_DATA.get("nfs_client_vm_name")
                if not nfs_client_vm_cloud or not nfs_client_vm_name:
                    raise ConfigurationError(
                        "NFS Client VM is not accessible and ENV_DATA nfs_client_vm_cloud and/or nfs_client_vm_name "
                        "parameters are not configured to be able to automatically reboot the NFS Client VM."
                    )
                cmd = f"openstack --os-cloud {nfs_client_vm_cloud} server reboot --hard --wait {nfs_client_vm_name}"
                exec_cmd(cmd)

                time.sleep(60)
                self.__nfs_client_connection = self.get_nfs_client_connection()
        return self.__nfs_client_connection

    def get_nfs_client_connection(self, re_try=True):
        """
        Create connection to NFS Client VM.

        After establishing the SSH connection, if the NFS LB endpoint is a
        hostname (not a raw IP), the hostname is resolved from within the
        cluster and /etc/hosts on the client VM is updated. This is required
        when the NFS client VM is in a different VPC from the OpenShift cluster
        and cannot resolve IBM Cloud VPC LB hostnames via its DNS servers.

        If hostname resolution from the cluster fails (timeout), the code will
        proceed without updating /etc/hosts, assuming the NFS client VM can
        resolve the hostname via its own DNS configuration.

        Args:
            re_try (bool): Whether to retry connection on failure (default: True)

        Returns:
            Connection: SSH connection object to the NFS client VM

        Raises:
            TimeoutError: If connection cannot be established
            socket.gaierror: If hostname resolution fails
        """
        log.info("Connecting to nfs client test VM")
        tries = 3 if re_try else 1

        @retry((TimeoutError, socket.gaierror), tries=tries, delay=60, backoff=1)
        def __make_connection():
            return Connection(
                self.nfs_client_ip,
                self.nfs_client_user,
                private_key=self.nfs_client_private_key,
            )

        con = __make_connection()
        hostname_add = getattr(self, "hostname_add", None)
        if hostname_add:
            try:
                resolved_ip = nfs_utils.resolve_hostname_from_cluster(hostname_add)
                if resolved_ip:
                    log.info(
                        f"Resolved NFS hostname {hostname_add} to {resolved_ip} from cluster"
                    )
                    con.exec_cmd(
                        f"grep -q ' {hostname_add}$' /etc/hosts && "
                        f"sed -i.bak '/ {hostname_add}$/d' /etc/hosts || true"
                    )
                    con.exec_cmd(f"echo '{resolved_ip} {hostname_add}' >> /etc/hosts")
                    log.info(
                        f"Added '{resolved_ip} {hostname_add}' to /etc/hosts on NFS client VM"
                    )
            except TimeoutError:
                log.warning(
                    f"Timed out resolving hostname {hostname_add} from cluster; "
                    "continuing without /etc/hosts update on NFS client VM"
                )
        return con

    def _mount_nfs_with_retry(self, mount_dir, export_path, options="", retries=3):
        """
        Mount NFS export with retry logic and cleanup.

        Args:
            mount_dir (str): Local mount point directory
            export_path (str): NFS export path in format server:/path
            options (str): Additional mount options
            retries (int): Number of retry attempts

        Raises:
            CommandFailed: If mount fails after all retries
        """
        mount_cmd = f"mount -t nfs {options} {export_path} {mount_dir}".strip()

        for attempt in range(retries):
            try:
                self.con.exec_cmd(mount_cmd)
                log.info(f"Successfully mounted NFS export on attempt {attempt + 1}")
                return
            except CommandFailed as ex:
                if attempt == retries - 1:
                    raise

                log.warning(
                    f"Mount attempt {attempt + 1} failed: {ex}. "
                    f"Retrying in 10 seconds..."
                )

                try:
                    self.con.exec_cmd(f"umount -f {mount_dir}", timeout=30)
                except CommandFailed:
                    pass

                time.sleep(10)

    def _is_ip_address(self, value):
        """
        Check if the provided value is a valid IP address.

        Args:
            value (str): Value to check

        Returns:
            bool: True if value is a valid IP address, False otherwise
        """
        try:
            ipaddress.ip_address(value)
            return True
        except ValueError:
            return False


# Made with Bob
