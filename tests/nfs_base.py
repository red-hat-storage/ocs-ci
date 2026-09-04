"""
Base class for NFS tests that require out-of-cluster client connection.

This module provides a base test class with common functionality for NFS tests
that need to connect to an external NFS client VM for testing NFS exports.
"""

import ipaddress
import logging
import os
import socket
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed, ConfigurationError
from ocs_ci.ocs.resources import ocs
from ocs_ci.utility import nfs_utils
from ocs_ci.utility import version as version_module
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

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

                # Wait until SSH port is reachable before attempting connection
                log.info(
                    f"Waiting for SSH to become available on {self.nfs_client_ip}..."
                )
                for _ in TimeoutSampler(
                    timeout=300,
                    sleep=10,
                    func=lambda: socket.create_connection(
                        (self.nfs_client_ip, 22), timeout=5
                    ),
                ):
                    log.info(f"SSH port is reachable on {self.nfs_client_ip}")
                    break

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

        If the endpoint is already a raw IP, the /etc/hosts update is skipped.
        If hostname resolution from the cluster times out, the code will proceed
        without updating /etc/hosts, assuming the NFS client VM can resolve the
        hostname via its own DNS configuration.

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
            is_ip = False
            try:
                ipaddress.ip_address(hostname_add)
                is_ip = True
            except ValueError:
                pass
            if not is_ip:
                log.info(
                    "NFS LB endpoint %s is a hostname, resolving and "
                    "updating /etc/hosts on NFS client VM",
                    hostname_add,
                )
                try:
                    nfs_utils.update_etc_hosts_on_nfs_client(con, hostname_add)
                except TimeoutError:
                    log.warning(
                        f"Timed out resolving hostname {hostname_add} from cluster; "
                        "continuing without /etc/hosts update on NFS client VM"
                    )
        return con

    def reconnect_if_needed(self):
        """
        Check if the cached NFS client SSH connection is still alive and
        reconnect if it is not. Call this after any event that may have
        dropped the TCP session (e.g. cluster shutdown/reboot).

        Returns:
            Connection: Active SSH connection to the NFS client VM
        """
        con = self._NFSClientTestBase__nfs_client_connection
        transport = con.client.get_transport() if con else None
        if transport is None or not transport.is_active():
            log.info("NFS client SSH connection is not active — reconnecting...")
            self._NFSClientTestBase__nfs_client_connection = None
            con = self.get_nfs_client_connection()
            self._NFSClientTestBase__nfs_client_connection = con
            log.info("NFS client connection re-established")
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
        con = self.con

        for attempt in range(retries):
            retcode, stdout, stderr = con.exec_cmd(mount_cmd)
            if retcode == 0:
                log.info(f"Successfully mounted NFS export on attempt {attempt + 1}")
                return

            msg = (
                f"Mount attempt {attempt + 1} failed (rc={retcode}): {stderr or stdout}"
            )
            if attempt == retries - 1:
                raise CommandFailed(msg)

            log.warning(f"{msg}. Retrying in 10 seconds...")
            con.exec_cmd(f"umount -f {mount_dir}")
            time.sleep(10)

    @pytest.fixture(scope="class", autouse=True)
    def nfs_enable_disable(self, request):
        """
        Class-scoped autouse fixture that enables the NFS feature for the
        duration of the test class.

        Setup:
            1. Initialise OCP resource objects (StorageCluster, ConfigMap,
               Pod, Service, PVC, PV, StorageClass)
            2. Enable the NFS feature on the storage cluster
            3. Create a LoadBalancer service for NFS (AWS, IBM Cloud,
               HCI Bare Metal only; skipped on vSphere)

        Teardown:
            4. Disable the NFS feature on the storage cluster
            5. Delete the NFS LoadBalancer service (AWS, IBM Cloud,
               HCI Bare Metal only)

        """
        cls = request.cls
        log.info("Setting up NFS feature for test class")
        cls.nfs_app_deployment = "nfs-test-pod"
        cls.namespace = config.ENV_DATA["cluster_namespace"]
        cls.storage_cluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER, namespace=cls.namespace
        )
        cls.sc_obj = ocp.OCP(kind=constants.STORAGECLASS)
        cls.config_map_obj = ocp.OCP(kind=constants.CONFIGMAP, namespace=cls.namespace)
        cls.pod_obj = ocp.OCP(kind=constants.POD, namespace=cls.namespace)
        cls.service_obj = ocp.OCP(kind=constants.SERVICE, namespace=cls.namespace)
        cls.pvc_obj = ocp.OCP(kind=constants.PVC, namespace=cls.namespace)
        cls.pv_obj = ocp.OCP(kind=constants.PV, namespace=cls.namespace)
        cls.nfs_sc = constants.NFS_STORAGECLASS_NAME
        cls.sc = ocs.OCS(kind=constants.STORAGECLASS, metadata={"name": cls.nfs_sc})
        cls.retain_nfs_sc_name = "ocs-storagecluster-ceph-nfs-retain"
        platform = config.ENV_DATA.get("platform", "").lower()
        cls.is_vsphere = platform == constants.VSPHERE_PLATFORM
        if cls.is_vsphere:
            log.info(
                "vSphere platform detected: out-of-cluster NFS operations will be skipped. "
                "All I/O and data validation will be performed from in-cluster pods."
            )
        cls.run_id = config.RUN.get("run_id")
        cls.test_folder = f"mnt/test_nfs_{cls.run_id}"
        log.info(f"NFS out-of-cluster mount base path: {cls.test_folder}")
        cls.nfs_client_ip = config.ENV_DATA.get("nfs_client_ip")
        log.info(f"NFS client IP: {cls.nfs_client_ip}")
        cls.nfs_client_user = config.ENV_DATA.get("nfs_client_user")
        log.info(f"NFS client user: {cls.nfs_client_user}")

        if not cls.is_vsphere:
            cls.nfs_client_private_key = os.path.expanduser(
                config.ENV_DATA.get("nfs_client_private_key")
                or config.DEPLOYMENT["ssh_key_private"]
            )

        log.info("Enabling NFS feature on the storage cluster")
        if (
            config.default_cluster_ctx.ENV_DATA["cluster_type"].lower()
            == constants.HCI_CLIENT
        ):
            nfs_ganesha_pod, cls.hostname_add = nfs_utils.nfs_access_for_clients(
                cls.nfs_sc
            )

            if (
                version_module.get_semantic_ocs_version_from_config()
                < version_module.VERSION_4_21
            ):
                log.info(
                    f"OCS < 4.21: creating copy NFS StorageClass "
                    f"{constants.COPY_NFS_STORAGECLASS_NAME} with server {cls.hostname_add}"
                )
                _ = nfs_utils.create_nfs_sc(
                    sc_name_to_create=constants.COPY_NFS_STORAGECLASS_NAME,
                    sc_name_to_copy=cls.nfs_sc,
                    server=cls.hostname_add,
                )
                cls.nfs_sc = constants.COPY_NFS_STORAGECLASS_NAME

            yield

            log.info("Disabling NFS feature on the storage cluster (HCI client)")
            log.info("Removing NFS StorageClass from all consumers")
            nfs_utils.remove_nfs_storage_class_from_all_consumers(
                constants.NFS_STORAGECLASS_NAME
            )
            nfs_utils.disable_nfs_service_from_provider(cls.sc, nfs_ganesha_pod)

            if ocp.OCP(kind=constants.STORAGECLASS).is_exist(
                resource_name=constants.COPY_NFS_STORAGECLASS_NAME
            ):
                log.info(
                    f"Deleting copy NFS StorageClass {constants.COPY_NFS_STORAGECLASS_NAME}"
                )
                cls.sc_obj.delete(resource_name=constants.COPY_NFS_STORAGECLASS_NAME)

        else:
            nfs_ganesha_pod_name = nfs_utils.nfs_enable(
                cls.storage_cluster_obj,
                cls.config_map_obj,
                cls.pod_obj,
                cls.namespace,
            )

            if (
                platform == constants.AWS_PLATFORM
                or platform == constants.IBMCLOUD_PLATFORM
                or platform == constants.HCI_BAREMETAL
            ):
                log.info("Creating NFS LoadBalancer service")
                cls.hostname_add = nfs_utils.create_nfs_load_balancer_service(
                    cls.storage_cluster_obj,
                )

            yield

            log.info("Disabling NFS feature on the storage cluster")
            nfs_utils.nfs_disable(
                cls.storage_cluster_obj,
                cls.config_map_obj,
                cls.pod_obj,
                cls.sc,
                nfs_ganesha_pod_name,
            )
            if (
                platform == constants.AWS_PLATFORM
                or platform == constants.IBMCLOUD_PLATFORM
                or platform == constants.HCI_BAREMETAL
            ):
                log.info("Deleting NFS LoadBalancer service")
                nfs_utils.delete_nfs_load_balancer_service(
                    cls.storage_cluster_obj,
                )

        if cls.sc_obj.is_exist(resource_name=cls.retain_nfs_sc_name):
            log.info(f"Deleting retain NFS StorageClass {cls.retain_nfs_sc_name}")
            cls.sc_obj.delete(resource_name=cls.retain_nfs_sc_name)
            log.info(
                f"Waiting for retain NFS StorageClass {cls.retain_nfs_sc_name} to be deleted"
            )
            cls.sc_obj.wait_for_delete(resource_name=cls.retain_nfs_sc_name)

        if not cls.is_vsphere and hasattr(
            cls, "_NFSClientTestBase__nfs_client_connection"
        ):
            try:
                con = cls._NFSClientTestBase__nfs_client_connection
                retcode, stdout, _ = con.exec_cmd("findmnt -t nfs4 " + cls.test_folder)
                if stdout:
                    log.info("unmounting existing nfs mount")
                    nfs_utils.unmount(con, cls.test_folder)
                log.info("Delete mount point")
                _, _, _ = con.exec_cmd("rm -rf " + cls.test_folder)
            except Exception as e:
                log.warning(f"Failed to cleanup NFS mount: {e}")
