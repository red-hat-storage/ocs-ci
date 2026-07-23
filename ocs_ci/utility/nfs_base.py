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
    - Reconnecting a dropped SSH session after cluster events (e.g. shutdown)

    Test classes should inherit from this base class and will have access to:
    - self.con: Property that returns a cached active SSH connection to the NFS client VM
    - self.get_nfs_client_connection(): Method to create a new SSH connection
    - self.reconnect_if_needed(): Method to verify and re-establish the SSH connection
    - self._mount_nfs_with_retry(): Method to mount NFS with retry logic
    - self.nfs_enable_disable: Autouse fixture that enables/disables NFS for the test class
    """

    # If the connection to the NFS Client VM fails, it's possible that
    # the NFS Client VM might not be healthy, so rebooting it and re-trying
    @property
    def con(self):
        """
        Return a cached SSH connection to the NFS client VM, creating it if needed.

        On first access (or after the cache is cleared) the property calls
        get_nfs_client_connection(). If that raises TimeoutError or
        socket.gaierror, it attempts a hard reboot of the NFS client VM via
        OpenStack CLI, then polls TCP port 22 (up to 300 s) until SSH is
        reachable before retrying the connection.

        Returns:
            Connection: Active SSH connection to the NFS client VM

        Raises:
            ConfigurationError: If the VM is unreachable and nfs_client_vm_cloud /
                nfs_client_vm_name are not set in ENV_DATA
            TimeoutError: If the SSH connection cannot be established after retries
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
        def _make_connection():
            return Connection(
                self.nfs_client_ip,
                self.nfs_client_user,
                private_key=self.nfs_client_private_key,
            )

        con = _make_connection()
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
        Verify the cached NFS client SSH connection is alive and reconnect if not.

        Call this after any event that may have dropped the TCP session
        (e.g. cluster shutdown/reboot).

        Paramiko's ``transport.is_active()`` can return True for a connection
        whose underlying TCP socket was reset during a cluster shutdown —
        the transport does not discover the dead socket until the first
        packet is actually sent. A lightweight ``echo ping`` probe is used
        to surface that condition before callers attempt real commands.

        Returns:
            Connection: Active SSH connection to the NFS client VM

        Raises:
            TimeoutError: If reconnection attempt fails
            socket.gaierror: If hostname resolution fails during reconnection
        """
        con = self._NFSClientTestBase__nfs_client_connection
        transport = con.client.get_transport() if con else None
        needs_reconnect = transport is None or not transport.is_active()
        if not needs_reconnect:
            # Probe the live socket; a reset connection raises here before
            # any real command would fail mid-execution.
            try:
                con.client.exec_command("echo ping")
            except Exception:
                needs_reconnect = True
        if needs_reconnect:
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
        duration of the test class and tears it down afterwards.

        Setup:

        1. Initialise OCP resource objects (StorageCluster, ConfigMap, Pod,
           Service, PVC, PV, StorageClass) and set class-level attributes:
           is_vsphere, test_folder, nfs_client_ip, nfs_client_user,
           nfs_client_private_key, retain_nfs_sc_name.
        2. Enable the NFS feature on the storage cluster.
           HCI client: calls nfs_access_for_clients(); on OCS < 4.21 also
           creates a copy NFS StorageClass with the resolved LB hostname.
           Other platforms: calls nfs_enable().
        3. Create a LoadBalancer service for NFS and resolve its hostname
           (AWS, IBM Cloud, HCI Bare Metal only; skipped on vSphere).

        Teardown:

        4. Disable the NFS feature on the storage cluster.
           HCI client: removes NFS StorageClass from all consumers and disables
           the NFS service from the provider; deletes the copy StorageClass if
           it was created.
           Other platforms: calls nfs_disable(); deletes the LB service on
           AWS, IBM Cloud, and HCI Bare Metal.
        5. Delete the retain NFS StorageClass if it exists.
        6. Unmount and remove the out-of-cluster NFS test folder on the
           NFS client VM (non-vSphere only).
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
        cls.test_folder = f"/mnt/test_nfs_{cls.run_id}"
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

    def write_io_to_single_file(
        self, pod_obj, file_path, num_iterations=20, delay=0.5, max_errors=2
    ):
        """
        Write IO operations to a single file from an in-cluster pod.

        Args:
            pod_obj: Pod object used to execute commands (in-cluster)
            file_path (str): Full path to the file inside the pod (e.g. /mnt/file.txt)
            num_iterations (int): Number of write iterations
            delay (float): Delay between iterations in seconds
            max_errors (int): Maximum tolerated transient write errors before
                reporting failure.  Allows a small number of transient
                ``connection reset`` errors that can occur while the cluster
                is recovering after a node reboot.

        Returns:
            tuple: (success, error_list) - success is bool, error_list contains any errors
        """
        log.info(f"Writing IO to single file via pod {pod_obj.name}: {file_path}")
        errors = []

        # Write initial data inside the pod
        initial_data = f"IO test started at {time.time()}"
        init_cmd = f"echo '{initial_data}' > {file_path}"
        try:
            pod_obj.exec_sh_cmd_on_pod(init_cmd)
        except Exception as exc:
            errors.append(f"Failed to initialize file: {exc}")
            return False, errors

        # Append multiple lines
        for i in range(1, num_iterations + 1):
            io_data = f"IO iteration {i} - {time.time()}"
            write_cmd = f"echo '{io_data}' >> {file_path}"
            try:
                pod_obj.exec_sh_cmd_on_pod(write_cmd)
            except Exception as exc:
                errors.append(f"Write failed at iteration {i}: {exc}")
                log.warning(f"Write error at iteration {i}: {exc}")
            time.sleep(delay)

        log.info(f"Completed {num_iterations} IO iterations on {file_path}")
        success = len(errors) <= max_errors
        if errors:
            log.warning(
                f"{len(errors)} write error(s) on {file_path} "
                f"({'tolerated' if success else 'exceeded threshold'},"
                f" max_errors={max_errors}): {errors}"
            )
        return success, errors

    def calculate_checksum_and_lines_from_nfs_mount(self, con, file_path):
        """
        Calculate MD5 checksum and line count for a file on NFS mount (out-of-cluster).

        Args:
            con: Connection object to NFS client VM
            file_path (str): Full path to the file on NFS mount

        Returns:
            dict: {'checksum': str, 'line_count': int, 'success': bool, 'error': str}
        """
        result = {"checksum": None, "line_count": 0, "success": False, "error": None}

        # Calculate checksum
        checksum_cmd = f"md5sum {file_path}"
        retcode, stdout, stderr = con.exec_cmd(checksum_cmd)
        if retcode != 0:
            result["error"] = f"Failed to calculate checksum: {stderr}"
            return result

        result["checksum"] = stdout.split()[0]

        # Get line count
        line_cmd = f"wc -l {file_path}"
        retcode, stdout, stderr = con.exec_cmd(line_cmd)
        if retcode == 0:
            result["line_count"] = int(stdout.split()[0])
        else:
            result["error"] = f"Failed to get line count: {stderr}"
            return result

        result["success"] = True
        log.info(
            f"File: {file_path} - Checksum: {result['checksum']}, "
            f"Lines: {result['line_count']}"
        )
        return result

    def calculate_checksum_and_lines_from_pod(self, pod_obj, file_path_in_pod):
        """
        Calculate MD5 checksum and line count for a file from within the pod (in-cluster).

        Args:
            pod_obj: Pod object
            file_path_in_pod (str): Path to file inside pod (e.g., /mnt/filename)

        Returns:
            dict: {'checksum': str, 'line_count': int, 'success': bool, 'error': str}
        """
        log.info(f"Verifying data from pod: {pod_obj.name}")
        result = {"checksum": None, "line_count": 0, "success": False, "error": None}

        # Calculate checksum from pod
        checksum_cmd = f"md5sum {file_path_in_pod}"
        try:
            pod_output = pod_obj.exec_cmd_on_pod(
                command=checksum_cmd, out_yaml_format=False
            )
            result["checksum"] = pod_output.split()[0]
        except Exception as e:
            result["error"] = f"Failed to get checksum from pod: {str(e)}"
            return result

        # Get line count from pod
        line_cmd = f"wc -l {file_path_in_pod}"
        try:
            pod_output = pod_obj.exec_cmd_on_pod(
                command=line_cmd, out_yaml_format=False
            )
            result["line_count"] = int(pod_output.split()[0])
        except Exception as e:
            result["error"] = f"Failed to get line count from pod: {str(e)}"
            return result

        result["success"] = True
        log.info(
            f"Pod data - Checksum: {result['checksum']}, "
            f"Lines: {result['line_count']}"
        )
        return result

    def verify_data_integrity(
        self, before_data, after_data, operation_name="operation"
    ):
        """
        Verify data integrity by comparing checksums and line counts.

        Args:
            before_data (dict): Data before operation (from calculate_checksum_and_lines)
            after_data (dict): Data after operation (from calculate_checksum_and_lines)
            operation_name (str): Name of the operation for logging

        Raises:
            AssertionError: If data integrity check fails
        """
        log.info(f"Verifying data integrity after {operation_name}")

        # Verify line count
        assert before_data["line_count"] == after_data["line_count"], (
            f"Line count mismatch after {operation_name}!\n"
            f"Before: {before_data['line_count']}\n"
            f"After: {after_data['line_count']}"
        )

        # Verify checksum
        assert before_data["checksum"] == after_data["checksum"], (
            f"Checksum mismatch after {operation_name}! Data integrity check failed.\n"
            f"Before checksum: {before_data['checksum']}\n"
            f"After checksum: {after_data['checksum']}"
        )

        log.info(f"✓ Data integrity verified after {operation_name}")
        log.info(f"  - Line count: {after_data['line_count']}")
        log.info(f"  - Checksum: {after_data['checksum']}")

    def verify_data_integrity_from_pod(
        self,
        pod_obj,
        file_path_in_pod,
        con=None,
        file_path_on_nfs=None,
        expected_checksum=None,
        expected_line_count=None,
        operation_name="operation",
    ):
        """
        Verify data integrity from the in-cluster pod.

        Always:
          - Reads checksum and line count from the pod.
          - If expected_checksum / expected_line_count are provided, asserts the
            pod values match them (validates data correctness on all platforms,
            including vSphere where no external NFS mount exists).

        Additionally on non-vSphere (when con and file_path_on_nfs are provided):
          - Reads checksum and line count from the external NFS mount.
          - Asserts pod values match NFS mount values.

        Args:
            pod_obj: Pod object
            file_path_in_pod (str): Path to file inside pod (e.g. /mnt/file.txt)
            con: Connection object to NFS client (None on vSphere)
            file_path_on_nfs (str): Path to file on NFS mount (None on vSphere)
            expected_checksum (str): Baseline checksum to assert against pod data (optional)
            expected_line_count (int): Baseline line count to assert against pod data (optional)
            operation_name (str): Name of operation for logging

        Returns:
            dict with pod_data and (optionally) nfs_data

        Raises:
            AssertionError: If any data integrity check fails
        """
        log.info("=" * 80)
        log.info(f"Verifying data integrity after {operation_name}")
        log.info("=" * 80)

        # Step 1: always get data from pod
        pod_data = self.calculate_checksum_and_lines_from_pod(pod_obj, file_path_in_pod)
        assert pod_data["success"], f"Failed to get data from pod: {pod_data['error']}"
        log.info(f"  - Pod checksum : {pod_data['checksum']}")
        log.info(f"  - Pod lines    : {pod_data['line_count']}")

        # Step 2: assert against expected values if provided (works on all platforms)
        if expected_checksum is not None:
            assert pod_data["checksum"] == expected_checksum, (
                f"Checksum mismatch after {operation_name}!\n"
                f"Expected : {expected_checksum}\n"
                f"Pod got  : {pod_data['checksum']}"
            )
            log.info("  ✓ Pod checksum matches expected value")

        if expected_line_count is not None:
            assert pod_data["line_count"] == expected_line_count, (
                f"Line count mismatch after {operation_name}!\n"
                f"Expected : {expected_line_count}\n"
                f"Pod got  : {pod_data['line_count']}"
            )
            log.info("  ✓ Pod line count matches expected value")

        result = {"pod_data": pod_data}

        # Step 3: cross-check against external NFS mount (non-vSphere only)
        if con is not None and file_path_on_nfs is not None:
            nfs_data = self.calculate_checksum_and_lines_from_nfs_mount(
                con, file_path_on_nfs
            )
            assert nfs_data[
                "success"
            ], f"Failed to get data from NFS mount: {nfs_data['error']}"
            assert pod_data["checksum"] == nfs_data["checksum"], (
                f"Checksum mismatch between pod and NFS mount after {operation_name}!\n"
                f"Pod checksum: {pod_data['checksum']}\n"
                f"NFS checksum: {nfs_data['checksum']}"
            )
            assert pod_data["line_count"] == nfs_data["line_count"], (
                f"Line count mismatch between pod and NFS mount after {operation_name}!\n"
                f"Pod lines: {pod_data['line_count']}\n"
                f"NFS lines: {nfs_data['line_count']}"
            )
            log.info(f"  - NFS checksum : {nfs_data['checksum']}")
            log.info("  ✓ Pod and NFS mount data match!")
            result["nfs_data"] = nfs_data
        else:
            log.info(
                "  (out-of-cluster NFS check skipped — not applicable on this platform)"
            )

        log.info("=" * 80)
        return result

    def get_nfs_export_details(self, pvc_obj):
        """
        Get NFS share path for a PVC from its backing PV.

        Args:
            pvc_obj (PVC): PVC object

        Returns:
            str: NFS share path
        """
        pv_obj = pvc_obj.backed_pv_obj
        return pv_obj.get()["spec"]["csi"]["volumeAttributes"]["share"]

    def mount_nfs_export(self, con, share_details, mount_point):
        """
        Mount NFS export on client with retry and verification.

        Args:
            con: Connection object to NFS client
            share_details (str): NFS share path
            mount_point (str): Local mount point path

        Returns:
            bool: True if mount successful
        """
        # Create mount point
        retcode, _, _ = con.exec_cmd(f"mkdir -p {mount_point}")
        assert retcode == 0, f"Failed to create mount point {mount_point}"

        # Mount NFS export
        export_path = f"{self.hostname_add}:{share_details}"
        mount_options = "-o proto=tcp"

        log.info(
            f"Mounting NFS export: mount -t nfs {mount_options} {export_path} {mount_point}"
        )

        platform = config.ENV_DATA.get("platform", "").lower()
        if platform == constants.IBMCLOUD_PLATFORM:
            log.info(
                "IBM Cloud: waiting 30s before mount to ensure security group rules "
                "and DNS resolution are fully active..."
            )
            time.sleep(30)

        self._mount_nfs_with_retry(
            mount_dir=mount_point,
            export_path=export_path,
            options=mount_options,
        )

        retcode, stdout, _ = con.exec_cmd(f"findmnt -M {mount_point}")
        assert retcode == 0, f"Mount verification failed for {mount_point}"
        log.info(f"✓ Successfully mounted NFS export at {mount_point}")

        return True

    def continuous_io_operations(
        self,
        pod_obj,
        pod_io_file,
        io_errors,
        io_stop_event,
        io_completed,
        checksum_interval=10,
    ):
        """
        Perform continuous read/write operations on the NFS PVC from
        the in-cluster pod. Checksums are also computed from the pod.

        When the pod is temporarily unavailable (e.g. node reboot causes
        "container not found"), the loop backs off for 10 s per retry and
        counts the whole unavailability window as a single error entry rather
        than one error per retry.  This prevents spurious failures caused by
        the ~3–5 minute pod-rescheduling time on IBM Cloud.

        Args:
            pod_obj: Pod object used to execute commands.
            pod_io_file (str): Path to the test file inside the pod.
            io_errors (list): Shared list to collect error messages.
            io_stop_event (threading.Event): Set to signal the loop to stop.
            io_completed (threading.Event): Set when the function exits.
            checksum_interval (int): Compute md5sum every N successful write
                iterations instead of every iteration.  The cheap tail-based
                content check still runs every iteration.  Defaults to 10.
        """
        iteration = 0
        previous_checksum = None
        # Track pod-unavailable window to append only one error entry per outage
        pod_unavailable = False

        try:
            # Initialize the file inside the pod
            initial_data = f"IO test started at {time.time()}"
            init_cmd = f"echo '{initial_data}' > {pod_io_file}"
            try:
                pod_obj.exec_sh_cmd_on_pod(init_cmd)
            except Exception as exc:
                io_errors.append(f"Failed to initialize test file: {exc}")
                log.error(f"Initialization error: {exc}")
                return

            chk = self.calculate_checksum_and_lines_from_pod(pod_obj, pod_io_file)
            previous_checksum = chk["checksum"]
            log.info(f"File initialized; initial checksum: {previous_checksum}")

            while not io_stop_event.is_set():
                iteration += 1
                test_data = f"IO test iteration {iteration} - {time.time()}"

                # Write via pod — exec_sh_cmd_on_pod handles >> inside the pod
                write_cmd = f"echo '{test_data}' >> {pod_io_file}"
                try:
                    pod_obj.exec_sh_cmd_on_pod(write_cmd)
                    if pod_unavailable:
                        log.info(
                            f"Pod recovered at iteration {iteration} — "
                            f"resuming normal I/O"
                        )
                        pod_unavailable = False
                except Exception as exc:
                    exc_str = str(exc)
                    if "container not found" in exc_str or "not found" in exc_str:
                        if not pod_unavailable:
                            io_errors.append(
                                f"Pod became unavailable at iteration "
                                f"{iteration}: {exc}"
                            )
                            log.warning(
                                f"Pod {pod_obj.name} container 'fedora' not yet ready "
                                f"at iteration {iteration} — pod restarting after node reboot, "
                                f"backing off 10 s and retrying"
                            )
                            pod_unavailable = True
                        else:
                            log.debug(
                                f"Pod {pod_obj.name} container 'fedora' still not ready "
                                f"at iteration {iteration} — waiting 10 s"
                            )
                        time.sleep(10)
                        continue
                    else:
                        io_errors.append(
                            f"Write failed at iteration {iteration}: {exc}"
                        )
                        log.error(f"Write error: {exc}")
                        continue

                # Read last line via pod on every iteration — cheap content check
                read_cmd = f"tail -n 1 {pod_io_file}"
                try:
                    stdout = pod_obj.exec_cmd_on_pod(read_cmd, out_yaml_format=False)
                    if stdout.strip() != test_data:
                        io_errors.append(f"Data mismatch at iteration {iteration}")
                        log.error(
                            f"Data mismatch: expected '{test_data}', "
                            f"got '{stdout.strip()}'"
                        )
                except Exception as exc:
                    io_errors.append(f"Read failed at iteration {iteration}: {exc}")
                    log.error(f"Read error: {exc}")

                # Checksum from pod — sampled every checksum_interval iterations
                if iteration % checksum_interval == 0:
                    chk = self.calculate_checksum_and_lines_from_pod(
                        pod_obj, pod_io_file
                    )
                    current_checksum = chk["checksum"]

                    if current_checksum == previous_checksum:
                        io_errors.append(
                            f"Checksum unchanged at iteration {iteration}: "
                            f"file may not have been updated"
                        )
                        log.error(
                            f"Data integrity error at iteration {iteration}: "
                            f"checksum unchanged ({current_checksum})"
                        )

                    previous_checksum = current_checksum
                    log.info(
                        f"Iteration {iteration}: File checksum - {current_checksum}"
                    )
                time.sleep(2)
                if iteration % 20 == 0:
                    log.info(f"Completed {iteration} I/O iterations successfully")

        except Exception as e:
            io_errors.append(f"Exception during I/O: {str(e)}")
            log.error(f"I/O thread exception: {e}")
        finally:
            io_completed.set()
            log.info(f"I/O operations completed. Total iterations: {iteration}")
            try:
                chk = self.calculate_checksum_and_lines_from_pod(pod_obj, pod_io_file)
                log.info(
                    f"Final file: {chk['line_count']} lines, "
                    f"checksum {chk['checksum']}"
                )
            except Exception as stat_error:
                log.warning(f"Failed to get final file statistics: {stat_error}")
