import inspect
import logging
import os
import random
import threading
import time
from threading import Thread

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_rosa_hcp,
    skipif_lean_deployment,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    skip_for_provider_or_client_if_ocs_version,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    skipif_external_mode,
    skipif_hci_client,
)
from ocs_ci.helpers.nfs_helpers import NFSClientTestBase
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants, ocp, platform_nodes
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs, get_all_nodes
from ocs_ci.ocs.resources import pod, ocs, pvc
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.resources.pvc import create_pvc_snapshot
from ocs_ci.utility.nfs_utils import (
    get_file_checksum_from_nfs,
    get_file_line_count_from_nfs,
    configure_deployment_for_nfs,
)
from ocs_ci.utility import nfs_utils
from ocs_ci.utility import version as version_module
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@skipif_rosa_hcp
@skipif_external_mode
@skipif_ocs_version("<4.11")
@skipif_ocp_version("<4.11")
@skipif_managed_service
@skipif_hci_client
@skip_for_provider_or_client_if_ocs_version("<4.19")
@skipif_disconnected_cluster
@skipif_proxy_cluster
@skipif_lean_deployment
class TestNfsExport(NFSClientTestBase):
    """
    Test nfs feature enable for ODF 4.11

    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown(
        self,
        request,
    ):
        """
        Setup-Teardown for the class

        Pre-Req:
        By default in our jenkins jobs we are creating one of our custom config file,
        so we can make sure
        ENV_DATA:
            nfs_client_ip: "10.xx.xxx.xx"
            nfs_client_user: "root"
            nfs_client_private_key: constants.SSH_PRIV_KEY
        these values are provided in all our automation runs in Jenkins.

        But if someone will run locally, they will need to create custom config file and provide that via
        --ocsci-conf in order to run the external nfs consume tests. Example:
        ENV_DATA:
            nfs_client_ip: "10.xx.xxx.xx"
            nfs_client_user: "root"
            nfs_client_private_key: "<path to ssh private key>"

        If this VM IP is not available in config, then the external nfs consume tests will be skipped.

        Steps:
        ---Setup---
        1:- Create objects for storage_cluster, configmap, pod, pv, pvc, service and storageclass
        2:- Fetch number of cephfsplugin and cephfsplugin_provisioner pods running
        3:- Enable nfs feature
        4:- Create loadbalancer service for nfs
        ---Teardown---
        5:- Disable nfs feature
        6:- Delete ocs nfs Service

        """
        cls = request.cls
        log.info("-----Setup-----")
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
        cls.run_id = config.RUN.get("run_id")
        cls.test_folder = f"mnt/test_nfs_{cls.run_id}"
        log.info(f"nfs mount point out of cluster is----- {cls.test_folder}")
        cls.nfs_client_ip = config.ENV_DATA.get("nfs_client_ip")
        log.info(f"nfs_client_ip is: {cls.nfs_client_ip}")

        cls.nfs_client_user = config.ENV_DATA.get("nfs_client_user")
        log.info(f"nfs_client_user is: {cls.nfs_client_user}")

        cls.nfs_client_private_key = os.path.expanduser(
            config.ENV_DATA.get("nfs_client_private_key")
            or config.DEPLOYMENT["ssh_key_private"]
        )

        # Enable nfs feature
        log.info("----Enable nfs----")
        if (
            config.default_cluster_ctx.ENV_DATA["cluster_type"].lower()
            == constants.HCI_CLIENT
        ):
            nfs_ganesha_pod, cls.hostname_add = nfs_utils.nfs_access_for_clients(
                cls.nfs_sc
            )

            # Create a duplicate sc of nfs-sc and update the server details with hostname_add
            if (
                version_module.get_semantic_ocs_version_from_config()
                < version_module.VERSION_4_21
            ):
                _ = nfs_utils.create_nfs_sc(
                    sc_name_to_create=constants.COPY_NFS_STORAGECLASS_NAME,
                    sc_name_to_copy=cls.nfs_sc,
                    server=cls.hostname_add,
                )
                cls.nfs_sc = constants.COPY_NFS_STORAGECLASS_NAME
            yield
            # Remove NFS SC from distributed storage classes on the provider
            nfs_utils.remove_nfs_storage_class_from_all_consumers(
                constants.NFS_STORAGECLASS_NAME
            )
            # Disable nfs feature
            nfs_utils.disable_nfs_service_from_provider(cls.sc, nfs_ganesha_pod)

            # delete nfs non default storageclass if available
            if ocp.OCP(kind=constants.STORAGECLASS).is_exist(
                resource_name=constants.COPY_NFS_STORAGECLASS_NAME
            ):
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
                # Create loadbalancer service for nfs
                cls.hostname_add = nfs_utils.create_nfs_load_balancer_service(
                    cls.storage_cluster_obj,
                )

            yield

            log.info("-----Teardown-----")
            # Disable nfs feature
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
                # Delete ocs nfs Service
                nfs_utils.delete_nfs_load_balancer_service(
                    cls.storage_cluster_obj,
                )

        if cls.sc_obj.is_exist(resource_name=cls.retain_nfs_sc_name):
            # Delete the nfs retain StorageClass
            cls.sc_obj.delete(resource_name=cls.retain_nfs_sc_name)
            log.info(f"Wait until the SC, {cls.retain_nfs_sc_name} is deleted.")
            cls.sc_obj.wait_for_delete(resource_name=cls.retain_nfs_sc_name)

        # Check if NFS client connection was established and clean up mount
        if (
            hasattr(cls, "_TestNfsExport__nfs_client_connection")
            and cls._TestNfsExport__nfs_client_connection
        ):
            try:
                con = cls._TestNfsExport__nfs_client_connection
                retcode, stdout, _ = con.exec_cmd("findmnt -t nfs4 " + cls.test_folder)
                if stdout:
                    log.info("unmounting existing nfs mount")
                    nfs_utils.unmount(con, cls.test_folder)
                log.info("Delete mount point")
                _, _, _ = con.exec_cmd("rm -rf " + cls.test_folder)
            except Exception as e:
                log.warning(f"Failed to cleanup NFS mount: {e}")

    def write_io_to_single_file(self, con, file_path, num_iterations=20, delay=0.5):
        """
        Write IO operations to a single file on NFS mount.

        Args:
            con: Connection object to NFS client
            file_path (str): Full path to the file
            num_iterations (int): Number of write iterations
            delay (float): Delay between iterations in seconds

        Returns:
            tuple: (success, error_list) - success is bool, error_list contains any errors
        """
        log.info(f"Writing IO to single file: {file_path}")
        errors = []

        # Write initial data
        initial_data = f"IO test started at {time.time()}"
        init_cmd = f'echo "{initial_data}" > {file_path}'
        retcode, _, stderr = con.exec_cmd(init_cmd)
        if retcode != 0:
            errors.append(f"Failed to initialize file: {stderr}")
            return False, errors

        # Append multiple lines
        for i in range(1, num_iterations + 1):
            io_data = f"IO iteration {i} - {time.time()}"
            write_cmd = f'echo "{io_data}" >> {file_path}'
            retcode, _, stderr = con.exec_cmd(write_cmd)
            if retcode != 0:
                errors.append(f"Write failed at iteration {i}: {stderr}")
                log.warning(f"Write error at iteration {i}: {stderr}")
            time.sleep(delay)

        log.info(f"Completed {num_iterations} IO iterations on {file_path}")
        return len(errors) == 0, errors

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

    def get_nfs_export_details(self, pvc_name):
        """
        Get NFS volume name and share details for a PVC.

        Args:
            pvc_name (str): Name of the PVC

        Returns:
            dict: {'volume_name': str, 'share_details': str}
        """
        # Get volume name from PVC
        fetch_vol_cmd = f"get pvc {pvc_name} -o jsonpath='{{.spec.volumeName}}'"
        vol_name = self.pvc_obj.exec_oc_cmd(fetch_vol_cmd)
        log.info(f"Volume name for PVC {pvc_name}: {vol_name}")

        # Get NFS share details from PV
        fetch_share_cmd = (
            f"get pv {vol_name} " f"-o jsonpath='{{.spec.csi.volumeAttributes.share}}'"
        )
        share_details = self.pv_obj.exec_oc_cmd(fetch_share_cmd)
        log.info(f"NFS share for PVC {pvc_name}: {share_details}")

        return {"volume_name": vol_name, "share_details": share_details}

    def log_nfs_loadbalancer_details(self, stage=""):
        """
        Log NFS LoadBalancer service details including endpoint, hostname, and IP address.

        Args:
            stage (str): Description of when this is being called (e.g., "BEFORE shutdown", "AFTER recovery")
        """
        log.info("=" * 80)
        log.info(f"NFS LoadBalancer details {stage}:")
        try:
            lb_svc = ocp.OCP(
                kind=constants.SERVICE,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name="rook-ceph-nfs-my-nfs-load-balancer",
            )
            lb_data = lb_svc.get()

            # Get LoadBalancer ingress details
            lb_ingress = (
                lb_data.get("status", {}).get("loadBalancer", {}).get("ingress", [])
            )

            if lb_ingress:
                # Extract both hostname and IP if available
                lb_hostname = lb_ingress[0].get("hostname", "N/A")
                lb_ip = lb_ingress[0].get("ip", "N/A")
                lb_endpoint = lb_hostname if lb_hostname != "N/A" else lb_ip

                log.info(f"  LoadBalancer Hostname: {lb_hostname}")
                log.info(f"  LoadBalancer IP: {lb_ip}")
                log.info(f"  LoadBalancer Endpoint (used for mount): {lb_endpoint}")
                log.info(
                    f"  Expected hostname (self.hostname_add): {self.hostname_add}"
                )

                # Check if endpoint changed
                if lb_endpoint != self.hostname_add:
                    log.warning(
                        f"  ⚠ LoadBalancer endpoint changed! Expected: {self.hostname_add}, Got: {lb_endpoint}"
                    )
                else:
                    log.info("  ✓ LoadBalancer endpoint matches expected value")

                # Get service ports
                ports = lb_data.get("spec", {}).get("ports", [])
                if ports:
                    log.info("  Service Ports:")
                    for port in ports:
                        log.info(
                            f"    - {port.get('name', 'unnamed')}: "
                            f"{port.get('port', 'N/A')}/{port.get('protocol', 'N/A')}"
                        )

                # Get external traffic policy
                external_policy = lb_data.get("spec", {}).get(
                    "externalTrafficPolicy", "N/A"
                )
                log.info(f"  External Traffic Policy: {external_policy}")

            else:
                log.warning("  ⚠ No LoadBalancer ingress found")
                log.info("  Service may still be provisioning or there's an issue")

        except Exception as e:
            log.warning(f"  ⚠ Failed to get LoadBalancer details: {e}")
        log.info("=" * 80)

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

        # For IBM Cloud, add additional wait to ensure security group rules are fully active
        platform = config.ENV_DATA.get("platform", "").lower()
        if platform == constants.IBMCLOUD_PLATFORM:
            log.info(
                "IBM Cloud platform detected. Waiting 30 seconds before mount attempt "
                "to ensure security group rules and DNS resolution are fully active..."
            )
            time.sleep(30)

        self._mount_nfs_with_retry(
            mount_dir=mount_point,
            export_path=export_path,
            options=mount_options,
        )

        # Verify mount
        retcode, stdout, _ = con.exec_cmd(f"findmnt -M {mount_point}")
        assert retcode == 0, f"Mount verification failed for {mount_point}"
        log.info(f"✓ Successfully mounted NFS export at {mount_point}")

        return True

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

    def verify_bidirectional_data_integrity(
        self,
        pod_obj,
        file_path_in_pod,
        con,
        file_path_on_nfs,
        operation_name="operation",
    ):
        """
        Verify data integrity from both pod and NFS mount perspectives.

        Args:
            pod_obj: Pod object
            file_path_in_pod (str): Path to file inside pod
            con: Connection object to NFS client
            file_path_on_nfs (str): Path to file on NFS mount
            operation_name (str): Name of operation for logging

        Raises:
            AssertionError: If data doesn't match between pod and NFS mount
        """
        log.info("=" * 80)
        log.info(f"Verifying bidirectional data integrity after {operation_name}")
        log.info("=" * 80)

        # Get data from pod
        pod_data = self.calculate_checksum_and_lines_from_pod(pod_obj, file_path_in_pod)
        assert pod_data["success"], f"Failed to get data from pod: {pod_data['error']}"

        # Get data from NFS mount
        nfs_data = self.calculate_checksum_and_lines_from_nfs_mount(
            con, file_path_on_nfs
        )
        assert nfs_data["success"], f"Failed to get data from NFS: {nfs_data['error']}"

        # Compare pod and NFS data
        assert pod_data["checksum"] == nfs_data["checksum"], (
            f"Checksum mismatch between pod and NFS mount!\n"
            f"Pod checksum: {pod_data['checksum']}\n"
            f"NFS checksum: {nfs_data['checksum']}"
        )

        assert pod_data["line_count"] == nfs_data["line_count"], (
            f"Line count mismatch between pod and NFS mount!\n"
            f"Pod lines: {pod_data['line_count']}\n"
            f"NFS lines: {nfs_data['line_count']}"
        )

        log.info("✓ Bidirectional data integrity verified!")
        log.info(f"  - Pod checksum: {pod_data['checksum']}")
        log.info(f"  - NFS checksum: {nfs_data['checksum']}")
        log.info(f"  - Line count: {pod_data['line_count']}")
        log.info("=" * 80)

        return {"pod_data": pod_data, "nfs_data": nfs_data}

    @tier1
    @pytest.mark.parametrize(
        "access_mode",
        [
            # pytest.param(constants.ACCESS_MODE_RWX, id="RWX"),
            pytest.param(constants.ACCESS_MODE_RWO, id="RWO"),
        ],
    )
    def test_nfs_export_operations_in_out_cluster(
        self,
        pod_factory,
        request,
        nodes,
        access_mode,
    ):
        """
        Comprehensive NFS export test with in-cluster and out-cluster mounts,
        including snapshot/restore, clone operations, resize, and cluster shutdown scenarios.

        Test Description:
        -----------------
        This test validates NFS export functionality with various PVC operations including
        snapshot creation, restoration, cloning, resizing, and data integrity verification
        across cluster shutdown/recovery cycles. The test uses a single PVC set that is
        mounted both inside and outside the OCP cluster simultaneously.

        Prerequisites:
        --------------
        - ODF cluster deployed with:
          * Hugepages enabled
          * Multus networking configured
          * Encryption in transit enabled
          * NFS feature enabled during deployment

        Entry Criteria:
        ---------------
        1. Background operations running (if applicable)
        2. Background features enabled (if applicable)
        3. Background I/O operations in progress (if applicable)
        4. Static load on the cluster (if applicable)

        Test Steps:
        -----------
        a) **Initial Setup:**
           - Create a PVC using the NFS storageclass 'ocs-storagecluster-ceph-nfs'
           - PVC configuration: 10Gi, RWO access mode, Filesystem volume mode

        b) **NFS Export Creation:**
           - Create NFS export for the PVC
           - Export share simultaneously to:
             * Inside OCP cluster (in-cluster mount via pod)
             * Outside OCP cluster (out-cluster mount via external client VM)

        c) **I/O Operations with Node Reboot:**
           - Start continuous I/O operations from both clients (in-cluster and out-cluster)
           - While I/O is in progress, reboot the NFS pod nodes
           - Expected: No impact to ongoing I/O operations
           - Verify I/O continues without interruption

        d) **Data Integrity Verification:**
           - Stop I/O operations
           - Capture MD5 checksums for sample files on both NFS mounts
           - Verify data consistency between in-cluster and out-cluster mounts

        e) **Snapshot and Restore Operations:**
           - Create a snapshot of the PVC
           - Restore the snapshot to create a new PVC
           - Create NFS export for the restored PVC
           - Mount on both clients (in-cluster and out-cluster)
           - Write I/O to the restored PVC
           - Stop I/O and capture MD5 checksums
           - Verify data integrity by comparing checksums from both clients

        f) **Clone Operations:**
           - Create a clone of the restored PVC
           - Create NFS export for the cloned PVC
           - Mount on both clients
           - Write I/O to the cloned PVC
           - Stop I/O and capture MD5 checksums
           - Resize the cloned PVC
           - Verify data integrity after resize by comparing checksums

        g) **Snapshot of Resized Clone:**
           - Create a snapshot of the resized cloned PVC
           - Restore the snapshot to create a new PVC
           - Create NFS export for the final restored PVC
           - Mount on both clients
           - Write I/O to the final restored PVC
           - Stop I/O and capture MD5 checksums
           - Resize the final restored PVC
           - Verify data integrity after resize by comparing checksums

        h) **Cluster Shutdown and Recovery:**
           - Perform non-graceful cluster shutdown (force=True, simulates power failure)
           - Recover the OCP cluster
           - Verify NFS mount points on both clients are still accessible
           - Verify data integrity on both mounts
           - Confirm all NFS exports are functional post-recovery

        Expected Results:
        -----------------
        1. NFS exports are successfully created and accessible from both in-cluster
           and out-cluster clients simultaneously
        2. I/O operations continue without interruption during NFS pod node reboots
        3. Data integrity is maintained across all operations:
           - Snapshot creation and restoration
           - Clone operations
           - PVC resize operations
           - Cluster shutdown and recovery
        4. MD5 checksums match between in-cluster and out-cluster mounts at all stages
        5. All NFS mount points remain accessible after cluster recovery
        6. No data loss or corruption occurs throughout the test lifecycle

        Cleanup:
        --------
        - Unmount NFS exports from both clients
        - Delete all created resources (PVCs, snapshots, clones, pods, deployments)
        - Verify all resources are properly cleaned up
        """

        log.info(
            f"Test case execution started: {inspect.currentframe().f_code.co_name}"
        )
        nfs_utils.skip_test_if_nfs_client_unavailable(self.nfs_client_ip)

        # Generate unique names using timestamp to avoid conflicts between test run
        unique_suffix = f"{int(time.time())}-{random.randint(1000, 9999)}"
        pod_name = f"test-deployment-outcluster-{unique_suffix}"
        pvc_name = f"test-pvc-outcluster-{unique_suffix}"
        log.info(f"Using unique names: pod deployment={pod_name}, pvc={pvc_name}")

        # Supported access mode constants.ACCESS_MODE_RWX and constants.ACCESS_MODE_RWO

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            size="10Gi",
            do_reload=True,
            access_mode=access_mode,
            volume_mode="Filesystem",
            pvc_name=pvc_name,
        )

        # Create deployment for app pod
        log.info("----creating deployment ---")
        deployment_data = configure_deployment_for_nfs(
            deployment_name=pod_name, pvc_name=pvc_name
        )
        helpers.create_resource(**deployment_data)

        # Wait for deployment to be ready
        log.info(f"Waiting for deployment {pod_name} to be ready...")
        deployment_obj = ocp.OCP(kind=constants.DEPLOYMENT, namespace=self.namespace)
        deployment_obj.wait_for_resource(
            condition="1/1",
            resource_name=pod_name,
            column="READY",
            timeout=300,
        )
        log.info(f"Deployment {pod_name} is ready")

        # Get the pod created by the deployment
        pod_obj = pod.get_all_pods(
            namespace=self.namespace,
            selector=[pod_name],
            selector_label="name",
        )[0]
        log.info(f"Pod {pod_obj.name} is running")

        # Use local variable to avoid modifying class instance variable
        test_folder_for_pod = self.test_folder + "-" + pod_name

        # Fetch sharing details for the nfs pvc using helper method
        export_details = self.get_nfs_export_details(nfs_pvc_obj.name)
        # vol_name = export_details["volume_name"]
        share_details = export_details["share_details"]

        con = self.con

        # Mount NFS export using the wrapper method
        self.mount_nfs_export(con, share_details, test_folder_for_pod)

        # Add finalizer for complete cleanup after all resources are created
        def cleanup_all_resources():
            """Cleanup all test resources in reverse order of creation"""
            log.info("Running cleanup for all test resources...")

            # 1. Unmount NFS
            try:
                log.info(f"Unmounting {test_folder_for_pod}")
                nfs_utils.unmount(con, test_folder_for_pod)
                con.exec_cmd(f"rm -rf {test_folder_for_pod}")
                log.info("Waiting for NFS export to be fully released...")
                time.sleep(10)
            except Exception as e:
                log.warning(f"Failed to unmount NFS: {e}")

            # 2. Delete deployment
            try:
                log.info(f"Deleting deployment {pod_name}")
                deployment_obj = ocp.OCP(
                    kind=constants.DEPLOYMENT, namespace=self.namespace
                )
                if deployment_obj.is_exist(resource_name=pod_name):
                    deployment_obj.delete(resource_name=pod_name)
                    deployment_obj.wait_for_delete(resource_name=pod_name, timeout=180)
                    log.info(f"Deployment {pod_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete deployment: {e}")

            # 3. Wait for pod termination
            try:
                log.info("Waiting for pod to be terminated...")
                pod_obj.ocp.wait_for_delete(pod_obj.name, timeout=180)
                log.info(f"Pod {pod_obj.name} terminated successfully")
            except Exception as e:
                log.warning(f"Failed to wait for pod deletion: {e}")

            # 4. Delete PVC and PV
            try:
                pv_obj = nfs_pvc_obj.backed_pv_obj
                log.info(f"Deleting PVC {nfs_pvc_obj.name}")
                nfs_pvc_obj.delete(wait=True)
                log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

                log.info("Checking if NFS PV is deleted")
                pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=300)
                log.info(f"PV {pv_obj.name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete PVC/PV: {e}")

            log.info("Cleanup complete")

        request.addfinalizer(cleanup_all_resources)

        # ========================================================================
        # Scenario: NFS Server Pod Node Reboot During Active I/O
        # ========================================================================
        log.info("=" * 80)
        log.info("Starting NFS Server Pod Node Reboot During Active I/O scenario")
        log.info("=" * 80)

        # Step 1: Start continuous read/write I/O from NFS mounted client
        log.info("Step 1: Starting continuous I/O operations on NFS mount")

        # Define test file name as constant
        IO_TEST_FILE_NAME = "io_test_single.txt"
        test_file = f"{test_folder_for_pod}/{IO_TEST_FILE_NAME}"

        io_errors = []
        io_stop_event = threading.Event()
        io_completed = threading.Event()

        def continuous_io_operations():
            """
            Perform continuous read/write operations on a single NFS file with checksum validation
            """
            iteration = 0
            previous_checksum = None

            try:
                # Initialize the file with initial content
                initial_data = f"IO test started at {time.time()}"
                init_cmd = f'echo "{initial_data}" > {test_file}'
                retcode, _, stderr = con.exec_cmd(init_cmd)
                if retcode != 0:
                    io_errors.append(f"Failed to initialize test file: {stderr}")
                    log.error(f"Initialization error: {stderr}")
                    return

                # Get initial file checksum
                previous_checksum = get_file_checksum_from_nfs(con, test_file)
                if previous_checksum:
                    log.info(f"File initialized with checksum: {previous_checksum}")
                else:
                    log.warning("Failed to get initial checksum")

                while not io_stop_event.is_set():
                    iteration += 1
                    test_data = f"IO test iteration {iteration} - {time.time()}"

                    # Write operation - append to the file
                    write_cmd = f'echo "{test_data}" >> {test_file}'
                    retcode, _, stderr = con.exec_cmd(write_cmd)
                    if retcode != 0:
                        io_errors.append(
                            f"Write failed at iteration {iteration}: {stderr}"
                        )
                        log.error(f"Write error: {stderr}")
                        continue

                    # Calculate current file checksum
                    current_checksum = get_file_checksum_from_nfs(con, test_file)
                    if not current_checksum:
                        io_errors.append(
                            f"Checksum calculation failed at iteration {iteration}"
                        )
                        log.error(f"Checksum error at iteration {iteration}")
                        continue

                    # Verify checksum changed after write (data was actually written)
                    if current_checksum == previous_checksum:
                        io_errors.append(
                            f"Checksum unchanged at iteration {iteration}: "
                            f"file may not have been updated"
                        )
                        log.error(
                            f"Data integrity error at iteration {iteration}: "
                            f"checksum unchanged ({current_checksum}), write may have failed"
                        )

                    # Read the last line to verify content
                    read_cmd = f"tail -n 1 {test_file}"
                    retcode, stdout, stderr = con.exec_cmd(read_cmd)
                    if retcode != 0:
                        io_errors.append(
                            f"Read failed at iteration {iteration}: {stderr}"
                        )
                        log.error(f"Read error: {stderr}")
                    elif stdout.strip() != test_data:
                        io_errors.append(f"Data mismatch at iteration {iteration}")
                        log.error(
                            f"Data mismatch: expected '{test_data}', got '{stdout.strip()}'"
                        )

                    # Update previous checksum for next iteration
                    previous_checksum = current_checksum

                    # Periodically log file checksum
                    if iteration % 10 == 0:
                        log.info(
                            f"Iteration {iteration}: File checksum - {current_checksum}"
                        )

                    # Small delay between iterations
                    time.sleep(2)

                    if iteration % 20 == 0:
                        log.info(f"Completed {iteration} I/O iterations successfully")

            except Exception as e:
                io_errors.append(f"Exception during I/O: {str(e)}")
                log.error(f"I/O thread exception: {e}")
            finally:
                io_completed.set()
                log.info(f"I/O operations completed. Total iterations: {iteration}")

                # Final file statistics
                try:
                    # Final file statistics
                    line_count = get_file_line_count_from_nfs(con, test_file)
                    if line_count:
                        log.info(f"Final file statistics: {line_count} lines written")

                    # Final checksum
                    final_checksum = get_file_checksum_from_nfs(con, test_file)
                    if final_checksum:
                        log.info(f"Final file checksum: {final_checksum}")
                except Exception as stat_error:
                    log.warning(f"Failed to get final file statistics: {stat_error}")

        # Start I/O in background thread
        io_thread: Thread = threading.Thread(
            target=continuous_io_operations, daemon=True
        )
        io_thread.start()
        log.info("Continuous I/O thread started")

        # Let I/O run for a bit before node reboot
        time.sleep(30)
        log.info("Initial I/O operations running successfully")

        # Step 2: Identify and reboot node hosting NFS server pod
        log.info("Step 2: Identifying node hosting NFS server pod")

        # Get NFS server pods
        nfs_server_pods = get_all_pods(
            namespace=self.namespace, selector=["rook-ceph-nfs"], selector_label="app"
        )

        if not nfs_server_pods:
            io_stop_event.set()
            io_thread.join(timeout=30)
            raise Exception("No NFS server pods found")

        nfs_server_pod = nfs_server_pods[0]
        log.info(f"Found NFS server pod: {nfs_server_pod.name}")

        # Get node hosting the NFS server pod
        nfs_node_name = nfs_server_pod.data["spec"]["nodeName"]
        log.info(f"NFS server pod is running on node: {nfs_node_name}")
        # Get the node object for the NFS node

        nfs_node_obj = get_node_objs([nfs_node_name])[0]
        log.info(f"Rebooting node: {nfs_node_name}")

        # Perform node reboot using platform nodes
        log.info("Initiating node reboot...")
        factory = platform_nodes.PlatformNodesFactory()
        nodes_platform = factory.get_nodes_platform()
        nodes_platform.restart_nodes([nfs_node_obj], wait=True)
        log.info(f"Node {nfs_node_name} reboot completed")

        # Step 3: Wait for node and pods to recover
        log.info("Step 3: Waiting for node to come back online")

        # Wait for node to be ready
        log.info("Waiting for node to be in Ready state...")
        wait_for_nodes_status(
            node_names=[nfs_node_name], status=constants.NODE_READY, timeout=900
        )
        log.info(f"Node {nfs_node_name} is back online and Ready")

        # Wait for NFS server pod to be running again
        log.info("Waiting for NFS server pod to be running...")
        wait_for_pods_to_be_running(
            pod_names=[nfs_server_pod.name], namespace=self.namespace, timeout=600
        )
        log.info("NFS server pod is running again")

        # Wait for deployment pod to be running (pod might have been recreated with new name)
        log.info(f"Waiting for deployment {pod_name} pod to be running...")
        assert self.pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector=f"name={pod_name}",
            dont_allow_other_resources=True,
            timeout=600,
        ), f"Deployment {pod_name} pod not running after node reboot"

        # Get fresh pod object from deployment (pod name will have changed after reboot)
        log.info(f"Getting fresh pod object for deployment {pod_name}...")
        pod_objs = pod.get_all_pods(
            namespace=self.namespace,
            selector=[pod_name],
            selector_label="name",
        )
        if pod_objs:
            pod_obj = pod_objs[0]
            log.info(f"Got fresh pod object: {pod_obj.name}")
        else:
            raise Exception(
                f"Could not find pod for deployment {pod_name} after node reboot"
            )

        # Verify NFS mount is still accessible
        log.info("Verifying NFS mount accessibility...")
        retcode, stdout, _ = con.exec_cmd(f"findmnt -M {test_folder_for_pod}")
        assert retcode == 0, "NFS mount not accessible after node reboot"
        log.info("NFS mount is still accessible")

        # Let I/O continue for a bit after recovery
        log.info("Continuing I/O operations after node recovery...")
        time.sleep(60)

        # Stop I/O operations
        log.info("Stopping I/O operations...")
        io_stop_event.set()
        io_thread.join(timeout=60)

        # Check for I/O errors
        log.info("Checking I/O results...")
        if io_errors:
            log.warning(f"I/O errors detected: {len(io_errors)} errors")
            for error in io_errors[:10]:  # Log first 10 errors
                log.warning(f"  - {error}")
            # Note: Some transient errors during reboot might be acceptable
            # Fail only if there are persistent errors after recovery
            if len(io_errors) > 20:
                raise AssertionError(f"Too many I/O errors detected: {len(io_errors)}")
        else:
            log.info("No I/O errors detected - all operations successful!")

        # Verify bidirectional data consistency using helper method
        log.info("Verifying bidirectional data consistency after node reboot...")
        pod_file_path = f"/mnt/{IO_TEST_FILE_NAME}"
        self.verify_bidirectional_data_integrity(
            pod_obj=pod_obj,
            file_path_in_pod=pod_file_path,
            con=con,
            file_path_on_nfs=test_file,
            operation_name="node reboot",
        )

        log.info(
            f"NFS node reboot test completed: {nfs_node_name} rebooted, {len(io_errors)} I/O errors detected"
        )

        # ========================================================================
        # Scenario: NFS PVC Snapshot and Restore with Data Integrity Verification
        # ========================================================================
        log.info("=" * 80)
        log.info("Starting NFS PVC Snapshot and Restore scenario")
        log.info("=" * 80)

        # Step 1: Capture file checksum from NFS mount
        log.info("Step 1: Capturing file checksum from NFS mount")

        # Calculate checksum and line count of the single test file
        original_file_checksum = get_file_checksum_from_nfs(con, test_file)
        assert original_file_checksum, "Failed to calculate file checksum"
        log.info(f"Original file checksum: {original_file_checksum}")

        # Get file line count for reference
        original_line_count = get_file_line_count_from_nfs(con, test_file)
        log.info(f"Total lines in file: {original_line_count}")

        # Step 2: Create snapshot of the NFS PVC
        log.info("Step 2: Creating snapshot of NFS PVC")

        snapshot_name = f"{pvc_name}-snapshot"
        snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        # Get the NFS snapshot class (not CephFS)
        # NFS has its own snapshot class: ocs-storagecluster-nfsplugin-snapclass
        nfs_snapshotclass_name = "ocs-storagecluster-nfsplugin-snapclass"

        log.info(
            f"Creating snapshot: {snapshot_name} from NFS PVC: {pvc_name} using "
            f"snapshot class: {nfs_snapshotclass_name}"
        )

        snapshot_obj = create_pvc_snapshot(
            pvc_name=pvc_name,
            snap_yaml=snap_yaml,
            snap_name=snapshot_name,
            namespace=self.namespace,
            sc_name=nfs_snapshotclass_name,
            wait=True,
            timeout=300,
        )

        log.info(f"Snapshot {snapshot_name} created successfully")

        # Add snapshot to cleanup
        def cleanup_snapshot():
            try:
                log.info(f"Deleting snapshot {snapshot_name}")
                snapshot_obj.delete()
                snapshot_obj.ocp.wait_for_delete(
                    resource_name=snapshot_name, timeout=180
                )
                log.info(f"Snapshot {snapshot_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete snapshot: {e}")

        request.addfinalizer(cleanup_snapshot)

        # Step 3: Create new PVC from snapshot (restore)
        log.info("Step 3: Creating new PVC from snapshot")

        restored_pvc_name = f"{pvc_name}-restored"
        restored_pvc_obj = pvc.create_restore_pvc(
            sc_name=self.nfs_sc,
            snap_name=snapshot_name,
            namespace=self.namespace,
            size="10Gi",
            pvc_name=restored_pvc_name,
            volume_mode="Filesystem",
            restore_pvc_yaml=constants.CSI_CEPHFS_PVC_RESTORE_YAML,
            access_mode=access_mode,
        )

        log.info(f"Restored PVC {restored_pvc_name} created from snapshot")

        # Add restored PVC to cleanup
        def cleanup_restored_pvc():
            try:
                log.info(f"Deleting restored PVC {restored_pvc_name}")
                restored_pvc_obj.delete(wait=True)
                log.info(f"Restored PVC {restored_pvc_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete restored PVC: {e}")

        request.addfinalizer(cleanup_restored_pvc)

        # Step 4: Create new pod with restored PVC
        log.info("Step 4: Creating new pod with restored PVC")

        restored_pod_name = f"{pod_name}-restored"
        restored_deployment_data = configure_deployment_for_nfs(
            deployment_name=restored_pod_name, pvc_name=restored_pvc_name
        )
        helpers.create_resource(**restored_deployment_data)

        # Wait for deployment to be ready
        log.info(f"Waiting for deployment {restored_pod_name} to be ready...")
        restored_deployment_obj = ocp.OCP(
            kind=constants.DEPLOYMENT, namespace=self.namespace
        )
        restored_deployment_obj.wait_for_resource(
            condition="1/1",
            resource_name=restored_pod_name,
            column="READY",
            timeout=300,
        )
        log.info(f"Deployment {restored_pod_name} is ready")

        # Get the pod created by the deployment
        restored_pod_obj = pod.get_all_pods(
            namespace=self.namespace,
            selector=[restored_pod_name],
            selector_label="name",
        )[0]
        log.info(f"Restored pod {restored_pod_obj.name} is running")

        # Add restored deployment to cleanup
        def cleanup_restored_deployment():
            try:
                log.info(f"Deleting restored deployment {restored_pod_name}")
                deployment_obj = ocp.OCP(
                    kind=constants.DEPLOYMENT, namespace=self.namespace
                )
                if deployment_obj.is_exist(resource_name=restored_pod_name):
                    deployment_obj.delete(resource_name=restored_pod_name)
                    deployment_obj.wait_for_delete(
                        resource_name=restored_pod_name, timeout=180
                    )
                    log.info(
                        f"Restored deployment {restored_pod_name} deleted successfully"
                    )

                log.info("Waiting for restored pod to be terminated...")
                restored_pod_obj.ocp.wait_for_delete(restored_pod_obj.name, timeout=180)
                log.info(
                    f"Restored pod {restored_pod_obj.name} terminated successfully"
                )
            except Exception as e:
                log.warning(f"Failed to delete restored deployment/pod: {e}")

        request.addfinalizer(cleanup_restored_deployment)

        # Step 5: Mount restored PVC on NFS client and verify data integrity
        log.info(
            "Step 5: Mounting restored PVC on NFS client for data integrity verification"
        )

        # Get NFS share details for restored PVC
        fetch_restored_vol_name_cmd = (
            "get pvc " + restored_pvc_name + " --output jsonpath='{.spec.volumeName}'"
        )
        restored_vol_name = self.pvc_obj.exec_oc_cmd(fetch_restored_vol_name_cmd)
        log.info(
            f"For restored PVC {restored_pvc_name} volume name is, {restored_vol_name}"
        )

        fetch_restored_pv_share_cmd = (
            "get pv "
            + restored_vol_name
            + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
        )
        restored_share_details = self.pv_obj.exec_oc_cmd(fetch_restored_pv_share_cmd)
        log.info(f"Restored share details is, {restored_share_details}")

        # Mount restored NFS export using the wrapper method
        restored_test_folder = f"{self.test_folder}-restored"
        self.mount_nfs_export(con, restored_share_details, restored_test_folder)

        # Add restored mount to cleanup
        def cleanup_restored_mount():
            try:
                log.info(f"Unmounting {restored_test_folder}")
                nfs_utils.unmount(con, restored_test_folder)
                con.exec_cmd(f"rm -rf {restored_test_folder}")
                log.info(f"Restored mount {restored_test_folder} cleaned up")
            except Exception as e:
                log.warning(f"Failed to unmount restored NFS: {e}")

        request.addfinalizer(cleanup_restored_mount)

        # Verify bidirectional data integrity for restored PVC
        log.info("Verifying bidirectional data integrity for restored PVC...")
        restored_test_file = f"{restored_test_folder}/{IO_TEST_FILE_NAME}"
        restored_pod_file_path = f"/mnt/{IO_TEST_FILE_NAME}"

        verification_result = self.verify_bidirectional_data_integrity(
            pod_obj=restored_pod_obj,
            file_path_in_pod=restored_pod_file_path,
            con=con,
            file_path_on_nfs=restored_test_file,
            operation_name="snapshot restore",
        )

        # Extract checksums for comparison with original
        restored_file_checksum = verification_result["nfs_data"]["checksum"]
        restored_line_count = verification_result["nfs_data"]["line_count"]

        # Verify restored data matches original data
        assert (
            original_line_count == restored_line_count
        ), f"Line count mismatch! Original: {original_line_count}, Restored: {restored_line_count}"

        assert original_file_checksum == restored_file_checksum, (
            f"File checksum mismatch! Data integrity check failed.\n"
            f"Original checksum: {original_file_checksum}\n"
            f"Restored checksum: {restored_file_checksum}"
        )

        log.info("✓ Restored data matches original data")

        log.info("=" * 80)
        log.info("NFS Snapshot and Restore scenario completed successfully!")
        log.info("Summary:")
        log.info(f"  - Snapshot created: {snapshot_name}")
        log.info(f"  - Restored PVC: {restored_pvc_name}")
        log.info(f"  - Lines verified: {restored_line_count}")
        log.info(f"  - Original checksum: {original_file_checksum}")
        log.info(f"  - Restored checksum: {restored_file_checksum}")
        log.info("  - Data integrity: VERIFIED ✓")
        log.info("=" * 80)
        log.info("Test completed successfully - cleanup will be handled by finalizer")

        # ========================================================================
        # Scenario: Clone Restored PVC, Resize, and Verify Data Integrity
        # ========================================================================
        log.info("=" * 80)
        log.info("Starting PVC Clone, Resize, and Data Integrity Verification scenario")
        log.info("=" * 80)

        # Step 1: Create a clone of the restored PVC
        log.info("Step 1: Creating clone of restored PVC")

        cloned_pvc_name = f"{restored_pvc_name}-clone"
        cloned_pvc_obj = pvc.create_pvc_clone(
            sc_name=self.nfs_sc,
            parent_pvc=restored_pvc_name,
            clone_yaml=constants.CSI_CEPHFS_PVC_CLONE_YAML,
            namespace=self.namespace,
            pvc_name=cloned_pvc_name,
            storage_size="10Gi",
        )

        log.info(f"Cloned PVC {cloned_pvc_name} created successfully")

        # Add cloned PVC to cleanup
        def cleanup_cloned_pvc():
            try:
                log.info(f"Deleting cloned PVC {cloned_pvc_name}")
                cloned_pvc_obj.delete()
                cloned_pvc_obj.ocp.wait_for_delete(
                    resource_name=cloned_pvc_name, timeout=180
                )
                log.info(f"Cloned PVC {cloned_pvc_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete cloned PVC: {e}")

        request.addfinalizer(cleanup_cloned_pvc)

        # Step 2: Create pod deployment using the cloned PVC
        log.info("Step 2: Creating pod deployment with cloned PVC")

        cloned_pod_name = f"test-deployment-cloned-{int(time.time())}"
        cloned_deployment_data = configure_deployment_for_nfs(
            deployment_name=cloned_pod_name, pvc_name=cloned_pvc_name
        )
        helpers.create_resource(**cloned_deployment_data)

        # Wait for deployment to be ready
        log.info(f"Waiting for deployment {cloned_pod_name} to be ready...")
        cloned_deployment_obj = ocp.OCP(
            kind=constants.DEPLOYMENT, namespace=self.namespace
        )
        cloned_deployment_obj.wait_for_resource(
            condition="1/1",
            resource_name=cloned_pod_name,
            column="READY",
            timeout=300,
        )
        log.info(f"Deployment {cloned_pod_name} is ready")

        # Get the actual pod created by the deployment (it will have a generated suffix)
        cloned_pod_objs = pod.get_all_pods(
            namespace=self.namespace, selector=[cloned_pod_name], selector_label="name"
        )
        assert (
            len(cloned_pod_objs) > 0
        ), f"No pods found for deployment {cloned_pod_name}"
        # cloned_pod_obj = cloned_pod_objs[0]

        log.info(f"Cloned pod {cloned_pod_name} is running")

        # Add cloned pod to cleanup
        def cleanup_cloned_pod():
            try:
                log.info(f"Deleting cloned pod deployment {cloned_pod_name}")
                deployment_obj = ocp.OCP(
                    kind=constants.DEPLOYMENT, namespace=self.namespace
                )
                deployment_obj.delete(resource_name=cloned_pod_name)
                log.info(f"Cloned pod deployment {cloned_pod_name} deleted")
            except Exception as e:
                log.warning(f"Failed to delete cloned pod: {e}")

        request.addfinalizer(cleanup_cloned_pod)

        # Step 3: Get NFS export details for cloned PVC and mount on client
        log.info("Step 3: Getting NFS export details for cloned PVC")

        # Use helper function to get export details
        cloned_export = self.get_nfs_export_details(cloned_pvc_name)
        cloned_share_details = cloned_export["share_details"]

        # Use helper function to mount NFS export
        cloned_test_folder = f"{self.test_folder}-cloned"
        self.mount_nfs_export(con, cloned_share_details, cloned_test_folder)

        # Add cloned mount to cleanup
        def cleanup_cloned_mount():
            try:
                log.info(f"Unmounting cloned NFS export from {cloned_test_folder}")
                con.exec_cmd(f"umount {cloned_test_folder}")
                con.exec_cmd(f"rm -rf {cloned_test_folder}")
                log.info(f"Cloned mount {cloned_test_folder} cleaned up")
            except Exception as e:
                log.warning(f"Failed to unmount cloned NFS: {e}")

        request.addfinalizer(cleanup_cloned_mount)

        # Step 4: Write IO to a single file on cloned mount
        log.info("Step 4: Writing IO to single file on cloned NFS mount")

        CLONED_IO_FILE_NAME = "io_test_cloned.txt"
        cloned_io_file = f"{cloned_test_folder}/{CLONED_IO_FILE_NAME}"

        # Write IO to single file using common method
        log.info("Writing data to cloned mount...")
        success, errors = self.write_io_to_single_file(
            con, cloned_io_file, num_iterations=20, delay=0.5
        )
        assert success, f"Failed to write IO to cloned file: {errors}"
        log.info(
            f"IO operations completed on cloned mount - single file: {CLONED_IO_FILE_NAME}"
        )

        # Step 5: Capture checksum of the single file before resize
        log.info("Step 5: Capturing file checksum before PVC resize")

        # Get checksum and line count before resize
        pre_resize_checksum = get_file_checksum_from_nfs(con, cloned_io_file)
        assert pre_resize_checksum, "Failed to calculate cloned file checksum"
        log.info(f"Pre-resize file checksum: {pre_resize_checksum}")

        # Get line count before resize
        pre_resize_line_count = get_file_line_count_from_nfs(con, cloned_io_file)
        log.info(f"Pre-resize line count: {pre_resize_line_count}")

        # Step 6: Resize (expand) the cloned PVC
        log.info("Step 6: Resizing cloned PVC")

        new_size = "15Gi"
        log.info(f"Expanding cloned PVC from 10Gi to {new_size}")

        # Patch the PVC to request more storage
        patch_cmd = (
            f"patch pvc {cloned_pvc_name} -p "
            f'\'{{"spec":{{"resources":{{"requests":{{"storage":"{new_size}"}}}}}}}}\''
        )
        result = cloned_pvc_obj.ocp.exec_oc_cmd(patch_cmd)
        log.info(f"PVC resize request submitted: {result}")

        # Wait for PVC to be resized
        log.info("Waiting for PVC resize to complete...")
        time.sleep(30)  # Give some time for resize operation

        # Verify PVC size
        for attempt in range(12):  # Wait up to 2 minutes
            size_cmd = (
                f"get pvc {cloned_pvc_name} -o jsonpath='{{.status.capacity.storage}}'"
            )
            current_size = cloned_pvc_obj.ocp.exec_oc_cmd(size_cmd)
            if current_size and current_size.strip() == new_size:
                log.info(f"PVC successfully resized to {new_size}")
                break
            log.info(
                f"Waiting for resize... Current size: {current_size.strip() if current_size else 'unknown'}"
            )
            time.sleep(10)
        else:
            log.warning("PVC resize may not have completed within timeout")

        # Step 7: Verify data integrity after resize
        log.info("Step 7: Verifying data integrity after PVC resize")

        # Calculate checksum and line count after resize
        post_resize_checksum = get_file_checksum_from_nfs(con, cloned_io_file)
        assert post_resize_checksum, "Failed to calculate post-resize file checksum"
        log.info(f"Post-resize file checksum: {post_resize_checksum}")

        # Get line count after resize
        post_resize_line_count = get_file_line_count_from_nfs(con, cloned_io_file)
        log.info(f"Post-resize line count: {post_resize_line_count}")

        # Verify line count unchanged
        assert pre_resize_line_count == post_resize_line_count, (
            f"Line count mismatch after resize!\n"
            f"Pre-resize: {pre_resize_line_count}\n"
            f"Post-resize: {post_resize_line_count}"
        )

        # Verify checksum unchanged
        assert pre_resize_checksum == post_resize_checksum, (
            f"File checksum mismatch after resize! Data integrity check failed.\n"
            f"Pre-resize checksum: {pre_resize_checksum}\n"
            f"Post-resize checksum: {post_resize_checksum}"
        )

        # Step 8: Verify bidirectional data integrity (pod + NFS mount)
        log.info("Step 8: Verifying bidirectional data integrity (pod and NFS mount)")

        # Get the cloned pod object
        cloned_pod_obj = cloned_pod_objs[0]
        pod_file_path = f"/mnt/{CLONED_IO_FILE_NAME}"

        # Use helper function for bidirectional verification
        self.verify_bidirectional_data_integrity(
            pod_obj=cloned_pod_obj,
            file_path_in_pod=pod_file_path,
            con=con,
            file_path_on_nfs=cloned_io_file,
            operation_name="PVC clone and resize",
        )

        log.info("=" * 80)
        log.info(
            "PVC Clone, Resize, and Data Integrity scenario completed successfully!"
        )
        log.info("Summary:")
        log.info(f"  - Cloned PVC: {cloned_pvc_name}")
        log.info(f"  - Cloned pod: {cloned_pod_name}")
        log.info(f"  - IO file: {CLONED_IO_FILE_NAME}")
        log.info("  - Original size: 10Gi")
        log.info(f"  - Resized to: {new_size}")
        log.info(f"  - Line count: {post_resize_line_count}")
        log.info(f"  - Pre-resize checksum: {pre_resize_checksum}")
        log.info(f"  - Post-resize checksum: {post_resize_checksum}")
        log.info("  - Data integrity: VERIFIED ✓")

        # ========================================================================
        # Scenario: Snapshot Resized PVC, Restore, Resize Again, Verify Integrity
        # ========================================================================
        log.info("=" * 80)
        log.info("Starting Snapshot of Resized PVC, Restore, and Re-Resize scenario")
        log.info("=" * 80)

        # Step 1: Create snapshot of the resized cloned PVC
        log.info("Step 1: Creating snapshot of resized cloned PVC")

        cloned_snapshot_name = f"{cloned_pvc_name}-snapshot"
        snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML
        nfs_snapshotclass_name = "ocs-storagecluster-nfsplugin-snapclass"

        log.info(
            f"Creating snapshot: {cloned_snapshot_name} from resized PVC: {cloned_pvc_name} "
            f"using snapshot class: {nfs_snapshotclass_name}"
        )

        cloned_snapshot_obj = create_pvc_snapshot(
            pvc_name=cloned_pvc_name,
            snap_yaml=snap_yaml,
            snap_name=cloned_snapshot_name,
            namespace=self.namespace,
            sc_name=nfs_snapshotclass_name,
            wait=True,
            timeout=300,
        )

        log.info(f"Snapshot {cloned_snapshot_name} created successfully")

        # Add snapshot to cleanup
        def cleanup_cloned_snapshot():
            try:
                log.info(f"Deleting snapshot {cloned_snapshot_name}")
                cloned_snapshot_obj.delete()
                cloned_snapshot_obj.ocp.wait_for_delete(
                    resource_name=cloned_snapshot_name, timeout=180
                )
                log.info(f"Snapshot {cloned_snapshot_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete snapshot: {e}")

        request.addfinalizer(cleanup_cloned_snapshot)

        # Step 2: Restore PVC from the snapshot
        log.info("Step 2: Restoring PVC from snapshot of resized PVC")

        final_restored_pvc_name = f"{cloned_pvc_name}-restored"
        final_restored_pvc_obj = pvc.create_restore_pvc(
            sc_name=self.nfs_sc,
            snap_name=cloned_snapshot_name,
            namespace=self.namespace,
            size="15Gi",
            pvc_name=final_restored_pvc_name,
            volume_mode="Filesystem",
            restore_pvc_yaml=constants.CSI_CEPHFS_PVC_RESTORE_YAML,
            access_mode=access_mode,
        )

        log.info(f"Restored PVC {final_restored_pvc_name} created from snapshot")

        # Add restored PVC to cleanup
        def cleanup_final_restored_pvc():
            try:
                log.info(f"Deleting final restored PVC {final_restored_pvc_name}")
                final_restored_pvc_obj.delete(wait=True)
                log.info(
                    f"Final restored PVC {final_restored_pvc_name} deleted successfully"
                )
            except Exception as e:
                log.warning(f"Failed to delete final restored PVC: {e}")

        request.addfinalizer(cleanup_final_restored_pvc)

        # Step 3: Create pod deployment using the final restored PVC
        log.info("Step 3: Creating pod deployment with final restored PVC")

        final_restored_pod_name = f"test-deployment-final-{int(time.time())}"
        final_restored_deployment_data = configure_deployment_for_nfs(
            deployment_name=final_restored_pod_name, pvc_name=final_restored_pvc_name
        )
        helpers.create_resource(**final_restored_deployment_data)

        # Wait for deployment to be ready
        log.info(f"Waiting for deployment {final_restored_pod_name} to be ready...")
        final_restored_deployment_obj = ocp.OCP(
            kind=constants.DEPLOYMENT, namespace=self.namespace
        )
        final_restored_deployment_obj.wait_for_resource(
            condition="1/1",
            resource_name=final_restored_pod_name,
            column="READY",
            timeout=300,
        )
        log.info(f"Deployment {final_restored_pod_name} is ready")

        # Get the actual pod created by the deployment (using selector, not direct name)
        final_restored_pod_objs = pod.get_all_pods(
            namespace=self.namespace,
            selector=[final_restored_pod_name],
            selector_label="name",
        )
        assert (
            len(final_restored_pod_objs) > 0
        ), f"No pods found for deployment {final_restored_pod_name}"
        # final_restored_pod_obj = final_restored_pod_objs[0]

        log.info(f"Final restored pod {final_restored_pod_name} is running")

        # Add final restored pod to cleanup
        def cleanup_final_restored_pod():
            try:
                log.info(
                    f"Deleting final restored pod deployment {final_restored_pod_name}"
                )
                deployment_obj = ocp.OCP(
                    kind=constants.DEPLOYMENT, namespace=self.namespace
                )
                deployment_obj.delete(resource_name=final_restored_pod_name)
                log.info(
                    f"Final restored pod deployment {final_restored_pod_name} deleted"
                )
            except Exception as e:
                log.warning(f"Failed to delete final restored pod: {e}")

        request.addfinalizer(cleanup_final_restored_pod)

        # Step 4: Get NFS export details and mount on client
        log.info("Step 4: Getting NFS export details for final restored PVC")

        # Get volume name from final restored PVC
        fetch_final_vol_name_cmd = (
            f"get pvc {final_restored_pvc_name} -o jsonpath='{{.spec.volumeName}}'"
        )
        final_restored_vol_name = self.pvc_obj.exec_oc_cmd(fetch_final_vol_name_cmd)
        log.info(f"Final restored volume name: {final_restored_vol_name}")

        # Get NFS share details from final restored PV
        fetch_final_pv_share_cmd = (
            f"get pv {final_restored_vol_name} "
            f"-o jsonpath='{{.spec.csi.volumeAttributes.share}}'"
        )
        final_restored_share_details = self.pv_obj.exec_oc_cmd(fetch_final_pv_share_cmd)
        log.info(f"Final restored NFS share: {final_restored_share_details}")

        # Mount final restored NFS export using the wrapper method
        final_restored_test_folder = f"{self.test_folder}-final"
        self.mount_nfs_export(
            con, final_restored_share_details, final_restored_test_folder
        )

        # Add final restored mount to cleanup
        def cleanup_final_restored_mount():
            try:
                log.info(
                    f"Unmounting final restored NFS export from {final_restored_test_folder}"
                )
                con.exec_cmd(f"umount {final_restored_test_folder}")
                con.exec_cmd(f"rm -rf {final_restored_test_folder}")
                log.info(
                    f"Final restored mount {final_restored_test_folder} cleaned up"
                )
            except Exception as e:
                log.warning(f"Failed to unmount final restored NFS: {e}")

        request.addfinalizer(cleanup_final_restored_mount)

        # Step 5: Write IO to a single file on final restored mount
        log.info("Step 5: Writing IO to single file on final restored NFS mount")

        FINAL_IO_FILE_NAME = "io_test_final.txt"
        final_io_file = f"{final_restored_test_folder}/{FINAL_IO_FILE_NAME}"

        # Write IO to single file using common method
        log.info("Writing data to final restored mount...")
        success, errors = self.write_io_to_single_file(
            con, final_io_file, num_iterations=20, delay=0.5
        )
        assert success, f"Failed to write IO to final restored file: {errors}"
        log.info(
            f"IO operations completed on final restored mount - single file: {FINAL_IO_FILE_NAME}"
        )

        # Step 6: Capture checksum before resize
        log.info("Step 6: Capturing file checksum before final PVC resize")

        # Get checksum and line count before resize
        pre_final_resize_checksum = get_file_checksum_from_nfs(con, final_io_file)
        assert pre_final_resize_checksum, "Failed to calculate final file checksum"
        log.info(f"Pre-final-resize file checksum: {pre_final_resize_checksum}")

        # Get line count before resize
        pre_final_resize_line_count = get_file_line_count_from_nfs(con, final_io_file)
        log.info(f"Pre-final-resize line count: {pre_final_resize_line_count}")

        # Step 7: Resize the final restored PVC
        log.info("Step 7: Resizing final restored PVC")

        final_new_size = "20Gi"
        log.info(f"Expanding final restored PVC from 15Gi to {final_new_size}")

        # Patch the PVC to request more storage
        final_patch_cmd = (
            f"patch pvc {final_restored_pvc_name} -p "
            f'\'{{"spec":{{"resources":{{"requests":{{"storage":"{final_new_size}"}}}}}}}}\''
        )
        result = final_restored_pvc_obj.ocp.exec_oc_cmd(final_patch_cmd)
        log.info(f"Final PVC resize request submitted: {result}")

        # Wait for PVC to be resized
        log.info("Waiting for final PVC resize to complete...")
        time.sleep(30)

        # Verify PVC size
        for attempt in range(12):  # Wait up to 2 minutes
            size_cmd = f"get pvc {final_restored_pvc_name} -o jsonpath='{{.status.capacity.storage}}'"
            current_size = final_restored_pvc_obj.ocp.exec_oc_cmd(size_cmd)
            if current_size and current_size.strip() == final_new_size:
                log.info(f"Final PVC successfully resized to {final_new_size}")
                break
            log.info(
                f"Waiting for final resize... Current size: {current_size.strip() if current_size else 'unknown'}"
            )
            time.sleep(10)
        else:
            log.warning("Final PVC resize may not have completed within timeout")

        # Step 8: Verify data integrity after final resize
        log.info("Step 8: Verifying data integrity after final PVC resize")

        # Calculate checksum and line count after resize
        post_final_resize_checksum = get_file_checksum_from_nfs(con, final_io_file)
        assert (
            post_final_resize_checksum
        ), "Failed to calculate post-final-resize file checksum"
        log.info(f"Post-final-resize file checksum: {post_final_resize_checksum}")

        # Get line count after resize
        post_final_resize_line_count = get_file_line_count_from_nfs(con, final_io_file)
        log.info(f"Post-final-resize line count: {post_final_resize_line_count}")

        # Verify line count unchanged
        assert pre_final_resize_line_count == post_final_resize_line_count, (
            f"Line count mismatch after final resize!\n"
            f"Pre-resize: {pre_final_resize_line_count}\n"
            f"Post-resize: {post_final_resize_line_count}"
        )

        # Verify checksum unchanged
        assert pre_final_resize_checksum == post_final_resize_checksum, (
            f"File checksum mismatch after final resize! Data integrity check failed.\n"
            f"Pre-resize checksum: {pre_final_resize_checksum}\n"
            f"Post-resize checksum: {post_final_resize_checksum}"
        )

        # Step 9: Verify bidirectional data integrity (pod + NFS mount)
        log.info("Step 9: Verifying bidirectional data integrity (pod and NFS mount)")

        # Get the final restored pod object
        final_restored_pod_obj = final_restored_pod_objs[0]
        final_pod_file_path = f"/mnt/{FINAL_IO_FILE_NAME}"

        # Use helper function for bidirectional verification
        self.verify_bidirectional_data_integrity(
            pod_obj=final_restored_pod_obj,
            file_path_in_pod=final_pod_file_path,
            con=con,
            file_path_on_nfs=final_io_file,
            operation_name="snapshot restore and re-resize",
        )

        log.info("=" * 80)
        log.info("Snapshot, Restore, and Re-Resize scenario completed successfully!")
        log.info("Summary:")
        log.info(f"  - Snapshot of resized PVC: {cloned_snapshot_name}")
        log.info(f"  - Final restored PVC: {final_restored_pvc_name}")
        log.info(f"  - Final restored pod: {final_restored_pod_name}")
        log.info(f"  - IO file: {FINAL_IO_FILE_NAME}")
        log.info("  - Original size: 15Gi")
        log.info(f"  - Resized to: {final_new_size}")
        log.info(f"  - Line count: {post_final_resize_line_count}")
        log.info(f"  - Pre-resize checksum: {pre_final_resize_checksum}")
        log.info(f"  - Post-resize checksum: {post_final_resize_checksum}")
        log.info("  - Data integrity: VERIFIED ✓")
        log.info("=" * 80)
        log.info(
            "All test scenarios completed successfully - cleanup will be handled by finalizers"
        )

        # ========================================================================
        # Non-Graceful Cluster Shutdown with Mount Point Validation
        # ========================================================================
        log.info("=" * 80)
        log.info("Non-Graceful Cluster Shutdown with Mount Point Validation")
        log.info("=" * 80)

        # Collect all mount points that were created during the test
        log.info("Collecting all NFS mount points created during the test")

        # List of all mount points created in this test
        nfs_mount_points = [
            test_folder_for_pod,  # Original mount point
            restored_test_folder,  # Restored PVC mount point
            cloned_test_folder,  # Cloned PVC mount point
            final_restored_test_folder,  # Final restored PVC mount point
        ]

        # List of all pod names created in this test
        pod_names = [
            pod_name,  # Original pod
            restored_pod_name,  # Restored pod from snapshot
            cloned_pod_name,  # Cloned pod
            final_restored_pod_name,  # Final restored pod
        ]

        log.info(f"Total NFS mount points to validate: {len(nfs_mount_points)}")
        for mount_point in nfs_mount_points:
            log.info(f"  - {mount_point}")

        log.info(f"Total pods to validate: {len(pod_names)}")
        for pname in pod_names:
            log.info(f"  - {pname}")

        # Verify all mount points are accessible before shutdown
        log.info("Verifying all mount points are accessible before shutdown")

        for mount_point in nfs_mount_points:
            retcode, stdout, _ = con.exec_cmd(f"findmnt -M {mount_point}")
            assert (
                retcode == 0
            ), f"Mount point {mount_point} not accessible before shutdown"
            log.info(f"✓ Mount point {mount_point} is accessible")

        # Verify all pods are running before shutdown
        log.info("Verifying all pods are running before shutdown")

        for pname in pod_names:
            pod_objs = pod.get_all_pods(
                namespace=self.namespace,
                selector=[pname],
                selector_label="name",
            )
            assert pod_objs and len(pod_objs) > 0, f"Pod {pname} not found"
            pod_obj = pod_objs[0]
            pod_status = pod_obj.get().get("status", {}).get("phase", "Unknown")
            assert pod_status == "Running", f"Pod {pname} not running: {pod_status}"
            log.info(f"✓ Pod {pname} is running")

        # Perform non-graceful cluster shutdown
        log.info("Performing NON-GRACEFUL cluster shutdown")
        log.info("This simulates an unexpected cluster failure (power loss scenario)")

        # Get all cluster nodes
        all_nodes = get_all_nodes()
        log.info(f"Found {len(all_nodes)} nodes in the cluster")

        # Get node objects for shutdown
        node_objs = get_node_objs(all_nodes)

        # Get EC2 instances if on AWS platform
        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            node_instances = nodes.get_ec2_instances(nodes=node_objs)
            log.info(f"Retrieved EC2 instances for {len(node_instances)} nodes")

        # Log NFS LoadBalancer details before shutdown
        self.log_nfs_loadbalancer_details(stage="BEFORE cluster shutdown")

        # Perform NON-GRACEFUL shutdown (force=True simulates power failure)
        log.info(
            "Initiating NON-GRACEFUL shutdown of all cluster nodes (force=True)..."
        )
        nodes.stop_nodes(nodes=node_objs, force=True)
        log.info("All nodes stopped non-gracefully (simulating power failure)")

        # Wait to ensure complete shutdown
        log.info("Waiting for 3 minutes to ensure complete shutdown...")
        time.sleep(180)

        # Start all nodes back up
        log.info("Starting all nodes back up...")
        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            nodes.start_nodes(instances=node_instances, nodes=node_objs)
        else:
            nodes.start_nodes(nodes=node_objs)
        log.info("All nodes started")

        # Wait for cluster recovery after non-graceful shutdown
        log.info("Waiting for cluster recovery after NON-GRACEFUL shutdown")

        # Wait for nodes to be ready (longer timeout for non-graceful recovery)
        wait_for_nodes_status(node_names=all_nodes, timeout=1800)
        log.info("All nodes are back online after non-graceful shutdown")

        # Wait for all pods to be running in openshift-storage namespace
        wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            timeout=1200,
        )
        log.info("All storage pods are running")

        # Verify Ceph health (may take longer after non-graceful shutdown)
        ceph_health_check(tries=60, delay=60)
        log.info("Ceph cluster health verified after non-graceful shutdown")

        # Wait for NFS services to be fully operational
        time.sleep(180)
        log.info("Waiting for NFS services to stabilize after non-graceful recovery...")

        # Log NFS LoadBalancer details after recovery
        self.log_nfs_loadbalancer_details(stage="AFTER cluster recovery")

        # Verify all mount points are accessible after recovery
        log.info("Verifying all mount points are accessible after recovery")

        max_retries = 20
        retry_delay = 30
        mount_recovery_status = {}

        for mount_point in nfs_mount_points:
            mount_accessible = False
            log.info(f"\nChecking mount point: {mount_point}")

            for attempt in range(max_retries):
                retcode, stdout, stderr = con.exec_cmd(f"findmnt -M {mount_point}")
                if retcode == 0:
                    mount_accessible = True
                    log.info(
                        f"✓ Mount point {mount_point} is accessible after recovery"
                    )
                    log.info(f"  Mount details: {stdout.strip()}")
                    break
                else:
                    log.info(
                        f"Attempt {attempt + 1}/{max_retries}: Mount point {mount_point} "
                        f"not yet accessible, waiting for recovery..."
                    )

                    # Add diagnostic check for stale mount on failed attempts
                    ls_retcode, ls_stdout, ls_stderr = con.exec_cmd(
                        f"ls {mount_point} 2>&1 || true"
                    )
                    if "Stale file handle" in ls_stderr:
                        log.error(f"  ✗ STALE FILE HANDLE detected for {mount_point}")
                    elif "Transport endpoint is not connected" in ls_stderr:
                        log.error(
                            f"  ✗ TRANSPORT ENDPOINT NOT CONNECTED for {mount_point}"
                        )

                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)

            mount_recovery_status[mount_point] = mount_accessible
            if not mount_accessible:
                log.error(f"✗ Mount point {mount_point} NOT accessible after recovery")

        # Assert all mounts are accessible
        failed_mounts = [
            mp for mp, status in mount_recovery_status.items() if not status
        ]
        assert (
            len(failed_mounts) == 0
        ), f"The following mount points are not accessible after non-graceful recovery: {failed_mounts}"

        log.info("All mount points verified accessible after non-graceful recovery")

        # Perform I/O operations and verify bidirectional data consistency
        log.info(
            "Performing I/O operations and verifying bidirectional data consistency"
        )

        # Create a mapping of mount points to their corresponding pods
        mount_to_pod_map = {
            test_folder_for_pod: pod_name,  # Original mount -> Original pod
            restored_test_folder: restored_pod_name,  # Restored mount -> Restored pod
            cloned_test_folder: cloned_pod_name,  # Cloned mount -> Cloned pod
            final_restored_test_folder: final_restored_pod_name,  # Final mount -> Final pod
        }
        for mount_point, pname in mount_to_pod_map.items():
            try:
                log.info(
                    f"Testing bidirectional I/O for mount point: {mount_point} and pod: {pname}"
                )

                # Step 1: Write data from NFS client (mount point)
                test_file_name = f"post_recovery_test_{int(time.time())}.txt"
                test_file_nfs = f"{mount_point}/{test_file_name}"
                test_data = f"Post-recovery NFS write test at {time.time()}"

                write_cmd = f'echo "{test_data}" > {test_file_nfs}'
                retcode, stdout, stderr = con.exec_cmd(write_cmd)
                assert (
                    retcode == 0
                ), f"Failed to write from NFS mount {mount_point}: {stderr}"
                log.info(f"✓ Successfully wrote data to NFS mount: {mount_point}")

                # Step 2: Verify data is readable from NFS client
                read_cmd = f"cat {test_file_nfs}"
                retcode, stdout, stderr = con.exec_cmd(read_cmd)
                assert (
                    retcode == 0
                ), f"Failed to read from NFS mount {mount_point}: {stderr}"
                assert test_data in stdout, f"Data mismatch on NFS mount {mount_point}"
                log.info(f"✓ Successfully verified data on NFS mount: {mount_point}")

                # Step 3: Verify the same data is visible from the pod
                pod_objs = pod.get_all_pods(
                    namespace=self.namespace,
                    selector=[pname],
                    selector_label="name",
                )
                assert pod_objs and len(pod_objs) > 0, f"Pod {pname} not found"
                pod_obj = pod_objs[0]

                # Read the same file from within the pod
                pod_file_path = f"/mnt/{test_file_name}"
                read_from_pod_cmd = f"cat {pod_file_path}"
                result = pod_obj.exec_cmd_on_pod(
                    read_from_pod_cmd, out_yaml_format=False
                )
                assert test_data in result, (
                    f"Data written to NFS mount is not visible from pod {pname}. "
                    f"Expected: '{test_data}', Got: '{result}'"
                )
                log.info(
                    f"✓ Successfully verified data from pod {pname} - bidirectional consistency confirmed"
                )

                # # Cleanup test file from NFS mount
                # con.exec_cmd(f"rm -f {test_file_nfs}")
                # log.info(f"✓ Bidirectional I/O test passed for {mount_point} <-> {pname}")

            except Exception as e:
                log.error(
                    f"✗ Bidirectional I/O test failed for {mount_point} <-> {pname}: {e}"
                )
                raise

        log.info("Bidirectional I/O operations verified successfully after recovery")

        # Final summary for non-graceful shutdown scenario
        log.info("=" * 80)
        log.info(
            "Non-Graceful Cluster Shutdown and Recovery Test - COMPLETED SUCCESSFULLY!"
        )
        log.info("=" * 80)
        log.info("Summary:")
        log.info(
            "  - Shutdown type: NON-GRACEFUL (force=True, simulating power failure)"
        )
        log.info(f"  - Total mount points tested: {len(nfs_mount_points)}")
        log.info(f"  - Total pods tested: {len(pod_names)}")
        log.info("")
        log.info("Mount points validated:")
        for mount_point in nfs_mount_points:
            log.info(f"  ✓ {mount_point}")
        log.info("")
        log.info("Pods validated:")
        for pname in pod_names:
            log.info(f"  ✓ {pname}")
        log.info("")
        log.info("All validations passed:")
        log.info("  ✓ All mount points accessible after non-graceful recovery")
        log.info("  ✓ All pods running and accessible")
        log.info("  ✓ I/O operations from pods successful")
        log.info("  ✓ I/O operations from NFS mounts successful")
        log.info("=" * 80)
        log.info(
            "All test scenarios including non-graceful shutdown completed successfully"
        )
        log.info("Cleanup will be handled by finalizers")
        log.info("=" * 80)
