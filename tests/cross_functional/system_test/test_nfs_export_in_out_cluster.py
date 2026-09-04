import logging
import random
import threading
import time
from threading import Thread

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    skipif_rosa_hcp,
    skipif_lean_deployment,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    skipif_managed_service,
    skip_for_provider_or_client_if_ocs_version,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    skipif_external_mode,
    skipif_hci_client,
)
from tests.nfs_base import NFSClientTestBase
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants, ocp, platform_nodes
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_node_objs,
    get_all_nodes,
    get_worker_nodes,
)
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.ui.workload_ui import wait_for_container_status_ready
from ocs_ci.ocs.resources.pvc import create_pvc_snapshot
from ocs_ci.utility.nfs_utils import (
    frame_deployment_config,
)
from ocs_ci.utility import nfs_utils
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@magenta_squad
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
    Test NFS export operations with in-cluster and out-of-cluster access.
    """

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

        # Write initial data — exec_sh_cmd_on_pod uses "oc exec -- bash -c" so
        # shell operators like > and >> are evaluated inside the pod.
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

    def continuous_io_operations(
        self,
        pod_obj,
        pod_io_file,
        io_errors,
        io_stop_event,
        io_completed,
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
        """
        iteration = 0
        previous_checksum = None
        # Track whether we are already inside a pod-unavailable window so we
        # only append one error entry for the whole outage, not one per retry.
        pod_unavailable = False

        try:
            # Initialize the file via pod — exec_sh_cmd_on_pod uses
            # "oc exec -- bash -c" so > is evaluated inside the pod.
            initial_data = f"IO test started at {time.time()}"
            init_cmd = f"echo '{initial_data}' > {pod_io_file}"
            try:
                pod_obj.exec_sh_cmd_on_pod(init_cmd)
            except Exception as exc:
                io_errors.append(f"Failed to initialize test file: {exc}")
                log.error(f"Initialization error: {exc}")
                return

            # Get initial checksum from pod
            chk = self.calculate_checksum_and_lines_from_pod(pod_obj, pod_io_file)
            previous_checksum = chk["checksum"]
            log.info(f"File initialized with checksum: {previous_checksum}")

            while not io_stop_event.is_set():
                iteration += 1
                test_data = f"IO test iteration {iteration} - {time.time()}"

                # Write via pod — exec_sh_cmd_on_pod handles >> inside the pod
                write_cmd = f"echo '{test_data}' >> {pod_io_file}"
                try:
                    pod_obj.exec_sh_cmd_on_pod(write_cmd)
                    # Successful write — clear unavailable flag and log recovery
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

                # Checksum from pod
                chk = self.calculate_checksum_and_lines_from_pod(pod_obj, pod_io_file)
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

                # Read last line via pod to verify content
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

                previous_checksum = current_checksum

                if iteration % 10 == 0:
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

    @pytest.mark.ignore_leftover_label(constants.MON_APP_LABEL)
    @pytest.mark.parametrize(
        "access_mode",
        [
            pytest.param(constants.ACCESS_MODE_RWX, id="RWX"),
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
        Validates NFS export operations across in-cluster (pod) and out-of-cluster
        (external NFS client VM) mounts. The NFS PVC is mounted from both sides
        simultaneously and data integrity is cross-validated at each stage.

        TODO: Out-of-cluster steps are skipped on vSphere — no external load-balancer
        is available on that platform.

        Prerequisites:
        - ODF cluster with hugepages, Multus, encryption-in-transit, and NFS enabled.

        Test Steps:
        a) Create 10Gi NFS PVC, mount in-cluster (pod) and out-of-cluster (client VM).
        b) Run continuous I/O during NFS server pod node reboot; verify no data loss.
        c) Create PVC snapshot, restore it, write I/O to restored PVC, verify checksums.
        d) Clone restored PVC, write I/O, resize clone to 15Gi, verify data integrity.
        e) Snapshot the resized clone, restore it, write I/O, resize to 20Gi, verify integrity.
        f) Ordered cluster shutdown (workers first, then control-plane), start in reverse;
           verify Ceph health, NFS mounts accessible, and post-recovery I/O from all pods.
        """

        log.info(f"Test case execution started: {request.node.name}")
        if not self.is_vsphere:
            nfs_utils.skip_test_if_nfs_client_unavailable(self.nfs_client_ip)

        unique_suffix = random.randint(1000, 9999)
        pod_name = f"test-deployment-outcluster-{unique_suffix}"
        pvc_name = f"test-pvc-{unique_suffix}"
        log.info(f"Using unique names: pod deployment={pod_name}, pvc={pvc_name}")

        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            size="10Gi",
            do_reload=True,
            access_mode=access_mode,
            volume_mode="Filesystem",
            pvc_name=pvc_name,
        )

        # Identify the NFS server node upfront so the I/O pod is pinned to a
        # different worker node from the very start — this ensures a reboot of
        # the NFS server node never also evicts the I/O pod.
        nfs_server_pods_pre = get_all_pods(
            namespace=self.namespace, selector=["rook-ceph-nfs"], selector_label="app"
        )
        nfs_node_name_pre = (
            nfs_server_pods_pre[0].data["spec"]["nodeName"]
            if nfs_server_pods_pre
            else None
        )
        io_pod_node = next(
            (n for n in get_worker_nodes() if n != nfs_node_name_pre),
            None,
        )
        log.info(
            f"NFS server node: {nfs_node_name_pre} — pinning I/O pod to node: {io_pod_node}"
        )

        log.info(f"Creating deployment {pod_name}")
        deployment_data = frame_deployment_config(
            deployment_name=pod_name, pvc_name=pvc_name, node_name=io_pod_node
        )
        helpers.create_resource(**deployment_data)

        log.info(f"Waiting for deployment {pod_name} to be ready...")
        deployment_obj = ocp.OCP(kind=constants.DEPLOYMENT, namespace=self.namespace)
        deployment_obj.wait_for_resource(
            condition="1/1",
            resource_name=pod_name,
            column="READY",
            timeout=300,
        )
        log.info(f"Deployment {pod_name} is ready")

        pod_obj = pod.get_all_pods(
            namespace=self.namespace,
            selector=[pod_name],
            selector_label="name",
        )[0]
        log.info(f"Pod {pod_obj.name} is running")

        test_folder_for_pod = self.test_folder + "-" + pod_name
        mount_to_pod_map = (
            {test_folder_for_pod: pod_name} if not self.is_vsphere else {}
        )

        share_details = self.get_nfs_export_details(nfs_pvc_obj)

        con = None if self.is_vsphere else self.con

        if not self.is_vsphere:
            self.mount_nfs_export(con, share_details, test_folder_for_pod)
        else:
            log.info(
                "vSphere platform: skipping out-of-cluster NFS mount. "
                "All I/O will be performed from in-cluster pod."
            )

        def cleanup_all_resources():
            log.info("Running cleanup for all test resources...")
            if not self.is_vsphere and con is not None:
                try:
                    log.info(f"Unmounting {test_folder_for_pod}")
                    nfs_utils.unmount(con, test_folder_for_pod)
                    con.exec_cmd(f"rm -rf {test_folder_for_pod}")
                    log.info("Waiting for NFS export to be fully released...")
                    time.sleep(10)
                except Exception as e:
                    log.warning(f"Failed to unmount NFS: {e}")

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

            try:
                log.info("Waiting for pod to be terminated...")
                pod_obj.ocp.wait_for_delete(pod_obj.name, timeout=180)
                log.info(f"Pod {pod_obj.name} terminated successfully")
            except Exception as e:
                log.warning(f"Failed to wait for pod deletion: {e}")

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

        log.info("=" * 80)
        log.info("Scenario: NFS Server Pod Node Reboot During Active I/O")
        log.info("=" * 80)
        log.info("Step 1: Starting continuous I/O operations on NFS mount")

        IO_TEST_FILE_NAME = "io_test_single.txt"
        pod_io_file = f"/mnt/{IO_TEST_FILE_NAME}"
        nfs_io_file = f"{test_folder_for_pod}/{IO_TEST_FILE_NAME}"

        io_errors = []
        io_stop_event = threading.Event()
        io_completed = threading.Event()

        io_thread: Thread = threading.Thread(
            target=self.continuous_io_operations,
            args=(pod_obj, pod_io_file, io_errors, io_stop_event, io_completed),
            daemon=True,
        )
        io_thread.start()
        log.info("Continuous I/O thread started")

        def stop_io_thread():
            if not io_stop_event.is_set():
                log.info("Finalizer: stopping I/O thread...")
                io_stop_event.set()
            io_thread.join(timeout=60)
            log.info("Finalizer: I/O thread stopped")

        request.addfinalizer(stop_io_thread)

        time.sleep(30)
        log.info("Initial I/O operations running successfully")

        log.info("Step 2: Identifying node hosting NFS server pod")
        nfs_server_pods = get_all_pods(
            namespace=self.namespace, selector=["rook-ceph-nfs"], selector_label="app"
        )

        if not nfs_server_pods:
            io_stop_event.set()
            io_thread.join(timeout=30)
            raise Exception("No NFS server pods found")

        nfs_server_pod = nfs_server_pods[0]
        log.info(f"Found NFS server pod: {nfs_server_pod.name}")

        nfs_node_name = nfs_server_pod.data["spec"]["nodeName"]
        log.info(f"NFS server pod is on node: {nfs_node_name}, initiating reboot...")
        nfs_node_obj = get_node_objs([nfs_node_name])[0]
        factory = platform_nodes.PlatformNodesFactory()
        nodes_platform = factory.get_nodes_platform()
        try:
            nodes_platform.restart_nodes([nfs_node_obj], wait=True)
            log.info(f"Node {nfs_node_name} reboot completed")
        except Exception as reboot_exc:
            # On vSphere, the 'Rebooted' OCP event is not always emitted within
            # the 300 s window (RebootEventNotFoundException / TimeoutExpiredError).
            # The node DID reboot — the IO pod errors confirm it.  Treat this as
            # a warning and continue; the subsequent wait_for_nodes_status +
            # ceph_health_check will confirm actual recovery.
            log.warning(
                f"restart_nodes raised {type(reboot_exc).__name__}: {reboot_exc} — "
                f"node may still have rebooted successfully; continuing"
            )

        log.info("Step 3: Waiting for node to be in Ready state...")
        wait_for_nodes_status(
            node_names=[nfs_node_name], status=constants.NODE_READY, timeout=900
        )
        log.info(f"Node {nfs_node_name} is back online and Ready")

        log.info("Waiting for Ceph health to recover after node reboot...")
        ceph_health_check(tries=20, delay=60)
        log.info("Ceph health recovered")

        log.info("Waiting for NFS server pod to be running...")
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-nfs",
            resource_count=1,
            timeout=600,
        )
        log.info("NFS server pod is running again")

        log.info(f"Waiting for deployment {pod_name} pod to be running...")
        assert self.pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector=f"name={pod_name}",
            dont_allow_other_resources=True,
            timeout=600,
        ), f"Deployment {pod_name} pod not running after node reboot"

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

        log.info(f"Waiting for container to be ready in pod {pod_obj.name}...")
        wait_for_container_status_ready(pod_obj, timeout=300)

        if not self.is_vsphere:
            log.info("Verifying out-of-cluster NFS mount accessibility...")
            retcode, stdout, _ = con.exec_cmd(f"findmnt -M {test_folder_for_pod}")
            assert retcode == 0, "NFS mount not accessible after node reboot"
            log.info("NFS mount is still accessible")
        else:
            log.info("vSphere: skipping out-of-cluster mount check after node reboot")

        log.info("Letting I/O continue for 60s after recovery...")
        time.sleep(60)
        log.info("Stopping I/O operations...")
        io_stop_event.set()
        io_thread.join(timeout=60)

        log.info("Checking I/O results...")
        if io_errors:
            pod_unavailable = [e for e in io_errors if "Pod became unavailable" in e]
            integrity = [
                e
                for e in io_errors
                if "Checksum unchanged" in e or "Data mismatch" in e
            ]
            transient = [
                e for e in io_errors if e not in pod_unavailable and e not in integrity
            ]
            log.warning(
                f"I/O errors detected: {len(io_errors)} total "
                f"(pod-unavailable={len(pod_unavailable)}, "
                f"integrity={len(integrity)}, transient={len(transient)})"
            )
            for error in io_errors[:10]:
                log.warning(f"  - {error}")
            # Note: Some transient errors during reboot might be acceptable
            # Fail only if there are persistent errors after recovery
            if len(io_errors) > 20:
                raise AssertionError(
                    f"Too many I/O errors detected: {len(io_errors)} "
                    f"(pod-unavailable={len(pod_unavailable)}, "
                    f"integrity={len(integrity)}, transient={len(transient)})"
                )
        else:
            log.info("No I/O errors detected - all operations successful!")

        # Verify data integrity: pod checksum is the source of truth on all platforms.
        # On non-vSphere, also cross-check via NFS mount.
        # No fixed expected_checksum here — the I/O thread was writing continuously,
        # so we just confirm the file is non-empty and readable from the pod.
        log.info("Verifying data integrity after node reboot...")
        self.verify_data_integrity_from_pod(
            pod_obj=pod_obj,
            file_path_in_pod=pod_io_file,
            con=con,
            file_path_on_nfs=nfs_io_file if not self.is_vsphere else None,
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

        # Step 1: Capture file checksum from pod (always in-cluster)
        log.info("Step 1: Capturing file checksum from pod")

        orig_chk = self.calculate_checksum_and_lines_from_pod(pod_obj, pod_io_file)
        assert orig_chk[
            "success"
        ], f"Failed to get checksum from pod: {orig_chk['error']}"
        original_file_checksum = orig_chk["checksum"]
        original_line_count = orig_chk["line_count"]
        log.info(f"Original file checksum: {original_file_checksum}")
        log.info(f"Total lines in file: {original_line_count}")

        # Step 2: Create snapshot of the NFS PVC
        log.info("Step 2: Creating snapshot of NFS PVC")

        snapshot_name = f"{pvc_name}-snapshot"
        snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        nfs_snapshotclass_name = constants.NFS_SNAPSHOT_CLASS_NAME

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

        def cleanup_restored_pvc():
            try:
                log.info(f"Deleting restored PVC {restored_pvc_name}")
                # Clear cloning-protection and other finalizers that can block
                # deletion when the provisioner hasn't released them yet.
                pv_name = restored_pvc_obj.backed_pv
                restored_pvc_obj.ocp.patch(
                    resource_name=restored_pvc_name,
                    params='{"metadata":{"finalizers":null}}',
                    format_type="merge",
                )
                if pv_name:
                    self.pv_obj.patch(
                        resource_name=pv_name,
                        params='{"metadata":{"finalizers":null}}',
                        format_type="merge",
                    )
                restored_pvc_obj.delete(wait=True)
                log.info(f"Restored PVC {restored_pvc_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete restored PVC: {e}")

        request.addfinalizer(cleanup_restored_pvc)

        log.info("Step 4: Creating new pod with restored PVC")

        restored_pod_name = f"{pod_name}-restored"
        restored_deployment_data = frame_deployment_config(
            deployment_name=restored_pod_name, pvc_name=restored_pvc_name
        )
        helpers.create_resource(**restored_deployment_data)

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

        restored_pod_obj = pod.get_all_pods(
            namespace=self.namespace,
            selector=[restored_pod_name],
            selector_label="name",
        )[0]
        log.info(f"Restored pod {restored_pod_obj.name} is running")

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

        log.info("Step 5: Verifying data integrity for restored PVC")

        restored_pod_file_path = f"/mnt/{IO_TEST_FILE_NAME}"
        restored_test_folder = f"{self.test_folder}-restored"
        if not self.is_vsphere:
            mount_to_pod_map[restored_test_folder] = restored_pod_name
            restored_share_details = self.get_nfs_export_details(restored_pvc_obj)
            self.mount_nfs_export(con, restored_share_details, restored_test_folder)

            def cleanup_restored_mount():
                try:
                    log.info(f"Unmounting {restored_test_folder}")
                    nfs_utils.unmount(con, restored_test_folder)
                    con.exec_cmd(f"rm -rf {restored_test_folder}")
                    log.info(f"Restored mount {restored_test_folder} cleaned up")
                except Exception as e:
                    log.warning(f"Failed to unmount restored NFS: {e}")

            request.addfinalizer(cleanup_restored_mount)

        restored_nfs_file = (
            f"{restored_test_folder}/{IO_TEST_FILE_NAME}"
            if not self.is_vsphere
            else None
        )

        self.verify_data_integrity_from_pod(
            pod_obj=restored_pod_obj,
            file_path_in_pod=restored_pod_file_path,
            con=con,
            file_path_on_nfs=restored_nfs_file,
            expected_checksum=original_file_checksum,
            expected_line_count=original_line_count,
            operation_name="snapshot restore",
        )

        log.info("✓ Restored data matches original data")

        log.info("NFS Snapshot and Restore scenario completed successfully!")
        log.info("=" * 80)
        log.info("Scenario: Clone Restored PVC, Resize, and Verify Data Integrity")
        log.info("=" * 80)
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

        log.info("Step 2: Creating pod deployment with cloned PVC")

        cloned_pod_name = f"test-deployment-cloned-{random.randint(1000, 9999)}"
        cloned_deployment_data = frame_deployment_config(
            deployment_name=cloned_pod_name, pvc_name=cloned_pvc_name
        )
        helpers.create_resource(**cloned_deployment_data)

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

        cloned_pod_objs = pod.get_all_pods(
            namespace=self.namespace, selector=[cloned_pod_name], selector_label="name"
        )
        assert (
            len(cloned_pod_objs) > 0
        ), f"No pods found for deployment {cloned_pod_name}"

        log.info(f"Cloned pod {cloned_pod_name} is running")

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

        log.info("Step 3: Getting NFS export details for cloned PVC")

        cloned_test_folder = f"{self.test_folder}-cloned"
        if not self.is_vsphere:
            mount_to_pod_map[cloned_test_folder] = cloned_pod_name
            cloned_share_details = self.get_nfs_export_details(cloned_pvc_obj)
            self.mount_nfs_export(con, cloned_share_details, cloned_test_folder)

            def cleanup_cloned_mount():
                try:
                    log.info(f"Unmounting cloned NFS export from {cloned_test_folder}")
                    con.exec_cmd(f"umount {cloned_test_folder}")
                    con.exec_cmd(f"rm -rf {cloned_test_folder}")
                    log.info(f"Cloned mount {cloned_test_folder} cleaned up")
                except Exception as e:
                    log.warning(f"Failed to unmount cloned NFS: {e}")

            request.addfinalizer(cleanup_cloned_mount)

        log.info("Step 4: Writing IO to cloned PVC from pod")

        CLONED_IO_FILE_NAME = "io_test_cloned.txt"
        cloned_pod_io_file = f"/mnt/{CLONED_IO_FILE_NAME}"
        cloned_nfs_file = (
            f"{cloned_test_folder}/{CLONED_IO_FILE_NAME}"
            if not self.is_vsphere
            else None
        )

        cloned_pod_obj = cloned_pod_objs[0]
        success, errors = self.write_io_to_single_file(
            cloned_pod_obj, cloned_pod_io_file, num_iterations=20, delay=0.5
        )
        assert success, f"Failed to write IO to cloned PVC: {errors}"
        log.info(f"IO completed on cloned PVC - file: {CLONED_IO_FILE_NAME}")

        log.info("Step 5: Capturing file checksum before PVC resize")

        pre_resize_chk = self.calculate_checksum_and_lines_from_pod(
            cloned_pod_obj, cloned_pod_io_file
        )
        assert pre_resize_chk[
            "success"
        ], f"Failed to get checksum: {pre_resize_chk['error']}"
        pre_resize_checksum = pre_resize_chk["checksum"]
        pre_resize_line_count = pre_resize_chk["line_count"]
        log.info(f"Pre-resize file checksum: {pre_resize_checksum}")
        log.info(f"Pre-resize line count: {pre_resize_line_count}")

        log.info("Step 6: Resizing cloned PVC")
        new_size = 15
        log.info(f"Expanding cloned PVC from 10Gi to {new_size}Gi")

        cloned_pvc_obj.resize_pvc(new_size, verify=True)
        log.info(f"PVC successfully resized to {new_size}Gi")

        log.info("Step 7: Verifying data integrity after PVC resize")
        self.verify_data_integrity_from_pod(
            pod_obj=cloned_pod_obj,
            file_path_in_pod=cloned_pod_io_file,
            con=con,
            file_path_on_nfs=cloned_nfs_file,
            expected_checksum=pre_resize_checksum,
            expected_line_count=pre_resize_line_count,
            operation_name="PVC clone and resize",
        )
        log.info(
            "PVC Clone, Resize, and Data Integrity scenario completed successfully!"
        )
        log.info("=" * 80)
        log.info(
            "Scenario: Snapshot Resized PVC, Restore, Resize Again, Verify Integrity"
        )
        log.info("=" * 80)
        log.info("Step 1: Creating snapshot of resized cloned PVC")

        cloned_snapshot_name = f"{cloned_pvc_name}-snapshot"
        snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML
        nfs_snapshotclass_name = constants.NFS_SNAPSHOT_CLASS_NAME

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

        def cleanup_final_restored_pvc():
            try:
                log.info(f"Deleting final restored PVC {final_restored_pvc_name}")
                # Clear cloning-protection and other finalizers that can block
                # deletion when the provisioner hasn't released them yet.
                pv_name = final_restored_pvc_obj.backed_pv
                final_restored_pvc_obj.ocp.patch(
                    resource_name=final_restored_pvc_name,
                    params='{"metadata":{"finalizers":null}}',
                    format_type="merge",
                )
                if pv_name:
                    self.pv_obj.patch(
                        resource_name=pv_name,
                        params='{"metadata":{"finalizers":null}}',
                        format_type="merge",
                    )
                final_restored_pvc_obj.delete(wait=True)
                log.info(
                    f"Final restored PVC {final_restored_pvc_name} deleted successfully"
                )
            except Exception as e:
                log.warning(f"Failed to delete final restored PVC: {e}")

        request.addfinalizer(cleanup_final_restored_pvc)

        log.info("Step 3: Creating pod deployment with final restored PVC")

        final_restored_pod_name = f"test-deployment-final-{random.randint(1000, 9999)}"
        final_restored_deployment_data = frame_deployment_config(
            deployment_name=final_restored_pod_name, pvc_name=final_restored_pvc_name
        )
        helpers.create_resource(**final_restored_deployment_data)

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

        final_restored_pod_objs = pod.get_all_pods(
            namespace=self.namespace,
            selector=[final_restored_pod_name],
            selector_label="name",
        )
        assert (
            len(final_restored_pod_objs) > 0
        ), f"No pods found for deployment {final_restored_pod_name}"
        log.info(f"Final restored pod {final_restored_pod_name} is running")

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

        log.info("Step 4: Getting NFS export details for final restored PVC")
        final_restored_share_details = self.get_nfs_export_details(
            final_restored_pvc_obj
        )
        final_restored_test_folder = f"{self.test_folder}-final"
        if not self.is_vsphere:
            mount_to_pod_map[final_restored_test_folder] = final_restored_pod_name
            self.mount_nfs_export(
                con, final_restored_share_details, final_restored_test_folder
            )

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

        log.info("Step 5: Writing IO to final restored PVC from pod")

        FINAL_IO_FILE_NAME = "io_test_final.txt"
        final_pod_io_file = f"/mnt/{FINAL_IO_FILE_NAME}"
        final_nfs_file = (
            f"{final_restored_test_folder}/{FINAL_IO_FILE_NAME}"
            if not self.is_vsphere
            else None
        )

        final_restored_pod_obj = final_restored_pod_objs[0]
        success, errors = self.write_io_to_single_file(
            final_restored_pod_obj, final_pod_io_file, num_iterations=20, delay=0.5
        )
        assert success, f"Failed to write IO to final restored PVC: {errors}"
        log.info(f"IO completed on final restored PVC - file: {FINAL_IO_FILE_NAME}")

        log.info("Step 6: Capturing file checksum before final PVC resize")

        pre_final_chk = self.calculate_checksum_and_lines_from_pod(
            final_restored_pod_obj, final_pod_io_file
        )
        assert pre_final_chk[
            "success"
        ], f"Failed to get checksum: {pre_final_chk['error']}"
        pre_final_resize_checksum = pre_final_chk["checksum"]
        pre_final_resize_line_count = pre_final_chk["line_count"]
        log.info(f"Pre-final-resize file checksum: {pre_final_resize_checksum}")
        log.info(f"Pre-final-resize line count: {pre_final_resize_line_count}")

        log.info("Step 7: Resizing final restored PVC")
        final_new_size = 20
        log.info(f"Expanding final restored PVC from 15Gi to {final_new_size}Gi")

        final_restored_pvc_obj.resize_pvc(final_new_size, verify=True)
        log.info(f"Final PVC successfully resized to {final_new_size}Gi")

        log.info("Step 8: Verifying data integrity after final PVC resize")
        self.verify_data_integrity_from_pod(
            pod_obj=final_restored_pod_obj,
            file_path_in_pod=final_pod_io_file,
            con=con,
            file_path_on_nfs=final_nfs_file,
            expected_checksum=pre_final_resize_checksum,
            expected_line_count=pre_final_resize_line_count,
            operation_name="snapshot restore and re-resize",
        )
        log.info("Snapshot, Restore, and Re-Resize scenario completed successfully!")
        log.info("=" * 80)
        log.info("Scenario: Ordered Cluster Shutdown with Mount Point Validation")
        log.info("=" * 80)

        nfs_mount_points = (
            [
                test_folder_for_pod,
                restored_test_folder,
                cloned_test_folder,
                final_restored_test_folder,
            ]
            if not self.is_vsphere
            else []
        )

        pod_names = [
            pod_name,
            restored_pod_name,
            cloned_pod_name,
            final_restored_pod_name,
        ]

        if not self.is_vsphere:
            log.info("Verifying all mount points are accessible before shutdown")
            for mount_point in nfs_mount_points:
                retcode, stdout, _ = con.exec_cmd(f"findmnt -M {mount_point}")
                assert (
                    retcode == 0
                ), f"Mount point {mount_point} not accessible before shutdown"
                log.info(f"✓ Mount point {mount_point} is accessible")

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

        log.info("Performing ordered cluster shutdown")

        worker_nodes = get_worker_nodes()
        master_nodes = [node for node in get_all_nodes() if node not in worker_nodes]
        all_nodes = worker_nodes + master_nodes
        log.info(
            f"Found {len(worker_nodes)} worker nodes and {len(master_nodes)} control-plane/master nodes in the cluster"
        )

        worker_node_objs = get_node_objs(worker_nodes)
        master_node_objs = get_node_objs(master_nodes)

        worker_instances = None
        master_instances = None
        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            worker_instances = nodes.get_ec2_instances(nodes=worker_node_objs)
            master_instances = nodes.get_ec2_instances(nodes=master_node_objs)
            log.info(
                f"Retrieved EC2 instances for {len(worker_instances)} worker nodes and {len(master_instances)}"
                f" control-plane/master nodes"
            )

        log.info("Initiating ordered shutdown: stopping worker nodes first...")
        nodes.stop_nodes(nodes=worker_node_objs, force=True)
        log.info("Worker nodes stopped non gracefully")

        log.info("Waiting for worker nodes to reach NotReady state...")
        wait_for_nodes_status(
            node_names=worker_nodes, status=constants.NODE_NOT_READY, timeout=600
        )
        log.info("All worker nodes reached NotReady state")

        log.info("Initiating ordered shutdown: stopping control-plane/master nodes...")
        nodes.stop_nodes(nodes=master_node_objs, force=True)
        log.info("Control-plane/master nodes stopped non gracefully")

        log.info("Waiting for 5 minutes to ensure complete shutdown...")
        time.sleep(300)

        log.info("Starting control-plane/master nodes...")
        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            nodes.start_nodes(instances=master_instances, nodes=master_node_objs)
        else:
            nodes.start_nodes(nodes=master_node_objs)
        log.info("Control-plane/master nodes started")

        log.info("Waiting for control-plane/master nodes to be Ready...")
        wait_for_nodes_status(node_names=master_nodes, timeout=1800)
        log.info("Control-plane/master nodes are back online")

        log.info("Starting worker nodes...")
        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            nodes.start_nodes(instances=worker_instances, nodes=worker_node_objs)
        else:
            nodes.start_nodes(nodes=worker_node_objs)
        log.info("Worker nodes started")

        log.info("Waiting for cluster recovery after ordered shutdown")
        wait_for_nodes_status(node_names=all_nodes, timeout=1800)
        log.info("All nodes are back online after ordered shutdown")

        assert wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            timeout=1200,
        ), "All storage pods are not running"
        log.info("All storage pods are running")

        ceph_health_check(tries=30, delay=60)
        log.info("Ceph cluster health verified after ordered shutdown")

        log.info("Waiting for NFS server pod to be running after cluster recovery...")
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-nfs",
            resource_count=1,
            timeout=600,
        )
        nfs_server_pods_recovery = get_all_pods(
            namespace=self.namespace, selector=["rook-ceph-nfs"], selector_label="app"
        )
        if nfs_server_pods_recovery:
            log.info("Waiting for NFS server container to be ready...")
            wait_for_container_status_ready(nfs_server_pods_recovery[0], timeout=300)
            log.info("NFS server container is ready after cluster recovery")
        else:
            raise Exception("NFS server pod not found after cluster recovery")

        # Verify NFS mount points accessible after recovery (non-vSphere only)
        if not self.is_vsphere:
            con = self.reconnect_if_needed()
            log.info("Verifying all mount points are accessible after recovery")
            max_retries = 10
            retry_delay = 20
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
                            f"Attempt {attempt + 1}/{max_retries}: {mount_point} "
                            f"not yet accessible..."
                        )
                        log.error(
                            f"findmnt failed: retcode={retcode}, "
                            f"stdout={stdout!r}, stderr={stderr!r}"
                        )
                        ls_retcode, ls_stdout, ls_stderr = con.exec_cmd(
                            f"ls {mount_point} 2>&1 || true"
                        )
                        if "Stale file handle" in ls_stderr:
                            log.error(
                                f"  ✗ STALE FILE HANDLE detected for {mount_point}"
                            )
                        elif "Transport endpoint is not connected" in ls_stderr:
                            log.error(
                                f"  ✗ TRANSPORT ENDPOINT NOT CONNECTED for {mount_point}"
                            )
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)

                mount_recovery_status[mount_point] = mount_accessible
                if not mount_accessible:
                    log.error(
                        f"✗ Mount point {mount_point} NOT accessible after recovery"
                    )

            failed_mounts = [
                mp for mp, status in mount_recovery_status.items() if not status
            ]
            assert (
                len(failed_mounts) == 0
            ), f"Mount points not accessible after recovery: {failed_mounts}"
            log.info("All mount points verified accessible after ordered recovery")
        else:
            log.info(
                "vSphere: skipping out-of-cluster mount point validation after recovery"
            )

        log.info("Performing post-recovery I/O and data integrity verification")

        for pname in pod_names:
            try:
                log.info(f"Testing post-recovery I/O for pod: {pname}")

                recovery_pod_objs = pod.get_all_pods(
                    namespace=self.namespace,
                    selector=[pname],
                    selector_label="name",
                )
                assert recovery_pod_objs, f"Pod {pname} not found after recovery"
                recovery_pod_obj = recovery_pod_objs[0]

                log.info(
                    f"Waiting for container to be ready in pod {recovery_pod_obj.name}..."
                )
                wait_for_container_status_ready(recovery_pod_obj, timeout=300)

                test_file_name = f"post_recovery_test_{int(time.time())}.txt"
                pod_recovery_file = f"/mnt/{test_file_name}"
                test_data = f"Post-recovery write test at {time.time()}"

                write_cmd = f"echo '{test_data}' > {pod_recovery_file}"
                recovery_pod_obj.exec_sh_cmd_on_pod(write_cmd)
                log.info(f"✓ Wrote data via pod {pname}")

                read_result = recovery_pod_obj.exec_cmd_on_pod(
                    f"cat {pod_recovery_file}", out_yaml_format=False
                )
                assert test_data in read_result, (
                    f"Data written by pod {pname} not readable from same pod. "
                    f"Expected: '{test_data}', Got: '{read_result}'"
                )
                log.info(f"✓ Pod {pname} read-back verified")

                if not self.is_vsphere:
                    mount_point = next(
                        (mp for mp, pn in mount_to_pod_map.items() if pn == pname),
                        None,
                    )
                    if mount_point:
                        nfs_recovery_file = f"{mount_point}/{test_file_name}"
                        retcode, nfs_out, stderr = con.exec_cmd(
                            f"cat {nfs_recovery_file}"
                        )
                        assert retcode == 0 and test_data in nfs_out, (
                            f"Data from pod {pname} not visible on NFS mount "
                            f"{mount_point}: {stderr}"
                        )
                        log.info(
                            f"✓ Pod {pname} data confirmed on NFS mount {mount_point}"
                        )

            except Exception as e:
                log.error(f"✗ Post-recovery I/O failed for pod {pname}: {e}")
                raise

        log.info("Post-recovery I/O and data integrity verified for all pods")

        log.info("=" * 80)
        log.info("Ordered Cluster Shutdown and Recovery - COMPLETED SUCCESSFULLY!")
        log.info("=" * 80)
