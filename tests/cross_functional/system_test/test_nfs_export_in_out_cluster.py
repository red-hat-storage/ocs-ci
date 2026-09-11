import logging
import random
import threading
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    polarion_id,
    skipif_rosa_hcp,
    skipif_lean_deployment,
    system_test,
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
from ocs_ci.utility.nfs_base import NFSClientTestBase
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

    def verify_nongraceful_stopstart_mount_recovery(
        self, con, mount_points, max_retries=10, retry_delay=20
    ):
        """
        Verify NFS mount points on client VM are accessible after non-graceful
        cluster stop/start (ordered shutdown) and recovery.

        Args:
            con (Connection): Connection object to the NFS client VM.
            mount_points (list): List of mount point directory paths.
            max_retries (int): Maximum retry attempts for findmnt check.
            retry_delay (int): Delay in seconds between retries.

        Raises:
            AssertionError: If any mount point remains inaccessible after retries.
        """
        if self.is_vsphere:
            log.info(
                "vSphere: skipping out-of-cluster mount point validation after "
                "non-graceful cluster stop/start (ordered shutdown) recovery"
            )
            return

        con = self.reconnect_if_needed()
        log.info(
            "Verifying all mount points are accessible after non-graceful "
            "cluster stop/start (ordered shutdown) recovery"
        )
        mount_recovery_status = {}

        for mount_point in mount_points:
            mount_accessible = False
            log.info(f"\nChecking mount point: {mount_point}")

            for attempt in range(max_retries):
                try:
                    retcode, stdout, stderr = con.exec_cmd(f"findmnt -M {mount_point}")
                except (ConnectionResetError, EOFError, OSError) as exc:
                    log.warning(
                        f"Attempt {attempt + 1}/{max_retries}: SSH connection lost "
                        f"({type(exc).__name__}: {exc}) — reconnecting..."
                    )
                    con = self.reconnect_if_needed()
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    continue

                if retcode == 0:
                    mount_accessible = True
                    log.info(
                        f"✓ Mount point {mount_point} is accessible after non-graceful "
                        f"cluster stop/start (ordered shutdown) recovery"
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
                    try:
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
                    except (ConnectionResetError, EOFError, OSError) as exc:
                        log.warning(
                            f"  ls probe failed ({type(exc).__name__}: {exc}) — reconnecting..."
                        )
                        con = self.reconnect_if_needed()
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)

            mount_recovery_status[mount_point] = mount_accessible
            if not mount_accessible:
                log.error(
                    f"✗ Mount point {mount_point} NOT accessible after non-graceful "
                    f"cluster stop/start (ordered shutdown) recovery"
                )

        failed_mounts = [
            mp for mp, status in mount_recovery_status.items() if not status
        ]
        assert len(failed_mounts) == 0, (
            f"Mount points not accessible after non-graceful cluster "
            f"stop/start (ordered shutdown) recovery: {failed_mounts}"
        )
        log.info(
            "All mount points verified accessible after non-graceful "
            "cluster stop/start (ordered shutdown) recovery"
        )

    def verify_nongraceful_stopstart_post_recovery_io(
        self, pod_names, mount_to_pod_map=None, con=None
    ):
        """
        Verify post-recovery I/O and data integrity on pods and NFS mounts
        after non-graceful cluster stop/start (ordered shutdown).

        Args:
            pod_names (list): List of pod names to test post-recovery I/O against.
            mount_to_pod_map (dict, optional): Map of {mount_point: pod_name} for NFS verification.
            con (Connection, optional): Connection object to the NFS client VM.

        Raises:
            AssertionError: If pod read-back or NFS mount read fails to match written data.
        """
        log.info(
            "Performing post-recovery I/O and data integrity verification "
            "after non-graceful cluster stop/start (ordered shutdown)"
        )

        failures = []
        for pname in pod_names:
            try:
                log.info(f"Testing post-recovery I/O for pod: {pname}")

                recovery_pod_objs = pod.get_all_pods(
                    namespace=self.namespace,
                    selector=[pname],
                    selector_label="name",
                )
                assert recovery_pod_objs, (
                    f"Pod {pname} not found after non-graceful cluster "
                    f"stop/start (ordered shutdown) recovery"
                )
                recovery_pod_obj = recovery_pod_objs[0]

                log.info(
                    f"Waiting for container to be ready in pod {recovery_pod_obj.name}..."
                )
                wait_for_container_status_ready(recovery_pod_obj, timeout=300)

                test_file_name = f"post_recovery_test_{int(time.time())}.txt"
                pod_recovery_file = f"/mnt/{test_file_name}"
                test_data = (
                    f"Non-graceful stop/start (ordered shutdown) "
                    f"post-recovery write test at {time.time()}"
                )

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

                if not self.is_vsphere and mount_to_pod_map and con:
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
                log.error(
                    f"✗ Non-graceful stop/start (ordered shutdown) post-recovery "
                    f"I/O failed for pod {pname}: {e}"
                )
                failures.append(f"{pname}: {e}")

        assert not failures, (
            "Post-recovery I/O failed for the following pods after non-graceful "
            "cluster stop/start (ordered shutdown):\n" + "\n".join(failures)
        )
        log.info(
            "Post-recovery I/O and data integrity verified for all pods "
            "after non-graceful cluster stop/start (ordered shutdown)"
        )

    @system_test
    @polarion_id("OCS-8262")
    @pytest.mark.ignore_leftover_label(constants.MON_APP_LABEL)
    def test_nfs_export_operations_in_out_cluster(
        self,
        request,
        nodes,
    ):
        """
        Validates NFS export operations for both RWX and RWO access modes across
        in-cluster (pod) mounts. For RWX PVCs, the volume is additionally mounted
        from an out-of-cluster NFS client VM simultaneously and data integrity is
        cross-validated at each stage. RWO PVCs are only accessed from the
        in-cluster pod, as RWO semantics prohibit simultaneous multi-client mounts.

        Note: Out-of-cluster steps are skipped on vSphere — no external load-balancer
        is available on that platform.

        Prerequisites:
        - ODF cluster with hugepages, Multus, encryption-in-transit, and NFS enabled.

        Test Steps:
        a) Create 10Gi NFS PVC (RWX and RWO), mount in-cluster (pod). For RWX also
           mount out-of-cluster (client VM) simultaneously.
        b) Run continuous I/O during NFS server pod node reboot; verify no data loss.
        c) Create PVC snapshot, restore it, write I/O to restored PVC, verify checksums.
        d) Clone restored PVC, write I/O, resize clone to 15Gi, verify data integrity.
        e) Snapshot the resized clone, restore it, write I/O, resize to 20Gi, verify integrity.
        f) Non-graceful cluster stop/start (ordered shutdown, force=True; workers first,
           then control-plane), start in reverse order; verify Ceph health, NFS mounts
           accessible, and post-recovery I/O from all pods.
        """

        log.info(f"Test case execution started: {request.node.name}")
        if not self.is_vsphere:
            nfs_utils.skip_test_if_nfs_client_unavailable(self.nfs_client_ip)
        access_modes = [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO]
        snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML
        nfs_snapshotclass_name = constants.NFS_SNAPSHOT_CLASS_NAME
        data_sets = dict()
        for access_mode in access_modes:
            data_sets[access_mode] = {}
            unique_suffix = random.randint(1000, 9999)
            data_sets[access_mode]["pod_name"] = f"test-pod-outcluster-{unique_suffix}"
            data_sets[access_mode]["pvc_name"] = f"test-pvc-{unique_suffix}"
            log.info(
                f'Using unique names: pod deployment={data_sets[access_mode]["pod_name"]},'
                f' pvc={data_sets[access_mode]["pvc_name"]}'
            )

            data_sets[access_mode]["nfs_pvc_obj"] = helpers.create_pvc(
                sc_name=self.nfs_sc,
                namespace=self.namespace,
                size="10Gi",
                do_reload=True,
                access_mode=access_mode,
                volume_mode="Filesystem",
                pvc_name=data_sets[access_mode]["pvc_name"],
            )

        # Pin I/O pod to a worker node different from the NFS server node to survive reboot
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
        for access_mode in access_modes:
            log.info(f"Creating deployment {data_sets[access_mode]['pod_name']}")
            deployment_data = frame_deployment_config(
                deployment_name=data_sets[access_mode]["pod_name"],
                pvc_name=data_sets[access_mode]["pvc_name"],
                node_name=io_pod_node,
            )
            helpers.create_resource(**deployment_data)

        for access_mode in access_modes:
            log.info(
                f"Waiting for deployment {data_sets[access_mode]['pod_name']} to be ready..."
            )
            deployment_obj = ocp.OCP(
                kind=constants.DEPLOYMENT, namespace=self.namespace
            )

            deployment_obj.wait_for_resource(
                condition="1/1",
                resource_name=data_sets[access_mode]["pod_name"],
                column="READY",
                timeout=300,
            )
            log.info(f"Deployment {data_sets[access_mode]['pod_name']} is ready")

            data_sets[access_mode]["pod_obj"] = pod.get_all_pods(
                namespace=self.namespace,
                selector=[data_sets[access_mode]["pod_name"]],
                selector_label="name",
            )[0]
            log.info(f"Pod {data_sets[access_mode]['pod_obj'].name} is running")

        mount_to_pod_map = {}
        con = None if self.is_vsphere else self.con

        for access_mode in access_modes:
            log.info(f"Mounting NFS export {data_sets[access_mode]['pod_name']}")
            data_sets[access_mode]["test_folder_for_pod"] = (
                self.test_folder + "-" + data_sets[access_mode]["pod_name"]
            )
            data_sets[access_mode]["share_details"] = self.get_nfs_export_details(
                data_sets[access_mode]["nfs_pvc_obj"]
            )

            if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX:
                mount_to_pod_map[data_sets[access_mode]["test_folder_for_pod"]] = (
                    data_sets[access_mode]["pod_name"]
                )
                self.mount_nfs_export(
                    con,
                    data_sets[access_mode]["share_details"],
                    data_sets[access_mode]["test_folder_for_pod"],
                )
            else:
                log.info(
                    f"Skipping out-of-cluster NFS mount for {access_mode}: "
                    + (
                        "vSphere platform detected."
                        if self.is_vsphere
                        else "RWO volumes must not be mounted from multiple clients simultaneously."
                    )
                    + " All I/O will be performed from in-cluster pod."
                )

        def cleanup_initial_all_resources():
            log.info("Running cleanup for all test resources...")
            for am in access_modes:
                pod_name = data_sets[am]["pod_name"]
                pod_obj = data_sets[am]["pod_obj"]
                nfs_pvc_obj = data_sets[am]["nfs_pvc_obj"]
                test_folder = data_sets[am]["test_folder_for_pod"]

                if (
                    not self.is_vsphere
                    and am == constants.ACCESS_MODE_RWX
                    and con is not None
                ):
                    try:
                        log.info(f"Unmounting {test_folder}")
                        nfs_utils.unmount(con, test_folder)
                        con.exec_cmd(f"rm -rf {test_folder}")
                        log.info("Waiting for NFS export to be fully released...")
                        time.sleep(10)
                    except Exception as e:
                        log.warning(f"Failed to unmount NFS for {pod_name}: {e}")

                try:
                    log.info(f"Deleting deployment {pod_name}")
                    deployment_obj = ocp.OCP(
                        kind=constants.DEPLOYMENT, namespace=self.namespace
                    )
                    if deployment_obj.is_exist(resource_name=pod_name):
                        deployment_obj.delete(resource_name=pod_name)
                        deployment_obj.wait_for_delete(
                            resource_name=pod_name, timeout=180
                        )
                        log.info(f"Deployment {pod_name} deleted successfully")
                except Exception as e:
                    log.warning(f"Failed to delete deployment {pod_name}: {e}")

                try:
                    log.info(f"Waiting for pod {pod_obj.name} to be terminated...")
                    pod_obj.ocp.wait_for_delete(pod_obj.name, timeout=180)
                    log.info(f"Pod {pod_obj.name} terminated successfully")
                except Exception as e:
                    log.warning(f"Failed to wait for pod deletion {pod_obj.name}: {e}")

                try:
                    pv_obj = nfs_pvc_obj.backed_pv_obj
                    log.info(f"Deleting PVC {nfs_pvc_obj.name}")
                    nfs_pvc_obj.delete(wait=True)
                    log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

                    log.info("Checking if NFS PV is deleted")
                    pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=300)
                    log.info(f"PV {pv_obj.name} deleted successfully")
                except Exception as e:
                    log.warning(f"Failed to delete PVC/PV for {pod_name}: {e}")

            log.info("Cleanup complete")

        request.addfinalizer(cleanup_initial_all_resources)

        log.info("=" * 80)
        log.info("Scenario: NFS Server Pod Node Reboot During Active I/O")
        log.info("=" * 80)
        log.info("Step 1: Starting continuous I/O operations on NFS mount")

        IO_TEST_FILE_NAME = "io_test_single.txt"

        # Set up per-mode I/O state and store in data_sets
        for access_mode in access_modes:
            data_sets[access_mode]["pod_io_file"] = f"/mnt/{IO_TEST_FILE_NAME}"
            data_sets[access_mode]["nfs_io_file"] = (
                f"{data_sets[access_mode]['test_folder_for_pod']}/{IO_TEST_FILE_NAME}"
                if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX
                else None
            )
            data_sets[access_mode]["io_errors"] = []
            data_sets[access_mode]["io_stop_event"] = threading.Event()
            data_sets[access_mode]["io_completed"] = threading.Event()
            data_sets[access_mode]["io_thread"] = threading.Thread(
                target=self.continuous_io_operations,
                args=(
                    data_sets[access_mode]["pod_obj"],
                    data_sets[access_mode]["pod_io_file"],
                    data_sets[access_mode]["io_errors"],
                    data_sets[access_mode]["io_stop_event"],
                    data_sets[access_mode]["io_completed"],
                ),
                daemon=True,
            )

        for access_mode in access_modes:
            data_sets[access_mode]["io_thread"].start()
            log.info(
                f"Continuous I/O thread started for pod "
                f"{data_sets[access_mode]['pod_obj'].name} ({access_mode})"
            )

        def stop_io_threads():
            for am in access_modes:
                if not data_sets[am]["io_stop_event"].is_set():
                    log.info(
                        f"Finalizer: stopping I/O thread for "
                        f"{data_sets[am]['pod_obj'].name} ({am})..."
                    )
                    data_sets[am]["io_stop_event"].set()
                data_sets[am]["io_thread"].join(timeout=60)
                log.info(
                    f"Finalizer: I/O thread stopped for {data_sets[am]['pod_obj'].name} ({am})"
                )

        request.addfinalizer(stop_io_threads)

        time.sleep(10)
        log.info("Initial I/O operations running successfully")

        log.info("Step 2: Identifying node hosting NFS server pod")
        nfs_server_pods = get_all_pods(
            namespace=self.namespace, selector=["rook-ceph-nfs"], selector_label="app"
        )

        if not nfs_server_pods:
            for am in access_modes:
                data_sets[am]["io_stop_event"].set()
                data_sets[am]["io_thread"].join(timeout=30)
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
            # vSphere may miss 'Rebooted' OCP event within 300s; subsequent checks confirm recovery
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

        for access_mode in access_modes:
            pod_name = data_sets[access_mode]["pod_name"]
            log.info(f"Waiting for deployment {pod_name} pod to be running...")
            assert self.pod_obj.wait_for_resource(
                resource_count=1,
                condition=constants.STATUS_RUNNING,
                selector=f"name={pod_name}",
                dont_allow_other_resources=True,
                timeout=600,
            ), f"Deployment {pod_name} pod not running after node reboot"

        for access_mode in access_modes:
            pod_name = data_sets[access_mode]["pod_name"]
            log.info(f"Getting fresh pod object for deployment {pod_name}...")
            pod_objs = pod.get_all_pods(
                namespace=self.namespace,
                selector=[pod_name],
                selector_label="name",
            )
            if pod_objs:
                data_sets[access_mode]["pod_obj"] = pod_objs[0]
                log.info(
                    f"Got fresh pod object: {data_sets[access_mode]['pod_obj'].name}"
                )
            else:
                raise Exception(
                    f"Could not find pod for deployment {pod_name} after node reboot"
                )

            log.info(
                f"Waiting for container to be ready in pod {data_sets[access_mode]['pod_obj'].name}..."
            )
            wait_for_container_status_ready(
                data_sets[access_mode]["pod_obj"], timeout=300
            )

            if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX:
                log.info(
                    f"Verifying out-of-cluster NFS mount accessibility for {pod_name}..."
                )
                retcode, stdout, _ = con.exec_cmd(
                    f"findmnt -M {data_sets[access_mode]['test_folder_for_pod']}"
                )
                assert (
                    retcode == 0
                ), f"NFS mount not accessible after node reboot for {pod_name}"
                log.info(f"NFS mount is still accessible for {pod_name}")

        if self.is_vsphere:
            log.info("vSphere: skipping out-of-cluster mount check after node reboot")

        log.info("Letting I/O continue for 60s after recovery...")
        time.sleep(60)
        log.info("Stopping I/O operations...")
        for am in access_modes:
            data_sets[am]["io_stop_event"].set()
            data_sets[am]["io_thread"].join(timeout=60)

        total_io_errors = 0
        for am in access_modes:
            pod_name = data_sets[am]["pod_name"]
            io_errors = data_sets[am]["io_errors"]
            total_io_errors += len(io_errors)
            log.info(f"Checking I/O results for {pod_name}...")
            if io_errors:
                pod_unavailable = [
                    e for e in io_errors if "Pod became unavailable" in e
                ]
                integrity = [
                    e
                    for e in io_errors
                    if "Checksum unchanged" in e or "Data mismatch" in e
                ]
                transient = [
                    e
                    for e in io_errors
                    if e not in pod_unavailable and e not in integrity
                ]
                log.warning(
                    f"I/O errors for {pod_name}: {len(io_errors)} total "
                    f"(pod-unavailable={len(pod_unavailable)}, "
                    f"integrity={len(integrity)}, transient={len(transient)})"
                )
                for error in io_errors[:10]:
                    log.warning(f"  - {error}")
                if len(io_errors) > 20:
                    raise AssertionError(
                        f"Too many I/O errors for {pod_name}: {len(io_errors)} "
                        f"(pod-unavailable={len(pod_unavailable)}, "
                        f"integrity={len(integrity)}, transient={len(transient)})"
                    )
            else:
                log.info(f"No I/O errors for {pod_name} - all operations successful!")

        log.info(
            "Verifying data integrity after node reboot from pod (cross-checking NFS mount on non-vSphere)..."
        )
        for am in access_modes:
            self.verify_data_integrity_from_pod(
                pod_obj=data_sets[am]["pod_obj"],
                file_path_in_pod=data_sets[am]["pod_io_file"],
                con=con,
                file_path_on_nfs=(
                    data_sets[am]["nfs_io_file"] if not self.is_vsphere else None
                ),
                operation_name=f"node reboot ({am})",
            )

        log.info(
            f"NFS node reboot test completed: {nfs_node_name} rebooted, {total_io_errors} total I/O errors detected"
        )

        log.info("=" * 80)
        log.info("Starting NFS PVC Snapshot and Restore scenario")
        log.info("=" * 80)

        log.info("Step 1: Capturing file checksum from pod (always in-cluster)")

        for access_mode in access_modes:
            pod_name = data_sets[access_mode]["pod_name"]
            orig_chk = self.calculate_checksum_and_lines_from_pod(
                data_sets[access_mode]["pod_obj"],
                data_sets[access_mode]["pod_io_file"],
            )
            assert orig_chk[
                "success"
            ], f"Failed to get checksum from pod {pod_name}: {orig_chk['error']}"
            data_sets[access_mode]["original_file_checksum"] = orig_chk["checksum"]
            data_sets[access_mode]["original_line_count"] = orig_chk["line_count"]
            log.info(f"[{access_mode}] Original file checksum: {orig_chk['checksum']}")
            log.info(f"[{access_mode}] Total lines in file: {orig_chk['line_count']}")

        log.info("Step 2: Creating snapshot of NFS PVC")

        for access_mode in access_modes:
            pvc_name = data_sets[access_mode]["pvc_name"]
            snapshot_name = f"{pvc_name}-snapshot"
            data_sets[access_mode]["snapshot_name"] = snapshot_name

            log.info(
                f"[{access_mode}] Creating snapshot: {snapshot_name} from NFS PVC: {pvc_name} using "
                f"snapshot class: {nfs_snapshotclass_name}"
            )

            data_sets[access_mode]["snapshot_obj"] = create_pvc_snapshot(
                pvc_name=pvc_name,
                snap_yaml=snap_yaml,
                snap_name=snapshot_name,
                namespace=self.namespace,
                sc_name=nfs_snapshotclass_name,
                wait=True,
                timeout=300,
            )

            log.info(f"[{access_mode}] Snapshot {snapshot_name} created successfully")

        def cleanup_snapshot():
            for am in access_modes:
                try:
                    snap_name = data_sets[am]["snapshot_name"]
                    snap_obj = data_sets[am]["snapshot_obj"]
                    log.info(f"[{am}] Deleting snapshot {snap_name}")
                    snap_obj.delete()
                    snap_obj.ocp.wait_for_delete(resource_name=snap_name, timeout=180)
                    log.info(f"[{am}] Snapshot {snap_name} deleted successfully")
                except Exception as e:
                    log.warning(f"[{am}] Failed to delete snapshot: {e}")

        request.addfinalizer(cleanup_snapshot)

        log.info("Step 3: Creating new PVC from snapshot")

        for access_mode in access_modes:
            pvc_name = data_sets[access_mode]["pvc_name"]
            snapshot_name = data_sets[access_mode]["snapshot_name"]
            restored_pvc_name = f"{pvc_name}-restored"
            data_sets[access_mode]["restored_pvc_name"] = restored_pvc_name

            data_sets[access_mode]["restored_pvc_obj"] = pvc.create_restore_pvc(
                sc_name=self.nfs_sc,
                snap_name=snapshot_name,
                namespace=self.namespace,
                size="10Gi",
                pvc_name=restored_pvc_name,
                volume_mode="Filesystem",
                restore_pvc_yaml=constants.CSI_CEPHFS_PVC_RESTORE_YAML,
                access_mode=access_mode,
            )

            log.info(
                f"[{access_mode}] Restored PVC {restored_pvc_name} created from snapshot"
            )

        def cleanup_restored_pvc():
            for am in access_modes:
                try:
                    restored_pvc_name = data_sets[am]["restored_pvc_name"]
                    restored_pvc_obj = data_sets[am]["restored_pvc_obj"]
                    log.info(f"[{am}] Deleting restored PVC {restored_pvc_name}")
                    # Clear finalizers that can block deletion if provisioner has not released them
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
                    log.info(
                        f"[{am}] Restored PVC {restored_pvc_name} deleted successfully"
                    )
                except Exception as e:
                    log.warning(f"[{am}] Failed to delete restored PVC: {e}")

        request.addfinalizer(cleanup_restored_pvc)

        log.info("Step 4: Creating new pod with restored PVC")

        for access_mode in access_modes:
            pod_name = data_sets[access_mode]["pod_name"]
            restored_pvc_name = data_sets[access_mode]["restored_pvc_name"]
            restored_pod_name = f"{pod_name}-restored"
            data_sets[access_mode]["restored_pod_name"] = restored_pod_name

            restored_deployment_data = frame_deployment_config(
                deployment_name=restored_pod_name, pvc_name=restored_pvc_name
            )
            helpers.create_resource(**restored_deployment_data)

            log.info(
                f"[{access_mode}] Waiting for deployment {restored_pod_name} to be ready..."
            )
            restored_deployment_obj = ocp.OCP(
                kind=constants.DEPLOYMENT, namespace=self.namespace
            )
            restored_deployment_obj.wait_for_resource(
                condition="1/1",
                resource_name=restored_pod_name,
                column="READY",
                timeout=300,
            )
            log.info(f"[{access_mode}] Deployment {restored_pod_name} is ready")

            data_sets[access_mode]["restored_pod_obj"] = pod.get_all_pods(
                namespace=self.namespace,
                selector=[restored_pod_name],
                selector_label="name",
            )[0]
            log.info(
                f"[{access_mode}] Restored pod {data_sets[access_mode]['restored_pod_obj'].name} is running"
            )

        def cleanup_restored_deployment():
            for am in access_modes:
                try:
                    restored_pod_name = data_sets[am]["restored_pod_name"]
                    restored_pod_obj = data_sets[am]["restored_pod_obj"]
                    log.info(f"[{am}] Deleting restored deployment {restored_pod_name}")
                    deployment_obj = ocp.OCP(
                        kind=constants.DEPLOYMENT, namespace=self.namespace
                    )
                    if deployment_obj.is_exist(resource_name=restored_pod_name):
                        deployment_obj.delete(resource_name=restored_pod_name)
                        deployment_obj.wait_for_delete(
                            resource_name=restored_pod_name, timeout=180
                        )
                        log.info(
                            f"[{am}] Restored deployment {restored_pod_name} deleted successfully"
                        )

                    log.info(f"[{am}] Waiting for restored pod to be terminated...")
                    restored_pod_obj.ocp.wait_for_delete(
                        restored_pod_obj.name, timeout=180
                    )
                    log.info(
                        f"[{am}] Restored pod {restored_pod_obj.name} terminated successfully"
                    )
                except Exception as e:
                    log.warning(f"[{am}] Failed to delete restored deployment/pod: {e}")

        request.addfinalizer(cleanup_restored_deployment)

        log.info("Step 5: Verifying data integrity for restored PVC")

        restored_pod_file_path = f"/mnt/{IO_TEST_FILE_NAME}"

        for access_mode in access_modes:
            restored_pod_name = data_sets[access_mode]["restored_pod_name"]
            restored_pvc_obj = data_sets[access_mode]["restored_pvc_obj"]
            restored_pod_obj = data_sets[access_mode]["restored_pod_obj"]
            restored_test_folder = f"{self.test_folder}-restored-{access_mode}"
            data_sets[access_mode]["restored_test_folder"] = restored_test_folder

            if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX:
                mount_to_pod_map[restored_test_folder] = restored_pod_name
                restored_share_details = self.get_nfs_export_details(restored_pvc_obj)
                self.mount_nfs_export(con, restored_share_details, restored_test_folder)

            restored_nfs_file = (
                f"{restored_test_folder}/{IO_TEST_FILE_NAME}"
                if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX
                else None
            )

            self.verify_data_integrity_from_pod(
                pod_obj=restored_pod_obj,
                file_path_in_pod=restored_pod_file_path,
                con=con,
                file_path_on_nfs=restored_nfs_file,
                expected_checksum=data_sets[access_mode]["original_file_checksum"],
                expected_line_count=data_sets[access_mode]["original_line_count"],
                operation_name=f"snapshot restore ({access_mode})",
            )
            log.info(f"[{access_mode}] ✓ Restored data matches original data")

        def cleanup_restored_mount():
            for am in access_modes:
                if am != constants.ACCESS_MODE_RWX:
                    continue
                try:
                    folder = data_sets[am]["restored_test_folder"]
                    log.info(f"[{am}] Unmounting {folder}")
                    nfs_utils.unmount(con, folder)
                    con.exec_cmd(f"rm -rf {folder}")
                    log.info(f"[{am}] Restored mount {folder} cleaned up")
                except Exception as e:
                    log.warning(f"[{am}] Failed to unmount restored NFS: {e}")

        if not self.is_vsphere:
            request.addfinalizer(cleanup_restored_mount)

        log.info("NFS Snapshot and Restore scenario completed successfully!")
        log.info("=" * 80)
        log.info("Scenario: Clone Restored PVC, Resize, and Verify Data Integrity")
        log.info("=" * 80)
        log.info("Step 1: Creating clone of restored PVC")

        for access_mode in access_modes:
            restored_pvc_name = data_sets[access_mode]["restored_pvc_name"]
            cloned_pvc_name = f"{restored_pvc_name}-clone"
            data_sets[access_mode]["cloned_pvc_name"] = cloned_pvc_name

            data_sets[access_mode]["cloned_pvc_obj"] = pvc.create_pvc_clone(
                sc_name=self.nfs_sc,
                parent_pvc=restored_pvc_name,
                clone_yaml=constants.CSI_CEPHFS_PVC_CLONE_YAML,
                namespace=self.namespace,
                pvc_name=cloned_pvc_name,
                storage_size="10Gi",
            )
            log.info(
                f"[{access_mode}] Cloned PVC {cloned_pvc_name} created successfully"
            )

        def cleanup_cloned_pvc():
            for am in access_modes:
                try:
                    cloned_pvc_name = data_sets[am]["cloned_pvc_name"]
                    cloned_pvc_obj = data_sets[am]["cloned_pvc_obj"]
                    log.info(f"[{am}] Deleting cloned PVC {cloned_pvc_name}")
                    cloned_pvc_obj.delete()
                    cloned_pvc_obj.ocp.wait_for_delete(
                        resource_name=cloned_pvc_name, timeout=180
                    )
                    log.info(
                        f"[{am}] Cloned PVC {cloned_pvc_name} deleted successfully"
                    )
                except Exception as e:
                    log.warning(f"[{am}] Failed to delete cloned PVC: {e}")

        request.addfinalizer(cleanup_cloned_pvc)

        log.info("Step 2: Creating pod deployment with cloned PVC")

        for access_mode in access_modes:
            cloned_pvc_name = data_sets[access_mode]["cloned_pvc_name"]
            cloned_pod_name = f"test-pod-cloned-{random.randint(1000, 9999)}"
            data_sets[access_mode]["cloned_pod_name"] = cloned_pod_name

            cloned_deployment_data = frame_deployment_config(
                deployment_name=cloned_pod_name, pvc_name=cloned_pvc_name
            )
            helpers.create_resource(**cloned_deployment_data)

            log.info(
                f"[{access_mode}] Waiting for deployment {cloned_pod_name} to be ready..."
            )
            cloned_deployment_obj = ocp.OCP(
                kind=constants.DEPLOYMENT, namespace=self.namespace
            )
            cloned_deployment_obj.wait_for_resource(
                condition="1/1",
                resource_name=cloned_pod_name,
                column="READY",
                timeout=300,
            )
            log.info(f"[{access_mode}] Deployment {cloned_pod_name} is ready")

            cloned_pod_objs = pod.get_all_pods(
                namespace=self.namespace,
                selector=[cloned_pod_name],
                selector_label="name",
            )
            assert (
                len(cloned_pod_objs) > 0
            ), f"No pods found for deployment {cloned_pod_name}"
            data_sets[access_mode]["cloned_pod_obj"] = cloned_pod_objs[0]
            log.info(f"[{access_mode}] Cloned pod {cloned_pod_name} is running")

        def cleanup_cloned_pod():
            for am in access_modes:
                try:
                    cloned_pod_name = data_sets[am]["cloned_pod_name"]
                    log.info(f"[{am}] Deleting cloned pod deployment {cloned_pod_name}")
                    deployment_obj = ocp.OCP(
                        kind=constants.DEPLOYMENT, namespace=self.namespace
                    )
                    deployment_obj.delete(resource_name=cloned_pod_name)
                    log.info(f"[{am}] Cloned pod deployment {cloned_pod_name} deleted")
                except Exception as e:
                    log.warning(f"[{am}] Failed to delete cloned pod: {e}")

        request.addfinalizer(cleanup_cloned_pod)

        log.info("Step 3: Getting NFS export details for cloned PVC")

        CLONED_IO_FILE_NAME = "io_test_cloned.txt"

        for access_mode in access_modes:
            cloned_pvc_obj = data_sets[access_mode]["cloned_pvc_obj"]
            cloned_pod_name = data_sets[access_mode]["cloned_pod_name"]
            cloned_pod_obj = data_sets[access_mode]["cloned_pod_obj"]
            cloned_test_folder = f"{self.test_folder}-cloned-{access_mode}"
            data_sets[access_mode]["cloned_test_folder"] = cloned_test_folder

            if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX:
                mount_to_pod_map[cloned_test_folder] = cloned_pod_name
                cloned_share_details = self.get_nfs_export_details(cloned_pvc_obj)
                self.mount_nfs_export(con, cloned_share_details, cloned_test_folder)

            cloned_pod_io_file = f"/mnt/{CLONED_IO_FILE_NAME}"
            cloned_nfs_file = (
                f"{cloned_test_folder}/{CLONED_IO_FILE_NAME}"
                if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX
                else None
            )

            log.info(f"[{access_mode}] Step 4: Writing IO to cloned PVC from pod")
            success, errors = self.write_io_to_single_file(
                cloned_pod_obj, cloned_pod_io_file, num_iterations=20, delay=0.5
            )
            assert (
                success
            ), f"[{access_mode}] Failed to write IO to cloned PVC: {errors}"
            log.info(
                f"[{access_mode}] IO completed on cloned PVC - file: {CLONED_IO_FILE_NAME}"
            )

            log.info(
                f"[{access_mode}] Step 5: Capturing file checksum before PVC resize"
            )
            pre_resize_chk = self.calculate_checksum_and_lines_from_pod(
                cloned_pod_obj, cloned_pod_io_file
            )
            assert pre_resize_chk[
                "success"
            ], f"[{access_mode}] Failed to get checksum: {pre_resize_chk['error']}"
            data_sets[access_mode]["pre_resize_checksum"] = pre_resize_chk["checksum"]
            data_sets[access_mode]["pre_resize_line_count"] = pre_resize_chk[
                "line_count"
            ]
            log.info(
                f"[{access_mode}] Pre-resize checksum: {pre_resize_chk['checksum']}"
            )
            log.info(
                f"[{access_mode}] Pre-resize line count: {pre_resize_chk['line_count']}"
            )

            log.info(f"[{access_mode}] Step 6: Resizing cloned PVC from 10Gi to 15Gi")
            cloned_pvc_obj.resize_pvc(15, verify=True)
            log.info(f"[{access_mode}] PVC successfully resized to 15Gi")

            log.info(
                f"[{access_mode}] Step 7: Verifying data integrity after PVC resize"
            )
            self.verify_data_integrity_from_pod(
                pod_obj=cloned_pod_obj,
                file_path_in_pod=cloned_pod_io_file,
                con=con,
                file_path_on_nfs=cloned_nfs_file,
                expected_checksum=data_sets[access_mode]["pre_resize_checksum"],
                expected_line_count=data_sets[access_mode]["pre_resize_line_count"],
                operation_name=f"PVC clone and resize ({access_mode})",
            )
            log.info(f"[{access_mode}] ✓ Clone, resize, and data integrity verified")

        def cleanup_cloned_mount():
            for am in access_modes:
                if am != constants.ACCESS_MODE_RWX:
                    continue
                try:
                    folder = data_sets[am]["cloned_test_folder"]
                    log.info(f"[{am}] Unmounting cloned NFS export from {folder}")
                    con.exec_cmd(f"umount {folder}")
                    con.exec_cmd(f"rm -rf {folder}")
                    log.info(f"[{am}] Cloned mount {folder} cleaned up")
                except Exception as e:
                    log.warning(f"[{am}] Failed to unmount cloned NFS: {e}")

        if not self.is_vsphere:
            request.addfinalizer(cleanup_cloned_mount)

        log.info(
            "PVC Clone, Resize, and Data Integrity scenario completed successfully!"
        )
        log.info("=" * 80)
        log.info(
            "Scenario: Snapshot Resized PVC, Restore, Resize Again, Verify Integrity"
        )
        log.info("=" * 80)

        FINAL_IO_FILE_NAME = "io_test_final.txt"

        log.info("Step 1: Creating snapshot of resized cloned PVC")
        for access_mode in access_modes:
            cloned_pvc_name = data_sets[access_mode]["cloned_pvc_name"]
            cloned_snapshot_name = f"{cloned_pvc_name}-snapshot"
            data_sets[access_mode]["cloned_snapshot_name"] = cloned_snapshot_name

            log.info(
                f"[{access_mode}] Creating snapshot: {cloned_snapshot_name} from resized PVC: {cloned_pvc_name} "
                f"using snapshot class: {nfs_snapshotclass_name}"
            )
            data_sets[access_mode]["cloned_snapshot_obj"] = create_pvc_snapshot(
                pvc_name=cloned_pvc_name,
                snap_yaml=snap_yaml,
                snap_name=cloned_snapshot_name,
                namespace=self.namespace,
                sc_name=nfs_snapshotclass_name,
                wait=True,
                timeout=300,
            )
            log.info(
                f"[{access_mode}] Snapshot {cloned_snapshot_name} created successfully"
            )

        def cleanup_cloned_snapshot():
            for am in access_modes:
                try:
                    cloned_snapshot_name = data_sets[am]["cloned_snapshot_name"]
                    cloned_snapshot_obj = data_sets[am]["cloned_snapshot_obj"]
                    log.info(f"[{am}] Deleting snapshot {cloned_snapshot_name}")
                    cloned_snapshot_obj.delete()
                    cloned_snapshot_obj.ocp.wait_for_delete(
                        resource_name=cloned_snapshot_name, timeout=180
                    )
                    log.info(
                        f"[{am}] Snapshot {cloned_snapshot_name} deleted successfully"
                    )
                except Exception as e:
                    log.warning(f"[{am}] Failed to delete snapshot: {e}")

        request.addfinalizer(cleanup_cloned_snapshot)

        log.info("Step 2: Restoring PVC from snapshot of resized PVC")
        for access_mode in access_modes:
            cloned_pvc_name = data_sets[access_mode]["cloned_pvc_name"]
            cloned_snapshot_name = data_sets[access_mode]["cloned_snapshot_name"]
            final_restored_pvc_name = f"{cloned_pvc_name}-restored"
            data_sets[access_mode]["final_restored_pvc_name"] = final_restored_pvc_name

            data_sets[access_mode]["final_restored_pvc_obj"] = pvc.create_restore_pvc(
                sc_name=self.nfs_sc,
                snap_name=cloned_snapshot_name,
                namespace=self.namespace,
                size="15Gi",
                pvc_name=final_restored_pvc_name,
                volume_mode="Filesystem",
                restore_pvc_yaml=constants.CSI_CEPHFS_PVC_RESTORE_YAML,
                access_mode=access_mode,
            )
            log.info(
                f"[{access_mode}] Restored PVC {final_restored_pvc_name} created from snapshot"
            )

        def cleanup_final_restored_pvc():
            for am in access_modes:
                try:
                    final_restored_pvc_name = data_sets[am]["final_restored_pvc_name"]
                    final_restored_pvc_obj = data_sets[am]["final_restored_pvc_obj"]
                    log.info(
                        f"[{am}] Deleting final restored PVC {final_restored_pvc_name}"
                    )
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
                        f"[{am}] Final restored PVC {final_restored_pvc_name} deleted successfully"
                    )
                except Exception as e:
                    log.warning(f"[{am}] Failed to delete final restored PVC: {e}")

        request.addfinalizer(cleanup_final_restored_pvc)

        log.info("Step 3: Creating pod deployment with final restored PVC")
        for access_mode in access_modes:
            final_restored_pvc_name = data_sets[access_mode]["final_restored_pvc_name"]
            final_restored_pod_name = f"test-pod-final-{random.randint(1000, 9999)}"
            data_sets[access_mode]["final_restored_pod_name"] = final_restored_pod_name

            final_restored_deployment_data = frame_deployment_config(
                deployment_name=final_restored_pod_name,
                pvc_name=final_restored_pvc_name,
            )
            helpers.create_resource(**final_restored_deployment_data)

            log.info(
                f"[{access_mode}] Waiting for deployment {final_restored_pod_name} to be ready..."
            )
            final_restored_deployment_obj = ocp.OCP(
                kind=constants.DEPLOYMENT, namespace=self.namespace
            )
            final_restored_deployment_obj.wait_for_resource(
                condition="1/1",
                resource_name=final_restored_pod_name,
                column="READY",
                timeout=300,
            )
            log.info(f"[{access_mode}] Deployment {final_restored_pod_name} is ready")

            final_restored_pod_objs = pod.get_all_pods(
                namespace=self.namespace,
                selector=[final_restored_pod_name],
                selector_label="name",
            )
            assert (
                len(final_restored_pod_objs) > 0
            ), f"No pods found for deployment {final_restored_pod_name}"
            data_sets[access_mode]["final_restored_pod_obj"] = final_restored_pod_objs[
                0
            ]
            log.info(
                f"[{access_mode}] Final restored pod {final_restored_pod_name} is running"
            )

        def cleanup_final_restored_pod():
            for am in access_modes:
                try:
                    final_restored_pod_name = data_sets[am]["final_restored_pod_name"]
                    log.info(
                        f"[{am}] Deleting final restored pod deployment {final_restored_pod_name}"
                    )
                    deployment_obj = ocp.OCP(
                        kind=constants.DEPLOYMENT, namespace=self.namespace
                    )
                    deployment_obj.delete(resource_name=final_restored_pod_name)
                    log.info(
                        f"[{am}] Final restored pod deployment {final_restored_pod_name} deleted"
                    )
                except Exception as e:
                    log.warning(f"[{am}] Failed to delete final restored pod: {e}")

        request.addfinalizer(cleanup_final_restored_pod)

        log.info("Step 4: Mounting NFS, writing IO, resizing, and verifying per mode")
        for access_mode in access_modes:
            final_restored_pvc_obj = data_sets[access_mode]["final_restored_pvc_obj"]
            final_restored_pod_name = data_sets[access_mode]["final_restored_pod_name"]
            final_restored_pod_obj = data_sets[access_mode]["final_restored_pod_obj"]
            final_restored_test_folder = f"{self.test_folder}-final-{access_mode}"
            data_sets[access_mode][
                "final_restored_test_folder"
            ] = final_restored_test_folder

            if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX:
                mount_to_pod_map[final_restored_test_folder] = final_restored_pod_name
                final_restored_share_details = self.get_nfs_export_details(
                    final_restored_pvc_obj
                )
                self.mount_nfs_export(
                    con, final_restored_share_details, final_restored_test_folder
                )

            final_pod_io_file = f"/mnt/{FINAL_IO_FILE_NAME}"
            final_nfs_file = (
                f"{final_restored_test_folder}/{FINAL_IO_FILE_NAME}"
                if not self.is_vsphere and access_mode == constants.ACCESS_MODE_RWX
                else None
            )

            log.info(f"[{access_mode}] Step 5: Writing IO to final restored PVC")
            success, errors = self.write_io_to_single_file(
                final_restored_pod_obj, final_pod_io_file, num_iterations=20, delay=0.5
            )
            assert (
                success
            ), f"[{access_mode}] Failed to write IO to final restored PVC: {errors}"
            log.info(f"[{access_mode}] IO completed - file: {FINAL_IO_FILE_NAME}")

            log.info(f"[{access_mode}] Step 6: Capturing checksum before final resize")
            pre_final_chk = self.calculate_checksum_and_lines_from_pod(
                final_restored_pod_obj, final_pod_io_file
            )
            assert pre_final_chk[
                "success"
            ], f"[{access_mode}] Failed to get checksum: {pre_final_chk['error']}"
            data_sets[access_mode]["pre_final_resize_checksum"] = pre_final_chk[
                "checksum"
            ]
            data_sets[access_mode]["pre_final_resize_line_count"] = pre_final_chk[
                "line_count"
            ]
            log.info(
                f"[{access_mode}] Pre-final-resize checksum: {pre_final_chk['checksum']}"
            )
            log.info(
                f"[{access_mode}] Pre-final-resize line count: {pre_final_chk['line_count']}"
            )

            log.info(
                f"[{access_mode}] Step 7: Resizing final restored PVC from 15Gi to 20Gi"
            )
            final_restored_pvc_obj.resize_pvc(20, verify=True)
            log.info(f"[{access_mode}] Final PVC successfully resized to 20Gi")

            log.info(
                f"[{access_mode}] Step 8: Verifying data integrity after final resize"
            )
            self.verify_data_integrity_from_pod(
                pod_obj=final_restored_pod_obj,
                file_path_in_pod=final_pod_io_file,
                con=con,
                file_path_on_nfs=final_nfs_file,
                expected_checksum=data_sets[access_mode]["pre_final_resize_checksum"],
                expected_line_count=data_sets[access_mode][
                    "pre_final_resize_line_count"
                ],
                operation_name=f"snapshot restore and re-resize ({access_mode})",
            )
            log.info(f"[{access_mode}] ✓ Snapshot, restore, and re-resize verified")

        def cleanup_final_restored_mount():
            for am in access_modes:
                if am != constants.ACCESS_MODE_RWX:
                    continue
                try:
                    folder = data_sets[am]["final_restored_test_folder"]
                    log.info(
                        f"[{am}] Unmounting final restored NFS export from {folder}"
                    )
                    con.exec_cmd(f"umount {folder}")
                    con.exec_cmd(f"rm -rf {folder}")
                    log.info(f"[{am}] Final restored mount {folder} cleaned up")
                except Exception as e:
                    log.warning(f"[{am}] Failed to unmount final restored NFS: {e}")

        if not self.is_vsphere:
            request.addfinalizer(cleanup_final_restored_mount)

        log.info("Snapshot, Restore, and Re-Resize scenario completed successfully!")
        log.info("=" * 80)
        log.info(
            "Scenario: Non-Graceful Cluster Stop/Start (Ordered Shutdown) with Mount Point Validation"
        )
        log.info("=" * 80)

        # Build mount points and pod names lists from data_sets across all modes
        nfs_mount_points = []
        pod_names = []
        if not self.is_vsphere:
            for am in access_modes:
                if am == constants.ACCESS_MODE_RWX:
                    nfs_mount_points.extend(
                        [
                            data_sets[am]["test_folder_for_pod"],
                            data_sets[am]["restored_test_folder"],
                            data_sets[am]["cloned_test_folder"],
                            data_sets[am]["final_restored_test_folder"],
                        ]
                    )
        for am in access_modes:
            pod_names.extend(
                [
                    data_sets[am]["pod_name"],
                    data_sets[am]["restored_pod_name"],
                    data_sets[am]["cloned_pod_name"],
                    data_sets[am]["final_restored_pod_name"],
                ]
            )

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

        log.info("Performing ordered non-graceful cluster shutdown (force=True)")

        worker_nodes = get_worker_nodes()
        master_nodes = [node for node in get_all_nodes() if node not in worker_nodes]
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

        log.info(
            "Initiating non-graceful shutdown: force-stopping worker nodes first..."
        )
        nodes.stop_nodes(nodes=worker_node_objs, force=True)
        log.info("Worker nodes force-stopped (non-graceful)")

        log.info("Waiting for worker nodes to reach NotReady state...")
        wait_for_nodes_status(
            node_names=worker_nodes, status=constants.NODE_NOT_READY, timeout=600
        )
        log.info("All worker nodes reached NotReady state")

        log.info(
            "Initiating non-graceful shutdown: force-stopping control-plane/master nodes..."
        )
        nodes.stop_nodes(nodes=master_node_objs, force=True)
        log.info("Control-plane/master nodes force-stopped (non-graceful)")

        log.info(
            "Waiting for 5 minutes to ensure all nodes are fully powered off after non-graceful stop..."
        )
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

        log.info("Waiting for worker nodes to be Ready after non-graceful shutdown...")
        wait_for_nodes_status(node_names=worker_nodes, timeout=1800)
        log.info("All nodes are back online after non-graceful ordered shutdown")

        assert wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            timeout=1200,
        ), "All storage pods are not running"
        log.info("All storage pods are running")

        ceph_health_check(tries=30, delay=60)
        log.info(
            "Ceph cluster health verified after non-graceful cluster stop/start (ordered shutdown)"
        )

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

        if not self.is_vsphere:
            con = self.reconnect_if_needed()

        log.info(
            "Verifying NFS mount points accessible after non-graceful cluster stop/start"
            " (ordered shutdown) recovery (non-vSphere only)..."
        )
        self.verify_nongraceful_stopstart_mount_recovery(
            con=con,
            mount_points=nfs_mount_points,
        )

        # Verify post-recovery I/O and data integrity across pods and NFS mounts
        self.verify_nongraceful_stopstart_post_recovery_io(
            pod_names=pod_names,
            mount_to_pod_map=mount_to_pod_map,
            con=con if not self.is_vsphere else None,
        )

        log.info("=" * 80)
        log.info(
            "Non-Graceful Cluster Stop/Start (Ordered Shutdown) and Recovery - COMPLETED SUCCESSFULLY!"
        )
        log.info("=" * 80)
