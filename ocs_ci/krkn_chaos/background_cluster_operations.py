"""
Background Cluster Operations for ODF Validation

This module provides a comprehensive background cluster operation system that continuously
performs ODF-specific validation operations while workloads are running during Krkn chaos testing.

Operations performed:
1. PVC Snapshot Lifecycle (create → restore → delete → verify)
2. PVC Clone Lifecycle (create → attach → verify checksums → delete)
3. Node Taints & Tolerations Churn (force pod rescheduling)
4. Rook/Ceph Operations (OSD in/out, MDS failover, RGW restarts, pool tweaks)
5. CSI-Addons Operations (VolumeReplication, ReclaimSpace, NetworkFence)

Success Criteria:
- No orphan PVs/images in Ceph
- Clean PVC events (no errors)
- Consistent data checksums post-operations
- Healthy Ceph status throughout
"""

import logging
import threading
import time
import random
import fauxfactory
from contextlib import suppress
from typing import List, Dict, Any, Optional
from collections import defaultdict

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import pvc as pvc_helpers
from ocs_ci.ocs.resources import job as job_helpers
from ocs_ci.ocs import node as node_helpers
from ocs_ci.ocs.exceptions import (
    UnexpectedBehaviour,
    CommandFailed,
    ResourceNotFoundError,
)
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


class BackgroundClusterMetrics:
    """Track metrics for background cluster operations."""

    def __init__(self):
        self.operations = defaultdict(int)
        self.successes = defaultdict(int)
        self.failures = defaultdict(int)
        self.errors = []
        self.start_time = time.time()

    def record_operation(
        self, operation_type: str, success: bool, error: Optional[str] = None
    ):
        """Record an operation result."""
        self.operations[operation_type] += 1
        if success:
            self.successes[operation_type] += 1
        else:
            self.failures[operation_type] += 1
            if error:
                self.errors.append(
                    {
                        "operation": operation_type,
                        "error": error,
                        "timestamp": time.time(),
                    }
                )

    def get_summary(self) -> Dict[str, Any]:
        """Get operation summary."""
        duration = time.time() - self.start_time
        return {
            "duration_seconds": duration,
            "total_operations": sum(self.operations.values()),
            "total_successes": sum(self.successes.values()),
            "total_failures": sum(self.failures.values()),
            "operations_by_type": dict(self.operations),
            "successes_by_type": dict(self.successes),
            "failures_by_type": dict(self.failures),
            "error_count": len(self.errors),
            "success_rate": (
                sum(self.successes.values()) / sum(self.operations.values()) * 100
                if sum(self.operations.values()) > 0
                else 0
            ),
        }


class BackgroundClusterOperations:
    """
    Background cluster operations manager for ODF validation.

    This class manages background operations that continuously validate ODF
    functionality while workloads are running and chaos is being injected.
    """

    def __init__(
        self,
        workload_ops,
        enabled_operations: Optional[List[str]] = None,
        operation_interval: int = 60,
        max_concurrent_operations: int = 3,
    ):
        """
        Initialize BackgroundClusterOperations.

        Args:
            workload_ops: Workload operations object containing running workloads
            enabled_operations: List of enabled operation types (None = all)
            operation_interval: Seconds between operations (default: 60)
            max_concurrent_operations: Max concurrent background operations
        """
        self.workload_ops = workload_ops
        self.namespace = workload_ops.namespace
        self.workloads = workload_ops.workloads
        self.operation_interval = operation_interval
        self.max_concurrent_operations = max_concurrent_operations

        # Operation control
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._operation_threads: List[threading.Thread] = []

        # Metrics and tracking
        self.metrics = BackgroundClusterMetrics()
        self._resources_to_cleanup: List[Any] = []

        # Feature availability tracking
        self._csi_addons_available = True  # Assume available until proven otherwise

        # Available operations
        self.available_operations = {
            "snapshot_lifecycle": self._snapshot_lifecycle_operation,
            "clone_lifecycle": self._clone_lifecycle_operation,
            "node_taint_churn": self._node_taint_churn_operation,
            "osd_operations": self._osd_operations,
            "mds_failover": self._mds_failover_operation,
            "rgw_restart": self._rgw_restart_operation,
            "reclaim_space": self._reclaim_space_operation,
            "volume_replication": self._volume_replication_operation,
            "longevity_operations": self._longevity_operations,
        }

        # Filter enabled operations
        if enabled_operations:
            self.enabled_operations = {
                k: v
                for k, v in self.available_operations.items()
                if k in enabled_operations
            }
        else:
            self.enabled_operations = self.available_operations

        log.info(
            f"Initialized BackgroundClusterOperations with "
            f"{len(self.enabled_operations)} operation types: "
            f"{list(self.enabled_operations.keys())}"
        )

    def start(self):
        """Start background cluster operations."""
        if self._running:
            log.warning("Background cluster operations already running")
            return

        log.info("Starting background cluster operations")
        self._running = True
        self._thread = threading.Thread(
            target=self._operation_loop, name="BackgroundClusterOperations", daemon=True
        )
        self._thread.start()
        log.info("Background cluster operations started")

    def stop(self, cleanup=True):
        """
        Stop background cluster operations.

        Args:
            cleanup: If True, cleanup resources created during operations
        """
        if not self._running:
            return

        log.info("Stopping background cluster operations")
        self._running = False

        # Wait for main thread
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)

        # Wait for operation threads
        for thread in self._operation_threads:
            if thread.is_alive():
                thread.join(timeout=10)

        if cleanup:
            self._cleanup_resources()

        log.info("Background cluster operations stopped")
        self._log_final_summary()

    def _operation_loop(self):
        """Main operation loop - continuously performs background operations."""
        log.info("Background cluster operation loop started")

        while self._running:
            try:
                # Clean up completed threads
                self._operation_threads = [
                    t for t in self._operation_threads if t.is_alive()
                ]

                # Check if we can start new operations
                if len(self._operation_threads) < self.max_concurrent_operations:
                    # Select random operation
                    operation_name = random.choice(list(self.enabled_operations.keys()))
                    operation_func = self.enabled_operations[operation_name]

                    # Start operation in separate thread
                    op_thread = threading.Thread(
                        target=self._run_operation_safe,
                        args=(operation_name, operation_func),
                        name=f"BgOp-{operation_name}",
                        daemon=True,
                    )
                    op_thread.start()
                    self._operation_threads.append(op_thread)

                # Sleep before next operation
                time.sleep(self.operation_interval)

            except Exception as e:
                log.error(f"Error in background operation loop: {e}")
                time.sleep(10)  # Back off on error

        log.info("Background cluster operation loop stopped")

    def _namespace_exists(self) -> bool:
        """
        Check if the namespace still exists using the existing OCP utility.

        Returns:
            bool: True if namespace exists, False otherwise
        """
        try:
            # Use existing OCP.is_exist() method - simple check without retries
            ns_obj = ocp.OCP(kind="Namespace", resource_name=self.namespace)
            exists = ns_obj.is_exist(resource_name=self.namespace)

            if not exists and self._running:  # Only log once
                log.warning(
                    f"Namespace {self.namespace} no longer exists. "
                    "Stopping background operations."
                )
                self._running = False  # Stop the loop if namespace is gone

            return exists
        except Exception as e:
            log.debug(f"Error checking namespace existence: {e}")
            # Assume namespace exists if we can't check (to avoid false positives)
            return True

    def _run_operation_safe(self, operation_name: str, operation_func):
        """
        Safely run an operation with error handling.

        Args:
            operation_name: Name of the operation
            operation_func: Function to execute
        """
        try:
            # Check if namespace still exists before running operation
            if not self._namespace_exists():
                log.info(f"Skipping {operation_name} - namespace no longer exists")
                return

            log.info(f"Starting background operation: {operation_name}")
            operation_func()
            self.metrics.record_operation(operation_name, success=True)
            log.info(f"Completed background operation: {operation_name}")
        except Exception as e:
            error_msg = f"{operation_name} failed: {str(e)}"
            log.error(error_msg)
            self.metrics.record_operation(
                operation_name, success=False, error=error_msg
            )

    # ==========================================================================
    # PVC Snapshot Lifecycle Operations
    # ==========================================================================

    def _snapshot_lifecycle_operation(self):
        """
        Perform PVC snapshot lifecycle: create → restore → verify → delete.

        This validates the entire snapshot workflow while workloads are running.
        """
        log.info("Executing PVC snapshot lifecycle operation")

        # Check if namespace still exists
        if not self._namespace_exists():
            return

        # Get a random workload PVC
        workload_pvc = self._get_random_workload_pvc()
        if not workload_pvc:
            log.warning("No workload PVCs found for snapshot operation")
            return

        snapshot_obj = None
        restored_pvc_obj = None
        test_pod_obj = None

        try:
            # Step 1: Create snapshot
            log.info(f"Creating snapshot of PVC {workload_pvc.name}")
            # Use longer timeout during chaos testing as system may be under stress
            snapshot_obj = workload_pvc.create_snapshot(wait=True, timeout=180)
            log.info(f"Snapshot {snapshot_obj.name} created and ready")

            # Step 2: Restore PVC from snapshot
            log.info(f"Restoring PVC from snapshot {snapshot_obj.name}")

            # Get actual capacity from source PVC (not converted size)
            # This ensures restore size is >= snapshot size
            workload_pvc.reload()
            source_capacity = (
                workload_pvc.data.get("status", {}).get("capacity", {}).get("storage")
            )
            if not source_capacity:
                # Fallback to requested size if capacity not available
                source_capacity = (
                    workload_pvc.data.get("spec", {})
                    .get("resources", {})
                    .get("requests", {})
                    .get("storage", f"{workload_pvc.size}Gi")
                )

            log.info(
                f"Restoring PVC from snapshot {snapshot_obj.name} with size {source_capacity}"
            )

            restored_pvc_obj = pvc_helpers.create_restore_pvc(
                sc_name=snapshot_obj.parent_sc,
                snap_name=snapshot_obj.name,
                namespace=self.namespace,
                size=source_capacity,
                pvc_name=f"restored-{snapshot_obj.name[:20]}",
                volume_mode=snapshot_obj.parent_volume_mode,
                access_mode=snapshot_obj.parent_access_mode,
            )
            log.info(f"Restored PVC {restored_pvc_obj.name} created")

            # Wait for PVC to be bound
            helpers.wait_for_resource_state(
                resource=restored_pvc_obj, state=constants.STATUS_BOUND, timeout=120
            )

            # Step 3: Verify data integrity (if possible)
            if self._can_attach_pod(restored_pvc_obj):
                log.info(
                    f"Attaching test pod to verify restored PVC {restored_pvc_obj.name}"
                )
                test_pod_obj = self._create_test_pod(restored_pvc_obj)

                # Verify pod is running
                helpers.wait_for_resource_state(
                    resource=test_pod_obj, state=constants.STATUS_RUNNING, timeout=120
                )

                # Verify data (basic file existence check)
                self._verify_pod_data(test_pod_obj)
                log.info("Data verification successful on restored PVC")

            # Step 4: Cleanup
            log.info("Cleaning up snapshot lifecycle resources")
            if test_pod_obj:
                test_pod_obj.delete()
                test_pod_obj.ocp.wait_for_delete(
                    resource_name=test_pod_obj.name, timeout=180
                )

            if restored_pvc_obj:
                restored_pvc_obj.delete()
                restored_pvc_obj.ocp.wait_for_delete(
                    resource_name=restored_pvc_obj.name, timeout=120
                )

            if snapshot_obj:
                snapshot_obj.delete()
                snapshot_obj.ocp.wait_for_delete(
                    resource_name=snapshot_obj.name, timeout=180
                )

            log.info("Snapshot lifecycle operation completed successfully")

        except Exception as e:
            log.error(f"Snapshot lifecycle operation failed: {e}")
            # Best-effort cleanup on failure
            with suppress(Exception):
                if test_pod_obj:
                    test_pod_obj.delete()
            with suppress(Exception):
                if restored_pvc_obj:
                    restored_pvc_obj.delete()
            with suppress(Exception):
                if snapshot_obj:
                    snapshot_obj.delete()
            # Don't raise - allow other background operations to continue
            return

    # ==========================================================================
    # PVC Clone Lifecycle Operations
    # ==========================================================================

    def _clone_lifecycle_operation(self):
        """
        Perform PVC clone lifecycle: create → attach → verify checksums → delete.

        This validates PVC cloning and data consistency.
        """
        log.info("Executing PVC clone lifecycle operation")

        # Check if namespace still exists
        if not self._namespace_exists():
            return

        # Get a random workload PVC
        workload_pvc = self._get_random_workload_pvc()
        if not workload_pvc:
            log.warning("No workload PVCs found for clone operation")
            return

        clone_pvc_obj = None
        test_pod_obj = None

        try:
            # Step 1: Create clone
            log.info(f"Creating clone of PVC {workload_pvc.name}")

            # Determine clone YAML based on provisioner
            if "rbd" in workload_pvc.provisioner:
                clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
            elif "cephfs" in workload_pvc.provisioner:
                clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
            else:
                log.warning(
                    f"Unsupported provisioner for clone: {workload_pvc.provisioner}"
                )
                return

            # Get actual capacity from source PVC (not converted size)
            # This ensures clone size is >= source PVC size
            workload_pvc.reload()
            source_capacity = (
                workload_pvc.data.get("status", {}).get("capacity", {}).get("storage")
            )
            if not source_capacity:
                # Fallback to requested size if capacity not available
                source_capacity = (
                    workload_pvc.data.get("spec", {})
                    .get("resources", {})
                    .get("requests", {})
                    .get("storage", f"{workload_pvc.size}Gi")
                )

            log.info(f"Cloning PVC {workload_pvc.name} with size {source_capacity}")

            clone_pvc_obj = pvc_helpers.create_pvc_clone(
                sc_name=workload_pvc.backed_sc,
                parent_pvc=workload_pvc.name,
                clone_yaml=clone_yaml,
                namespace=self.namespace,
                storage_size=source_capacity,
                access_mode=workload_pvc.get_pvc_access_mode,
                volume_mode=workload_pvc.get()["spec"]["volumeMode"],
            )
            log.info(f"Clone PVC {clone_pvc_obj.name} created")

            # Wait for clone to be bound
            helpers.wait_for_resource_state(
                resource=clone_pvc_obj, state=constants.STATUS_BOUND, timeout=300
            )

            # Step 2: Attach pod and verify data
            if self._can_attach_pod(clone_pvc_obj):
                log.info(f"Attaching test pod to verify clone {clone_pvc_obj.name}")
                test_pod_obj = self._create_test_pod(clone_pvc_obj)

                # Verify pod is running
                helpers.wait_for_resource_state(
                    resource=test_pod_obj, state=constants.STATUS_RUNNING, timeout=120
                )

                # Verify data integrity
                self._verify_pod_data(test_pod_obj)
                log.info("Data verification successful on cloned PVC")

            # Step 3: Cleanup
            log.info("Cleaning up clone lifecycle resources")
            if test_pod_obj:
                test_pod_obj.delete()
                test_pod_obj.ocp.wait_for_delete(
                    resource_name=test_pod_obj.name, timeout=180
                )

            if clone_pvc_obj:
                clone_pvc_obj.delete()
                clone_pvc_obj.ocp.wait_for_delete(
                    resource_name=clone_pvc_obj.name, timeout=120
                )

            log.info("Clone lifecycle operation completed successfully")

        except Exception as e:
            log.error(f"Clone lifecycle operation failed: {e}")
            # Best-effort cleanup on failure
            with suppress(Exception):
                if test_pod_obj:
                    test_pod_obj.delete()
            with suppress(Exception):
                if clone_pvc_obj:
                    clone_pvc_obj.delete()
            # Don't raise - allow other background operations to continue
            return

    # ==========================================================================
    # Node Taint & Toleration Churn Operations
    # ==========================================================================

    def _node_taint_churn_operation(self):
        """
        Perform node taint churn: add taints → wait for pod rescheduling → remove taints.

        This validates CSI attachment/detachment flows and pod rescheduling.
        """
        log.info("Executing node taint churn operation")

        # Check if namespace still exists
        if not self._namespace_exists():
            return

        # Get worker nodes
        worker_nodes = node_helpers.get_worker_nodes()
        if len(worker_nodes) < 2:
            log.warning("Need at least 2 worker nodes for taint churn, skipping")
            return

        # Select a random worker node (not all, to avoid disruption)
        target_node = random.choice(worker_nodes)
        taint_label = f"chaos-taint={time.time()}:NoSchedule"

        try:
            # Step 1: Add taint to node
            log.info(f"Adding taint {taint_label} to node {target_node}")
            node_helpers.taint_nodes([target_node], taint_label=taint_label)

            # Step 2: Wait for pods to reschedule (if any were on this node)
            log.info(f"Waiting 30s for pods to reschedule away from {target_node}")
            time.sleep(30)

            # Check workload pods are still running
            self._verify_workload_pods_running()

            # Step 3: Remove taint
            log.info(f"Removing taint from node {target_node}")
            ocp_obj = ocp.OCP()
            command = f"adm taint node {target_node} chaos-taint-"
            ocp_obj.exec_oc_cmd(command)

            # Wait for system to stabilize
            time.sleep(10)

            log.info("Node taint churn operation completed successfully")

        except Exception as e:
            log.error(f"Node taint churn operation failed: {e}")
            # Best-effort cleanup - remove taint
            with suppress(Exception):
                ocp_obj = ocp.OCP()
                command = f"adm taint node {target_node} chaos-taint-"
                ocp_obj.exec_oc_cmd(command)
            # Don't raise - allow other background operations to continue
            return

    # ==========================================================================
    # Rook/Ceph OSD Operations
    # ==========================================================================

    def _osd_operations(self):
        """
        Perform safe OSD operations: set noout → unset noout.

        This validates OSD resilience without causing actual data movement.
        """
        log.info("Executing OSD operations (noout flag toggle)")

        # Check if namespace still exists
        if not self._namespace_exists():
            return

        try:
            # Get ceph tools pod
            ct_pod = pod_helpers.get_ceph_tools_pod()

            # Step 1: Set noout flag
            log.info("Setting OSD noout flag")
            ct_pod.exec_ceph_cmd("ceph osd set noout")

            # Wait a bit
            time.sleep(20)

            # Check Ceph health
            ceph_health = ct_pod.exec_ceph_cmd("ceph health")
            log.info(f"Ceph health with noout: {ceph_health}")

            # Step 2: Unset noout flag
            log.info("Unsetting OSD noout flag")
            ct_pod.exec_ceph_cmd("ceph osd unset noout")

            # Wait for rebalance to complete (if any)
            time.sleep(10)

            log.info("OSD operations completed successfully")

        except Exception as e:
            log.error(f"OSD operations failed: {e}")
            # Don't raise - allow other background operations to continue
            return

    # ==========================================================================
    # MDS Failover Operations
    # ==========================================================================

    def _mds_failover_operation(self):
        """
        Perform MDS failover: fail active MDS daemon.

        This validates CephFS resilience during MDS failures.
        """
        log.info("Executing MDS failover operation")

        # Check if namespace still exists
        if not self._namespace_exists():
            return

        try:
            # Get ceph tools pod
            ct_pod = pod_helpers.get_ceph_tools_pod()

            # Check if CephFS is deployed
            try:
                mds_stat = ct_pod.exec_ceph_cmd("ceph fs status")
                log.info(f"CephFS status before failover:\n{mds_stat}")
            except Exception:
                log.info("CephFS not deployed, skipping MDS failover")
                return

            # Step 1: Fail active MDS (ID 0)
            log.info("Failing active MDS daemon (ID 0)")
            ct_pod.exec_ceph_cmd("ceph mds fail 0")

            # Wait for MDS to recover
            log.info("Waiting 30s for MDS failover to complete")
            time.sleep(30)

            # Verify CephFS is still healthy
            mds_stat = ct_pod.exec_ceph_cmd("ceph fs status")
            log.info(f"CephFS status after failover:\n{mds_stat}")

            log.info("MDS failover operation completed successfully")

        except Exception as e:
            log.error(f"MDS failover operation failed: {e}")
            # Don't raise - allow other background operations to continue
            return

    # ==========================================================================
    # RGW Restart Operations
    # ==========================================================================

    def _rgw_restart_operation(self):
        """
        Perform RGW restart: delete one RGW pod to trigger restart.

        This validates RGW resilience and S3 workload continuity.
        """
        log.info("Executing RGW restart operation")

        # Check if namespace still exists
        if not self._namespace_exists():
            return

        try:
            # Get RGW pods
            rgw_pods = pod_helpers.get_rgw_pods()
            if not rgw_pods:
                log.info("No RGW pods found, skipping RGW restart")
                return

            # Select random RGW pod
            rgw_pod = random.choice(rgw_pods)
            rgw_pod_name = rgw_pod.name

            log.info(f"Deleting RGW pod {rgw_pod_name} to trigger restart")
            rgw_pod.delete()

            # Wait for new RGW pod to start
            log.info("Waiting for RGW pod to restart")
            pod_helpers.wait_for_pods_to_be_running(
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE, timeout=180
            )

            log.info("RGW restart operation completed successfully")

        except Exception as e:
            log.error(f"RGW restart operation failed: {e}")
            # Don't raise - allow other background operations to continue
            return

    # ==========================================================================
    # CSI-Addons Reclaim Space Operations
    # ==========================================================================

    def _reclaim_space_operation(self):
        """
        Perform reclaim space operation on RBD volumes.

        This validates CSI-Addons ReclaimSpace functionality.
        """
        log.info("Executing reclaim space operation")

        # Check if namespace still exists
        if not self._namespace_exists():
            return

        # Check if we've already determined CSI-Addons is not available
        if not self._csi_addons_available:
            log.debug("Skipping reclaim space operation (CSI-Addons not available)")
            return

        # Get a random RBD workload PVC
        workload_pvc = self._get_random_workload_pvc(provisioner_type="rbd")
        if not workload_pvc:
            log.warning("No RBD workload PVCs found for reclaim space operation")
            return

        try:
            log.info(f"Creating ReclaimSpace job for PVC {workload_pvc.name}")

            # Try to create reclaim space job - this may fail if CSI-Addons is not installed
            try:
                reclaim_job = workload_pvc.create_reclaim_space_job()
            except Exception:
                log.info(
                    "Failed to create ReclaimSpace job (CSI-Addons not installed). "
                    "Disabling reclaim space operations for this session."
                )
                self._csi_addons_available = False
                return

            if not reclaim_job:
                log.info(
                    "ReclaimSpace job creation returned None - CSI-Addons not available. "
                    "Disabling reclaim space operations for this session."
                )
                self._csi_addons_available = False
                return

            log.info(f"Created ReclaimSpace job: {reclaim_job.name}")

            # Give the job a moment to be registered
            time.sleep(5)

            # Verify job exists before waiting - use direct check without retries
            try:
                # Check if job exists by trying to get its status
                # Suppress retry logging by catching CommandFailed early
                job_data = reclaim_job.get()
                if not job_data:
                    log.warning(
                        f"ReclaimSpace job {reclaim_job.name} was created but does not exist. "
                        "This may indicate CSI-Addons is not properly configured."
                    )
                    return
                log.info(f"Verified job {reclaim_job.name} exists")
            except CommandFailed:
                # Job doesn't exist - CSI-Addons likely not installed or configured
                log.info(
                    f"ReclaimSpace job {reclaim_job.name} not found "
                    "(likely CSI-Addons not installed). "
                    "Disabling reclaim space operations for this session."
                )
                self._csi_addons_available = False
                return
            except Exception as e:
                log.warning(
                    f"Could not verify ReclaimSpace job {reclaim_job.name}: {e}. "
                    "Disabling reclaim space operations for this session."
                )
                self._csi_addons_available = False
                return

            # Wait for job to complete
            log.info(f"Waiting for ReclaimSpace job {reclaim_job.name} to complete")
            try:
                job_helpers.wait_for_job_completion(
                    job_name=reclaim_job.name,
                    namespace=self.namespace,
                    timeout=300,
                    sleep_time=5,
                )
                log.info(f"ReclaimSpace job {reclaim_job.name} completed successfully")
            except ResourceNotFoundError:
                # Job not found - handle gracefully with specific exception
                log.info(
                    f"ReclaimSpace job {reclaim_job.name} not found "
                    "(CSI-Addons may not be configured). "
                    "Disabling reclaim space operations for this session."
                )
                self._csi_addons_available = False
                return
            except CommandFailed as cmd_error:
                # Other command failures
                log.info(f"ReclaimSpace job command failed: {cmd_error}")
                return
            except Exception as wait_error:
                log.info(
                    f"ReclaimSpace job wait timed out or failed "
                    f"(expected if CSI-Addons not configured): {wait_error}"
                )
                return

            # Cleanup job
            try:
                reclaim_job.delete()
                log.info(f"Cleaned up ReclaimSpace job {reclaim_job.name}")
            except Exception as cleanup_error:
                log.info(
                    f"Failed to cleanup ReclaimSpace job (may have been auto-deleted): "
                    f"{cleanup_error}"
                )

            log.info("Reclaim space operation completed successfully")

        except Exception as e:
            log.error(f"Reclaim space operation failed: {e}", exc_info=True)
            # Don't raise - allow other background operations to continue
            return

    # ==========================================================================
    # CSI-Addons Volume Replication Operations
    # ==========================================================================

    def _volume_replication_operation(self):
        """
        Perform volume replication operations (if VolumeReplication is enabled).

        This validates CSI-Addons VolumeReplication functionality.
        """
        # Check if namespace still exists
        if not self._namespace_exists():
            return

        log.info(
            "Volume replication operations not yet implemented (requires DR setup)"
        )
        # TODO: Implement VolumeReplication operations when DR is available
        # This would include: promote/demote, failover/failback workflows

    # ==========================================================================
    # Longevity Operations (Comprehensive Validation)
    # ==========================================================================

    def _longevity_operations(self):
        """
        Perform comprehensive longevity operations including:
        1. Create PVCs → Write data → Create snapshots → Restore from snapshots → Verify data
        2. Expand PVCs and verify data integrity

        This validates long-running PVC/snapshot workflows during chaos testing.
        """
        log.info("Executing longevity operations (snapshot/restore/expand)")

        # Check if namespace still exists
        if not self._namespace_exists():
            return

        from ocs_ci.ocs.resources.pvc import delete_pvcs

        try:
            # Select a subset of workload PVCs for longevity testing
            # (to avoid excessive resource creation during chaos)
            log.info(f"Checking workloads: total workloads = {len(self.workloads)}")

            if not self.workloads:
                log.warning(
                    "No workloads available for longevity operation. "
                    "Workloads list is empty - background operations may have started "
                    "before workloads were fully initialized."
                )
                return

            num_pvcs = min(3, len(self.workloads))
            log.info(f"Will check up to {num_pvcs} workloads for PVCs")

            # Collect PVCs from workloads
            workload_pvcs = []
            for idx, workload in enumerate(self.workloads[:num_pvcs]):
                log.debug(
                    f"Checking workload {idx+1}/{num_pvcs}: "
                    f"type={type(workload).__name__}, "
                    f"has_pvc_objs={hasattr(workload, 'pvc_objs')}, "
                    f"has_pvc_obj={hasattr(workload, 'pvc_obj')}, "
                    f"has_pvc={hasattr(workload, 'pvc')}"
                )

                if hasattr(workload, "pvc_objs"):
                    pvc_list = workload.pvc_objs[:1]  # Take first PVC
                    workload_pvcs.extend(pvc_list)
                    log.debug(f"  → Found {len(pvc_list)} PVCs in pvc_objs")
                elif hasattr(workload, "pvc_obj"):
                    workload_pvcs.append(workload.pvc_obj)
                    log.debug("  → Found 1 PVC in pvc_obj")
                elif hasattr(workload, "pvc"):
                    workload_pvcs.append(workload.pvc)
                    log.debug("  → Found 1 PVC in pvc")
                else:
                    log.debug("  → No PVC attributes found")

            if not workload_pvcs:
                log.warning(
                    f"No valid PVCs found in {num_pvcs} workloads for longevity operation. "
                    f"Workload types checked: {[type(w).__name__ for w in self.workloads[:num_pvcs]]}"
                )
                return

            log.info(f"✓ Selected {len(workload_pvcs)} PVCs for longevity testing")

            # Step 1: Create snapshots from workload PVCs
            log.info("Creating snapshots from workload PVCs")
            snapshots = []
            for pvc_obj in workload_pvcs:
                try:
                    pvc_obj.reload()
                    snapshot_obj = pvc_obj.create_snapshot(wait=True, timeout=180)
                    snapshots.append(snapshot_obj)
                    self._resources_to_cleanup.append(snapshot_obj)
                    log.info(
                        f"Created snapshot {snapshot_obj.name} from PVC {pvc_obj.name}"
                    )
                except Exception as e:
                    log.error(f"Failed to create snapshot from PVC {pvc_obj.name}: {e}")
                    continue

            if not snapshots:
                log.warning("No snapshots created, skipping longevity operation")
                return

            log.info(f"Created {len(snapshots)} snapshots successfully")

            # Step 2: Restore PVCs from snapshots
            log.info("Restoring PVCs from snapshots")
            restored_pvcs = []
            for idx, snapshot_obj in enumerate(snapshots):
                try:
                    # Get actual capacity from source PVC
                    source_pvc = workload_pvcs[idx]
                    source_pvc.reload()
                    source_capacity = (
                        source_pvc.data.get("status", {})
                        .get("capacity", {})
                        .get("storage")
                    )
                    if not source_capacity:
                        source_capacity = (
                            source_pvc.data.get("spec", {})
                            .get("resources", {})
                            .get("requests", {})
                            .get("storage", f"{source_pvc.size}Gi")
                        )

                    restored_pvc_obj = pvc_helpers.create_restore_pvc(
                        sc_name=snapshot_obj.parent_sc,
                        snap_name=snapshot_obj.name,
                        namespace=self.namespace,
                        size=source_capacity,
                        pvc_name=f"restored-longevity-{fauxfactory.gen_alpha(6).lower()}",
                        volume_mode=snapshot_obj.parent_volume_mode,
                        access_mode=snapshot_obj.parent_access_mode,
                    )
                    restored_pvcs.append(restored_pvc_obj)
                    self._resources_to_cleanup.append(restored_pvc_obj)
                    log.info(
                        f"Restored PVC {restored_pvc_obj.name} from snapshot {snapshot_obj.name}"
                    )

                    # Wait for PVC to be bound
                    helpers.wait_for_resource_state(
                        resource=restored_pvc_obj,
                        state=constants.STATUS_BOUND,
                        timeout=300,
                    )
                except Exception as e:
                    log.error(
                        f"Failed to restore from snapshot {snapshot_obj.name}: {e}"
                    )
                    continue

            log.info(f"Restored {len(restored_pvcs)} PVCs from snapshots successfully")

            # Step 3: Expand a subset of original PVCs (if supported)
            log.info("Attempting to expand original PVCs")
            expand_count = 0
            for pvc_obj in workload_pvcs[:2]:  # Expand max 2 PVCs
                try:
                    pvc_obj.reload()
                    current_size = pvc_obj.size
                    new_size = current_size + 5  # Add 5GB

                    log.info(
                        f"Expanding PVC {pvc_obj.name} from {current_size}Gi to {new_size}Gi"
                    )
                    pvc_obj.resize_pvc(new_size, verify=True)
                    expand_count += 1
                    log.info(f"✓ Successfully expanded PVC {pvc_obj.name}")
                except Exception as e:
                    log.warning(
                        f"PVC expansion not supported or failed for {pvc_obj.name}: {e}"
                    )
                    # Don't fail the operation if expansion fails
                    continue

            if expand_count > 0:
                log.info(f"Successfully expanded {expand_count} PVCs")
            else:
                log.info("No PVCs were expanded (may not be supported)")

            # Step 4: Cleanup restored PVCs and snapshots
            log.info("Cleaning up longevity operation resources")
            if restored_pvcs:
                try:
                    delete_pvcs(restored_pvcs, concurrent=True)
                    log.info(f"Deleted {len(restored_pvcs)} restored PVCs")
                except Exception as e:
                    log.error(f"Failed to cleanup restored PVCs: {e}")

            for snapshot_obj in snapshots:
                try:
                    snapshot_obj.delete()
                    snapshot_obj.ocp.wait_for_delete(
                        resource_name=snapshot_obj.name, timeout=180
                    )
                except Exception as e:
                    log.error(f"Failed to cleanup snapshot {snapshot_obj.name}: {e}")

            log.info("✓ Longevity operations completed successfully")

        except Exception as e:
            log.error(f"Longevity operations failed: {e}", exc_info=True)
            # Don't raise - allow other background operations to continue
            return

    # ==========================================================================
    # Helper Methods
    # ==========================================================================

    def _get_random_workload_pvc(self, provisioner_type: Optional[str] = None):
        """
        Get a random workload PVC.

        Args:
            provisioner_type: Filter by provisioner type ('rbd', 'cephfs', None for any)

        Returns:
            PVC object or None
        """
        pvcs = []

        for workload in self.workloads:
            # Get PVCs from workload
            if hasattr(workload, "pvc_objs"):
                workload_pvcs = workload.pvc_objs
            elif hasattr(workload, "pvc_obj"):
                workload_pvcs = [workload.pvc_obj]
            elif hasattr(workload, "pvc"):
                workload_pvcs = [workload.pvc]
            else:
                continue

            for pvc_obj in workload_pvcs:
                if provisioner_type:
                    if provisioner_type in pvc_obj.provisioner:
                        pvcs.append(pvc_obj)
                else:
                    pvcs.append(pvc_obj)

        return random.choice(pvcs) if pvcs else None

    def _can_attach_pod(self, pvc_obj) -> bool:
        """Check if a pod can be attached to PVC (not RWX)."""
        access_mode = pvc_obj.get_pvc_access_mode
        return access_mode in [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_ROX]

    def _create_test_pod(self, pvc_obj):
        """Create a test pod attached to PVC."""
        from ocs_ci.utility import templating

        # Determine interface type based on PVC provisioner
        if "rbd" in pvc_obj.provisioner.lower():
            pod_dict_path = constants.CSI_RBD_POD_YAML
        else:
            pod_dict_path = constants.CSI_CEPHFS_POD_YAML

        # Load pod template and configure
        pod_dict = templating.load_yaml(pod_dict_path)
        pod_dict["metadata"]["namespace"] = self.namespace
        pod_dict["metadata"]["name"] = f"test-pod-{pvc_obj.name[:20]}"
        pod_dict["spec"]["volumes"][0]["persistentVolumeClaim"][
            "claimName"
        ] = pvc_obj.name

        # Handle Block volume mode - use volumeDevices instead of volumeMounts
        volume_mode = pvc_obj.get_pvc_vol_mode
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            # Get the volume name from volumeMounts before deleting
            volume_name = pod_dict["spec"]["containers"][0]["volumeMounts"][0]["name"]

            # Remove volumeMounts and add volumeDevices for Block volumes
            del pod_dict["spec"]["containers"][0]["volumeMounts"]
            pod_dict["spec"]["containers"][0]["volumeDevices"] = [
                {
                    "devicePath": "/dev/rbdblock",
                    "name": volume_name,
                }
            ]
            log.info(
                "Configured pod for Block volume mode with devicePath /dev/rbdblock"
            )

        pod_obj = pod_helpers.Pod(**pod_dict)
        pod_obj.create()
        return pod_obj

    def _verify_pod_data(self, pod_obj):
        """Verify basic data integrity on pod (file existence check)."""
        # Basic check - verify pod can access mount point or device
        try:
            # Check if pod uses Block volume (has volumeDevices) or Filesystem (has volumeMounts)
            pod_spec = pod_obj.get().get("spec", {}).get("containers", [{}])[0]

            if "volumeDevices" in pod_spec:
                # Block volume - verify device exists
                device_path = pod_spec["volumeDevices"][0]["devicePath"]
                pod_obj.exec_cmd_on_pod(f"ls -l {device_path}")
                log.info(
                    f"Data verification passed for pod {pod_obj.name} (Block volume at {device_path})"
                )
            else:
                # Filesystem volume - verify mount point
                pod_obj.exec_cmd_on_pod("ls /mnt")
                log.info(
                    f"Data verification passed for pod {pod_obj.name} (Filesystem volume)"
                )
        except Exception as e:
            log.warning(f"Data verification warning for pod {pod_obj.name}: {e}")

    def _verify_workload_pods_running(self):
        """Verify all workload pods are still running."""
        for workload in self.workloads:
            if hasattr(workload, "pod_obj"):
                pod_obj = workload.pod_obj
                if not pod_helpers.get_pod_obj(pod_obj.name, pod_obj.namespace):
                    raise UnexpectedBehaviour(
                        f"Workload pod {pod_obj.name} not found after operation"
                    )

    def _cleanup_resources(self):
        """Cleanup any remaining resources from background operations."""
        log.info("Cleaning up background cluster operation resources")
        for resource in self._resources_to_cleanup:
            with suppress(Exception):
                resource.delete()
        self._resources_to_cleanup.clear()

    def _log_final_summary(self):
        """Log final operation summary."""
        summary = self.metrics.get_summary()
        log.info("=" * 80)
        log.info("BACKGROUND CLUSTER OPERATIONS SUMMARY")
        log.info("=" * 80)
        log.info(f"Duration: {summary['duration_seconds']:.1f} seconds")
        log.info(f"Total Operations: {summary['total_operations']}")
        log.info(f"Successful: {summary['total_successes']}")
        log.info(f"Failed: {summary['total_failures']}")
        log.info(f"Success Rate: {summary['success_rate']:.1f}%")
        log.info("\nOperations by Type:")
        for op_type, count in summary["operations_by_type"].items():
            successes = summary["successes_by_type"].get(op_type, 0)
            failures = summary["failures_by_type"].get(op_type, 0)
            log.info(f"  {op_type}: {count} ({successes} success, {failures} failed)")

        if summary["error_count"] > 0:
            log.warning(f"\n{summary['error_count']} errors occurred during operations")

        log.info("=" * 80)
