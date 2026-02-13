"""
Factory for creating workloads for resiliency testing.

This module provides workload creation and management for resiliency tests,
similar to the KrknWorkloadFactory used in krkn chaos tests.
"""

import logging
import fauxfactory  # type: ignore[import-untyped]

from ocs_ci.ocs import constants
from ocs_ci.resiliency.resiliency_workload_config import ResiliencyWorkloadConfig
from ocs_ci.helpers.vdbench_helpers import create_temp_config_file
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


class ResiliencyWorkloadOps:
    """
    Workload operations manager for resiliency testing.

    This class manages workloads during resiliency testing, providing methods
    to validate workload health, start background operations, and perform cleanup.
    """

    def __init__(self, project, workloads, workload_types=None, scaling_helper=None):
        """
        Initialize ResiliencyWorkloadOps.

        Args:
            project: OCS project object
            workloads: List of workload objects or dict of {workload_type: [workload_objects]}
            workload_types: List of workload types (VDBENCH, CNV_WORKLOAD, FIO, etc.)
            scaling_helper: Optional WorkloadScalingHelper instance
        """
        self.project = project
        self.namespace = project.namespace

        # Handle both old format (single type) and new format (multiple types)
        if isinstance(workloads, dict):
            self.workloads_by_type = workloads
            self.workloads = []
            for wl_list in workloads.values():
                self.workloads.extend(wl_list)
        else:
            self.workloads = workloads
            self.workloads_by_type = {}

        self.workload_types = workload_types or ["VDBENCH"]
        # Backward compatibility
        self.workload_type = (
            self.workload_types[0] if self.workload_types else "VDBENCH"
        )

        # Scaling helper
        self.scaling_helper = scaling_helper
        self.scaling_thread = None

        # Background cluster operations
        self.background_cluster_ops = None
        self.background_cluster_validator = None

    def setup_workloads(self):
        """
        Start all workloads.

        This method:
        1. Starts all configured workloads
        2. Optionally starts background cluster operations
        3. Optionally starts background scaling operations
        """
        log.info(f"Setting up {len(self.workloads)} workloads for resiliency testing")

        # Start all workloads
        for workload in self.workloads:
            log.info(f"Starting workload: {workload}")
            workload.start_workload()

        # Start background cluster operations if enabled
        config = ResiliencyWorkloadConfig()
        bg_ops_config = config.get_background_operations_config()

        if bg_ops_config.get("enabled", False):
            log.info("Starting background cluster operations")
            self._start_background_cluster_operations()

        # Start background scaling if enabled and helper is provided
        if self.scaling_helper and config.is_scaling_enabled():
            log.info("Starting background scaling operations")
            self._start_background_scaling()

        log.info("All workloads and background operations started successfully")

    def _start_background_cluster_operations(self):
        """Start background cluster operations during workload execution."""
        try:
            from ocs_ci.krkn_chaos.background_cluster_operations import (
                BackgroundClusterOperations,
                BackgroundClusterValidator,
            )

            self.background_cluster_ops = BackgroundClusterOperations()
            self.background_cluster_ops.start_operations()

            self.background_cluster_validator = BackgroundClusterValidator(
                self.background_cluster_ops
            )

            log.info("Background cluster operations started successfully")
        except Exception as e:
            log.warning(f"Failed to start background cluster operations: {e}")

    def _start_background_scaling(self):
        """Start background scaling operations."""
        if not self.scaling_helper:
            log.warning("Scaling helper not provided, skipping background scaling")
            return

        # Filter workloads eligible for scaling (RWX access modes)
        # Only workloads with RWX PVCs can be scaled
        scale_workloads = [
            wl
            for wl in self.workloads
            if hasattr(wl, "pvc")
            and wl.pvc.get_pvc_access_mode
            not in {constants.ACCESS_MODE_RWO, f"{constants.ACCESS_MODE_RWO}-Block"}
        ]

        if not scale_workloads:
            log.info("No workloads eligible for scaling (need RWX access mode)")
            return

        log.info(f"Starting scaling for {len(scale_workloads)} eligible workloads")

        config = ResiliencyWorkloadConfig()
        delay = config.get_scaling_delay()

        self.scaling_thread = self.scaling_helper.start_background_scaling(
            scale_workloads, delay=delay
        )

    def validate_and_cleanup(self):
        """
        Validate workload results and cleanup all resources.

        This method:
        1. Waits for background scaling to complete
        2. Stops and validates all workloads
        3. Stops background cluster operations
        4. Cleans up all resources
        """
        log.info("Starting workload validation and cleanup")

        validation_errors = []

        # Wait for scaling operations to complete
        if self.scaling_thread and self.scaling_helper:
            log.info("Waiting for scaling operations to complete")
            scaling_completed = self.scaling_helper.wait_for_scaling_completion(
                self.scaling_thread, timeout=120
            )
            if not scaling_completed:
                log.warning("Scaling operations may still be running during cleanup")

        # Stop background cluster operations
        if self.background_cluster_ops:
            log.info("Stopping background cluster operations")
            try:
                self.background_cluster_ops.stop_operations()

                # Validate background operations
                if self.background_cluster_validator:
                    validation_result = (
                        self.background_cluster_validator.validate_all_operations()
                    )
                    if not validation_result:
                        validation_errors.append(
                            "Background cluster operations validation failed"
                        )
            except Exception as e:
                log.warning(f"Failed to stop background cluster operations: {e}")

        # Validate and cleanup workloads
        for workload in self.workloads:
            try:
                log.info(f"Validating workload: {workload}")

                # Stop workload
                workload.stop_workload()

                # Get workload results (if available)
                # VDBENCH workloads have workload_impl with logs
                if hasattr(workload, "workload_impl") and hasattr(
                    workload.workload_impl, "get_all_deployment_pod_logs"
                ):
                    result = workload.workload_impl.get_all_deployment_pod_logs()

                    # Validate results
                    if result is None:
                        validation_errors.append(
                            f"Workload {workload.workload_impl.deployment_name} returned no logs"
                        )
                    elif "error" in result.lower():
                        validation_errors.append(
                            f"Workload {workload.workload_impl.deployment_name} failed"
                        )
                else:
                    # For other workloads, just log that they completed
                    log.info(
                        f"Workload {workload} completed - detailed validation not available"
                    )

                # Cleanup workload
                workload.cleanup_workload()

            except UnexpectedBehaviour as e:
                workload_name = (
                    workload.workload_impl.deployment_name
                    if hasattr(workload, "workload_impl")
                    else str(workload)
                )
                validation_errors.append(
                    f"Failed to validate/cleanup workload {workload_name}: {e}"
                )

        # Report validation errors
        if validation_errors:
            error_msg = "\n".join(validation_errors)
            log.error(f"Workload validation errors:\n{error_msg}")
            raise UnexpectedBehaviour(error_msg)

        log.info("All workloads validated and cleaned up successfully")


class ResiliencyWorkloadFactory:
    """
    Factory class for creating different types of workloads for resiliency testing.

    This factory creates workloads based on the configuration in resiliency_tests_config.yaml
    and provides a unified interface for workload management.
    """

    def __init__(self):
        """
        Initialize the workload factory.
        """
        self.config = ResiliencyWorkloadConfig()
        self.workload_types = self.config.get_workloads()
        # Backward compatibility
        self.workload_type = (
            self.workload_types[0] if self.workload_types else "VDBENCH"
        )

    def create_workload_ops(
        self,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
        awscli_pod=None,
        scaling_helper=None,
        timeout=180,
        storageclass_factory=None,
    ):
        """
        Create ResiliencyWorkloadOps based on the configured workload types.

        Args:
            project_factory: Project factory fixture
            multi_pvc_factory: Multi-PVC factory fixture
            resiliency_workload: Resiliency workload fixture
            vdbench_block_config: VDBENCH block config fixture
            vdbench_filesystem_config: VDBENCH filesystem config fixture
            awscli_pod: AWS CLI pod fixture (required for RGW workloads)
            scaling_helper: Optional WorkloadScalingHelper instance
            timeout: Timeout for operations
            storageclass_factory: Storage class factory fixture (for encrypted PVCs)

        Returns:
            ResiliencyWorkloadOps: Configured workload operations manager
        """
        log.info(f"Creating workloads for types: {self.workload_types}")

        # Create project
        proj_obj = project_factory()
        log.info(f"Created project: {proj_obj.namespace}")

        all_workloads = []

        # Create workloads for each configured type
        for workload_type in self.workload_types:
            if workload_type == "VDBENCH":
                workloads = self._create_vdbench_workloads(
                    proj_obj,
                    multi_pvc_factory,
                    resiliency_workload,
                    vdbench_block_config,
                    vdbench_filesystem_config,
                    storageclass_factory,
                )
                all_workloads.extend(workloads)
            elif workload_type == "RGW_WORKLOAD":
                if awscli_pod is None:
                    log.error("RGW workload requires awscli_pod fixture")
                    raise ValueError("awscli_pod fixture is required for RGW workloads")
                workloads = self._create_rgw_workloads(proj_obj, awscli_pod)
                all_workloads.extend(workloads)
            elif workload_type == "CNV_WORKLOAD":
                log.warning("CNV workloads not yet implemented for resiliency tests")
            elif workload_type == "FIO":
                log.warning("FIO workloads not yet implemented for resiliency tests")
            else:
                log.warning(f"Unknown workload type: {workload_type}")

        log.info(f"Created {len(all_workloads)} workloads")

        return ResiliencyWorkloadOps(
            proj_obj, all_workloads, self.workload_types, scaling_helper
        )

    def _create_vdbench_workloads(
        self,
        project,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
        storageclass_factory=None,
    ):
        """
        Create VDBENCH workloads for resiliency testing.

        Args:
            project: OCS project object
            multi_pvc_factory: Multi-PVC factory fixture
            resiliency_workload: Resiliency workload fixture
            vdbench_block_config: VDBENCH block config fixture
            vdbench_filesystem_config: VDBENCH filesystem config fixture

        Returns:
            list: List of VDBENCH workload objects
        """
        log.info("Creating VDBENCH workloads for resiliency testing")

        workloads = []
        config = self.config.get_vdbench_config()

        # Get configuration values
        threads = config.get("threads", 10)
        elapsed = config.get("elapsed", 1200)
        interval = config.get("interval", 60)
        workload_loop = self.config.get_workload_loop()

        block_config = config.get("block", {})
        filesystem_config = config.get("filesystem", {})

        # Create interface configurations
        interface_configs = {}

        # CephFS workloads
        if filesystem_config:
            fs_size = filesystem_config.get("size", "10m")
            fs_depth = filesystem_config.get("depth", 4)
            fs_width = filesystem_config.get("width", 5)
            fs_files = filesystem_config.get("files", 10)
            fs_openflags = filesystem_config.get("openflags", "o_direct")
            fs_patterns = filesystem_config.get("patterns", [])

            interface_configs[constants.CEPHFILESYSTEM] = {
                "access_modes": [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO],
                "config_file": lambda: create_temp_config_file(  # noqa: E501
                    vdbench_filesystem_config(
                        size=fs_size,
                        depth=fs_depth,
                        width=fs_width,
                        files=fs_files,
                        open_flags=fs_openflags,
                        default_threads=threads,
                        elapsed=elapsed,
                        interval=interval,
                        default_rdpct=0,  # All writes
                        precreate_then_run=True,
                        precreate_elapsed=120,
                        precreate_interval=60,
                        precreate_iorate="max",
                        anchor=f"/vdbench-data/{fauxfactory.gen_alpha(8).lower()}",
                        patterns=fs_patterns,
                    )
                ),
            }

        # Block workloads
        if block_config:
            block_size = block_config.get("size", "20g")
            block_patterns = block_config.get("patterns", [])

            interface_configs[constants.CEPHBLOCKPOOL] = {
                "access_modes": [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ],
                "config_file": lambda: create_temp_config_file(  # noqa: E501
                    vdbench_block_config(
                        threads=threads,
                        size=block_size,
                        elapsed=elapsed,
                        interval=interval,
                        patterns=block_patterns,
                    )
                ),
            }

        # Get PVC configuration from config
        num_pvcs_per_interface = self.config.get_num_pvcs_per_interface()
        pvc_size = self.config.get_pvc_size()
        use_encrypted = self.config.use_encrypted_pvc()

        log.info(
            f"Creating {num_pvcs_per_interface} PVCs per storage interface with size {pvc_size}Gi"
        )
        if use_encrypted:
            log.info(
                "Encrypted PVCs are enabled - will create encrypted storage classes"
            )

        # Create encrypted storage classes if needed
        # NOTE: Only RBD (CEPHBLOCKPOOL) supports per-PVC encryption via storage class
        # CephFS does NOT support per-PVC encryption via storage class parameters
        encrypted_storage_classes = {}
        if use_encrypted and storageclass_factory is not None:
            log.info("Creating encrypted storage classes for VDBENCH workloads")
            try:
                # Create encrypted RBD storage class ONLY (CephFS not supported)
                encrypted_rbd_sc = storageclass_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    encrypted=True,
                )
            except Exception as e:
                log.error(f"Failed to create encrypted storage class: {e}")
                log.warning("Falling back to default storage classes")
                use_encrypted = False
            else:
                # Only execute if storage class creation succeeded
                encrypted_storage_classes[constants.CEPHBLOCKPOOL] = encrypted_rbd_sc
                log.info(
                    f"✓ Created encrypted RBD storage class: {encrypted_rbd_sc.name}"
                )

                # IMPORTANT: CephFS encryption is NOT supported via storage class parameters
                # CephFS can only use cluster-wide encryption (configured at StorageCluster level)
                log.info(
                    "NOTE: CephFS PVCs will use default storage class (no per-PVC encryption support)"
                )
                log.info(
                    "      CephFS encryption requires cluster-wide encryption in StorageCluster"
                )

        # Create workloads for each interface
        for interface, config_data in interface_configs.items():
            log.info(f"Creating workloads for interface: {interface}")

            # Use encrypted storage class if available and encryption is enabled
            storageclass = None
            if use_encrypted and interface in encrypted_storage_classes:
                storageclass = encrypted_storage_classes[interface]
                log.info(f"Using encrypted storage class: {storageclass.name}")

            # Create PVCs with increased timeout for resiliency tests
            pvcs = multi_pvc_factory(
                interface=interface,
                project=project,
                storageclass=storageclass,  # Pass encrypted SC if available
                access_modes=config_data["access_modes"],
                size=pvc_size,
                num_of_pvc=num_pvcs_per_interface,
                timeout=180,  # Increased timeout to 180 seconds for PVC bound state
            )

            # Create config file
            config_file = config_data["config_file"]()

            # Create workload for each PVC
            for pvc in pvcs:
                workload = resiliency_workload(
                    "VDBENCH",
                    pvc,
                    vdbench_config_file=config_file,
                    workload_runs=workload_loop,
                )
                workloads.append(workload)

        log.info(f"Created {len(workloads)} VDBENCH workloads")
        return workloads

    def _create_rgw_workloads(self, project, awscli_pod):
        """
        Create RGW workloads for resiliency testing.

        Args:
            project: OCS project object
            awscli_pod: Pod with AWS CLI for S3 operations

        Returns:
            list: List of RGW workload objects
        """
        import time
        from ocs_ci.resiliency.resiliency_workload import RGWWorkload
        from ocs_ci.ocs.resources.objectbucket import RGWOCBucket
        from ocs_ci.ocs.ocp import OCP

        log.info("Creating RGW workloads for resiliency testing")

        # Get RGW configuration from resiliency config
        rgw_config = self.config.get_rgw_config()

        # Configure workload parameters from config
        num_buckets = rgw_config.get("num_buckets", 3)
        iteration_count = rgw_config.get("iteration_count", 10)
        operation_types = rgw_config.get(
            "operation_types", ["upload", "download", "list", "delete"]
        )
        upload_multiplier = rgw_config.get("upload_multiplier", 1)
        metadata_ops_enabled = rgw_config.get("metadata_ops_enabled", False)
        delay_between_iterations = rgw_config.get("delay_between_iterations", 30)
        delete_bucket_on_cleanup = rgw_config.get("delete_bucket_on_cleanup", True)

        workloads = []

        log.info(f"Creating {num_buckets} RGW workloads")

        # Pre-flight check: Verify RGW pods are running
        try:
            log.info("Checking RGW pod health before creating buckets...")
            rgw_pods = OCP(
                kind="pod", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            ).get(selector="app=rook-ceph-rgw")

            if not rgw_pods or "items" not in rgw_pods or not rgw_pods["items"]:
                log.warning(
                    "No RGW pods found - RGW may not be deployed on this cluster"
                )
                log.warning("RGW workload requires RGW to be enabled in ODF deployment")
            else:
                running_pods = sum(
                    1
                    for pod in rgw_pods["items"]
                    if pod.get("status", {}).get("phase") == "Running"
                )
                total_pods = len(rgw_pods["items"])
                log.info(f"RGW pods: {running_pods}/{total_pods} running")

                if running_pods == 0:
                    log.error("No RGW pods are running - cannot create RGW workloads")
                    raise RuntimeError(
                        "RGW service is not available. "
                        "Ensure RGW is enabled in your ODF deployment."
                    )
                elif running_pods < total_pods:
                    log.warning(
                        f"Only {running_pods}/{total_pods} RGW pods are running. "
                        f"This may cause bucket creation failures."
                    )
        except Exception as e:
            log.warning(f"Could not verify RGW pod health: {e}")
            log.warning("Proceeding with bucket creation anyway...")

        # Create RGW workloads
        for i in range(num_buckets):
            try:
                # Create RGW bucket
                bucket_name = f"rgw-workload-{fauxfactory.gen_alpha(4).lower()}"
                log.info(f"Creating RGW bucket: {bucket_name}")

                # Create RGW bucket - it creates the OBC
                rgw_bucket = RGWOCBucket(bucket_name)

                # Give OBC a moment to be reconciled by the operator
                log.info(f"Waiting 15s for OBC {bucket_name} to be reconciled...")
                time.sleep(15)

                # Wait for bucket to be bound and ready (timeout 300s)
                log.info(
                    f"Waiting for bucket {bucket_name} to be ready (up to 5 minutes)..."
                )
                try:
                    rgw_bucket.verify_health(timeout=300)
                    log.info(f"✓ Bucket {bucket_name} is ready")
                except KeyError as e:
                    log.error(f"OBC {bucket_name} missing status field after 300s: {e}")
                    log.error(
                        "The OBC controller has not added status field. "
                        "This indicates the controller is not processing OBC requests."
                    )

                    # Show OBC describe output
                    try:
                        obc_obj = OCP(
                            kind="obc",
                            namespace=rgw_bucket.namespace,
                            resource_name=bucket_name,
                        )
                        describe_out = obc_obj.exec_oc_cmd(
                            f"describe obc {bucket_name}"
                        )
                        log.error(f"OBC {bucket_name} details:\n{describe_out}")
                    except Exception as desc_err:
                        log.warning(f"Could not get OBC describe: {desc_err}")

                    # Don't raise - continue with next bucket
                    continue
                except Exception as e:
                    log.error(f"Bucket {bucket_name} failed to become healthy: {e}")
                    # Check if this is due to cluster health issues
                    if "did not reach a healthy state" in str(e):
                        log.warning(
                            "OBC binding timeout - possible RGW service issue. "
                            "Check cluster health before creating more buckets."
                        )
                    # Continue with next bucket instead of failing completely
                    continue

                # Workload configuration
                workload_config = {
                    "iteration_count": iteration_count,
                    "operation_types": operation_types,
                    "upload_multiplier": upload_multiplier,
                    "metadata_ops_enabled": metadata_ops_enabled,
                    "delay_between_iterations": delay_between_iterations,
                }

                # Create RGW workload
                rgw_workload = RGWWorkload(
                    rgw_bucket=rgw_bucket,
                    awscli_pod=awscli_pod,
                    namespace=project.namespace,
                    workload_config=workload_config,
                    delete_bucket_on_cleanup=delete_bucket_on_cleanup,
                )

                # Start the workload
                rgw_workload.start_workload()

                workloads.append(rgw_workload)
                log.info(f"✓ Created and started RGW workload: {bucket_name}")

            except Exception as e:
                log.error(f"Failed to create RGW workload {i + 1}: {e}")
                import traceback

                log.error(traceback.format_exc())
                # Continue with next workload instead of failing completely
                continue

        if not workloads:
            log.error("Failed to create any RGW workloads")
            log.error(
                "This may be due to cluster health issues. "
                "Check RGW pod status and OBC controller health."
            )
            raise RuntimeError("Failed to create any RGW workloads")

        if len(workloads) < num_buckets:
            log.warning(
                f"Only created {len(workloads)} out of {num_buckets} requested RGW workloads. "
                f"Some buckets failed to bind - check cluster health."
            )
        else:
            log.info(f"✓ Successfully created all {len(workloads)} RGW workloads")

        return workloads
