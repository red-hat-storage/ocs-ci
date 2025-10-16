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
            workload_types: List of workload types (VDBENCH, CNV_WORKLOAD, GOSBENCH, etc.)
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
        scale_workloads = [
            wl
            for wl in self.workloads
            if wl.pvc.get_pvc_access_mode
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

                # Get workload results
                result = workload.workload_impl.get_all_deployment_pod_logs()

                # Stop workload
                workload.stop_workload()

                # Validate results
                if result is None:
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} returned no logs"
                    )
                elif "error" in result.lower():
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} failed"
                    )

                # Cleanup workload
                workload.cleanup_workload()

            except UnexpectedBehaviour as e:
                validation_errors.append(
                    f"Failed to validate/cleanup workload {workload.workload_impl.deployment_name}: {e}"
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
        scaling_helper=None,
        timeout=180,
    ):
        """
        Create ResiliencyWorkloadOps based on the configured workload types.

        Args:
            project_factory: Project factory fixture
            multi_pvc_factory: Multi-PVC factory fixture
            resiliency_workload: Resiliency workload fixture
            vdbench_block_config: VDBENCH block config fixture
            vdbench_filesystem_config: VDBENCH filesystem config fixture
            scaling_helper: Optional WorkloadScalingHelper instance
            timeout: Timeout for operations

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
                )
                all_workloads.extend(workloads)
            elif workload_type == "GOSBENCH":
                log.warning(
                    "GOSBENCH workloads not yet implemented for resiliency tests"
                )
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
            fs_patterns = filesystem_config.get("patterns", [])

            interface_configs[constants.CEPHFILESYSTEM] = {
                "access_modes": [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO],
                "config_file": lambda: create_temp_config_file(
                    vdbench_filesystem_config(
                        size=fs_size,
                        depth=fs_depth,
                        width=fs_width,
                        files=fs_files,
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
                "config_file": lambda: create_temp_config_file(
                    vdbench_block_config(
                        threads=threads,
                        size=block_size,
                        elapsed=elapsed,
                        interval=interval,
                        patterns=block_patterns,
                    )
                ),
            }

        # Create workloads for each interface
        for interface, config_data in interface_configs.items():
            log.info(f"Creating workloads for interface: {interface}")

            # Get PVC size from config
            if interface == constants.CEPHFILESYSTEM:
                pvc_size = 20  # Default for filesystem
            else:
                pvc_size = 20  # Default for block

            # Create PVCs
            pvcs = multi_pvc_factory(
                interface=interface,
                project=project,
                access_modes=config_data["access_modes"],
                size=pvc_size,
                num_of_pvc=4,
            )

            # Create config file
            config_file = config_data["config_file"]()

            # Create workload for each PVC
            for pvc in pvcs:
                workload = resiliency_workload(
                    "VDBENCH", pvc, vdbench_config_file=config_file
                )
                workloads.append(workload)

        log.info(f"Created {len(workloads)} VDBENCH workloads")
        return workloads
