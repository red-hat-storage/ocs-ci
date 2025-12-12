import logging
import subprocess
import tempfile
import time
from contextlib import suppress
from ocs_ci.ocs import constants
from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

log = logging.getLogger(__name__)


class WorkloadOps:
    """
    Workload operations manager for Krkn chaos testing.

    This class manages workloads during chaos testing, providing methods
    to validate workload health, start background operations, and perform cleanup.
    """

    def __init__(self, project, workloads, workload_types=None):
        """
        Initialize WorkloadOps.

        Args:
            project: OCS project object
            workloads: List of workload objects or dict of {workload_type: [workload_objects]}
            workload_types: List of workload types (VDBENCH, CNV_WORKLOAD, etc.)
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

        # Background cluster operations
        self.background_cluster_ops = None
        self.background_cluster_validator = None

    def setup_workloads(self):
        """
        Set up workloads for chaos testing and start background cluster operations.

        This method:
        1. Validates workloads are ready
        2. Starts background cluster operations for continuous validation
        """
        log.info(f"Setting up {len(self.workloads)} workloads for chaos testing")

        # Validate workloads are ready
        ready_count = 0
        for i, workload in enumerate(self.workloads, 1):
            try:
                workload_type = self._get_workload_type_for_workload(workload)

                if workload_type == KrknWorkloadConfig.VDBENCH:
                    self._validate_vdbench_workload(workload)
                elif workload_type == KrknWorkloadConfig.CNV_WORKLOAD:
                    self._validate_cnv_workload(workload)
                elif workload_type == KrknWorkloadConfig.RGW_WORKLOAD:
                    self._validate_rgw_workload(workload)

                ready_count += 1
            except Exception as e:
                log.warning(f"Issue validating workload {i}: {e}")

        if ready_count == 0:
            raise RuntimeError("No workloads are ready for chaos testing")

        log.info(f"{ready_count}/{len(self.workloads)} workloads ready")

        # Start background cluster operations if enabled
        self._start_background_cluster_operations()

    def _start_background_cluster_operations(self):
        """Start background cluster operations if enabled in configuration."""
        from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

        config = KrknWorkloadConfig()

        # Check if background cluster operations are enabled
        if not config.is_background_cluster_operations_enabled():
            log.info("Background cluster operations disabled in config")
            return

        log.info("Starting background cluster operations")

        try:
            from ocs_ci.krkn_chaos.background_cluster_operations import (
                BackgroundClusterOperations,
            )

            # Get configuration
            enabled_operations = config.get_enabled_background_operations()
            operation_interval = config.get_background_operations_interval()
            max_concurrent = config.get_background_operations_max_concurrent()

            # Create and start background operations
            self.background_cluster_ops = BackgroundClusterOperations(
                workload_ops=self,
                enabled_operations=enabled_operations if enabled_operations else None,
                operation_interval=operation_interval,
                max_concurrent_operations=max_concurrent,
            )
            self.background_cluster_ops.start()

            ops_count = len(enabled_operations) if enabled_operations else "all"
            log.info(
                f"Background cluster operations started with {ops_count} "
                f"operation types (interval: {operation_interval}s, "
                f"max concurrent: {max_concurrent})"
            )

        except Exception as e:
            log.error(f"Failed to start background cluster operations: {e}")
            self.background_cluster_ops = None

    def validate_workload_operations(self):
        """
        Validate workload health without performing cleanup.

        This method validates that workloads are still running correctly
        after chaos testing, without stopping or cleaning them up.
        Useful for intermediate validation during test execution.

        Raises:
            UnexpectedBehaviour: If workload validation fails
            CommandFailed: If command execution fails during validation
        """
        log.info(f"Validating {len(self.workloads)} workloads...")

        validation_errors = []
        for i, workload in enumerate(self.workloads, 1):
            try:
                # Determine workload type for this specific workload
                workload_type = self._get_workload_type_for_workload(workload)

                if workload_type == KrknWorkloadConfig.VDBENCH:
                    self._validate_vdbench_workload(workload)
                elif workload_type == KrknWorkloadConfig.CNV_WORKLOAD:
                    self._validate_cnv_workload(workload)
                elif workload_type == KrknWorkloadConfig.RGW_WORKLOAD:
                    self._validate_rgw_workload(workload)
                else:
                    log.warning(f"Unknown workload type: {workload_type}")

            except Exception as e:
                error_msg = f"Issue validating workload {i}: {e}"
                log.warning(error_msg)
                validation_errors.append(error_msg)

        if validation_errors:
            error_summary = "\n".join(validation_errors)
            log.error(f"Workload validation errors:\n{error_summary}")
            from ocs_ci.ocs.exceptions import UnexpectedBehaviour

            raise UnexpectedBehaviour(
                f"Workload validation failed for {len(validation_errors)} workload(s):\n{error_summary}"
            )

        log.info(f"✓ All {len(self.workloads)} workloads validated successfully")

    def validate_and_cleanup(self):
        """
        Validate workload health and perform cleanup.

        This method:
        1. Stops background cluster operations
        2. Validates workloads are still running
        3. Stops and cleans up all workloads
        """
        # Stop background cluster operations first
        self._stop_background_cluster_operations()

        log.info(f"Validating and cleaning up {len(self.workloads)} workloads")

        for i, workload in enumerate(self.workloads, 1):
            try:

                # Determine workload type for this specific workload
                workload_type = self._get_workload_type_for_workload(workload)

                if workload_type == KrknWorkloadConfig.VDBENCH:
                    self._validate_vdbench_workload(workload)
                elif workload_type == KrknWorkloadConfig.CNV_WORKLOAD:
                    self._validate_cnv_workload(workload)
                elif workload_type == KrknWorkloadConfig.RGW_WORKLOAD:
                    self._validate_rgw_workload(workload)
                else:
                    log.warning(f"Unknown workload type: {workload_type}")

                # Stop and cleanup workload
                workload.stop_workload()
                workload.cleanup_workload()

            except Exception as e:
                log.warning(f"Issue with workload {i} validation/cleanup: {e}")
                # Best effort cleanup even if validation fails
                with suppress(Exception):
                    workload.stop_workload()
                with suppress(Exception):
                    workload.cleanup_workload()

    def _stop_background_cluster_operations(self):
        """Stop background cluster operations."""
        if not self.background_cluster_ops:
            return

        log.info("Stopping background cluster operations")

        try:
            self.background_cluster_ops.stop(cleanup=True)
            log.info("Background cluster operations stopped")
        except Exception as e:
            log.error(f"Error stopping background cluster operations: {e}")
        finally:
            self.background_cluster_ops = None
            self.background_cluster_validator = None

    def _get_workload_type_for_workload(self, workload):
        """
        Determine the workload type for a specific workload object.

        Args:
            workload: Workload object

        Returns:
            str: Workload type
        """
        # Try to find workload in workloads_by_type mapping
        for wl_type, wl_list in self.workloads_by_type.items():
            if workload in wl_list:
                return wl_type

        # Fallback to first workload type or detect from workload object
        if hasattr(workload, "workload_type"):
            return workload.workload_type
        elif hasattr(workload, "__class__"):
            class_name = workload.__class__.__name__.lower()
            if "cnv" in class_name or "vm" in class_name:
                return KrknWorkloadConfig.CNV_WORKLOAD
            elif "vdbench" in class_name:
                return KrknWorkloadConfig.VDBENCH
            elif "rgw" in class_name:
                return KrknWorkloadConfig.RGW_WORKLOAD

        return (
            self.workload_types[0]
            if self.workload_types
            else KrknWorkloadConfig.VDBENCH
        )

    def _validate_vdbench_workload(self, workload):
        """
        Validate VDBENCH workload health and data integrity.

        Checks:
        1. Workload running state
        2. Data integrity validation (parses logs for corruption)
        """
        # Check if workload is still running
        if hasattr(workload, "is_running") and callable(workload.is_running):
            if not workload.is_running():
                log.warning("VDBENCH workload is not running")

        # Validate data integrity if verification is enabled
        try:
            from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

            krkn_config = KrknWorkloadConfig()
            if krkn_config.should_run_verification():
                if hasattr(workload, "validate_data_integrity") and callable(
                    workload.validate_data_integrity
                ):
                    workload.validate_data_integrity()
        except Exception as e:
            log.error(f"Failed to validate data integrity: {e}")
            raise

    def _validate_cnv_workload(self, workload):
        """Validate CNV workload health."""
        # Check if VM is still running
        if hasattr(workload, "vm_obj") and workload.vm_obj:
            vm_status = workload.vm_obj.get_vm_status()
            if vm_status != "Running":
                log.warning(f"CNV VM is not running. Status: {vm_status}")

    def _validate_rgw_workload(self, workload):
        """Validate RGW workload health."""
        # Check if RGW workload is still running
        if hasattr(workload, "is_running") and callable(workload.is_running):
            if not workload.is_running():
                log.warning("RGW workload is not running")

        # Check workload status
        if hasattr(workload, "get_workload_status") and callable(
            workload.get_workload_status
        ):
            try:
                status = workload.get_workload_status()
                log.info(f"RGW workload status: {status}")
                if not status.get("is_running", False):
                    log.warning("RGW workload reports as not running")
            except Exception as e:
                log.warning(f"Failed to get RGW workload status: {e}")


class KrknWorkloadFactory:
    """
    Factory class for creating different types of workloads for Krkn chaos testing.

    This factory creates workloads based on the configuration in krkn_chaos_config.yaml
    and provides a unified interface for workload management.
    """

    def __init__(self):
        """
        Initialize the workload factory.
        """
        self.config = KrknWorkloadConfig()
        self.workload_types = self.config.get_workloads()
        # Backward compatibility
        self.workload_type = (
            self.workload_types[0] if self.workload_types else "VDBENCH"
        )

    def create_workload_ops(
        self,
        project_factory,
        multi_pvc_factory,
        loaded_fixtures=None,
        timeout=360,
        # Backward compatibility - old signature
        resiliency_workload=None,
        vdbench_block_config=None,
        vdbench_filesystem_config=None,
        multi_cnv_workload=None,
    ):
        """
        Create WorkloadOps based on the configured workload types.

        Uses the workload registry for automatic fixture discovery and workload creation.
        This makes it easy to add new workload types - just register them in
        krkn_workload_registry.py!

        Args:
            project_factory: Project factory fixture
            multi_pvc_factory: Multi-PVC factory fixture
            loaded_fixtures: Dict of loaded fixtures (preferred, registry-based)
            timeout: Timeout for operations

            # Backward compatibility (deprecated - use loaded_fixtures)
            resiliency_workload: VDBENCH fixture (optional)
            vdbench_block_config: VDBENCH config (optional)
            vdbench_filesystem_config: VDBENCH config (optional)
            multi_cnv_workload: CNV fixture (optional)

        Returns:
            WorkloadOps: Configured workload operations manager
        """
        from ocs_ci.krkn_chaos.krkn_workload_registry import KrknWorkloadRegistry

        log.info(f"Creating workloads for types: {self.workload_types}")

        # Handle backward compatibility - convert old args to loaded_fixtures
        if loaded_fixtures is None:
            loaded_fixtures = {}
            if resiliency_workload is not None:
                loaded_fixtures["resiliency_workload"] = resiliency_workload
            if vdbench_block_config is not None:
                loaded_fixtures["vdbench_block_config"] = vdbench_block_config
            if vdbench_filesystem_config is not None:
                loaded_fixtures["vdbench_filesystem_config"] = vdbench_filesystem_config
            if multi_cnv_workload is not None:
                loaded_fixtures["multi_cnv_workload"] = multi_cnv_workload

        # Create a shared project for all workloads
        proj_obj = project_factory()

        # Dictionary to store workloads by type
        workloads_by_type = {}
        all_workloads = []

        # Create workloads for each configured type using registry
        for workload_type in self.workload_types:
            if not KrknWorkloadRegistry.is_registered(workload_type):
                log.warning(f"Workload type '{workload_type}' not registered, skipping")
                continue

            # Get required fixtures for this workload type
            required_fixtures = KrknWorkloadRegistry.get_required_fixtures(
                workload_type
            )

            # Check if all required fixtures are available
            missing_fixtures = [
                f for f in required_fixtures if f not in loaded_fixtures
            ]
            if missing_fixtures:
                log.error(
                    f"{workload_type} configured but required fixtures not loaded: "
                    f"{missing_fixtures}. Skipping."
                )
                continue

            # Get factory method for this workload type
            factory_method_name = KrknWorkloadRegistry.get_factory_method(workload_type)
            factory_method = getattr(self, factory_method_name, None)

            if not factory_method:
                log.error(
                    f"Factory method '{factory_method_name}' not found for {workload_type}. "
                    f"Skipping."
                )
                continue

            # Prepare arguments for factory method
            fixture_params = KrknWorkloadRegistry.get_fixture_params(workload_type)
            args = [proj_obj, multi_pvc_factory]  # Common args

            # Add fixture-specific args
            for param in fixture_params:
                args.append(loaded_fixtures.get(param))

            # Create workloads using factory method
            try:
                log.info(f"Creating {workload_type} workloads...")
                workloads = factory_method(*args)
                workloads_by_type[workload_type] = workloads
                all_workloads.extend(workloads)
                log.info(f"✓ Created {len(workloads)} {workload_type} workloads")
            except Exception as e:
                log.error(
                    f"Failed to create {workload_type} workloads: {e}", exc_info=True
                )
                continue

        if not all_workloads:
            # Try fallback to VDBENCH if fixtures are available
            vdbench_fixtures = KrknWorkloadRegistry.get_required_fixtures("VDBENCH")
            fallback_error = None

            if all(f in loaded_fixtures for f in vdbench_fixtures):
                log.warning("No workloads created, falling back to VDBENCH")
                try:
                    vdbench_workloads = self._create_vdbench_workloads_for_project(
                        proj_obj,
                        multi_pvc_factory,
                        loaded_fixtures.get("resiliency_workload"),
                        loaded_fixtures.get("vdbench_block_config"),
                        loaded_fixtures.get("vdbench_filesystem_config"),
                    )
                    workloads_by_type[KrknWorkloadConfig.VDBENCH] = vdbench_workloads
                    all_workloads.extend(vdbench_workloads)
                except Exception as e:
                    fallback_error = e
                    log.error(f"Fallback to VDBENCH failed: {e}", exc_info=True)

            if not all_workloads:
                error_msg = (
                    "No workloads could be created. "
                    f"Configured workload types: {self.workload_types}\n"
                )

                if fallback_error:
                    error_msg += (
                        f"\nVDBENCH fallback also failed with: {type(fallback_error).__name__}: {fallback_error}\n"
                        f"\nThis may indicate cluster issues (PVC provisioning failure, CSI timeout, etc.)"
                    )
                else:
                    error_msg += (
                        "\nCheck that:\n"
                        "1. Required fixtures are loaded for configured workload types\n"
                        "2. Cluster has sufficient resources for workload creation\n"
                        "3. Storage provisioners (CSI) are healthy and responsive"
                    )

                raise RuntimeError(error_msg)

        return WorkloadOps(proj_obj, workloads_by_type, self.workload_types)

    def _create_vdbench_workloads(
        self,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """Create VDBENCH workloads (backward compatibility)."""
        proj_obj = project_factory()
        workloads = self._create_vdbench_workloads_for_project(
            proj_obj,
            multi_pvc_factory,
            resiliency_workload,
            vdbench_block_config,
            vdbench_filesystem_config,
        )
        return WorkloadOps(proj_obj, workloads, [KrknWorkloadConfig.VDBENCH])

    def _create_vdbench_workloads_for_project(
        self,
        proj_obj,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """Create VDBENCH workloads for a given project."""

        def create_temp_config_file(config_dict):
            """Create temporary config file from dictionary."""
            temp_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            )
            import yaml

            yaml.dump(config_dict, temp_file, default_flow_style=False)
            temp_file.flush()
            return temp_file.name

        def get_fs_config():
            # Get configuration from krkn_chaos_config
            vdbench_config = self.config.get_vdbench_config()
            fs_config = vdbench_config.get("filesystem", {})

            # Get common parameters
            threads = vdbench_config.get("threads", 16)
            elapsed = vdbench_config.get("elapsed", 600)
            interval = vdbench_config.get("interval", 60)

            # Get filesystem-specific parameters
            depth = fs_config.get("depth", 4)
            width = fs_config.get("width", 5)
            files = fs_config.get("files", 10)
            file_size = fs_config.get("file_size", "1m")
            openflags = fs_config.get("openflags", "o_direct")

            # Get original patterns from config
            original_patterns = fs_config.get("patterns", [])

            # Check if verification is enabled
            enable_verification = self.config.should_run_verification()
            all_patterns = original_patterns.copy()

            # Add verification patterns only if verification is enabled
            if enable_verification:
                verification_patterns = [
                    {
                        "name": "verify_write_data",
                        "rdpct": 0,  # Write-only
                        "seekpct": 100,  # Random I/O
                        "xfersize": "256k",
                        "skew": 0,
                    },
                    {
                        "name": "verify_read_data",
                        "rdpct": 100,  # Read-only
                        "seekpct": 100,  # Random I/O
                        "xfersize": "256k",
                        "skew": 0,
                    },
                ]
                all_patterns.extend(verification_patterns)

            # Create krkn_chaos_config format that will be processed by _convert_krkn_chaos_config_to_vdbench
            krkn_chaos_config = {
                "vdbench_config": {
                    "threads": threads,
                    "elapsed": elapsed,
                    "interval": interval,
                    "filesystem": {
                        "size": file_size,
                        "depth": depth,
                        "width": width,
                        "files": files,
                        "file_size": file_size,
                        "openflags": openflags,
                        "group_all_fwds_in_one_rd": True,
                        "patterns": all_patterns,
                    },
                }
            }

            return create_temp_config_file(krkn_chaos_config)

        def get_blk_config():
            # Get configuration from krkn_chaos_config
            vdbench_config = self.config.get_vdbench_config()
            block_config = vdbench_config.get("block", {})

            # Get common parameters
            threads = vdbench_config.get("threads", 16)
            elapsed = vdbench_config.get("elapsed", 600)
            interval = vdbench_config.get("interval", 60)

            # Get block-specific parameters
            size = block_config.get("size", "15g")

            # Get original patterns from config
            original_patterns = block_config.get("patterns", [])

            # Check if verification is enabled
            enable_verification = self.config.should_run_verification()
            all_patterns = original_patterns.copy()

            # Add verification patterns only if verification is enabled
            if enable_verification:
                verification_patterns = [
                    {
                        "name": "verify_write_data",
                        "rdpct": 0,  # Write-only
                        "seekpct": 100,  # Random I/O
                        "xfersize": "4k",
                        "skew": 0,
                    },
                    {
                        "name": "verify_read_data",
                        "rdpct": 100,  # Read-only
                        "seekpct": 100,  # Random I/O
                        "xfersize": "4k",
                        "skew": 0,
                    },
                ]
                all_patterns.extend(verification_patterns)

            # Create krkn_chaos_config format that will be processed by _convert_krkn_chaos_config_to_vdbench
            krkn_chaos_config = {
                "vdbench_config": {
                    "threads": threads,
                    "elapsed": elapsed,
                    "interval": interval,
                    "block": {"size": size, "patterns": all_patterns},
                }
            }

            return create_temp_config_file(krkn_chaos_config)

        interface_configs = {
            constants.CEPHFILESYSTEM: {
                "access_modes": [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO],
                "config_file": get_fs_config,
            },
            constants.CEPHBLOCKPOOL: {
                "access_modes": [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ],
                "config_file": get_blk_config,
            },
        }

        workloads = []
        # Get configurable values from krkn config
        num_pvcs = self.config.get_num_pvcs_per_interface()
        pvc_size = self.config.get_pvc_size()

        log.info(
            f"Creating {num_pvcs} PVCs per storage interface with size {pvc_size}Gi"
        )

        for interface, cfg in interface_configs.items():
            try:
                log.info(f"Creating {num_pvcs} PVCs for {interface} interface...")
                pvcs = multi_pvc_factory(
                    interface=interface,
                    project=proj_obj,
                    access_modes=cfg["access_modes"],
                    size=pvc_size,
                    num_of_pvc=num_pvcs,
                    timeout=600,
                )
                config_file = cfg["config_file"]()
                for pvc in pvcs:
                    wl = resiliency_workload(
                        "VDBENCH", pvc, vdbench_config_file=config_file
                    )
                    wl.start_workload()
                    workloads.append(wl)
                log.info(
                    f"✓ Successfully created {len(pvcs)} VDBENCH workloads for {interface}"
                )
            except subprocess.TimeoutExpired as e:
                log.error(f"PVC binding timeout for {interface} interface after 600s")

                # Extract PVC name from the timeout error
                pvc_name = None
                cmd_str = str(e.cmd) if hasattr(e, "cmd") else str(e)
                if "PersistentVolumeClaim" in cmd_str:
                    # Extract PVC name from command
                    parts = cmd_str.split("PersistentVolumeClaim")
                    if len(parts) > 1:
                        # Get next element after 'PersistentVolumeClaim'
                        remaining = parts[1].strip().strip("',[]")
                        pvc_name = remaining.split("'")[0].split(",")[0].strip()

                # Get PVC describe output for debugging
                if pvc_name:
                    try:
                        from ocs_ci.ocs.ocp import OCP

                        pvc_obj = OCP(
                            kind="PersistentVolumeClaim",
                            namespace=proj_obj.namespace,
                            resource_name=pvc_name,
                        )
                        describe_output = pvc_obj.exec_oc_cmd(
                            f"describe pvc {pvc_name}"
                        )
                        log.error(f"PVC {pvc_name} details:\n{describe_output}")
                    except Exception as desc_err:
                        log.warning(f"Could not get PVC describe output: {desc_err}")
                else:
                    log.warning("Could not extract PVC name from timeout error")

                log.warning(
                    f"Skipping {interface} workloads - this may indicate cluster storage issues"
                )
                continue
            except Exception as e:
                log.error(f"Failed to create VDBENCH workloads for {interface}: {e}")
                log.warning("Continuing with other interfaces...")
                continue

        log.info(
            f"Created {len(workloads)} total vdbench workloads "
            f"({num_pvcs} per interface x 2 interfaces)"
        )

        if not workloads:
            log.error("Failed to create any VDBENCH workloads")
            log.error(
                "All PVC bindings failed. This indicates serious cluster storage issues."
            )
            log.error(
                "Check: 1) Storage provisioner health, "
                "2) Available storage capacity, "
                "3) StorageClass configuration"
            )
            raise RuntimeError(
                "Failed to create any VDBENCH workloads - all PVCs failed to bind"
            )
        elif len(workloads) < (num_pvcs * 2):  # 2 interfaces
            log.warning(
                f"Only created {len(workloads)} workloads out of "
                f"{num_pvcs * 2} expected. Some PVCs failed to bind."
            )

        return workloads

    def _create_cnv_workloads(
        self, project_factory, multi_pvc_factory, multi_cnv_workload
    ):
        """Create CNV workloads using the multi_cnv_workload fixture (backward compatibility)."""
        proj_obj = project_factory()
        workloads = self._create_cnv_workloads_for_project(
            proj_obj, multi_pvc_factory, multi_cnv_workload
        )
        return WorkloadOps(proj_obj, workloads, [KrknWorkloadConfig.CNV_WORKLOAD])

    def _create_cnv_workloads_for_project(
        self, proj_obj, multi_pvc_factory, multi_cnv_workload
    ):
        """
        Create CNV workloads for a given project.

        Args:
            proj_obj: Project object
            multi_pvc_factory: Multi-PVC factory (not used by CNV, but required for consistency)
            multi_cnv_workload: Multi CNV workload fixture

        Returns:
            List of CNV VM workload objects
        """
        # Create CNV workloads using the multi_cnv_workload fixture
        # This returns (vm_list_default_compr, vm_list_agg_compr, sc_obj_def_compr, sc_obj_aggressive)
        cnv_workload_result = multi_cnv_workload(namespace=proj_obj.namespace)

        # Extract VM lists from the result
        (
            vm_list_default_compr,
            vm_list_agg_compr,
            sc_obj_def_compr,
            sc_obj_aggressive,
        ) = cnv_workload_result

        # Combine all VMs into a single workload list
        all_vms = vm_list_default_compr + vm_list_agg_compr

        return all_vms

    def _create_rgw_workloads_for_project(
        self, proj_obj, multi_pvc_factory, awscli_pod
    ):
        """
        Create multiple RGW workloads with different configurations.

        Args:
            proj_obj: Project object
            multi_pvc_factory: Multi-PVC factory (not used by RGW, but required for consistency)
            awscli_pod: Pod with AWS CLI for S3 operations

        Returns:
            List of RGW workload objects
        """
        from ocs_ci.resiliency.resiliency_workload import RGWWorkload

        # Get RGW configuration from krkn_config
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
            from ocs_ci.ocs.ocp import OCP
            from ocs_ci.ocs import constants

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

        # Get rgw_bucket_factory from conftest dynamically
        try:
            # This requires the rgw_bucket_factory fixture to be available
            # For now, we'll create buckets using the project's RGW capabilities
            log.info("Creating RGW buckets for workload testing")

            for i in range(num_buckets):
                try:
                    # Create RGW bucket
                    from ocs_ci.ocs.resources.objectbucket import RGWOCBucket
                    import fauxfactory

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
                        log.error(
                            f"OBC {bucket_name} missing status field after 300s: {e}"
                        )
                        log.error(
                            "The OBC controller has not added status field. "
                            "This indicates the controller is not processing OBC requests."
                        )

                        # Show OBC describe output
                        try:
                            from ocs_ci.ocs.ocp import OCP

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
                        namespace=proj_obj.namespace,
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

        except Exception as e:
            log.error(f"Failed to create RGW workloads: {e}")
            raise RuntimeError(f"Failed to create any RGW workloads: {e}")

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
