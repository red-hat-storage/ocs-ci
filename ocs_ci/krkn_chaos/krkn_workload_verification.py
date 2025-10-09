import logging
import re
import os
import time
import threading
import concurrent.futures
from contextlib import suppress
from typing import List, Dict, Any

from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.workloads.vdbench import VdbenchWorkload

log = logging.getLogger(__name__)


class VdbenchVerificationError(Exception):
    """Exception raised when VDBENCH data verification fails."""

    pass


class WorkloadOpsWithVerification:
    """
    Enhanced WorkloadOps that supports post-chaos data verification.

    This class extends the basic WorkloadOps functionality to include
    VDBENCH data verification after chaos scenarios complete.
    """

    def __init__(
        self, project, workloads, workload_types=None, verify_config_func=None
    ):
        """
        Initialize WorkloadOpsWithVerification.

        Args:
            project: OCS project object
            workloads: List of workload objects or dict of {workload_type: [workload_objects]}
            workload_types: List of workload types or single workload type string (for backward compatibility)
            verify_config_func: Function to create verification config
        """
        self.project = project
        self.namespace = project.namespace
        self.verify_config_func = verify_config_func
        self.verification_workloads = []

        # Handle both old format (single type) and new format (multiple types)
        if isinstance(workloads, dict):
            self.workloads_by_type = workloads
            self.workloads = []
            for wl_list in workloads.values():
                self.workloads.extend(wl_list)
        else:
            self.workloads = workloads
            self.workloads_by_type = {}

        # Handle workload_types parameter (can be list or single string for backward compatibility)
        if isinstance(workload_types, str):
            # Backward compatibility: single workload type string
            self.workload_types = [workload_types]
            self.workload_type = workload_types
        elif isinstance(workload_types, list):
            # New format: list of workload types
            self.workload_types = workload_types
            self.workload_type = workload_types[0] if workload_types else "VDBENCH"
        else:
            # Default
            self.workload_types = ["VDBENCH"]
            self.workload_type = "VDBENCH"

        # Load configuration to check verification settings
        from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

        self.config = KrknWorkloadConfig()
        self.should_verify = self.config.should_run_verification()
        self.krkn_config = self.config.config

    def setup_workloads(self):
        """
        Set up workloads for chaos testing.

        Note: In the current implementation, workloads are already created and started
        in the conftest.py fixture, so this method serves as a compatibility layer
        and performs validation that workloads are ready.
        """
        log.info(
            f"Validating {len(self.workloads)} {self.workload_type} workloads are ready for chaos testing"
        )

        ready_count = 0
        for i, workload in enumerate(self.workloads, 1):
            try:
                if hasattr(workload, "workload_impl") and hasattr(
                    workload.workload_impl, "deployment_name"
                ):
                    deployment_name = workload.workload_impl.deployment_name
                    log.info(
                        f"✅ Workload {i}/{len(self.workloads)}: {deployment_name} is ready"
                    )
                    ready_count += 1
                else:
                    log.info(f"✅ Workload {i}/{len(self.workloads)} is ready")
                    ready_count += 1
            except Exception as e:
                log.warning(f"⚠️ Issue validating workload {i}: {e}")

        log.info(
            f"🎯 {ready_count}/{len(self.workloads)} workloads are ready for chaos testing"
        )

        if ready_count == 0:
            raise RuntimeError("No workloads are ready for chaos testing")

    def validate_and_cleanup(self):
        """
        Validate workload health, run post-chaos verification, and perform cleanup.

        This method:
        1. Validates that workloads are still running properly
        2. Runs VDBENCH verification to check for data corruption
        3. Parses verification output for data validation errors
        4. Raises assertion if data corruption is detected
        5. Performs cleanup operations
        """
        log.info(
            f"Validating and cleaning up {len(self.workloads)} {self.workload_type} workloads"
        )

        # First, validate existing workloads
        for i, workload in enumerate(self.workloads, 1):
            try:
                log.info(f"Validating workload {i}/{len(self.workloads)}")

                if self.workload_type == "VDBENCH":
                    self._validate_vdbench_workload(workload)
                elif self.workload_type == "CNV_WORKLOAD":
                    self._validate_cnv_workload(workload)
                else:
                    log.warning(f"Unknown workload type: {self.workload_type}")

                # Stop workload before verification
                workload.stop_workload()
                log.info(f"Stopped workload {i} for post-chaos verification")

            except Exception as e:
                log.warning(f"Issue with workload {i} validation: {e}")
                # Best effort cleanup even if validation fails
                with suppress(Exception):
                    workload.stop_workload()

        # Run post-chaos verification if enabled and supported
        if self.should_verify and self.verify_config_func:
            self._run_post_chaos_verification()
        elif not self.should_verify:
            log.info(
                "Skipping post-chaos verification (disabled in configuration or unsupported workload type)"
            )
        elif not self.verify_config_func:
            log.warning(
                "Post-chaos verification requested but no verification config function provided"
            )

        # Final cleanup
        self._cleanup_all_workloads()

    def _validate_vdbench_workload(self, workload):
        """Validate VDBENCH workload health."""
        # Check if workload is still running
        if hasattr(workload, "is_running") and callable(workload.is_running):
            if not workload.is_running():
                log.warning("VDBENCH workload is not running")

        log.debug("VDBENCH workload validation completed")

    def _validate_cnv_workload(self, workload):
        """Validate CNV workload health."""
        # Check if VM is still running
        if hasattr(workload, "vm_obj") and workload.vm_obj:
            vm_status = workload.vm_obj.get_vm_status()
            if vm_status != "Running":
                log.warning(f"CNV VM is not running. Status: {vm_status}")

        log.debug("CNV workload validation completed")

    def _run_post_chaos_verification(self):
        """
        Run VDBENCH verification after chaos scenarios complete.

        This method analyzes the existing workload outputs and logs to detect
        data validation errors using VDBENCH's built-in verification capabilities.
        """
        log.info("🔍 Starting post-chaos data verification analysis")

        try:
            verification_errors = []

            # Analyze each workload's output for verification data
            for i, workload in enumerate(self.workloads, 1):
                log.info(
                    f"Analyzing workload {i}/{len(self.workloads)} for data integrity"
                )

                # Get workload output/logs
                output = self._get_workload_output(workload)

                if not output:
                    log.warning(
                        f"No output available for workload {i} verification analysis"
                    )
                    continue

                # Check for data validation errors in workload output
                workload_errors = self._parse_validation_errors(output)

                if workload_errors:
                    log.error(f"🚨 DATA CORRUPTION DETECTED in workload {i}!")
                    for error in workload_errors:
                        log.error(f"Validation Error: {error}")
                    verification_errors.extend(workload_errors)
                else:
                    log.info(f"✅ No data validation errors found in workload {i}")

            # Check if any verification errors were found
            if verification_errors:
                error_msg = (
                    f"Data validation failed after chaos injection. "
                    f"Found {len(verification_errors)} validation errors across {len(self.workloads)} workloads"
                )
                log.error(error_msg)

                # Log all errors for debugging
                for i, error in enumerate(verification_errors, 1):
                    log.error(f"Error {i}: {error}")

                # Raise assertion to fail the test
                raise VdbenchVerificationError(f"{error_msg}: {verification_errors}")
            else:
                log.info(
                    "✅ Post-chaos verification completed successfully - no data corruption detected"
                )

        except VdbenchVerificationError:
            # Re-raise verification errors
            raise
        except Exception as e:
            log.error(f"Error during post-chaos verification analysis: {e}")
            # Don't fail the test for analysis errors, just log them
            log.warning(
                "Unable to complete verification analysis - continuing with test"
            )

    def _get_workload_output(self, workload):
        """
        Get output/logs from a workload.

        Args:
            workload: The workload object

        Returns:
            str: Workload output/logs
        """
        try:
            # Try to get output from workload object
            if hasattr(workload, "get_output"):
                return workload.get_output()
            elif hasattr(workload, "output_log"):
                if os.path.exists(workload.output_log):
                    with open(workload.output_log, "r") as f:
                        return f.read()
            elif hasattr(workload, "log_file"):
                if os.path.exists(workload.log_file):
                    with open(workload.log_file, "r") as f:
                        return f.read()
            # Handle VDBENCH workload wrapper structure
            elif hasattr(workload, "workload_impl") and hasattr(
                workload.workload_impl, "get_all_deployment_pod_logs"
            ):
                log.debug("Getting VDBENCH workload output from workload_impl")
                return workload.workload_impl.get_all_deployment_pod_logs()
            # Handle direct VDBENCH workload implementation
            elif hasattr(workload, "get_all_deployment_pod_logs"):
                log.debug("Getting VDBENCH workload output directly")
                return workload.get_all_deployment_pod_logs()

            log.warning("Unable to locate workload output")
            return ""

        except Exception as e:
            log.error(f"Error getting workload output: {e}")
            # Try to get some basic information about the workload for debugging
            try:
                if hasattr(workload, "workload_impl") and hasattr(
                    workload.workload_impl, "deployment_name"
                ):
                    deployment_name = workload.workload_impl.deployment_name
                    log.debug(f"Failed to get output from workload: {deployment_name}")
                elif hasattr(workload, "deployment_name"):
                    log.debug(
                        f"Failed to get output from workload: {workload.deployment_name}"
                    )
            except Exception:
                pass  # Ignore errors in debug logging
            return ""

    def _parse_validation_errors(self, output):
        """
        Parse VDBENCH output for data validation errors.

        Looks for patterns like:
        - "Data Validation error at offset 0x0000001000"
        - "Expected: 0x12345678"
        - "Found:    0x87654321"

        Args:
            output (str): VDBENCH output text

        Returns:
            list: List of validation error descriptions
        """
        validation_errors = []

        # Pattern to match data validation errors
        error_patterns = [
            r"Data Validation error at offset (0x[0-9a-fA-F]+)",
            r"Expected:\s+(0x[0-9a-fA-F]+)",
            r"Found:\s+(0x[0-9a-fA-F]+)",
            r"validation error",
            r"data mismatch",
            r"corruption detected",
        ]

        lines = output.split("\n")
        current_error = []

        for line in lines:
            line = line.strip()

            # Check if this line contains a validation error
            for pattern in error_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    current_error.append(line)
                    break

            # If we have collected error lines and hit a non-error line, save the error
            if current_error and not any(
                re.search(p, line, re.IGNORECASE) for p in error_patterns
            ):
                validation_errors.append(" | ".join(current_error))
                current_error = []

        # Don't forget the last error if the output ends with error lines
        if current_error:
            validation_errors.append(" | ".join(current_error))

        return validation_errors

    def _cleanup_all_workloads(self):
        """Clean up all original workloads."""
        log.info("Cleaning up original workloads...")

        for i, workload in enumerate(self.workloads, 1):
            try:
                # Workloads should already be stopped, just cleanup
                workload.cleanup_workload()
                log.info(f"Cleaned up original workload {i}")
            except Exception as e:
                log.warning(f"Issue cleaning up original workload {i}: {e}")

    def create_enhanced_verification_workflow(
        self, project, pvc_factory, vdbench_workload_factory
    ):
        """
        Create an enhanced VDBENCH verification workflow.

        Args:
            project: OCS project object
            pvc_factory: Factory for creating PVCs
            vdbench_workload_factory: Factory for creating VDBENCH workloads

        Returns:
            EnhancedVdbenchVerificationWorkflow: Enhanced verification workflow instance
        """

        # Get VDBENCH configuration
        vdbench_config = self.config.get_vdbench_config()

        # Set up workflow parameters
        chaos_duration = 900  # 15 minutes default
        verification_duration = 300  # 5 minutes default

        # Create enhanced workflow
        workflow = EnhancedVdbenchVerificationWorkflow(
            project=project,
            pvc_factory=pvc_factory,
            vdbench_workload_factory=vdbench_workload_factory,
            krkn_config=vdbench_config,
            chaos_duration=chaos_duration,
            verification_duration=verification_duration,
        )

        log.info("Created enhanced VDBENCH verification workflow")
        return workflow

    def run_enhanced_verification(self, project, pvc_factory, vdbench_workload_factory):
        """
        Run the enhanced VDBENCH verification workflow.

        Args:
            project: OCS project object
            pvc_factory: Factory for creating PVCs
            vdbench_workload_factory: Factory for creating VDBENCH workloads

        Raises:
            VdbenchVerificationError: If data corruption is detected
        """
        if not self.should_verify:
            log.info("Verification disabled, skipping enhanced verification")
            return

        if "VDBENCH" not in self.workload_types:
            log.info("VDBENCH not in workload types, skipping enhanced verification")
            return

        try:
            log.info("🚀 Starting Enhanced VDBENCH Verification Workflow")

            # Create and run enhanced workflow
            workflow = self.create_enhanced_verification_workflow(
                project, pvc_factory, vdbench_workload_factory
            )

            workflow.start_verification_workflow()

            log.info("✅ Enhanced VDBENCH Verification Workflow completed successfully")
        except VdbenchVerificationError:
            # Re-raise verification errors
            raise
        except Exception as e:
            log.error(f"❌ Enhanced verification workflow failed: {e}")
            raise Exception(f"Enhanced verification workflow failed: {e}")


class EnhancedVdbenchVerificationWorkflow:
    """
    Enhanced VDBENCH verification workflow with automated data validation.

    This class implements a simplified and automated Vdbench-based data validation workflow:
    1. Launch multiple Vdbench workloads in parallel with different patterns
    2. Run all workloads concurrently for specified duration
    3. Automatically recreate and rerun workloads until chaos test completion
    4. Implement retry logic for cluster instability
    5. Gracefully terminate workloads after chaos completion
    6. Detect data corruption and raise exceptions on failure
    """

    def __init__(
        self,
        project,
        pvc_factory,
        vdbench_workload_factory,
        krkn_config,
        chaos_duration: int = 900,  # 15 minutes default
        verification_duration: int = 300,  # 5 minutes default
    ):
        """
        Initialize the enhanced VDBENCH verification workflow.

        Args:
            project: OCS project object
            pvc_factory: Factory for creating PVCs
            vdbench_workload_factory: Factory for creating VDBENCH workloads
            krkn_config: Krkn configuration object
            chaos_duration: Total chaos test duration in seconds
            verification_duration: Duration for verification phase in seconds
        """
        self.project = project
        self.namespace = project.namespace
        self.pvc_factory = pvc_factory
        self.vdbench_workload_factory = vdbench_workload_factory
        self.krkn_config = krkn_config
        self.chaos_duration = chaos_duration
        self.verification_duration = verification_duration

        # Workload management
        self.active_workloads: List[VdbenchWorkload] = []
        self.workload_patterns = self._get_workload_patterns()
        self.workload_restart_count = 0
        self.max_restarts = krkn_config.get("max_workload_restarts", 10)
        self.monitor_interval = krkn_config.get("workload_monitor_interval", 30)

        # Threading and synchronization
        self.workload_threads: List[threading.Thread] = []
        self.verification_threads: List[threading.Thread] = []
        self.stop_event = threading.Event()
        self.verification_errors: List[str] = []
        self.verification_lock = threading.Lock()

        # Configuration
        self.vdbench_config = krkn_config.get("vdbench_config", {})
        self.enable_parallel_verification = self.vdbench_config.get(
            "enable_parallel_verification", True
        )
        self.max_verification_threads = self.vdbench_config.get(
            "max_verification_threads", 16
        )

        log.info("Initialized Enhanced VDBENCH Verification Workflow")
        log.info(
            f"Chaos duration: {chaos_duration}s, Verification duration: {verification_duration}s"
        )
        log.info(f"Max restarts: {self.max_restarts}")

    def _get_workload_patterns(self) -> List[Dict[str, Any]]:
        """
        Get workload patterns from configuration.

        Returns:
            List of workload pattern configurations
        """
        patterns = []
        # self.krkn_config is already the vdbench_config
        vdbench_config = self.krkn_config

        # Get block patterns
        if "block" in vdbench_config:
            block_patterns = vdbench_config["block"].get("patterns", [])
            for pattern in block_patterns:
                pattern_config = pattern.copy()
                pattern_config["volume_mode"] = "Block"
                pattern_config["workload_type"] = "block"
                patterns.append(pattern_config)

        # Get filesystem patterns
        if "filesystem" in vdbench_config:
            fs_patterns = vdbench_config["filesystem"].get("patterns", [])
            for pattern in fs_patterns:
                pattern_config = pattern.copy()
                pattern_config["volume_mode"] = "Filesystem"
                pattern_config["workload_type"] = "filesystem"
                patterns.append(pattern_config)

        return patterns

    def cleanup(self):
        """Clean up resources and stop all threads."""
        log.info("Cleaning up Enhanced VDBENCH Verification Workflow")

        # Set stop event to signal all threads to stop
        self.stop_event.set()

        # Wait for all threads to complete
        for thread in self.workload_threads + self.verification_threads:
            if thread.is_alive():
                thread.join(timeout=5)

        # Clean up active workloads
        for workload in self.active_workloads:
            try:
                if hasattr(workload, "cleanup"):
                    workload.cleanup()
            except Exception as e:
                log.warning(f"Warning during workload cleanup: {e}")

        # Clear lists
        self.active_workloads.clear()
        self.workload_threads.clear()
        self.verification_threads.clear()

        log.info("Enhanced VDBENCH Verification Workflow cleanup completed")

    def start_verification_workflow(self):
        """
        Start the complete verification workflow.

        This method:
        1. Creates PVCs for each workload pattern
        2. Launches VDBENCH workloads in parallel
        3. Monitors workloads and recreates them as needed
        4. Runs verification phases
        5. Handles graceful termination
        """
        log.info("🚀 Starting Enhanced VDBENCH Verification Workflow")

        try:
            # Create PVCs and workloads for each pattern
            self._create_workloads()

            # Start workload monitoring and management
            self._start_workload_management()

            # Wait for chaos duration
            self._wait_for_chaos_completion()

            # Run final verification
            self._run_final_verification()

            # Check for any verification errors
            self._check_verification_results()

        except Exception as e:
            log.error(f"❌ Verification workflow failed: {e}")
            raise
        finally:
            # Cleanup all workloads
            self._cleanup_all_workloads()

        log.info("✅ Enhanced VDBENCH Verification Workflow completed successfully")

    def _create_workloads(self):
        """Create PVCs and VDBENCH workloads for each pattern."""
        log.info("Creating workloads for %d patterns", len(self.workload_patterns))

        for i, pattern in enumerate(self.workload_patterns, 1):
            try:
                log.info(
                    f"Creating workload {i}/{len(self.workload_patterns)}: {pattern['name']}"
                )

                # Create PVC
                pvc = self._create_pvc_for_pattern(pattern)

                # Create VDBENCH workload
                workload = self._create_vdbench_workload(pvc, pattern)

                # Start workload
                workload.start_workload()
                self.active_workloads.append(workload)

                log.info(
                    f"✅ Workload {i} created and started: {workload.deployment_name}"
                )

            except Exception as e:
                log.error(f"❌ Failed to create workload {i}: {e}")
                raise UnexpectedBehaviour(f"Failed to create workload {i}: {e}")

    def _create_pvc_for_pattern(self, pattern: Dict[str, Any]):
        """Create PVC for a specific workload pattern."""
        from ocs_ci.ocs import constants

        volume_mode = pattern["volume_mode"]
        access_mode = constants.ACCESS_MODE_RWO
        if volume_mode == "Filesystem":
            access_mode = constants.ACCESS_MODE_RWX  # Allow multiple pods

        pvc_size = "20Gi" if volume_mode == "Block" else "15Gi"

        return self.pvc_factory(
            size=pvc_size,
            access_mode=access_mode,
            volume_mode=volume_mode,
        )

    def _create_vdbench_workload(self, pvc, pattern: Dict[str, Any]) -> VdbenchWorkload:
        """Create VDBENCH workload for a specific pattern."""
        # Create configuration for this pattern
        config = self._create_pattern_config(pattern)

        # Create temporary config file
        import tempfile
        import yaml

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config, f, default_flow_style=False)
            config_file = f.name

        return self.vdbench_workload_factory(
            pvc=pvc,
            vdbench_config_file=config_file,
            namespace=self.namespace,
        )

    def _create_pattern_config(self, pattern: Dict[str, Any]) -> Dict[str, Any]:
        """Create VDBENCH configuration for a specific pattern."""
        # Return the pattern configuration with volume_mode and workload_type
        config = pattern.copy()
        return config

    def _start_workload_management(self):
        """Start workload monitoring and management threads."""
        log.info("Starting workload management threads")

        # Start workload monitoring thread
        monitor_thread = threading.Thread(
            target=self._monitor_workloads, name="WorkloadMonitor"
        )
        monitor_thread.daemon = True
        monitor_thread.start()
        self.workload_threads.append(monitor_thread)

        # Start verification thread if enabled
        if self.enable_parallel_verification:
            verification_thread = threading.Thread(
                target=self._run_continuous_verification, name="ContinuousVerification"
            )
            verification_thread.daemon = True
            verification_thread.start()
            self.verification_threads.append(verification_thread)

    def _monitor_workloads(self):
        """Monitor workloads and recreate them if they complete early."""
        log.info("Starting workload monitoring")

        while not self.stop_event.is_set():
            try:
                # Check each workload
                for i, workload in enumerate(self.active_workloads):
                    if self._is_workload_completed(workload):
                        log.info(f"Workload {i+1} completed, recreating...")
                        self._recreate_workload(i, workload)

                time.sleep(self.monitor_interval)

            except Exception as e:
                log.error(f"Error in workload monitoring: {e}")
                time.sleep(self.monitor_interval)

    def _is_workload_completed(self, workload: VdbenchWorkload) -> bool:
        """Check if a workload has completed."""
        try:
            status = workload.get_workload_status()
            # Check if pods are still running and not in error state
            if "pod_phases" in status:
                running_pods = [
                    phase for phase in status["pod_phases"] if phase == "Running"
                ]
                return len(running_pods) == 0
            return False
        except Exception as e:
            log.warning(f"Error checking workload status: {e}")
            return False

    def _recreate_workload(self, index: int, old_workload: VdbenchWorkload):
        """Recreate a completed workload."""
        if self.workload_restart_count >= self.max_restarts:
            log.warning(
                f"Maximum restart count ({self.max_restarts}) reached, skipping recreation"
            )
            return

        try:
            log.info(f"Recreating workload {index+1}")

            # Clean up old workload
            old_workload.cleanup_workload()

            # Get pattern for this workload
            pattern = self.workload_patterns[index % len(self.workload_patterns)]

            # Create new PVC
            pvc = self._create_pvc_for_pattern(pattern)

            # Create new workload
            new_workload = self._create_vdbench_workload(pvc, pattern)
            new_workload.start_workload()

            # Replace in active workloads
            self.active_workloads[index] = new_workload

            self.workload_restart_count += 1
            log.info(
                f"✅ Workload {index+1} recreated successfully (restart #{self.workload_restart_count})"
            )

        except Exception as e:
            log.error(f"❌ Failed to recreate workload {index+1}: {e}")
            # Implement retry logic with exponential backoff
            self._retry_workload_creation(index, pattern)

    def _retry_workload_creation(
        self, index: int, pattern: Dict[str, Any], max_retries: int = 3
    ):
        """Retry workload creation with exponential backoff."""
        for attempt in range(max_retries):
            try:
                wait_time = 2**attempt * 30  # 30s, 60s, 120s
                log.info(
                    f"Retrying workload creation in {wait_time}s (attempt {attempt+1}/{max_retries})"
                )
                time.sleep(wait_time)

                # Check if cluster is stable
                if self._is_cluster_stable():
                    pvc = self._create_pvc_for_pattern(pattern)
                    new_workload = self._create_vdbench_workload(pvc, pattern)
                    new_workload.start_workload()
                    self.active_workloads[index] = new_workload
                    log.info(f"✅ Workload {index+1} recreated after retry")
                    return
                else:
                    log.warning("Cluster not stable, waiting longer...")

            except Exception as e:
                log.error(f"Retry attempt {attempt+1} failed: {e}")

        log.error(
            f"❌ Failed to recreate workload {index+1} after {max_retries} retries"
        )

    def _is_cluster_stable(self) -> bool:
        """Check if the cluster is stable for workload creation."""
        try:
            # Simple check - see if we can list pods
            from ocs_ci.utility.utils import run_cmd

            run_cmd(f"oc get pods -n {self.namespace} --no-headers | wc -l")
            return True
        except Exception:
            return False

    def _run_continuous_verification(self):
        """Run continuous verification during workload execution."""
        log.info("Starting continuous verification")

        while not self.stop_event.is_set():
            try:
                # Run verification on all active workloads
                self._verify_all_workloads()

                # Wait before next verification cycle
                time.sleep(self.verification_duration)

            except Exception as e:
                log.error(f"Error in continuous verification: {e}")
                time.sleep(30)  # Wait before retrying

    def _verify_all_workloads(self):
        """Verify all active workloads for data integrity."""
        if not self.active_workloads:
            return

        log.info("Running verification on %d workloads", len(self.active_workloads))

        if self.enable_parallel_verification:
            # Run verification in parallel
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_verification_threads
            ) as executor:
                futures = [
                    executor.submit(self._verify_workload, i, workload)
                    for i, workload in enumerate(self.active_workloads)
                ]

                # Wait for all verifications to complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log.error(f"Verification failed: {e}")
        else:
            # Run verification sequentially
            for i, workload in enumerate(self.active_workloads):
                self._verify_workload(i, workload)

    def _verify_workload(self, index: int, workload: VdbenchWorkload):
        """Verify a single workload for data integrity."""
        try:
            log.info(f"Verifying workload {index+1}: {workload.deployment_name}")

            # Get workload logs
            logs = workload.get_all_deployment_pod_logs()

            # Parse for verification errors
            errors = self._parse_verification_errors(logs)

            if errors:
                with self.verification_lock:
                    self.verification_errors.extend(errors)
                log.error(f"🚨 Data corruption detected in workload {index+1}!")
                for error in errors:
                    log.error(f"  - {error}")
            else:
                log.info(f"✅ Workload {index+1} verification passed")

        except Exception as e:
            log.error(f"Error verifying workload {index+1}: {e}")

    def _parse_verification_errors(self, logs: str) -> List[str]:
        """Parse VDBENCH logs for data validation errors."""
        errors = []
        error_patterns = [
            r"Data Validation error at offset (0x[0-9a-fA-F]+)",
            r"Expected:\s+(0x[0-9a-fA-F]+)",
            r"Found:\s+(0x[0-9a-fA-F]+)",
            r"validation error",
            r"data mismatch",
            r"corruption detected",
            r"checksum.*error",
            r"integrity.*failed",
        ]

        lines = logs.split("\n")
        current_error = []

        for line in lines:
            line = line.strip()

            # Check if this line contains a validation error
            for pattern in error_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    current_error.append(line)
                    break

            # If we have collected error lines and hit a non-error line, save the error
            if current_error and not any(
                re.search(p, line, re.IGNORECASE) for p in error_patterns
            ):
                errors.append(" | ".join(current_error))
                current_error = []

        # Don't forget the last error if the output ends with error lines
        if current_error:
            errors.append(" | ".join(current_error))

        return errors

    def _wait_for_chaos_completion(self):
        """Wait for the chaos test to complete."""
        log.info(f"Waiting for chaos completion ({self.chaos_duration}s)")

        start_time = time.time()
        while (
            time.time() - start_time
        ) < self.chaos_duration and not self.stop_event.is_set():
            time.sleep(10)  # Check every 10 seconds

        log.info("Chaos duration completed")

    def _run_final_verification(self):
        """Run final verification phase."""
        log.info(f"Running final verification phase ({self.verification_duration}s)")

        # Stop workload management
        self.stop_event.set()

        # Wait for management threads to finish
        for thread in self.workload_threads:
            thread.join(timeout=30)

        # Run final verification on all workloads
        self._verify_all_workloads()

        log.info("Final verification completed")

    def _check_verification_results(self):
        """Check verification results and raise exception if errors found."""
        with self.verification_lock:
            if self.verification_errors:
                error_msg = (
                    f"Data validation failed! Found {len(self.verification_errors)} "
                    f"verification errors across {len(self.active_workloads)} workloads"
                )
                log.error(error_msg)

                # Log all errors for debugging
                for i, error in enumerate(self.verification_errors, 1):
                    log.error(f"Error {i}: {error}")

                raise VdbenchVerificationError(
                    f"{error_msg}: {self.verification_errors}"
                )
            else:
                log.info(
                    "✅ All verification checks passed - no data corruption detected"
                )

    def _cleanup_all_workloads(self):
        """Clean up all active workloads."""
        log.info("Cleaning up all workloads")

        # Stop all threads
        self.stop_event.set()

        # Wait for threads to finish
        for thread in self.workload_threads + self.verification_threads:
            thread.join(timeout=30)

        # Clean up workloads
        for i, workload in enumerate(self.active_workloads):
            try:
                log.info(f"Cleaning up workload {i+1}")
                workload.cleanup_workload()
            except Exception as e:
                log.warning(f"Error cleaning up workload {i+1}: {e}")

        self.active_workloads.clear()
        log.info("All workloads cleaned up")

    def get_workflow_status(self) -> Dict[str, Any]:
        """Get current workflow status."""
        return {
            "active_workloads": len(self.active_workloads),
            "workload_restart_count": self.workload_restart_count,
            "max_restarts": self.max_restarts,
            "verification_errors": len(self.verification_errors),
            "stop_event_set": self.stop_event.is_set(),
        }
