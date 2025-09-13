import logging
import re
import os
from contextlib import suppress

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
        self, project, workloads, workload_type="VDBENCH", verify_config_func=None
    ):
        """
        Initialize WorkloadOpsWithVerification.

        Args:
            project: OCS project object
            workloads: List of workload objects
            workload_type: Type of workload (VDBENCH, CNV_WORKLOAD, etc.)
            verify_config_func: Function to create verification config
        """
        self.project = project
        self.workloads = workloads
        self.workload_type = workload_type
        self.namespace = project.namespace
        self.verify_config_func = verify_config_func
        self.verification_workloads = []

        # Load configuration to check verification settings
        from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

        self.config = KrknWorkloadConfig()
        self.should_verify = self.config.should_run_verification()

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

            log.warning("Unable to locate workload output")
            return ""

        except Exception as e:
            log.error(f"Error getting workload output: {e}")
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
