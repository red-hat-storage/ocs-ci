import os
import logging
import shlex
import threading
import subprocess
import time
import json
import re
import yaml

from ocs_ci.ocs.constants import KRKN_OUTPUT_DIR, KRKN_RUN_CMD, KRKNCTL
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_port_manager import KrknPortManager
from ocs_ci.framework import config
from ocs_ci.utility.ibmcloud import get_ibmcloud_cluster_region
from ocs_ci.utility.utils import exec_cmd

log = logging.getLogger(__name__)


class KrKnRunner:
    """
    Class to run Krkn chaos scenarios in a separate thread
    and periodically check its execution status.
    """

    def __init__(self, krkn_config):
        self.krkn_config = krkn_config
        self.output_log = f"{os.path.dirname(self.krkn_config)}/output.log"
        self.process = None
        self.thread = None
        self.thread_exception = None
        # True when we stopped waiting because FAILURE was detected in the log
        self._completed_due_to_failure = False
        os.makedirs(KRKN_OUTPUT_DIR, exist_ok=True)

    def _print_config_file(self):
        """Print the contents of the Krkn config file before execution."""
        try:
            log.info(f"Reading Krkn config file: {self.krkn_config}")
            with open(self.krkn_config, "r", encoding="utf-8") as f:
                config_contents = f.read()
                log.info(
                    f"Krkn config file contents:\n{'='*50}\n{config_contents}\n{'='*50}"
                )
        except FileNotFoundError:
            log.warning(f"Config file {self.krkn_config} not found")
        except Exception as e:
            log.warning(f"Failed to read config file {self.krkn_config}: {str(e)}")

    def _get_kubeconfig_from_config(self):
        """Extract kubeconfig_path from the krkn config file."""
        try:
            with open(self.krkn_config, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)
                kubeconfig_path = config_data.get("kraken", {}).get("kubeconfig_path")
                if kubeconfig_path:
                    # Expand user path if needed
                    return os.path.expanduser(kubeconfig_path)
        except Exception as e:
            log.warning(f"Failed to extract kubeconfig path from config: {str(e)}")
        return None

    def _run_krkn_command_wrapper(self):
        """Wrapper method to capture exceptions from the thread."""
        try:
            self._run_krkn_command()
        except Exception as e:
            self.thread_exception = e
            log.error(f"Exception in Krkn thread: {e}")

    def _validate_environment(self):
        """Validate the environment before running Krkn."""
        # Check if Krkn venv python exists
        # KRKN_RUN_CMD is data/krkn/run_kraken.py
        # venv is at data/krkn/venv/bin/python
        krkn_dir = os.path.dirname(KRKN_RUN_CMD)  # data/krkn
        krkn_python = os.path.join(krkn_dir, "venv", "bin", "python")

        if not os.path.exists(krkn_python):
            log.error(f"Krkn venv python not found at: {krkn_python}")
            return False

        # Check if Krkn run command exists
        if not os.path.exists(KRKN_RUN_CMD):
            log.error(f"Krkn run command not found at: {KRKN_RUN_CMD}")
            return False

        # Check if config file exists
        if not os.path.exists(self.krkn_config):
            log.error(f"Krkn config file not found at: {self.krkn_config}")
            return False

        # Check if kubeconfig exists
        kubeconfig_path = self._get_kubeconfig_from_config()
        if kubeconfig_path and not os.path.exists(kubeconfig_path):
            log.error(f"Kubeconfig file not found at: {kubeconfig_path}")
            return False

        log.info("Environment validation passed")
        return True

    def _handle_port_conflict(self, stderr_output):
        """
        Check if the error is due to port conflict and attempt to resolve it.

        Args:
            stderr_output (str): Stderr output from failed Krkn process

        Returns:
            bool: True if port conflict was detected and potentially resolved, False otherwise
        """
        if (
            "Address already in use" in stderr_output
            or "OSError: [Errno 98]" in stderr_output
        ):
            log.warning("Detected port conflict in Krkn execution")

            # Extract current port from config
            try:
                with open(self.krkn_config, "r") as f:
                    config_data = yaml.safe_load(f)
                current_port = config_data.get("kraken", {}).get("port", 8081)
                signal_address = config_data.get("kraken", {}).get(
                    "signal_address", "0.0.0.0"
                )

                log.info(f"Current Krkn config uses port {current_port}")

                # Find a new available port
                try:
                    new_port = KrknPortManager.find_available_port(host=signal_address)
                    log.info(f"Found alternative port {new_port} for Krkn server")

                    # Update the config file with new port
                    config_data["kraken"]["port"] = new_port
                    with open(self.krkn_config, "w") as f:
                        yaml.dump(config_data, f, default_flow_style=False)

                    log.info(f"Updated Krkn config file with new port {new_port}")
                    return True

                except RuntimeError as e:
                    log.error(f"Failed to find alternative port: {e}")
                    return False

            except Exception as e:
                log.error(f"Failed to handle port conflict: {e}")
                return False

        return False

    def _monitor_krkn_output(self, timeout):
        """
        Monitor Krkn output in real-time and detect completion messages.

        This method reads the process output line by line and looks for success
        indicators like "Successfully finished running Kraken." to allow early
        termination instead of waiting for the full timeout.

        Args:
            timeout (int): Maximum time to wait in seconds

        Returns:
            tuple: (stdout, stderr) strings

        Raises:
            subprocess.TimeoutExpired: If timeout is reached without completion
        """
        import select

        start_time = time.time()
        stdout_lines = []
        stderr_lines = []

        # Success patterns that indicate Krkn has completed successfully
        # Primary pattern matches exact grep behavior
        primary_success_pattern = "Successfully finished running Kraken"
        additional_success_patterns = [
            "Kraken has finished running",
            "All scenarios have been executed",
            "Chaos injection completed successfully",
        ]

        log.info("🔍 Monitoring Krkn output for completion messages...")

        try:
            # Set stdout and stderr to non-blocking mode for real-time monitoring
            while self.process.poll() is None:
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    raise subprocess.TimeoutExpired(self.process.args, timeout)

                # Read available output
                if self.process.stdout.readable():
                    try:
                        # Use select to check if data is available (Unix-like systems)
                        if hasattr(select, "select"):
                            ready, _, _ = select.select(
                                [self.process.stdout], [], [], 0.1
                            )
                            if ready:
                                line = self.process.stdout.readline()
                                if line:
                                    line_str = line.strip()
                                    stdout_lines.append(line_str)
                                    log.debug(f"Krkn output: {line_str}")

                                    # Check for success patterns - prioritize primary pattern
                                    if primary_success_pattern in line_str:
                                        log.info(
                                            f"🎉 Detected PRIMARY Krkn completion message: '{primary_success_pattern}'"
                                        )
                                        log.info(
                                            "✅ Krkn has finished successfully - terminating monitoring early"
                                        )

                                        # Read any remaining output
                                        remaining_stdout, remaining_stderr = (
                                            self.process.communicate(timeout=10)
                                        )
                                        if remaining_stdout:
                                            stdout_lines.extend(
                                                remaining_stdout.strip().split("\n")
                                            )
                                        if remaining_stderr:
                                            stderr_lines.extend(
                                                remaining_stderr.strip().split("\n")
                                            )

                                        return "\n".join(stdout_lines), "\n".join(
                                            stderr_lines
                                        )

                                    # Check additional success patterns
                                    for pattern in additional_success_patterns:
                                        if pattern in line_str:
                                            log.info(
                                                f"🎉 Detected Krkn completion message: '{pattern}'"
                                            )
                                            log.info(
                                                "✅ Krkn has finished successfully - terminating monitoring early"
                                            )

                                            # Read any remaining output
                                            remaining_stdout, remaining_stderr = (
                                                self.process.communicate(timeout=10)
                                            )
                                            if remaining_stdout:
                                                stdout_lines.extend(
                                                    remaining_stdout.strip().split("\n")
                                                )
                                            if remaining_stderr:
                                                stderr_lines.extend(
                                                    remaining_stderr.strip().split("\n")
                                                )

                                            return "\n".join(stdout_lines), "\n".join(
                                                stderr_lines
                                            )
                        else:
                            # Fallback for systems without select (like Windows)
                            time.sleep(0.1)
                    except Exception as e:
                        log.debug(f"Error reading stdout: {e}")
                        time.sleep(0.1)

                # Read stderr if available
                if self.process.stderr.readable():
                    try:
                        if hasattr(select, "select"):
                            ready, _, _ = select.select(
                                [self.process.stderr], [], [], 0.1
                            )
                            if ready:
                                line = self.process.stderr.readline()
                                if line:
                                    line_str = line.strip()
                                    stderr_lines.append(line_str)
                                    log.debug(f"Krkn stderr: {line_str}")
                    except Exception as e:
                        log.debug(f"Error reading stderr: {e}")

                # Small sleep to prevent excessive CPU usage
                time.sleep(0.1)

            # Process has completed, read any remaining output
            remaining_stdout, remaining_stderr = self.process.communicate(timeout=10)
            if remaining_stdout:
                stdout_lines.extend(remaining_stdout.strip().split("\n"))
            if remaining_stderr:
                stderr_lines.extend(remaining_stderr.strip().split("\n"))

        except subprocess.TimeoutExpired:
            log.error("Timeout expired while monitoring Krkn output")
            raise
        except Exception as e:
            log.warning(
                f"Error during output monitoring, falling back to standard communicate: {e}"
            )
            # Fallback to standard communicate if monitoring fails
            remaining_stdout, remaining_stderr = self.process.communicate(
                timeout=max(0, timeout - (time.time() - start_time))
            )
            if remaining_stdout:
                stdout_lines.extend(remaining_stdout.strip().split("\n"))
            if remaining_stderr:
                stderr_lines.extend(remaining_stderr.strip().split("\n"))

        return "\n".join(stdout_lines), "\n".join(stderr_lines)

    def _check_output_log_for_completion(self):
        """
        Check the output log file for completion messages.

        This provides an additional way to detect completion by monitoring
        the Krkn output log file for success messages. Also treats the
        presence of "Chaos data:" as completion (same marker as get_chaos_data()).
        The "Chaos data:" line is not at a fixed position—it varies with how much
        data Krkn processes—so the whole log is searched for it.

        Returns:
            bool: True if completion message found, False otherwise
        """
        if not os.path.exists(self.output_log):
            return False

        try:
            # Success patterns to look for in the log file
            success_patterns = [
                "Successfully finished running Kraken.",
                "Kraken has finished running",
                "All scenarios have been executed",
                "Chaos injection completed successfully",
            ]

            # Read the log file
            with open(self.output_log, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Success patterns: check last 50 lines (completion messages are at end)
            last_lines = lines[-50:] if len(lines) > 50 else lines
            for line in last_lines:
                for pattern in success_patterns:
                    if pattern in line:
                        log.info(
                            f"🎉 Found completion message in log: '{pattern.strip()}'"
                        )
                        return True

            # Chaos data: position varies with run size; search entire log (same marker as get_chaos_data())
            for line in lines:
                if "Chaos data:" in line:
                    log.info("🎉 Found 'Chaos data:' in log - Krkn has completed")
                    return True

        except Exception as e:
            log.debug(f"Error checking output log for completion: {e}")

        return False

    def _check_kraken_success_completion(self):
        """
        Check if Kraken completed successfully using precise pattern matching.

        This method mimics the bash command:
        if grep -q "Successfully finished running Kraken" kraken.log; then
            echo "SUCCESS: All scenarios completed successfully"
            exit 0
        else
            echo "FAILURE: Kraken execution failed or incomplete"
            exit 1
        fi

        Returns:
            tuple: (success: bool, message: str) - success status and descriptive message
        """
        if not os.path.exists(self.output_log):
            return False, f"Output log file not found: {self.output_log}"

        try:
            # Primary success pattern - exact match like grep
            primary_success_pattern = "Successfully finished running Kraken"

            # Additional success indicators
            additional_patterns = [
                "Kraken has finished running",
                "All scenarios have been executed successfully",
                "Chaos injection completed successfully",
            ]

            with open(self.output_log, "r", encoding="utf-8") as f:
                content = f.read()

                # Check for primary success pattern (exact grep-like match)
                if primary_success_pattern in content:
                    log.info(
                        f"🎉 SUCCESS: Found exact completion pattern: '{primary_success_pattern}'"
                    )
                    return (
                        True,
                        f"SUCCESS: All scenarios completed successfully - found '{primary_success_pattern}'",
                    )

                # Chaos data: Krkn writes "Chaos data:" at end of run (same marker as get_chaos_data())
                if "Chaos data:" in content:
                    log.info("🎉 SUCCESS: Found 'Chaos data:' - Krkn run completed")
                    return (
                        True,
                        "SUCCESS: Krkn completed - 'Chaos data:' written to log",
                    )

                # Check for additional success patterns
                for pattern in additional_patterns:
                    if pattern in content:
                        log.info(f"🎉 SUCCESS: Found completion pattern: '{pattern}'")
                        return True, f"SUCCESS: Kraken completed - found '{pattern}'"

                # Do NOT treat generic ERROR/Exception in the log as "run completed".
                # Krkn logs per-scenario failures while the run continues; only
                # process exit or "Chaos data:" / "Successfully finished" mean done.

                # No clear success pattern found - run may still be in progress
                return (
                    False,
                    "INCOMPLETE: Kraken execution status unclear - no definitive completion pattern found",
                )

        except Exception as e:
            error_msg = f"ERROR: Failed to check Kraken completion status: {e}"
            log.error(error_msg)
            return False, error_msg

    def validate_kraken_execution_success(self):
        """
        Validate Kraken execution success with exit-like behavior.

        This method provides a final validation similar to:
        if grep -q "Successfully finished running Kraken" kraken.log; then
            echo "SUCCESS: All scenarios completed successfully"
            exit 0
        else
            echo "FAILURE: Kraken execution failed or incomplete"
            exit 1
        fi

        Returns:
            bool: True if execution was successful, False otherwise

        Raises:
            CommandFailed: If execution failed with detailed error information
        """
        success, message = self._check_kraken_success_completion()

        if success:
            log.info(f"🎉 VALIDATION PASSED: {message}")
            return True
        else:
            # Log the failure and raise an exception with detailed information
            log.error(f"❌ VALIDATION FAILED: {message}")

            # Provide additional context from the log file
            try:
                if os.path.exists(self.output_log):
                    log.error(
                        f"📄 Check the full Kraken log for details: {self.output_log}"
                    )

                    # Show last 20 lines of the log for debugging
                    with open(self.output_log, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                        last_lines = lines[-20:] if len(lines) > 20 else lines
                        log.error("📄 Last 20 lines of Kraken log:")
                        for i, line in enumerate(last_lines, 1):
                            log.error(f"  {i:2d}: {line.rstrip()}")
                else:
                    log.error(f"📄 Kraken log file not found: {self.output_log}")

            except Exception as e:
                log.error(f"Failed to read Kraken log file: {e}")

            # Raise exception to indicate failure (similar to exit 1)
            raise CommandFailed(
                f"Kraken execution validation failed: {message}. "
                f"Check the Kraken log file at {self.output_log} for detailed error information."
            )

    def _run_krkn_command(self):
        """Internal method to start the Krkn process with port conflict handling."""
        # Validate environment first
        if not self._validate_environment():
            raise CommandFailed("Environment validation failed")

        # Print config file contents before starting
        self._print_config_file()

        # Set up environment with KUBECONFIG path from the krkn config
        env = os.environ.copy()
        kubeconfig_path = self._get_kubeconfig_from_config()
        if kubeconfig_path:
            env["KUBECONFIG"] = kubeconfig_path
            log.info(f"Setting KUBECONFIG environment variable to: {kubeconfig_path}")
        else:
            log.warning(
                "Could not extract kubeconfig_path from krkn config, krkn may fail"
            )

        # Export IBM Cloud API key and endpoint if platform is IBM Cloud
        # This is required for node scenarios on IBM Cloud
        try:
            platform = config.ENV_DATA.get("platform", "").lower()
            if "ibm" in platform or "ibmcloud" in platform:
                ibmcloud_auth = config.AUTH.get("ibmcloud", {})
                api_key = ibmcloud_auth.get("api_key")
                api_endpoint = ibmcloud_auth.get("api_endpoint")

                if api_key:
                    env["IBMC_APIKEY"] = api_key
                    env["IC_API_KEY"] = (
                        api_key  # Also set IC_API_KEY for backward compatibility
                    )
                    log.info(
                        "Setting IBMC_APIKEY and IC_API_KEY environment variables for IBM Cloud"
                    )
                else:
                    log.warning(
                        "IBM Cloud platform detected but api_key not found in AUTH config"
                    )

                if api_endpoint:
                    env["IBMC_URL"] = api_endpoint
                    log.info(
                        f"Setting IBMC_URL environment variable to: {api_endpoint}"
                    )
                else:
                    ibmc_url = (
                        f"https://{get_ibmcloud_cluster_region()}.iaas.cloud.ibm.com/v1"
                    )
                    env["IBMC_URL"] = ibmc_url
                    log.info(
                        f"No api_endpoint configured, using region-based IBMC_URL: {ibmc_url}"
                    )
        except Exception as e:
            log.warning(f"Failed to set IBM Cloud credentials: {str(e)}")

        # Use krkn venv python directly to run krkn
        # KRKN_RUN_CMD is data/krkn/run_kraken.py
        # venv is at data/krkn/venv/bin/python
        krkn_dir = os.path.dirname(KRKN_RUN_CMD)
        krkn_python = os.path.join(krkn_dir, "venv", "bin", "python")

        krkn_cmd = [
            krkn_python,
            KRKN_RUN_CMD,
            "--config",
            self.krkn_config,
            f"--output={self.output_log}",
        ]

        # Retry logic for port conflicts
        max_retries = 3
        for attempt in range(max_retries):
            log.info(
                f"Starting Krkn (attempt {attempt + 1}/{max_retries}): {' '.join(krkn_cmd)}"
            )

            try:
                self.process = subprocess.Popen(
                    krkn_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                )

                # CRITICAL FIX: Add timeout to prevent infinite blocking
                # Set timeout to 4 hours (14400 seconds) for large scenario sets
                # This prevents subprocess from hanging but allows long executions
                timeout = 14400
                log.info(
                    f"Waiting for Krkn process to complete (timeout: {timeout}s = 4 hours)"
                )

                try:
                    # Monitor output in real-time to detect completion
                    stdout, stderr = self._monitor_krkn_output(timeout)
                    log.info(f"Krkn stdout:\n{stdout}")
                    if stderr:
                        log.error(f"Krkn stderr:\n{stderr}")
                except subprocess.TimeoutExpired:
                    log.error(f"Krkn process timed out after {timeout} seconds")
                    # Kill the process and its children
                    self.process.kill()
                    try:
                        stdout, stderr = self.process.communicate(timeout=30)
                    except subprocess.TimeoutExpired:
                        log.error("Failed to kill Krkn process cleanly")
                        stdout, stderr = "", "Process killed due to timeout"

                    raise CommandFailed(
                        f"Krkn process timed out after {timeout} seconds. "
                        f"This may indicate the Krkn tool is stuck or scenarios are taking too long. "
                        f"Check Krkn logs and cluster status."
                    )

                if self.process.returncode != 0:
                    if self.process.returncode == 2:
                        # Exit code 2 typically means some scenarios failed, which is acceptable in chaos testing
                        log.warning(
                            "Krkn completed with exit code 2 - some scenarios may have failed"
                        )
                        log.warning(
                            "This is expected in chaos testing and does not indicate a framework failure"
                        )
                        if stderr:
                            log.warning(f"Krkn stderr: {stderr}")
                        # Don't raise an exception for exit code 2 - let the test continue
                        return  # Success case - exit retry loop
                    else:
                        # Check if this is a port conflict that we can resolve
                        if attempt < max_retries - 1 and self._handle_port_conflict(
                            stderr
                        ):
                            log.info(
                                "Port conflict resolved, retrying Krkn execution..."
                            )
                            time.sleep(2)  # Brief delay before retry
                            continue  # Retry with new port

                        # Other exit codes indicate actual framework failures
                        error_msg = (
                            f"Krkn command failed with code {self.process.returncode}"
                        )
                        if stderr:
                            error_msg += f"\nStderr: {stderr}"
                        if stdout:
                            error_msg += f"\nStdout: {stdout}"
                        error_msg += f"\nCommand: {' '.join(krkn_cmd)}"
                        error_msg += f"\nConfig file: {self.krkn_config}"

                        if attempt == max_retries - 1:
                            # Final attempt failed - log warning instead of raising
                            log.warning(
                                "Krkn command failed (non-zero exit code). "
                                "Treating as warning and continuing: %s",
                                error_msg,
                            )
                            return
                        else:
                            # Log error but continue retrying
                            log.warning(f"Attempt {attempt + 1} failed: {error_msg}")
                            time.sleep(2)
                            continue
                else:
                    # Success case
                    log.info("Krkn completed successfully")
                    return  # Exit retry loop

            except Exception as e:
                if attempt == max_retries - 1:
                    # Final attempt failed - log warning instead of raising
                    log.warning(
                        "Failed to run Krkn command. Treating as warning and continuing: %s",
                        str(e),
                    )
                    return
                else:
                    # Log error but continue retrying
                    log.warning(
                        f"Attempt {attempt + 1} failed with exception: {str(e)}"
                    )
                    time.sleep(2)
                    continue

    def run_async(self):
        """Run the Krkn command in a background thread."""
        self.thread = threading.Thread(
            target=self._run_krkn_command_wrapper, daemon=True
        )
        self.thread.start()

    def is_running(self):
        """Check if the Krkn process is still running."""
        return self.process and self.process.poll() is None

    def wait_for_completion(self, check_interval=30, max_wait_time=None):
        """
        Wait for Krkn to complete, checking status every `check_interval` seconds.

        Args:
            check_interval (int): Seconds to wait between status checks.
            max_wait_time (int): Maximum time to wait in seconds. If None, waits indefinitely
                                until completion is detected or thread exception occurs.
                                This is recommended for large scenario sets.

        Raises:
            CommandFailed: If the Krkn thread encountered an exception.
        """
        start_time = time.time()

        while self.thread.is_alive():
            # Check for thread exceptions during execution
            if self.thread_exception:
                log.error("Krkn thread encountered an exception")
                raise self.thread_exception

            # If the Krkn process has already exited, treat as completion and stop waiting
            if self.process is not None and self.process.poll() is not None:
                if self.process.returncode != 0:
                    self._completed_due_to_failure = True
                log.info(
                    "Krkn process has exited (return code %s) - waiting for thread to finish...",
                    self.process.returncode,
                )
                self.thread.join(timeout=60)
                break

            # Check for Kraken success completion using precise pattern matching
            success, message = self._check_kraken_success_completion()
            if success:
                log.info(f"🎉 {message}")
                log.info(
                    "✅ Detected Krkn success completion - waiting for thread to finish..."
                )
                # Give thread a moment to finish cleanly
                self.thread.join(timeout=30)
                if not self.thread.is_alive():
                    log.info(
                        "✅ Thread completed successfully after detecting success completion"
                    )
                    break
                else:
                    log.warning(
                        "Thread still alive after success detection - continuing to wait..."
                    )

            # Do NOT stop waiting just because the log contains ERROR/Exception.
            # Those can be per-scenario failures while Krkn is still running.
            # Only stop when: process exited, or we see "Chaos data:" / success message above.

            # Also check output log for completion messages (fallback)
            elif self._check_output_log_for_completion():
                log.info(
                    "✅ Detected Krkn completion in output log - waiting for thread to finish..."
                )
                # Give thread a moment to finish cleanly
                self.thread.join(timeout=30)
                if not self.thread.is_alive():
                    log.info(
                        "✅ Thread completed successfully after detecting completion message"
                    )
                    break
                else:
                    log.warning(
                        "Thread still alive after completion message - continuing to wait..."
                    )

            # Check for optional timeout (only if specified)
            if max_wait_time is not None:
                elapsed_time = time.time() - start_time
                if elapsed_time > max_wait_time:
                    log.error(f"Krkn execution timed out after {max_wait_time} seconds")
                    # Try to terminate the process gracefully
                    if self.process and self.process.poll() is None:
                        log.warning("Attempting to terminate stuck Krkn process...")
                        self.process.terminate()
                        time.sleep(5)
                        if self.process.poll() is None:
                            log.warning("Force killing stuck Krkn process...")
                            self.process.kill()

                    raise CommandFailed(
                        f"Krkn execution timed out after {max_wait_time} seconds. "
                        f"The process may be stuck. Check Krkn logs and cluster connectivity."
                    )

                log.info(
                    f"✅ Krkn is still running... (elapsed: {elapsed_time:.0f}s/{max_wait_time}s)"
                )
            else:
                # No timeout - just show elapsed time for large scenario sets
                elapsed_time = time.time() - start_time
                hours = int(elapsed_time // 3600)
                minutes = int((elapsed_time % 3600) // 60)
                seconds = int(elapsed_time % 60)

                if hours > 0:
                    time_str = f"{hours}h {minutes}m {seconds}s"
                elif minutes > 0:
                    time_str = f"{minutes}m {seconds}s"
                else:
                    time_str = f"{seconds}s"

                log.info(
                    f"✅ Krkn is still running... (elapsed: {time_str}) - Waiting for completion detection..."
                )
            time.sleep(check_interval)

        # Check for thread exceptions after completion
        if self.thread_exception:
            log.error("Krkn thread completed with an exception")
            raise self.thread_exception

        if self._completed_due_to_failure:
            log.info(
                "✅ Krkn wait ended (process exited with non-zero; Chaos data block may be absent)."
            )
        else:
            log.info("✅ Krkn process has completed.")

    def get_chaos_data(self):
        """
        Extract the 'Chaos data' JSON from self.output_log and
        ALWAYS return shape: {"telemetry": {...}, ...optional keys...}

        When "Chaos data:" is missing (timeout, process exited before writing report,
        or incomplete run), return minimal telemetry {"telemetry": {"scenarios": []}}
        and log a warning instead of raising, so the test can continue and fail on
        validation if needed.
        """
        with open(self.output_log, "r", encoding="utf-8") as f:
            text = f.read()

        # find the LAST occurrence of "Chaos data:"
        matches = list(re.finditer(r"Chaos data:\s*", text))
        if not matches:
            log.warning(
                "Chaos data block not found in the log (timeout, process exit before report, or incomplete run). "
                "Returning minimal telemetry (no scenarios)."
            )
            return {"telemetry": {"scenarios": []}}
        m = matches[-1]

        # first '{' after the marker
        start = text.find("{", m.end())
        if start == -1:
            raise ValueError("No JSON object found after 'Chaos data:' marker.")

        # brace-match to the end
        depth = 0
        i = start
        in_string = False
        escape = False
        end = None
        while i < len(text):
            ch = text[i]
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
            else:
                if ch == '"':
                    in_string = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            i += 1
        if end is None:
            raise ValueError("Unbalanced braces while reading Chaos data JSON.")

        chaos_json_str = text[start:end].strip()
        log.info(f"Extracted Chaos JSON: {chaos_json_str}")

        try:
            obj = json.loads(chaos_json_str)
            log.info("Krkn Chaos Scenario Output parsed successfully.")
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse chaos data JSON: {e}")

        # Normalize shape so callers can always do output["telemetry"]["scenarios"]
        telemetry = obj.get("telemetry", obj)
        normalized = {"telemetry": telemetry}
        # carry through other top-level keys if present (e.g., critical_alerts)
        for k, v in obj.items():
            if k != "telemetry":
                normalized[k] = v
        return normalized


class KrKnctlRunner:
    """
    Class to run krknctl CLI commands (run, random, list, describe, clean, attach,
    graph, query-status, etc.) with a consistent environment and optional
    global flags (e.g. private registry).
    """

    # Subcommands supported by krknctl
    SUBCOMMANDS = (
        "attach",
        "clean",
        "completion",
        "describe",
        "graph",
        "help",
        "list",
        "query-status",
        "random",
        "run",
    )

    # Global flags (before subcommand) from `krknctl --help`
    GLOBAL_FLAGS = (
        "private_registry",
        "private_registry_insecure",
        "private_registry_password",
        "private_registry_scenarios",
        "private_registry_skip_tls",
        "private_registry_token",
        "private_registry_username",
    )

    def __init__(
        self,
        krknctl_binary=None,
        kubeconfig=None,
        **global_flags,
    ):
        """
        Args:
            krknctl_binary (str): Path to krknctl binary. Defaults to
                {KRKNCTL}/krknctl.
            kubeconfig (str): Path to kubeconfig. If None, uses KUBECONFIG env
                or cluster path from config.
            **global_flags: Optional global flags for krknctl (e.g.
                private_registry="quay.io",
                private_registry_username="user",
                private_registry_password="secret").
                Names use underscores; they are converted to --kebab-case.
        """
        if krknctl_binary is None:
            krknctl_binary = os.path.join(KRKNCTL, "krknctl")
        self.krknctl_binary = krknctl_binary
        self.kubeconfig = kubeconfig
        self.global_flags = {
            k: v for k, v in global_flags.items() if k in self.GLOBAL_FLAGS
        }

    def _env(self):
        """Build environment dict for subprocess (e.g. KUBECONFIG)."""
        env = os.environ.copy()
        if self.kubeconfig is not None:
            env["KUBECONFIG"] = self.kubeconfig
        else:
            kubeconfig_path = config.RUN.get("kubeconfig")
            if kubeconfig_path:
                env["KUBECONFIG"] = kubeconfig_path
            else:
                cluster_path = config.ENV_DATA.get("cluster_path")
                if cluster_path:
                    kubeconfig_path = os.path.join(
                        cluster_path,
                        config.RUN.get("kubeconfig_location", "auth/kubeconfig"),
                    )
                    if os.path.exists(kubeconfig_path):
                        env["KUBECONFIG"] = kubeconfig_path
        return env

    def _flag_to_arg(self, key, value):
        """Convert a Python kwarg to krknctl flag (--kebab-case)."""
        flag = "--" + key.replace("_", "-")
        if value is True:
            return [flag]
        if value is False or value is None:
            return []
        return [flag, str(value)]

    def _build_cmd(self, subcommand, *args, **flags):
        """
        Build the full krknctl command as a list for exec_cmd.

        Args:
            subcommand (str): One of SUBCOMMANDS (run, random, list, ...).
            *args: Positional arguments (e.g. scenario name, plan path).
            **flags: Subcommand-specific flags (e.g. plan=path, wait_duration=60).
                    Converted to --flag=value or --flag value.

        Returns:
            list: Command list [binary, ...global_flags, subcommand, ...args, ...flags].
        """
        if subcommand not in self.SUBCOMMANDS:
            raise ValueError(
                f"Unknown subcommand '{subcommand}'. Must be one of {self.SUBCOMMANDS}"
            )
        cmd = [self.krknctl_binary]

        if self.kubeconfig is not None:
            cmd.extend(["--kubeconfig", self.kubeconfig])

        for key, value in self.global_flags.items():
            cmd.extend(self._flag_to_arg(key, value))

        cmd.append(subcommand)

        for a in args:
            cmd.append(str(a))

        for key, value in sorted(flags.items()):
            if key in self.GLOBAL_FLAGS:
                continue
            cmd.extend(self._flag_to_arg(key, value))

        return cmd

    def run_subcommand(
        self,
        subcommand,
        *args,
        timeout=600,
        ignore_error=False,
        **flags,
    ):
        """
        Run a krknctl subcommand and return the completed process.

        Args:
            subcommand (str): One of run, random, list, describe, clean, etc.
            *args: Positional arguments for the subcommand.
            timeout (int): Command timeout in seconds.
            ignore_error (bool): If True, do not raise CommandFailed on non-zero exit.
            **flags: Subcommand-specific flags.

        Returns:
            CompletedProcess: stdout, stderr, returncode (from exec_cmd).

        Raises:
            CommandFailed: If ignore_error is False and the command fails.
        """
        cmd = self._build_cmd(subcommand, *args, **flags)
        log.info("Executing krknctl: %s", shlex.join(cmd))
        try:
            completed = exec_cmd(
                cmd,
                timeout=timeout,
                ignore_error=ignore_error,
                env=self._env(),
            )
        except CommandFailed as e:
            log.error("krknctl %s failed: %s", subcommand, e)
            raise
        return completed

    def run(self, scenario_name, *args, timeout=600, ignore_error=False, **flags):
        """
        Run a single scenario: krknctl run <scenario_name> [flags].

        Equivalent to: krknctl run node-memory-hog --kubeconfig /path/to/kubeconfig

        Args:
            scenario_name (str): Scenario name (e.g. "node-memory-hog", "pod-scenarios",
                from plan or krknctl list).
            *args: Extra positional args for run.
            timeout (int): Command timeout.
            ignore_error (bool): If True, do not raise on non-zero exit.
            **flags: Flags for run (e.g. wait_duration=120).

        Returns:
            CompletedProcess.

        Example:
            runner = KrKnctlRunner(kubeconfig="/path/to/kubeconfig")
            runner.run("node-memory-hog")
        """
        return self.run_subcommand(
            "run",
            scenario_name,
            *args,
            timeout=timeout,
            ignore_error=ignore_error,
            **flags,
        )

    def run_node_memory_hog(
        self,
        chaos_duration=None,
        memory_workers=None,
        memory_consumption=None,
        namespace=None,
        node_selector=None,
        timeout=600,
        ignore_error=False,
        **extra_flags,
    ):
        """
        Run the node-memory-hog scenario with configurable parameters.

        Equivalent to: krknctl run node-memory-hog --chaos-duration 60 ...

        Args:
            chaos_duration (int): Chaos duration in seconds. Default 60.
            memory_workers (int): Number of memory workers. Default 1.
            memory_consumption (str): Memory consumption (e.g. "90%", "95%").
                Default "90%".
            namespace (str): Target namespace. Default "default".
            node_selector (str): Node selector (e.g. "node-role.kubernetes.io/worker").
            timeout (int): Command timeout in seconds.
            ignore_error (bool): If True, do not raise on non-zero exit.
            **extra_flags: Any other flags for the scenario.

        Returns:
            CompletedProcess.

        Example:
            runner = KrKnctlRunner(kubeconfig="/path/to/kubeconfig")
            runner.run_node_memory_hog(
                chaos_duration=80,
                memory_workers=2,
                memory_consumption="95%",
                namespace="openshift-storage",
                node_selector="node-role.kubernetes.io/worker",
            )
        """
        flags = dict(extra_flags)
        if chaos_duration is not None:
            flags["chaos_duration"] = chaos_duration
        if memory_workers is not None:
            flags["memory_workers"] = memory_workers
        if memory_consumption is not None:
            flags["memory_consumption"] = memory_consumption
        if namespace is not None:
            flags["namespace"] = namespace
        if node_selector is not None:
            flags["node_selector"] = node_selector
        return self.run(
            "node-memory-hog",
            timeout=timeout,
            ignore_error=ignore_error,
            **flags,
        )

    def run_node_io_hog(
        self,
        chaos_duration=None,
        io_block_size=None,
        io_workers=None,
        io_write_bytes=None,
        node_mount_path=None,
        namespace=None,
        node_selector=None,
        taints=None,
        number_of_nodes=None,
        timeout=600,
        ignore_error=False,
        **extra_flags,
    ):
        """
        Run the node-io-hog scenario (disk pressure on a node).

        Parameters map to: krknctl run node-io-hog --chaos-duration 60 --io-block-size 1m ...

        Args:
            chaos_duration (int): Chaos duration in seconds. Default 60.
            io_block_size (str): I/O block size (e.g. "1m"). Default "1m".
            io_workers (int): I/O workers. Default 5.
            io_write_bytes (str): I/O write bytes (e.g. "10m"). Default "10m".
            node_mount_path (str): Node mount path. Default "/root".
            namespace (str): Target namespace. Default "default".
            node_selector (str): Node selector (e.g. "node-role.kubernetes.io/worker").
            taints (str): Node taints (e.g. "[]").
            number_of_nodes (int): Number of nodes to target.
            timeout (int): Command timeout in seconds.
            ignore_error (bool): If True, do not raise on non-zero exit.
            **extra_flags: Any other flags for the scenario.

        Returns:
            CompletedProcess.
        """
        flags = dict(extra_flags)
        if chaos_duration is not None:
            flags["chaos_duration"] = chaos_duration
        if io_block_size is not None:
            flags["io_block_size"] = io_block_size
        if io_workers is not None:
            flags["io_workers"] = io_workers
        if io_write_bytes is not None:
            flags["io_write_bytes"] = io_write_bytes
        if node_mount_path is not None:
            flags["node_mount_path"] = node_mount_path
        if namespace is not None:
            flags["namespace"] = namespace
        if node_selector is not None:
            flags["node_selector"] = node_selector
        if taints is not None:
            flags["taints"] = taints
        if number_of_nodes is not None:
            flags["number_of_nodes"] = number_of_nodes
        return self.run(
            "node-io-hog",
            timeout=timeout,
            ignore_error=ignore_error,
            **flags,
        )

    def run_node_cpu_hog(
        self,
        chaos_duration=None,
        cores=None,
        cpu_percentage=None,
        namespace=None,
        node_selector=None,
        taints=None,
        number_of_nodes=None,
        timeout=600,
        ignore_error=False,
        **extra_flags,
    ):
        """
        Run the node-cpu-hog scenario (CPU pressure on a node).

        Parameters map to: krknctl run node-cpu-hog --chaos-duration 60 --cpu-percentage 50 ...

        Args:
            chaos_duration (int): Chaos duration in seconds. Default 60.
            cores (int): Number of CPU cores to stress.
            cpu_percentage (int): CPU percentage (e.g. 50, 95). Default 50.
            namespace (str): Target namespace. Default "default".
            node_selector (str): Node selector (e.g. "node-role.kubernetes.io/worker").
            taints (str): Node taints (e.g. "[]").
            number_of_nodes (int): Number of nodes to target.
            timeout (int): Command timeout in seconds.
            ignore_error (bool): If True, do not raise on non-zero exit.
            **extra_flags: Any other flags for the scenario.

        Returns:
            CompletedProcess.
        """
        flags = dict(extra_flags)
        if chaos_duration is not None:
            flags["chaos_duration"] = chaos_duration
        if cores is not None:
            flags["cores"] = cores
        if cpu_percentage is not None:
            flags["cpu_percentage"] = cpu_percentage
        if namespace is not None:
            flags["namespace"] = namespace
        if node_selector is not None:
            flags["node_selector"] = node_selector
        if taints is not None:
            flags["taints"] = taints
        if number_of_nodes is not None:
            flags["number_of_nodes"] = number_of_nodes
        return self.run(
            "node-cpu-hog",
            timeout=timeout,
            ignore_error=ignore_error,
            **flags,
        )

    def random(
        self,
        plan_path,
        *args,
        alerts_profile=None,
        exit_on_error=False,
        graph_dump=None,
        max_parallel=None,
        metrics_profile=None,
        number_of_scenarios=None,
        timeout=3600,
        ignore_error=False,
        **extra_flags,
    ):
        """
        Run a random chaos run from a plan file: krknctl random run <plan_path> [flags].

        Only non-empty options are passed to the command; omit a parameter to leave it unset.

        Args:
            plan_path (str): Path to the plan JSON file (e.g. from generate_random_plan_file).
            *args: Extra positional args for random (after plan_path).
            alerts_profile (str): Custom alerts profile file path (--alerts-profile).
            exit_on_error (bool): If True, interrupt workflow and exit non-zero on error
                (--exit-on-error).
            graph_dump (str): File path to persist the generated dependency graph
                (--graph-dump).
            max_parallel (int): Maximum number of parallel scenarios (--max-parallel).
            metrics_profile (str): Custom metrics profile file path (--metrics-profile).
            number_of_scenarios (int): Number of elements to select from the execution plan
                (--number-of-scenarios).
            timeout (int): Command timeout (default 1 hour for chaos run).
            ignore_error (bool): If True, do not raise on non-zero exit.
            **extra_flags: Any other flags for random run (only non-empty values are passed).

        Returns:
            CompletedProcess.
        """
        flags = dict(extra_flags)
        if alerts_profile is not None and str(alerts_profile).strip():
            flags["alerts_profile"] = alerts_profile
        if exit_on_error:
            flags["exit_on_error"] = True
        if graph_dump is not None and str(graph_dump).strip():
            flags["graph_dump"] = graph_dump
        if max_parallel is not None:
            flags["max_parallel"] = max_parallel
        if metrics_profile is not None and str(metrics_profile).strip():
            flags["metrics_profile"] = metrics_profile
        if number_of_scenarios is not None:
            flags["number_of_scenarios"] = number_of_scenarios
        return self.run_subcommand(
            "random",
            "run",
            plan_path,
            *args,
            timeout=timeout,
            ignore_error=ignore_error,
            **flags,
        )

    def list_scenarios(self, *args, timeout=60, ignore_error=False, **flags):
        """
        List available or running scenarios: krknctl list [flags].

        Returns:
            CompletedProcess.
        """
        return self.run_subcommand(
            "list", *args, timeout=timeout, ignore_error=ignore_error, **flags
        )

    def describe(self, scenario_name, *args, timeout=30, ignore_error=False, **flags):
        """
        Describe a scenario: krknctl describe <scenario_name> [flags].

        Returns:
            CompletedProcess.
        """
        return self.run_subcommand(
            "describe",
            scenario_name,
            *args,
            timeout=timeout,
            ignore_error=ignore_error,
            **flags,
        )

    def clean(self, *args, timeout=120, ignore_error=False, **flags):
        """
        Clean already run scenario files and containers: krknctl clean [flags].

        Returns:
            CompletedProcess.
        """
        return self.run_subcommand(
            "clean", *args, timeout=timeout, ignore_error=ignore_error, **flags
        )

    def attach(self, *args, timeout=30, ignore_error=False, **flags):
        """
        Connect to running scenario logs: krknctl attach [flags].

        Returns:
            CompletedProcess.
        """
        return self.run_subcommand(
            "attach", *args, timeout=timeout, ignore_error=ignore_error, **flags
        )

    def graph(self, *args, timeout=300, ignore_error=False, **flags):
        """
        Run or scaffold a dependency-graph run: krknctl graph [flags].

        Returns:
            CompletedProcess.
        """
        return self.run_subcommand(
            "graph", *args, timeout=timeout, ignore_error=ignore_error, **flags
        )

    def query_status(self, *args, timeout=30, ignore_error=False, **flags):
        """
        Check status of container(s): krknctl query-status [flags].

        Returns:
            CompletedProcess.
        """
        return self.run_subcommand(
            "query-status", *args, timeout=timeout, ignore_error=ignore_error, **flags
        )

    def completion(self, *args, timeout=10, ignore_error=False, **flags):
        """
        Generate shell completion script: krknctl completion [flags].

        Returns:
            CompletedProcess.
        """
        return self.run_subcommand(
            "completion", *args, timeout=timeout, ignore_error=ignore_error, **flags
        )

    def help_cmd(self, subcommand=None, *args, timeout=10, ignore_error=False):
        """
        Help for krknctl or a subcommand: krknctl help [command].

        Args:
            subcommand (str): Optional subcommand name (e.g. "random", "run").

        Returns:
            CompletedProcess.
        """
        if subcommand is not None:
            args = (subcommand,) + args
        return self.run_subcommand(
            "help", *args, timeout=timeout, ignore_error=ignore_error
        )
