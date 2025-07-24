import os
import logging
import threading
import subprocess
import time
import json
import re
import yaml

from ocs_ci.ocs.constants import KRKN_OUTPUT_DIR, KRKN_RUN_CMD
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_port_manager import KrknPortManager

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

        krkn_cmd = [
            "python3",
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
                stdout, stderr = self.process.communicate()
                log.info(f"Krkn stdout:\n{stdout}")
                if stderr:
                    log.error(f"Krkn stderr:\n{stderr}")

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
                            # Final attempt failed
                            raise CommandFailed(error_msg)
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
                    # Final attempt failed
                    raise CommandFailed(f"Failed to run Krkn command. Error: {str(e)}")
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

    def wait_for_completion(self, check_interval=30):
        """
        Wait for Krkn to complete, checking status every `check_interval` seconds.

        Args:
            check_interval (int): Seconds to wait between status checks.

        Raises:
            CommandFailed: If the Krkn thread encountered an exception.
        """
        while self.thread.is_alive():
            # Check for thread exceptions during execution
            if self.thread_exception:
                log.error("Krkn thread encountered an exception")
                raise self.thread_exception
            log.info("✅ Krkn is still running...")
            time.sleep(check_interval)

        # Check for thread exceptions after completion
        if self.thread_exception:
            log.error("Krkn thread completed with an exception")
            raise self.thread_exception

        log.info("✅ Krkn process has completed.")

    def get_chaos_data(self):
        """
        Extract the 'Chaos data' JSON from self.output_log and
        ALWAYS return shape: {"telemetry": {...}, ...optional keys...}
        """
        with open(self.output_log, "r", encoding="utf-8") as f:
            text = f.read()

        # find the LAST occurrence of "Chaos data:"
        matches = list(re.finditer(r"Chaos data:\s*", text))
        if not matches:
            raise ValueError("Chaos data block not found in the log.")
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
