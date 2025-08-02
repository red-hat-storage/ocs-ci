import os
import logging
import threading
import subprocess
import time
import json

from ocs_ci.ocs.constants import KRKN_OUTPUT_DIR, KRKN_RUN_CMD
from ocs_ci.ocs.exceptions import CommandFailed

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
        os.makedirs(KRKN_OUTPUT_DIR, exist_ok=True)

    def _run_krkn_command(self):
        """Internal method to start the Krkn process."""
        krkn_cmd = [
            "python3",
            KRKN_RUN_CMD,
            "--config",
            self.krkn_config,
            f"--output={self.output_log}",
        ]
        log.info(f"Starting Krkn: {' '.join(krkn_cmd)}")
        try:
            self.process = subprocess.Popen(
                krkn_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, stderr = self.process.communicate()
            log.info(f"Krkn stdout:\n{stdout}")
            if stderr:
                log.error(f"Krkn stderr:\n{stderr}")
            if self.process.returncode != 0:
                raise CommandFailed(
                    f"Krkn command failed with code {self.process.returncode}"
                )
        except Exception as e:
            raise CommandFailed(f"Failed to run Krkn command. Error: {str(e)}")

    def run_async(self):
        """Run the Krkn command in a background thread."""
        self.thread = threading.Thread(target=self._run_krkn_command, daemon=True)
        self.thread.start()

    def is_running(self):
        """Check if the Krkn process is still running."""
        return self.process and self.process.poll() is None

    def wait_for_completion(self, check_interval=30):
        """
        Wait for Krkn to complete, checking status every `check_interval` seconds.

        Args:
            check_interval (int): Seconds to wait between status checks.
        """
        while self.thread.is_alive():
            log.info("✅ Krkn is still running...")
            time.sleep(check_interval)

        log.info("✅ Krkn process has completed.")

    def get_chaos_data(self):
        """
        Extracts the 'Chaos data' JSON dict from a Krkn log file.

        Args:
            filepath (str): Path to the Krkn output log file

        Returns:
            dict: Parsed 'telemetry' chaos data
        """
        with open(self.output_log, "r") as file:
            lines = file.readlines()

        # Look for line where Chaos data starts
        start_index = None
        for i, line in enumerate(lines):
            if "[INFO] Chaos data:" in line:
                start_index = i + 1  # JSON starts after this line
                break

        if start_index is None:
            raise ValueError("Chaos data block not found in the log.")

        # Extract JSON block (look for the next balanced brace structure)
        json_lines = []
        brace_balance = 0
        started = False

        for line in lines[start_index:]:
            if "telemetry" in line and not started:
                started = True
            if started:
                brace_balance += line.count("{")
                brace_balance -= line.count("}")
                json_lines.append(line)
                if brace_balance == 0:
                    break

        chaos_json_str = "".join(json_lines)
        try:
            chaos_data = json.loads(chaos_json_str)
            return chaos_data
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse chaos data JSON: {e}")
