import os
import logging
import threading
import subprocess
import time
import json
import re

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
