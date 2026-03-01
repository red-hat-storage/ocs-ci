"""
Live Cluster Debugger -- investigates test failures on a live OpenShift cluster.

When a test fails during execution, this module spawns a Claude Code session
that reads the test source code and logs, then runs read-only ``oc`` commands
to diagnose the root cause and classify the failure.
"""

import json
import logging
import os
import re
import shutil
import subprocess
import time

from jinja2 import Environment, FileSystemLoader

from ocs_ci.framework import config as ocsci_config
from ocs_ci.utility.live_debugger.safety import audit_commands

logger = logging.getLogger(__name__)

PROMPT_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "prompt_templates")


class LiveClusterDebugger:
    """
    Spawns ``claude -p`` with Bash and Read tools to investigate a live
    cluster after a test failure.

    The Claude session can:
    - Read the test source file to understand the test flow
    - Read the test log file to see what commands ran
    - Run ``oc`` commands to inspect live cluster state
    """

    def __init__(self, model="sonnet", max_budget_usd=1.00, timeout=300):
        """
        Args:
            model: Claude model to use (sonnet, opus, haiku).
            max_budget_usd: Maximum spend per investigation in USD.
            timeout: Subprocess timeout in seconds.
        """
        self.model = model
        self.max_budget_usd = max_budget_usd
        self.timeout = timeout
        self.jinja_env = Environment(
            loader=FileSystemLoader(PROMPT_TEMPLATES_DIR),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def investigate(
        self,
        test_name,
        test_nodeid,
        test_source_path,
        traceback_text,
        markers,
        test_start_time,
        failure_phase="call",
        test_log_path=None,
        log_dir=None,
    ):
        """
        Spawn ``claude -p`` to investigate a test failure on the live cluster.

        Args:
            test_name: Short test name (e.g. ``test_create_pvc``).
            test_nodeid: Full pytest node ID.
            test_source_path: Absolute path to the test source file.
            traceback_text: The Python traceback string.
            markers: Comma-separated marker names (squad, feature tags).
            test_start_time: UTC datetime string (ISO format) when the test started.
            failure_phase: Which phase failed (setup, call, teardown).
            test_log_path: Path to the per-test log file (if available).
            log_dir: Directory to write result files into.

        Returns:
            dict with keys: investigation, category, root_cause, evidence,
            recommended_action, cost_usd, num_turns, duration_seconds, error
        """
        start = time.time()
        result = {
            "test_name": test_name,
            "test_nodeid": test_nodeid,
            "failure_phase": failure_phase,
            "investigation": "",
            "category": "unknown",
            "root_cause": "",
            "evidence": [],
            "recommended_action": "",
            "cost_usd": 0.0,
            "num_turns": 0,
            "duration_seconds": 0.0,
            "error": None,
            "commands_executed": [],
            "safety_violations": [],
        }

        if not shutil.which("claude"):
            result["error"] = "Claude Code CLI ('claude') not found on PATH"
            logger.error(result["error"])
            return result

        # Build the prompt
        cluster_namespace = ocsci_config.ENV_DATA.get(
            "cluster_namespace", "openshift-storage"
        )
        platform = ocsci_config.ENV_DATA.get("platform", "unknown")

        template = self.jinja_env.get_template("investigate_failure.j2")
        prompt = template.render(
            test_name=test_name,
            test_nodeid=test_nodeid,
            test_source_path=test_source_path,
            traceback_text=self._truncate(traceback_text, 6000),
            markers=markers,
            test_start_time=test_start_time,
            failure_phase=failure_phase,
            test_log_path=test_log_path or "",
            cluster_namespace=cluster_namespace,
            platform=platform,
        )

        # Resolve model short names to full model IDs to avoid stale
        # aliases in older Claude Code CLI versions
        model_aliases = {
            "opus": "claude-opus-4-6",
            "sonnet": "claude-sonnet-4-6",
            "haiku": "claude-haiku-4-5-20251001",
        }
        resolved_model = model_aliases.get(self.model, self.model)

        # Build the command — prompt is passed via stdin to avoid
        # shell argument length limits on large prompts
        cmd = [
            "claude",
            "-p",
            "--tools", "Bash,Read",
            "--dangerously-skip-permissions",
            "--output-format", "json",
            "--model", resolved_model,
            "--max-budget-usd", str(self.max_budget_usd),
        ]

        # Prepare environment -- same patterns as exec_cmd and claude_code_backend
        env = os.environ.copy()
        # Remove CLAUDECODE to allow nested sessions
        env.pop("CLAUDECODE", None)
        # Set KUBECONFIG so oc commands work
        kubeconfig = ocsci_config.RUN.get("kubeconfig")
        if kubeconfig:
            env["KUBECONFIG"] = kubeconfig

        logger.info(
            f"Live debugger: investigating {test_name} "
            f"(model={self.model}, budget=${self.max_budget_usd:.2f}, "
            f"timeout={self.timeout}s)"
        )

        try:
            proc = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env=env,
            )
        except subprocess.TimeoutExpired:
            result["error"] = (
                f"Live debugger timed out after {self.timeout}s for {test_name}"
            )
            result["duration_seconds"] = time.time() - start
            logger.error(result["error"])
            return result
        except FileNotFoundError:
            result["error"] = "Claude Code CLI ('claude') not found"
            result["duration_seconds"] = time.time() - start
            logger.error(result["error"])
            return result
        except OSError as e:
            result["error"] = f"Failed to run Claude Code CLI: {e}"
            result["duration_seconds"] = time.time() - start
            logger.error(result["error"])
            return result

        result["duration_seconds"] = time.time() - start

        if proc.returncode != 0:
            stderr_text = proc.stderr.strip()[:500] if proc.stderr else ""
            stdout_text = proc.stdout.strip()[:500] if proc.stdout else ""
            result["error"] = (
                f"Claude Code exited with code {proc.returncode}: "
                f"stderr={stderr_text} stdout={stdout_text}"
            )
            logger.error(result["error"])
            # Log the command (without the full prompt) for debugging
            cmd_summary = [c for c in cmd if c != prompt]
            logger.error(f"Command was: {' '.join(cmd_summary)}")
            return result

        # Parse the JSON response
        try:
            response = json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            result["error"] = f"Failed to parse Claude response as JSON: {e}"
            logger.error(result["error"])
            return result

        # Extract result text and metadata
        result_text = response.get("result", "")
        result["investigation"] = result_text
        result["cost_usd"] = response.get("total_cost_usd", 0.0)
        result["num_turns"] = response.get("num_turns", 0)

        # Parse structured fields from the investigation narrative
        result["category"] = self._extract_category(result_text)
        result["root_cause"] = self._extract_section(result_text, "ROOT CAUSE")
        result["evidence"] = self._extract_evidence(result_text)
        result["recommended_action"] = self._extract_section(
            result_text, "RECOMMENDED ACTION"
        )

        # Safety audit -- check what commands were executed
        commands = self._extract_commands_from_response(response)
        result["commands_executed"] = commands
        violations = audit_commands(commands)
        result["safety_violations"] = violations
        if violations:
            logger.warning(
                f"Live debugger safety violations for {test_name}: {violations}"
            )

        logger.info(
            f"Live debugger completed for {test_name}: "
            f"category={result['category']}, "
            f"cost=${result['cost_usd']:.4f}, "
            f"turns={result['num_turns']}, "
            f"duration={result['duration_seconds']:.1f}s"
        )

        # Save per-test results
        if log_dir:
            self._save_results(result, test_name, log_dir)

        return result

    def _extract_category(self, text):
        """Extract the failure category from the investigation text."""
        # Look for **CATEGORY:** pattern
        match = re.search(
            r"\*\*CATEGORY:\*\*\s*(product_bug|test_bug|infra_issue|known_issue)",
            text,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).lower()
        # Fallback: look for CATEGORY: without markdown
        match = re.search(
            r"CATEGORY:\s*(product_bug|test_bug|infra_issue|known_issue)",
            text,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).lower()
        return "unknown"

    def _extract_section(self, text, section_name):
        """Extract content after a **SECTION:** header."""
        pattern = rf"\*\*{re.escape(section_name)}:\*\*\s*\n?(.*?)(?:\n\*\*|\Z)"
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        # Fallback without markdown
        pattern = rf"{re.escape(section_name)}:\s*\n?(.*?)(?:\n[A-Z]+:|\Z)"
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return ""

    def _extract_evidence(self, text):
        """Extract evidence bullet points from the investigation text."""
        section = self._extract_section(text, "EVIDENCE")
        if not section:
            return []
        # Parse bullet points
        points = []
        for line in section.split("\n"):
            line = line.strip()
            if line.startswith("- ") or line.startswith("* "):
                points.append(line[2:].strip())
            elif line.startswith("1.") or line.startswith("2.") or line.startswith("3."):
                points.append(re.sub(r"^\d+\.\s*", "", line).strip())
        return points

    def _extract_commands_from_response(self, response):
        """Extract bash commands that Claude executed from the JSON response."""
        commands = []
        # The JSON response may contain tool use records in various formats
        # Try to extract from the result text or conversation history
        result_text = response.get("result", "")
        # Look for commands in code blocks within the result
        for match in re.finditer(r"```(?:bash|shell)?\s*\n(.*?)```", result_text, re.DOTALL):
            for line in match.group(1).strip().split("\n"):
                line = line.strip()
                if line and not line.startswith("#"):
                    commands.append(line)
        # Also look for $ prefixed commands
        for match in re.finditer(r"^\$\s+(.+)$", result_text, re.MULTILINE):
            commands.append(match.group(1).strip())
        return commands

    def _save_results(self, result, test_name, log_dir):
        """Save investigation results as JSON and HTML files."""
        os.makedirs(log_dir, exist_ok=True)

        # Sanitize test name for filesystem
        safe_name = re.sub(r"[^\w\-.]", "_", test_name)

        # Save JSON
        json_path = os.path.join(log_dir, f"{safe_name}_live_debug.json")
        try:
            with open(json_path, "w") as f:
                json.dump(result, f, indent=2, default=str)
            logger.info(f"Live debug JSON saved: {json_path}")
        except OSError as e:
            logger.error(f"Failed to save debug JSON: {e}")

        # Save HTML
        from ocs_ci.utility.live_debugger.report_builder import DebugReportBuilder
        builder = DebugReportBuilder()
        html_path = os.path.join(log_dir, f"{safe_name}_live_debug.html")
        try:
            html_content = builder.build_single_report(result)
            with open(html_path, "w") as f:
                f.write(html_content)
            logger.info(f"Live debug HTML saved: {html_path}")
        except OSError as e:
            logger.error(f"Failed to save debug HTML: {e}")

    @staticmethod
    def _truncate(text, max_chars):
        """Truncate text to max_chars with a notice."""
        if not text or len(text) <= max_chars:
            return text or ""
        return (
            text[:max_chars]
            + f"\n... [truncated, {len(text) - max_chars} chars omitted]"
        )
