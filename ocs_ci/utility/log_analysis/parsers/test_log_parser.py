"""
Parse per-test log files to extract relevant context for AI analysis.

OCS-CI test logs can be 2.4MB+ each. This parser extracts only the
information relevant for failure analysis:
- ERROR and WARNING level lines
- Last N lines before the test ended
- Ceph health check outputs
- oc command outputs that returned errors

The output is capped at ~16KB to stay within AI context budgets.
"""

import logging
import re

logger = logging.getLogger(__name__)

# Log line pattern: timestamp - thread - level - module - message
LOG_LINE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d+)\s+-\s+"  # timestamp
    r"(\S+)\s+-\s+"  # thread
    r"(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+-\s+"  # level
    r"(\S+)\s+-\s+"  # module
    r"(.*)$"  # message
)

# Patterns indicating Ceph health output
CEPH_HEALTH_PATTERNS = [
    r"HEALTH_OK",
    r"HEALTH_WARN",
    r"HEALTH_ERR",
    r"ceph health",
    r"ceph status",
    r"Ceph cluster health",
]
CEPH_HEALTH_RE = re.compile("|".join(CEPH_HEALTH_PATTERNS), re.IGNORECASE)

# Patterns indicating command execution errors
CMD_ERROR_PATTERNS = [
    r"Command return code: [1-9]",
    r"Command stderr:",
    r"CommandFailed",
    r"CalledProcessError",
]
CMD_ERROR_RE = re.compile("|".join(CMD_ERROR_PATTERNS))

# Noisy lines that match ERROR/WARNING keywords but are just YAML field names
# or Kubernetes resource dumps — not actual errors
NOISE_PATTERNS = [
    r"terminationMessagePolicy:\s*FallbackToLogsOnError",
    r"priorityClassName:\s*system-\w+-critical",
    r"failureThreshold:\s*\d+",
    r"failureDomain:\s*\w+",
    r"reason:\s*Error$",
    r"v4-0-config-user-template-error",
    r"replicasPerFailureDomain:\s*\d+",
]
NOISE_RE = re.compile("|".join(NOISE_PATTERNS))


class TestLogParser:
    """Extract relevant failure context from per-test log files."""

    # Limits for extracted content
    MAX_ERROR_LINES = 80
    MAX_TAIL_LINES = 40
    MAX_CEPH_LINES = 30
    MAX_CMD_ERROR_LINES = 30
    MAX_TOTAL_CHARS = 16000  # ~4000 tokens

    def parse(self, log_content: str) -> dict:
        """
        Parse a test log and extract failure-relevant context.

        Args:
            log_content: Full text content of the per-test log file

        Returns:
            dict with keys:
                errors: str - ERROR/WARNING lines
                tail: str - Last N lines of the log
                ceph_health: str - Ceph health check outputs
                cmd_errors: str - Command outputs with non-zero return codes
                stats: dict - Line counts and sizes
        """
        lines = log_content.splitlines()

        errors = self._extract_errors(lines)
        tail = self._extract_tail(lines)
        ceph_health = self._extract_ceph_health(lines)
        cmd_errors = self._extract_cmd_errors(lines)

        result = {
            "errors": "\n".join(errors[: self.MAX_ERROR_LINES]),
            "tail": "\n".join(tail[: self.MAX_TAIL_LINES]),
            "ceph_health": "\n".join(ceph_health[: self.MAX_CEPH_LINES]),
            "cmd_errors": "\n".join(cmd_errors[: self.MAX_CMD_ERROR_LINES]),
            "stats": {
                "total_lines": len(lines),
                "error_lines": len(errors),
                "ceph_health_lines": len(ceph_health),
                "cmd_error_lines": len(cmd_errors),
            },
        }

        # Enforce total budget
        result = self._enforce_budget(result)

        return result

    def build_excerpt(self, parsed: dict) -> str:
        """
        Build a single text excerpt from parsed log data for AI consumption.

        Args:
            parsed: Output from parse()

        Returns:
            Combined text excerpt ready for AI prompt
        """
        sections = []

        if parsed["errors"]:
            sections.append(
                f"=== ERROR/WARNING LINES ({parsed['stats']['error_lines']} total) ===\n"
                + parsed["errors"]
            )

        if parsed["cmd_errors"]:
            sections.append(
                f"=== COMMAND ERRORS ({parsed['stats']['cmd_error_lines']} total) ===\n"
                + parsed["cmd_errors"]
            )

        if parsed["ceph_health"]:
            sections.append(
                "=== CEPH HEALTH ===\n" + parsed["ceph_health"]
            )

        if parsed["tail"]:
            sections.append(
                f"=== LOG TAIL (last {self.MAX_TAIL_LINES} lines) ===\n"
                + parsed["tail"]
            )

        return "\n\n".join(sections)

    def _extract_errors(self, lines: list) -> list:
        """Extract ERROR and WARNING level log lines, filtering noise."""
        errors = []
        for line in lines:
            if NOISE_RE.search(line):
                continue
            match = LOG_LINE_RE.match(line)
            if match:
                level = match.group(3)
                if level in ("ERROR", "CRITICAL", "WARNING"):
                    errors.append(line)
            elif "Error" in line or "Exception" in line or "FAILED" in line:
                # Catch unformatted error lines (e.g., tracebacks in log output)
                errors.append(line)
        return errors

    def _extract_tail(self, lines: list) -> list:
        """Extract last N lines of the log."""
        return lines[-self.MAX_TAIL_LINES:]

    def _extract_ceph_health(self, lines: list) -> list:
        """Extract lines related to Ceph health checks."""
        ceph_lines = []
        in_ceph_block = False
        block_lines = 0

        for line in lines:
            if CEPH_HEALTH_RE.search(line):
                in_ceph_block = True
                block_lines = 0
                ceph_lines.append(line)
            elif in_ceph_block:
                block_lines += 1
                ceph_lines.append(line)
                # Capture a few lines after the health check header
                if block_lines >= 5:
                    in_ceph_block = False

        return ceph_lines

    def _extract_cmd_errors(self, lines: list) -> list:
        """Extract command execution outputs with non-zero return codes."""
        cmd_errors = []

        for i, line in enumerate(lines):
            if CMD_ERROR_RE.search(line):
                # Capture the error line and a few surrounding lines
                start = max(0, i - 2)
                end = min(len(lines), i + 3)
                for j in range(start, end):
                    if lines[j] not in cmd_errors:
                        cmd_errors.append(lines[j])

        return cmd_errors

    def _enforce_budget(self, result: dict) -> dict:
        """Ensure total character count stays within budget."""
        total = sum(len(v) for v in result.values() if isinstance(v, str))

        if total <= self.MAX_TOTAL_CHARS:
            return result

        # Prioritize: errors > cmd_errors > ceph_health > tail
        budget_remaining = self.MAX_TOTAL_CHARS
        for key in ["errors", "cmd_errors", "ceph_health", "tail"]:
            if budget_remaining <= 0:
                result[key] = "[omitted due to size limit]"
            elif len(result[key]) > budget_remaining:
                result[key] = result[key][:budget_remaining] + "\n... [truncated]"
                budget_remaining = 0
            else:
                budget_remaining -= len(result[key])

        return result
