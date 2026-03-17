"""
Claude Code CLI backend for AI-powered log analysis.

Calls `claude -p` via subprocess with --output-format json
and --json-schema for structured output. Requires no API key --
uses Claude Code's own authentication.
"""

import json
import logging
import os
import re
import shutil
import subprocess

from jinja2 import Environment, FileSystemLoader

from ocs_ci.utility.log_analysis.ai.base import AIBackend
from ocs_ci.utility.log_analysis.exceptions import AIBackendError

logger = logging.getLogger(__name__)

PROMPT_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "prompt_templates")

CLASSIFICATION_SCHEMA = json.dumps(
    {
        "type": "object",
        "properties": {
            "category": {
                "type": "string",
                "enum": [
                    "product_bug",
                    "test_bug",
                    "infra_issue",
                    "flaky_test",
                    "unknown",
                ],
            },
            "confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
            },
            "root_cause_summary": {
                "type": "string",
                "description": "One paragraph explaining the root cause",
            },
            "evidence": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Key evidence points supporting the classification",
            },
            "recommended_action": {
                "type": "string",
                "description": "What should be done about this failure",
            },
        },
        "required": [
            "category",
            "confidence",
            "root_cause_summary",
            "evidence",
            "recommended_action",
        ],
    }
)

SUMMARY_SCHEMA = json.dumps(
    {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": "2-4 sentence executive summary of the run",
            },
        },
        "required": ["summary"],
    }
)


class ClaudeCodeBackend(AIBackend):
    """
    AI backend that calls Claude Code CLI (`claude -p`) via subprocess.

    This is the default backend. It requires no API key -- it uses
    Claude Code's own authentication (login or ANTHROPIC_API_KEY env var).
    """

    # Timeout for subprocess calls in seconds
    SUBPROCESS_TIMEOUT = 180
    # Longer timeout for agentic calls (must-gather investigation)
    AGENTIC_TIMEOUT = 1200

    def __init__(self, model="sonnet", max_budget_usd=0.50, save_prompts_dir=None):
        """
        Args:
            model: Model to use (sonnet, opus, haiku)
            max_budget_usd: Max spend per AI call in USD
            save_prompts_dir: Directory to save prompts for debugging (None = disabled)
        """
        self.model = model
        self.max_budget_usd = max_budget_usd
        self.save_prompts_dir = save_prompts_dir
        self._total_cost = 0.0
        self._total_input_tokens = 0
        self._total_output_tokens = 0
        self.jinja_env = Environment(
            loader=FileSystemLoader(PROMPT_TEMPLATES_DIR),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def is_available(self) -> bool:
        """Check if claude CLI is installed and accessible."""
        return shutil.which("claude") is not None

    @property
    def total_cost_usd(self) -> float:
        return self._total_cost

    @property
    def total_input_tokens(self) -> int:
        return self._total_input_tokens

    @property
    def total_output_tokens(self) -> int:
        return self._total_output_tokens

    def classify_failure(
        self,
        test_name: str,
        test_class: str,
        duration: float,
        squad: str,
        traceback: str,
        log_excerpt: str = "",
        infra_context: str = "",
        must_gather_info: dict = None,
        test_log_url: str = "",
        ui_logs: dict = None,
    ) -> dict:
        """Classify a test failure using Claude Code CLI.

        When must_gather_info indicates available must-gather data,
        uses agentic mode with Bash tool so Claude can investigate.
        """
        if must_gather_info and must_gather_info.get("mg_type") != "none":
            return self._classify_agentic(
                test_name=test_name,
                test_class=test_class,
                duration=duration,
                squad=squad,
                traceback=traceback,
                log_excerpt=log_excerpt,
                must_gather_info=must_gather_info,
                test_log_url=test_log_url,
                ui_logs=ui_logs,
            )

        template = self.jinja_env.get_template("classify_failure.j2")
        prompt = template.render(
            test_name=test_name,
            test_class=test_class,
            duration=duration,
            squad=squad or "Unknown",
            traceback=traceback,
            log_excerpt=self._truncate(log_excerpt, 6000),
            infra_context=self._truncate(infra_context, 4000),
        )

        result = self._call_claude(prompt, CLASSIFICATION_SCHEMA, context=test_name)

        # Validate and provide defaults
        return {
            "category": result.get("category", "unknown"),
            "confidence": float(result.get("confidence", 0.5)),
            "root_cause_summary": result.get("root_cause_summary", ""),
            "evidence": result.get("evidence", []),
            "recommended_action": result.get("recommended_action", ""),
        }

    def _classify_agentic(
        self,
        test_name: str,
        test_class: str,
        duration: float,
        squad: str,
        traceback: str,
        log_excerpt: str,
        must_gather_info: dict,
        test_log_url: str = "",
        ui_logs: dict = None,
    ) -> dict:
        """Classify using agentic mode — Claude investigates must-gather."""
        template = self.jinja_env.get_template("classify_failure_agentic.j2")
        prompt = template.render(
            test_name=test_name,
            test_class=test_class,
            duration=duration,
            squad=squad or "Unknown",
            traceback=traceback,
            log_excerpt=self._truncate(log_excerpt, 6000),
            mg_type=must_gather_info.get("mg_type", "none"),
            ocs_mg=must_gather_info.get("ocs_mg", ""),
            ocp_mg=must_gather_info.get("ocp_mg", ""),
            cluster_id=must_gather_info.get("cluster_id", ""),
            test_log_url=test_log_url,
            ui_logs=ui_logs,
        )

        # Local must-gather needs Read tool; HTTP needs Bash for curl
        allowed_tools = "Bash"
        if must_gather_info.get("mg_type") == "local":
            allowed_tools = "Bash,Read"

        result = self._call_claude_agentic(
            prompt, context=test_name, allowed_tools=allowed_tools
        )

        return {
            "category": result.get("category", "unknown"),
            "confidence": float(result.get("confidence", 0.5)),
            "root_cause_summary": result.get("root_cause_summary", ""),
            "evidence": result.get("evidence", []),
            "recommended_action": result.get("recommended_action", ""),
            "session_id": result.get("session_id", ""),
            "session_text": result.get("session_text", ""),
        }

    def generate_run_summary(
        self,
        run_metadata: dict,
        failure_summaries: list,
    ) -> str:
        """Generate an overall run summary using Claude Code CLI."""
        if not failure_summaries:
            return "No failures to summarize."

        template = self.jinja_env.get_template("run_summary.j2")
        prompt = template.render(
            platform=run_metadata.get("platform", "unknown"),
            deployment_type=run_metadata.get("deployment_type", "unknown"),
            ocp_version=run_metadata.get("ocp_version", "unknown"),
            ocs_version=run_metadata.get("ocs_version", "unknown"),
            ocs_build=run_metadata.get("ocs_build", "unknown"),
            total_tests=run_metadata.get("total_tests", 0),
            passed=run_metadata.get("passed", 0),
            failed=run_metadata.get("failed", 0),
            error=run_metadata.get("error", 0),
            skipped=run_metadata.get("skipped", 0),
            failure_summaries=failure_summaries,
        )

        result = self._call_claude(prompt, SUMMARY_SCHEMA, context="run_summary")
        return result.get("summary", "")

    def _call_claude(self, prompt: str, json_schema: str, context: str = "") -> dict:
        """
        Call claude CLI in non-interactive mode.

        Args:
            prompt: The prompt text
            json_schema: JSON schema string for structured output
            context: Descriptive label for logging (e.g., test name)

        Returns:
            Parsed structured output dict

        Raises:
            AIBackendError: If the call fails
        """
        cmd = [
            "claude",
            "-p",
            prompt,
            "--output-format",
            "json",
            "--json-schema",
            json_schema,
            "--model",
            self.model,
            "--max-budget-usd",
            str(self.max_budget_usd),
        ]

        logger.debug(
            f"Calling Claude Code CLI (model={self.model}, "
            f"prompt_length={len(prompt)}, context={context})"
        )

        if self.save_prompts_dir:
            self._save_prompt(prompt, context)

        # Remove CLAUDECODE env var to allow launching from within
        # a Claude Code session (nested session guard bypass)
        env = os.environ.copy()
        env.pop("CLAUDECODE", None)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.SUBPROCESS_TIMEOUT,
                env=env,
            )
        except subprocess.TimeoutExpired:
            raise AIBackendError(
                f"Claude Code CLI timed out after {self.SUBPROCESS_TIMEOUT}s"
            )
        except FileNotFoundError:
            raise AIBackendError(
                "Claude Code CLI ('claude') not found. "
                "Install it or use --ai-backend anthropic"
            )
        except OSError as e:
            raise AIBackendError(f"Failed to run Claude Code CLI: {e}")

        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise AIBackendError(
                f"Claude Code CLI exited with code {result.returncode}: {stderr}"
            )

        # Parse the JSON response
        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            raise AIBackendError(
                f"Failed to parse Claude Code CLI output as JSON: {e}\n"
                f"stdout: {result.stdout[:500]}"
            )

        # Extract structured output from the response
        subtype = response.get("subtype", "")
        structured = response.get("structured_output")

        if subtype == "error_max_turns" and structured is None:
            logger.debug(
                f"Claude Code hit max_turns (num_turns={response.get('num_turns')}). "
                f"Retrying is not supported; raising error."
            )

        if structured is None:
            # Fall back to parsing the result text as JSON
            result_text = response.get("result", "")
            try:
                structured = json.loads(result_text)
            except (json.JSONDecodeError, TypeError):
                raise AIBackendError(
                    f"No structured_output in Claude Code response "
                    f"(subtype={subtype}, num_turns={response.get('num_turns')}). "
                    f"result: {result_text[:500]}"
                )

        # Accumulate cost and token usage
        cost = response.get("total_cost_usd")
        usage = response.get("usage", {})
        input_tokens, output_tokens = self._extract_tokens(usage)
        if cost is not None:
            self._total_cost += cost
        self._total_input_tokens += input_tokens
        self._total_output_tokens += output_tokens
        logger.debug(
            f"Claude Code call: ${cost or 0:.4f}, "
            f"{input_tokens:,} in / {output_tokens:,} out tokens"
        )

        return structured

    def _call_claude_agentic(
        self, prompt: str, context: str = "", allowed_tools: str = "Bash"
    ) -> dict:
        """
        Call claude CLI in agentic mode with Bash tool for must-gather investigation.

        Unlike _call_claude, this uses --allowedTools "Bash" instead of --json-schema.
        Claude can run curl commands to explore the must-gather HTTP directory.
        The classification JSON is extracted from the result text.

        Args:
            prompt: The prompt text (includes must-gather URL and instructions)
            context: Descriptive label for logging

        Returns:
            Parsed classification dict

        Raises:
            AIBackendError: If the call fails
        """
        cmd = [
            "claude",
            "-p",
            prompt,
            "--output-format",
            "json",
            "--allowedTools",
            allowed_tools,
            "--dangerously-skip-permissions",
            "--model",
            self.model,
            "--max-budget-usd",
            str(self.max_budget_usd),
        ]

        logger.debug(
            f"Calling Claude Code CLI agentic mode (model={self.model}, "
            f"prompt_length={len(prompt)}, context={context})"
        )

        if self.save_prompts_dir:
            self._save_prompt(prompt, context)

        env = os.environ.copy()
        env.pop("CLAUDECODE", None)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.AGENTIC_TIMEOUT,
                env=env,
            )
        except subprocess.TimeoutExpired:
            raise AIBackendError(
                f"Claude Code CLI (agentic) timed out after {self.AGENTIC_TIMEOUT}s"
            )
        except FileNotFoundError:
            raise AIBackendError(
                "Claude Code CLI ('claude') not found. "
                "Install it or use --ai-backend anthropic"
            )
        except OSError as e:
            raise AIBackendError(f"Failed to run Claude Code CLI: {e}")

        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise AIBackendError(
                f"Claude Code CLI (agentic) exited with code {result.returncode}: {stderr}"
            )

        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            raise AIBackendError(
                f"Failed to parse Claude Code CLI agentic output as JSON: {e}\n"
                f"stdout: {result.stdout[:500]}"
            )

        # Accumulate cost and token usage
        cost = response.get("total_cost_usd")
        num_turns = response.get("num_turns")
        session_id = response.get("session_id", "")
        usage = response.get("usage", {})
        input_tokens, output_tokens = self._extract_tokens(usage)
        if cost is not None:
            self._total_cost += cost
        self._total_input_tokens += input_tokens
        self._total_output_tokens += output_tokens
        logger.info(
            f"Agentic session complete: ${cost or 0:.4f}, "
            f"{input_tokens:,} in / {output_tokens:,} out tokens, "
            f"{num_turns} turns ({context})"
        )

        # Extract classification JSON from the result text
        result_text = response.get("result", "")
        if not result_text:
            raise AIBackendError(
                f"Empty result from Claude Code agentic call "
                f"(subtype={response.get('subtype')}, num_turns={num_turns})"
            )

        classification = self._extract_json(result_text)
        classification["session_id"] = session_id
        classification["session_text"] = result_text
        return classification

    @staticmethod
    def _extract_json(text: str) -> dict:
        """Extract JSON classification dict from Claude's result text."""
        candidates = []

        # Try JSON in code blocks first
        for match in re.finditer(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL):
            try:
                parsed = json.loads(match.group(1))
                if isinstance(parsed, dict) and "category" in parsed:
                    return parsed
                candidates.append(parsed)
            except json.JSONDecodeError:
                pass

        # Try to find JSON objects in the text
        for match in re.finditer(r"\{[^{}]*\}", text, re.DOTALL):
            try:
                parsed = json.loads(match.group(0))
                if isinstance(parsed, dict) and "category" in parsed:
                    return parsed
                candidates.append(parsed)
            except json.JSONDecodeError:
                pass

        # Fall back to any dict we found
        for c in candidates:
            if isinstance(c, dict):
                return c

        raise AIBackendError(
            f"Could not extract classification JSON from agentic result: "
            f"{text[:500]}"
        )

    def _save_prompt(self, prompt: str, context: str):
        """Save prompt to disk for debugging."""
        try:
            prompts_dir = os.path.expanduser(self.save_prompts_dir)
            os.makedirs(prompts_dir, exist_ok=True)
            safe_name = re.sub(r"[^\w\-]", "_", context)[:80]
            filepath = os.path.join(prompts_dir, f"{safe_name}.txt")
            with open(filepath, "w") as f:
                f.write(prompt)
            logger.debug(f"Saved prompt to {filepath}")
        except Exception as e:
            logger.warning(f"Failed to save prompt: {e}")

    @staticmethod
    def _extract_tokens(usage: dict) -> tuple:
        """Extract total input and output tokens from usage dict.

        The usage dict has separate fields for cached vs non-cached input:
        - input_tokens: non-cached input
        - cache_creation_input_tokens: tokens written to cache
        - cache_read_input_tokens: tokens read from cache
        Total input = sum of all three.
        """
        input_tokens = (
            usage.get("input_tokens", 0)
            + usage.get("cache_creation_input_tokens", 0)
            + usage.get("cache_read_input_tokens", 0)
        )
        output_tokens = usage.get("output_tokens", 0)
        return input_tokens, output_tokens

    @staticmethod
    def _truncate(text: str, max_chars: int) -> str:
        """Truncate text to max_chars, adding a truncation notice."""
        if not text or len(text) <= max_chars:
            return text or ""
        return (
            text[:max_chars]
            + f"\n... [truncated, {len(text) - max_chars} chars omitted]"
        )
