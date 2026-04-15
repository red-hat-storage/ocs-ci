"""
Direct Anthropic API backend for AI-powered log analysis.

Uses the anthropic Python SDK. Requires ANTHROPIC_API_KEY
environment variable or config. Fallback for environments
without Claude Code installed.
"""

import json
import logging
import os

from jinja2 import Environment, FileSystemLoader

from ocs_ci.utility.log_analysis.ai.base import AIBackend
from ocs_ci.utility.log_analysis.exceptions import AIBackendError

logger = logging.getLogger(__name__)

PROMPT_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "prompt_templates")

MODEL_MAP = {
    "sonnet": "claude-sonnet-4-20250514",
    "opus": "claude-opus-4-20250514",
    "haiku": "claude-haiku-4-20250414",
}


class AnthropicBackend(AIBackend):
    """
    AI backend using the Anthropic Python SDK directly.

    Requires:
        - pip install anthropic
        - ANTHROPIC_API_KEY env var set
    """

    def __init__(self, model="sonnet", api_key=None, max_tokens=4096):
        """
        Args:
            model: Model alias (sonnet, opus, haiku) or full model ID
            api_key: API key (falls back to ANTHROPIC_API_KEY env var)
            max_tokens: Max output tokens per request
        """
        self.model_id = MODEL_MAP.get(model, model)
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        self.max_tokens = max_tokens
        self._client = None
        self.jinja_env = Environment(
            loader=FileSystemLoader(PROMPT_TEMPLATES_DIR),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    @property
    def client(self):
        if self._client is None:
            try:
                import anthropic
            except ImportError:
                raise AIBackendError(
                    "anthropic package not installed. " "Run: pip install anthropic"
                )
            if not self.api_key:
                raise AIBackendError(
                    "ANTHROPIC_API_KEY not set. Either set the env var "
                    "or use --ai-backend claude-code instead."
                )
            self._client = anthropic.Anthropic(api_key=self.api_key)
        return self._client

    def is_available(self) -> bool:
        """Check if anthropic SDK is installed and API key is set."""
        try:
            import anthropic  # noqa: F401

            return bool(self.api_key)
        except ImportError:
            return False

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
        run_metadata: dict = None,
    ) -> dict:
        """Classify a test failure using Anthropic API."""
        template = self.jinja_env.get_template("classify_failure.j2")
        prompt = template.render(
            test_name=test_name,
            test_class=test_class,
            duration=duration,
            squad=squad or "Unknown",
            traceback=traceback,
            log_excerpt=self._truncate(log_excerpt, 6000),
            infra_context=self._truncate(infra_context, 4000),
            run_metadata=run_metadata,
        )

        response_text = self._call_api(
            prompt,
            system="You are an ODF/OCS test failure classifier. "
            "Respond ONLY with valid JSON matching the requested format. "
            "No markdown, no explanation outside the JSON.",
        )

        try:
            result = json.loads(response_text)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks
            result = self._extract_json(response_text)

        d = {
            "category": result.get("category", "unknown"),
            "confidence": float(result.get("confidence", 0.5)),
            "root_cause_summary": result.get("root_cause_summary", ""),
            "evidence": result.get("evidence", []),
            "recommended_action": result.get("recommended_action", ""),
        }
        if result.get("bug_details"):
            d["bug_details"] = result["bug_details"]
        if result.get("suggested_fix"):
            d["suggested_fix"] = result["suggested_fix"]
        return d

    def generate_run_summary(
        self,
        run_metadata: dict,
        failure_summaries: list,
    ) -> str:
        """Generate an overall run summary using Anthropic API."""
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

        return self._call_api(
            prompt,
            system="You are an ODF/OCS QE analyst. Write a concise run summary.",
        )

    def _call_api(self, prompt: str, system: str = "") -> str:
        """
        Make a single Anthropic API call.

        Args:
            prompt: User prompt
            system: System prompt

        Returns:
            Response text

        Raises:
            AIBackendError on failure
        """
        logger.debug(
            f"Calling Anthropic API (model={self.model_id}, "
            f"prompt_length={len(prompt)})"
        )

        try:
            message = self.client.messages.create(
                model=self.model_id,
                max_tokens=self.max_tokens,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            raise AIBackendError(f"Anthropic API call failed: {e}")

        # Extract text from response
        text_parts = []
        for block in message.content:
            if hasattr(block, "text"):
                text_parts.append(block.text)

        response_text = "\n".join(text_parts)

        # Log usage
        usage = message.usage
        logger.info(
            f"Anthropic API: {usage.input_tokens} input tokens, "
            f"{usage.output_tokens} output tokens"
        )

        return response_text

    @staticmethod
    def _extract_json(text: str) -> dict:
        """Try to extract JSON from text that may contain markdown code blocks."""
        import re

        # Try to find JSON in code blocks
        match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # Try to find a JSON object in the text
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        raise AIBackendError(f"Could not extract JSON from response: {text[:500]}")

    @staticmethod
    def _truncate(text: str, max_chars: int) -> str:
        """Truncate text to max_chars."""
        if not text or len(text) <= max_chars:
            return text or ""
        return (
            text[:max_chars]
            + f"\n... [truncated, {len(text) - max_chars} chars omitted]"
        )
