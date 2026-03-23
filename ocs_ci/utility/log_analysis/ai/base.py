"""
Abstract base class for AI backends.

All AI backends (Claude Code CLI, Claude SDK, Anthropic API)
implement this protocol so they can be swapped interchangeably.
"""

import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class AIBackend(ABC):
    """Protocol for AI backends used in log analysis."""

    @abstractmethod
    def classify_failure(
        self,
        test_name: str,
        test_class: str,
        duration: float,
        squad: str,
        traceback: str,
        log_excerpt: str,
        infra_context: str = "",
        must_gather_info: dict = None,
        test_log_url: str = "",
        ui_logs: dict = None,
        run_metadata: dict = None,
    ) -> dict:
        """
        Classify a test failure into a category.

        Args:
            test_name: Test function name
            test_class: Test class/module path
            duration: Test duration in seconds
            squad: Squad owning the test
            traceback: Full Python traceback
            log_excerpt: Extracted error/warning lines from test log
            infra_context: Ceph/OSD/pod info from must-gather
            must_gather_info: Pre-resolved must-gather paths dict with keys:
                mg_type: "local" | "http" | "none"
                mg_base: local path or HTTP URL to the data dir
                ocs_mg: path/URL to ocs_must_gather data dir
                ocp_mg: path/URL to ocp_must_gather data dir
                cluster_id: cluster ID string
            test_log_url: Direct URL to the per-test log directory
            ui_logs: UI logs info dict (only for UI tests) with keys:
                dom_url: URL to DOM snapshots directory
                screenshots_url: URL to screenshots directory

        Returns:
            dict with keys:
                category: str ("product_bug", "test_bug", "infra_issue", "flaky_test", "unknown")
                confidence: float (0.0 - 1.0)
                root_cause_summary: str
                evidence: list[str]
                recommended_action: str
        """
        pass

    @abstractmethod
    def generate_run_summary(
        self,
        run_metadata: dict,
        failure_summaries: list,
    ) -> str:
        """
        Generate an overall summary of the test run.

        Args:
            run_metadata: dict with platform, versions, etc.
            failure_summaries: list of dicts with test_name, category, root_cause_summary

        Returns:
            Human-readable summary paragraph
        """
        pass

    @property
    def requires_budget_limit(self) -> bool:
        """Whether this backend should be subject to max_failures limiting."""
        return True

    @property
    def total_cost_usd(self) -> float:
        """Total cost accumulated across all calls. Override in subclasses."""
        return 0.0

    def is_available(self) -> bool:
        """
        Check if this backend is available (dependencies installed, auth configured).

        Returns:
            True if the backend can be used
        """
        return True


def get_backend(backend_name: str, **kwargs) -> AIBackend:
    """
    Factory function to get an AI backend by name.

    Args:
        backend_name: One of "claude-code", "anthropic", "none"
        **kwargs: Backend-specific options (model, max_budget_usd, etc.)

    Returns:
        AIBackend instance

    Raises:
        ValueError: If backend_name is unknown or unavailable
    """
    if backend_name == "claude-code":
        from ocs_ci.utility.log_analysis.ai.claude_code_backend import (
            ClaudeCodeBackend,
        )

        return ClaudeCodeBackend(**kwargs)
    elif backend_name == "anthropic":
        from ocs_ci.utility.log_analysis.ai.anthropic_backend import (
            AnthropicBackend,
        )

        return AnthropicBackend(**kwargs)
    elif backend_name == "none":
        return NoOpBackend()
    else:
        raise ValueError(
            f"Unknown AI backend: {backend_name}. "
            f"Choose from: claude-code, anthropic, none"
        )


class NoOpBackend(AIBackend):
    """No-op backend that returns empty results. Used for --known-issues-only mode."""

    @property
    def requires_budget_limit(self) -> bool:
        return False

    def classify_failure(self, **kwargs) -> dict:
        return {
            "category": "unknown",
            "confidence": 0.0,
            "root_cause_summary": "",
            "evidence": [],
            "recommended_action": "Run with an AI backend for classification",
        }

    def generate_run_summary(self, run_metadata, failure_summaries) -> str:
        return ""
