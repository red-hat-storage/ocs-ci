"""Jenkins client factory and MCP read adapter."""

import logging
from typing import Any, Callable

from jenkins.rest_client import RestJenkinsClient
from models import JobRef

log = logging.getLogger(__name__)

McpCaller = Callable[[str, dict[str, Any]], Any]

_mcp_caller: McpCaller | None = None


def set_mcp_caller(caller: McpCaller | None) -> None:
    """Register a callable(tool_name, arguments) for Jenkins MCP (Claude Code)."""
    global _mcp_caller
    _mcp_caller = caller


class McpJenkinsReadClient:
    """
    Read builds via redhat-ai-tools/jenkins-mcp tools.

    Tools: getBuild, getJob, getBuildLog, getAllJobs, triggerBuild (not used here).
    """

    def __init__(self, caller: McpCaller | None = None):
        self._caller = caller or _mcp_caller
        if self._caller is None:
            raise RuntimeError(
                "Jenkins MCP caller not configured. "
                "Use set_mcp_caller() or RestJenkinsClient for reads."
            )

    def get_build(self, job_ref: JobRef) -> dict[str, Any]:
        if job_ref.build_number is None:
            raise ValueError("build_number required")
        args: dict[str, Any] = {
            "full_path": job_ref.full_path,
            "build_number": job_ref.build_number,
        }
        result = self._caller("getBuild", args)
        return _normalize_mcp_build(result)

    def get_build_log(
        self,
        job_ref: JobRef,
        *,
        start: int = 0,
    ) -> str:
        if job_ref.build_number is None:
            raise ValueError("build_number required")
        args: dict[str, Any] = {
            "full_path": job_ref.full_path,
            "build_number": job_ref.build_number,
            "start": start,
        }
        result = self._caller("getBuildLog", args)
        if isinstance(result, str):
            return result
        if isinstance(result, dict):
            return str(result.get("log") or result.get("text") or result)
        return str(result)


def _normalize_mcp_build(payload: Any) -> dict[str, Any]:
    """Coerce MCP getBuild response into REST-like build dict."""
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected getBuild response type: {type(payload)}")
    if "actions" in payload or "description" in payload:
        return payload
    nested = payload.get("build") or payload.get("data")
    if isinstance(nested, dict):
        return nested
    return payload


def get_rest_client(**kwargs: Any) -> RestJenkinsClient:
    """Return REST client for reads, writes, triggers, and abort."""
    return RestJenkinsClient(**kwargs)


def get_read_client(
    *, prefer_mcp: bool = False, **kwargs: Any
) -> RestJenkinsClient | McpJenkinsReadClient:
    """
    Return read client: MCP if prefer_mcp and caller set, else REST.

    Parameterized triggers always use REST via RestJenkinsClient.
    """
    if prefer_mcp and _mcp_caller is not None:
        try:
            return McpJenkinsReadClient()
        except RuntimeError:
            log.debug("MCP read client unavailable; using REST")
    return RestJenkinsClient(**kwargs)
