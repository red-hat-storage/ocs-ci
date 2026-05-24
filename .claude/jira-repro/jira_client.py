"""Shared JIRA HTTP helpers and ADF parsing."""

from __future__ import annotations

import os
from typing import Any

import requests

DEFAULT_URL = "https://redhat.atlassian.net"


def adf_to_text(node: Any) -> str:
    if isinstance(node, dict):
        if node.get("type") == "text":
            return str(node.get("text", ""))
        return "".join(adf_to_text(c) for c in node.get("content") or [])
    if isinstance(node, list):
        return "".join(adf_to_text(x) for x in node)
    return ""


def auth_headers() -> tuple[dict[str, str], tuple[str, str] | None]:
    email = os.environ.get("JIRA_EMAIL") or os.environ.get("JIRA_MCP_EMAIL", "")
    token = os.environ.get("JIRA_API_TOKEN") or os.environ.get("JIRA_MCP_TOKEN", "")
    if os.environ.get("JIRA_AUTH", "basic").lower() == "bearer":
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}, None
    return {"Accept": "application/json"}, (email, token) if email and token else None


def make_session() -> requests.Session:
    base = os.environ.get("JIRA_URL", DEFAULT_URL).rstrip("/")
    token = os.environ.get("JIRA_API_TOKEN") or os.environ.get("JIRA_MCP_TOKEN", "")
    if not token:
        raise SystemExit("Set JIRA_API_TOKEN or source mcp-env.sh")
    headers, auth = auth_headers()
    session = requests.Session()
    session.headers.update(headers)
    if auth:
        session.auth = auth
    session._base_url = base  # type: ignore[attr-defined]
    return session
