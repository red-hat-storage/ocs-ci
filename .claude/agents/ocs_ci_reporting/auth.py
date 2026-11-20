"""Load reporting credentials from data/auth.yaml or environment."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from config import AUTH_YAML_CANDIDATES


class ReportingAuthError(RuntimeError):
    """Reporting credentials could not be resolved."""


def _load_auth_file(path: Path | None = None) -> dict[str, Any]:
    if path is not None:
        if not path.is_file():
            raise ReportingAuthError(f"Auth file not found: {path}")
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    for candidate in AUTH_YAML_CANDIDATES:
        if candidate.is_file():
            return yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
    return {}


def load_reporting_auth(*, auth_path: Path | str | None = None) -> dict[str, Any]:
    """
    Return reporting section from auth file merged with env overrides.

    Expected auth.yaml shape::

        reporting:
          slack:
            webhook_url: https://hooks.slack.com/services/...
          email:
            smtp_host: smtp.example.com
            smtp_port: 587
            use_tls: true
            from: ocs-ci@example.com
            to: [team@example.com]
            username: ...
            password: ...
    """
    path = Path(auth_path) if auth_path else None
    data = _load_auth_file(path)
    reporting = dict(data.get("reporting") or {})

    slack = dict(reporting.get("slack") or {})
    slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if slack_webhook:
        slack["webhook_url"] = slack_webhook
    if slack:
        reporting["slack"] = slack

    email = dict(reporting.get("email") or {})
    for key, env_key in (
        ("smtp_host", "SMTP_HOST"),
        ("smtp_port", "SMTP_PORT"),
        ("from", "REPORT_EMAIL_FROM"),
        ("username", "SMTP_USERNAME"),
        ("password", "SMTP_PASSWORD"),
    ):
        if os.environ.get(env_key):
            email[key] = os.environ[env_key]
    recipients = os.environ.get("REPORT_EMAIL_TO")
    if recipients:
        email["to"] = [r.strip() for r in recipients.split(",") if r.strip()]
    if email:
        reporting["email"] = email

    return reporting
