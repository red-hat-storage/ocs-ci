"""High-level reporting API — build and deliver workflow reports."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from auth import load_reporting_auth
from channels.email_channel import deliver_email
from channels.file_channel import deliver_file
from channels.slack_channel import deliver_slack
from config import SUPPORTED_CHANNEL_TYPES
from models import ChannelResult, DeliveryResult, ReportArtifact
from renderer import render_report

log = logging.getLogger(__name__)

__all__ = [
    "STAGE_REPORTING",
    "build_report",
    "deliver_report",
    "build_and_deliver",
]

from models import STAGE_REPORTING


def build_report(
    context: dict[str, Any],
    *,
    template: str,
    report_format: str = "markdown",
    subject: str | None = None,
) -> ReportArtifact:
    """Render a report from template + context (workflow-agnostic)."""
    return render_report(
        context,
        template=template,
        report_format=report_format,
        subject=subject,
    )


def _resolve_channel_config(
    channel: dict[str, Any],
    auth: dict[str, Any],
) -> dict[str, Any]:
    """Merge per-channel config with auth.yaml defaults."""
    merged = dict(channel)
    channel_type = str(merged.get("type", "")).lower()
    if channel_type == "slack":
        slack_auth = auth.get("slack") or {}
        merged.setdefault("webhook_url", slack_auth.get("webhook_url"))
        merged.setdefault("channel", slack_auth.get("channel"))
    elif channel_type == "email":
        email_auth = auth.get("email") or {}
        for key in (
            "smtp_host",
            "smtp_port",
            "use_tls",
            "from",
            "to",
            "username",
            "password",
        ):
            merged.setdefault(key, email_auth.get(key))
    return merged


def deliver_report(
    report: ReportArtifact,
    *,
    channels: list[dict[str, Any]],
    output_dir: Path | str | None = None,
    dry_run: bool = True,
    auth_path: Path | str | None = None,
) -> DeliveryResult:
    """
    Deliver a rendered report to configured channels.

    Channel examples::

        [{"type": "file"}]
        [{"type": "slack"}]  # webhook from auth.yaml reporting.slack
        [{"type": "email", "to": ["team@example.com"]}]
    """
    auth = load_reporting_auth(auth_path=auth_path)
    results: list[ChannelResult] = []

    if not channels:
        channels = [{"type": "file"}]

    for raw_channel in channels:
        channel = _resolve_channel_config(raw_channel, auth)
        channel_type = str(channel.get("type", "file")).lower()
        if channel_type not in SUPPORTED_CHANNEL_TYPES:
            results.append(
                ChannelResult(
                    channel_type=channel_type,
                    status="failed",
                    dry_run=dry_run,
                    detail=f"Unsupported channel type: {channel_type}",
                )
            )
            continue

        if channel_type == "file":
            out = output_dir or channel.get("output_dir") or "."
            filename = channel.get("filename")
            results.append(
                deliver_file(
                    report,
                    output_dir=out,
                    filename=filename,
                    dry_run=dry_run,
                )
            )
        elif channel_type == "slack":
            results.append(
                deliver_slack(
                    report,
                    webhook_url=str(channel.get("webhook_url") or ""),
                    channel=channel.get("channel"),
                    dry_run=dry_run,
                )
            )
        elif channel_type == "email":
            to_addrs = channel.get("to") or []
            if isinstance(to_addrs, str):
                to_addrs = [to_addrs]
            results.append(
                deliver_email(
                    report,
                    smtp_host=str(channel.get("smtp_host") or ""),
                    smtp_port=int(channel.get("smtp_port") or 587),
                    use_tls=bool(channel.get("use_tls", True)),
                    username=channel.get("username"),
                    password=channel.get("password"),
                    from_addr=str(channel.get("from") or ""),
                    to_addrs=list(to_addrs),
                    dry_run=dry_run,
                )
            )

    return DeliveryResult(report=report, channels=results)


def build_and_deliver(
    context: dict[str, Any],
    *,
    template: str,
    channels: list[dict[str, Any]],
    report_format: str = "markdown",
    subject: str | None = None,
    output_dir: Path | str | None = None,
    dry_run: bool = True,
    auth_path: Path | str | None = None,
) -> DeliveryResult:
    """Render and deliver in one call."""
    report = build_report(
        context,
        template=template,
        report_format=report_format,
        subject=subject,
    )
    return deliver_report(
        report,
        channels=channels,
        output_dir=output_dir,
        dry_run=dry_run,
        auth_path=auth_path,
    )
