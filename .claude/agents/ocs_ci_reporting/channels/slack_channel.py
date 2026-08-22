"""Deliver reports via Slack incoming webhook."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any

from models import ChannelResult, ReportArtifact

log = logging.getLogger(__name__)

_MAX_SLACK_TEXT = 39000


def _slack_payload(
    report: ReportArtifact, *, channel: str | None = None
) -> dict[str, Any]:
    text = report.body
    if len(text) > _MAX_SLACK_TEXT:
        text = text[: _MAX_SLACK_TEXT - 50] + "\n\n...(truncated for Slack)"
    payload: dict[str, Any] = {
        "text": report.subject,
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": report.subject[:150]},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
            },
        ],
    }
    if channel:
        payload["channel"] = channel
    return payload


def deliver_slack(
    report: ReportArtifact,
    *,
    webhook_url: str,
    channel: str | None = None,
    dry_run: bool = False,
) -> ChannelResult:
    """POST report to Slack incoming webhook."""
    if not webhook_url:
        return ChannelResult(
            channel_type="slack",
            status="failed",
            dry_run=dry_run,
            detail="Missing slack webhook_url",
        )

    if dry_run:
        log.info("Dry-run: would post report to Slack webhook")
        return ChannelResult(
            channel_type="slack",
            status="dry_run",
            dry_run=True,
            detail="Would post to Slack webhook",
        )

    payload = _slack_payload(report, channel=channel)
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            detail = response.read().decode("utf-8", errors="replace")[:200]
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        log.error("Slack webhook failed: %s %s", exc.code, body)
        return ChannelResult(
            channel_type="slack",
            status="failed",
            dry_run=False,
            detail=f"HTTP {exc.code}: {body}",
        )
    except OSError as exc:
        log.error("Slack webhook error: %s", exc)
        return ChannelResult(
            channel_type="slack",
            status="failed",
            dry_run=False,
            detail=str(exc),
        )

    log.info("Posted report to Slack")
    return ChannelResult(
        channel_type="slack",
        status="sent",
        dry_run=False,
        detail=detail or "ok",
    )
