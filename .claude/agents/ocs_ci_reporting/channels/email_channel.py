"""Deliver reports via SMTP email."""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from models import ChannelResult, ReportArtifact

log = logging.getLogger(__name__)


def deliver_email(
    report: ReportArtifact,
    *,
    smtp_host: str,
    smtp_port: int = 587,
    use_tls: bool = True,
    username: str | None = None,
    password: str | None = None,
    from_addr: str,
    to_addrs: list[str],
    dry_run: bool = False,
) -> ChannelResult:
    """Send report by email."""
    recipients = [r.strip() for r in to_addrs if r and str(r).strip()]
    if not recipients:
        return ChannelResult(
            channel_type="email",
            status="failed",
            dry_run=dry_run,
            detail="No email recipients configured",
        )

    if dry_run:
        log.info(
            "Dry-run: would email report to %s via %s:%s",
            recipients,
            smtp_host,
            smtp_port,
        )
        return ChannelResult(
            channel_type="email",
            status="dry_run",
            dry_run=True,
            detail=f"Would email {', '.join(recipients)}",
        )

    subtype = "html" if report.format == "html" else "plain"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = report.subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(report.body, subtype, "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, int(smtp_port), timeout=60) as smtp:
            if use_tls:
                smtp.starttls()
            if username and password:
                smtp.login(username, password)
            smtp.sendmail(from_addr, recipients, msg.as_string())
    except OSError as exc:
        log.error("Email delivery failed: %s", exc)
        return ChannelResult(
            channel_type="email",
            status="failed",
            dry_run=False,
            detail=str(exc),
        )

    log.info("Emailed report to %s", recipients)
    return ChannelResult(
        channel_type="email",
        status="sent",
        dry_run=False,
        detail=f"Sent to {', '.join(recipients)}",
    )
