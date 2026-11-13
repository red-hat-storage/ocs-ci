"""Write report artifacts to disk."""

from __future__ import annotations

import logging
from pathlib import Path

from models import ChannelResult, ReportArtifact

log = logging.getLogger(__name__)


def deliver_file(
    report: ReportArtifact,
    *,
    output_dir: Path | str,
    filename: str | None = None,
    dry_run: bool = False,
) -> ChannelResult:
    """Save report body to a file."""
    out_dir = Path(output_dir)
    ext = (
        "html"
        if report.format == "html"
        else "md" if report.format == "markdown" else "txt"
    )
    name = (
        filename or f"report_{report.context.get('run', {}).get('run_id', 'run')}.{ext}"
    )
    dest = out_dir / name

    if dry_run:
        log.info("Dry-run: would write report to %s", dest)
        return ChannelResult(
            channel_type="file",
            status="dry_run",
            dry_run=True,
            detail=f"Would write {dest}",
            artifact_path=str(dest),
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    dest.write_text(report.body, encoding="utf-8")
    log.info("Wrote report to %s", dest)
    return ChannelResult(
        channel_type="file",
        status="saved",
        dry_run=False,
        detail=str(dest),
        artifact_path=str(dest.resolve()),
    )
