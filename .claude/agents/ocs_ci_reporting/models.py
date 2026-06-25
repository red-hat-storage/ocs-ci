"""Data models for the reporting agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

STAGE_REPORTING = "reporting"


@dataclass
class ReportArtifact:
    """Rendered report ready for delivery."""

    body: str
    format: str
    template: str
    subject: str
    context: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChannelResult:
    """Result of sending a report to one channel."""

    channel_type: str
    status: str
    dry_run: bool
    detail: str = ""
    artifact_path: str | None = None


@dataclass
class DeliveryResult:
    """Aggregate delivery results."""

    report: ReportArtifact
    channels: list[ChannelResult] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return all(c.status in ("sent", "dry_run", "saved") for c in self.channels)
