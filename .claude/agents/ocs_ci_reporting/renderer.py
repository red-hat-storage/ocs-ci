"""Render reports from Jinja2 templates and arbitrary context dicts."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from config import DEFAULT_TEMPLATE, SUPPORTED_FORMATS, TEMPLATES_DIR
from models import ReportArtifact

log = logging.getLogger(__name__)


def resolve_template_path(template: str) -> Path:
    """Resolve template by name (in agent templates/) or absolute/relative path."""
    candidate = Path(template)
    if candidate.is_file():
        return candidate.resolve()
    bundled = TEMPLATES_DIR / template
    if bundled.is_file():
        return bundled
    if not template.endswith(".j2"):
        bundled = TEMPLATES_DIR / f"{template}.j2"
        if bundled.is_file():
            return bundled
    raise FileNotFoundError(
        f"Report template not found: {template} (looked in {TEMPLATES_DIR})"
    )


def render_report(
    context: dict[str, Any],
    *,
    template: str = DEFAULT_TEMPLATE,
    report_format: str = "markdown",
    subject: str | None = None,
) -> ReportArtifact:
    """
    Render a report from a template and context dict.

    Any workflow can call this with its own context structure; templates
    document the expected variables.
    """
    if report_format not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported report_format={report_format!r}; "
            f"use one of {sorted(SUPPORTED_FORMATS)}"
        )

    try:
        from jinja2 import Environment, FileSystemLoader, StrictUndefined
    except ImportError as exc:
        raise ImportError(
            "jinja2 is required for report rendering. "
            "Install: pip install -r .claude/agents/ocs_ci_reporting/requirements-agent.txt"
        ) from exc

    template_path = resolve_template_path(template)
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        undefined=StrictUndefined,
        autoescape=report_format == "html",
    )
    jinja_template = env.get_template(template_path.name)
    body = jinja_template.render(**context).strip()

    default_subject = (
        context.get("subject")
        or context.get("run", {}).get("title")
        or f"Workflow report {context.get('run', {}).get('run_id', '')}".strip()
    )
    resolved_subject = subject or default_subject or "Workflow report"

    return ReportArtifact(
        body=body,
        format=report_format,
        template=str(template_path),
        subject=resolved_subject,
        context=context,
        metadata={
            "template_name": template_path.name,
            "workflow": context.get("workflow"),
        },
    )
