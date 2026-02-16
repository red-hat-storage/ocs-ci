"""
Build analysis reports in various formats (JSON, Markdown, HTML).
"""

import json
import logging
import os
from collections import defaultdict

from jinja2 import Environment, FileSystemLoader

from ocs_ci.utility.log_analysis.models import FailureAnalysis, RunAnalysis

logger = logging.getLogger(__name__)

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


class ReportBuilder:
    """Generate analysis reports from RunAnalysis objects."""

    def __init__(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(TEMPLATES_DIR),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def build(self, run_analysis: RunAnalysis, fmt: str = "markdown") -> str:
        """
        Build a report in the specified format.

        Args:
            run_analysis: The analysis results
            fmt: Output format ("json", "markdown", "html")

        Returns:
            Report as a string
        """
        if fmt == "json":
            return self.build_json(run_analysis)
        elif fmt == "markdown":
            return self.build_markdown(run_analysis)
        elif fmt == "html":
            return self.build_html(run_analysis)
        else:
            raise ValueError(f"Unknown format: {fmt}")

    def build_json(self, run_analysis: RunAnalysis) -> str:
        """Build JSON report."""
        return run_analysis.to_json(indent=2)

    def build_markdown(self, run_analysis: RunAnalysis) -> str:
        """Build Markdown report."""
        template = self.jinja_env.get_template("analysis_report.md.j2")
        context = self._build_template_context(run_analysis)
        return template.render(**context)

    def build_html(self, run_analysis: RunAnalysis) -> str:
        """Build HTML report by converting Markdown to rendered HTML."""
        md_content = self.build_markdown(run_analysis)
        return self._md_to_html(
            md_content, title="OCS-CI Log Analysis Report"
        )

    def _md_to_html(self, md_content: str, title: str = "Report") -> str:
        """
        Convert Markdown to a styled HTML page.

        Requires the 'markdown' library: pip install markdown
        """
        try:
            import markdown
        except ImportError:
            raise ImportError(
                "The 'markdown' library is required for HTML output. "
                "Install it with: pip install markdown"
            )

        html_body = markdown.markdown(
            md_content,
            extensions=["tables", "fenced_code"],
        )

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               max-width: 1200px; margin: 0 auto; padding: 20px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        code {{ background-color: #f4f4f4; padding: 2px 4px; border-radius: 3px; }}
        pre {{ background-color: #f4f4f4; padding: 10px; border-radius: 5px;
               overflow-x: auto; }}
        details {{ margin: 5px 0; }}
        hr {{ border: none; border-top: 1px solid #ddd; margin: 20px 0; }}
    </style>
</head>
<body>
{html_body}
</body>
</html>"""

    def build_jira_comment(
        self, fa: FailureAnalysis, run_url: str = ""
    ) -> str:
        """
        Build a Jira comment from a failure analysis.

        Args:
            fa: FailureAnalysis to generate comment for
            run_url: URL of the test run

        Returns:
            Jira-formatted comment text
        """
        template = self.jinja_env.get_template("jira_comment.j2")
        return template.render(fa=fa, run_url=run_url)

    def build_trends_report(self, trend_report, fmt: str = "markdown") -> str:
        """
        Build a cross-run trend analysis report.

        Args:
            trend_report: TrendReport from PatternDetector
            fmt: Output format ("json", "markdown", or "html")

        Returns:
            Report as a string
        """
        if fmt == "json":
            return json.dumps(trend_report.to_dict(), indent=2)

        template = self.jinja_env.get_template("trends_report.md.j2")
        md_content = template.render(report=trend_report)

        if fmt == "html":
            return self._md_to_html(
                md_content, title="OCS-CI Cross-Run Trend Analysis"
            )

        return md_content

    def _build_template_context(self, run_analysis: RunAnalysis) -> dict:
        """Build the Jinja2 template context with grouped data."""
        # Group failures by category
        categories = defaultdict(list)
        for fa in run_analysis.failure_analyses:
            categories[fa.category.value].append(fa.test_result.name)

        # Group failures by squad
        squads = defaultdict(list)
        for fa in run_analysis.failure_analyses:
            squad = fa.test_result.squad or "Unknown"
            squads[squad].append(fa.test_result.name)

        return {
            "run": run_analysis,
            "categories": dict(categories),
            "squads": dict(squads),
        }
