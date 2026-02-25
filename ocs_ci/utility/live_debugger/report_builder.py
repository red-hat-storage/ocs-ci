"""
HTML report generation for live cluster debugging results.

Generates per-test reports and a session-level aggregated report
with expandable sections for each investigated test failure.
"""

import html
import logging
import os
import re

logger = logging.getLogger(__name__)

# Category colors for the report
CATEGORY_COLORS = {
    "product_bug": "#dc3545",
    "test_bug": "#fd7e14",
    "infra_issue": "#6f42c1",
    "known_issue": "#17a2b8",
    "unknown": "#6c757d",
}

CATEGORY_LABELS = {
    "product_bug": "Product Bug",
    "test_bug": "Test Bug",
    "infra_issue": "Infra Issue",
    "known_issue": "Known Issue",
    "unknown": "Unknown",
}


class DebugReportBuilder:
    """Generates HTML reports from live debugging investigation results."""

    def build_single_report(self, result):
        """
        Generate HTML report for a single test investigation.

        Args:
            result: Investigation result dict from LiveClusterDebugger.investigate()

        Returns:
            str: HTML content
        """
        category = result.get("category", "unknown")
        color = CATEGORY_COLORS.get(category, CATEGORY_COLORS["unknown"])
        label = CATEGORY_LABELS.get(category, "Unknown")

        investigation_html = self._markdown_to_html(
            result.get("investigation", "No investigation data")
        )
        evidence_items = result.get("evidence", [])
        evidence_html = "\n".join(
            f"<li>{html.escape(e)}</li>" for e in evidence_items
        ) if evidence_items else "<li>No evidence recorded</li>"

        safety_violations = result.get("safety_violations", [])
        safety_html = ""
        if safety_violations:
            items = "\n".join(
                f'<li class="violation">{html.escape(v)}</li>'
                for v in safety_violations
            )
            safety_html = f"""
            <div class="safety-warning">
                <h3>Safety Violations Detected</h3>
                <ul>{items}</ul>
            </div>
            """

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Live Debug: {html.escape(result.get('test_name', 'Unknown'))}</title>
{self._css()}
</head>
<body>
<div class="container">
    <h1>Live Cluster Debug Report</h1>
    <div class="test-header">
        <h2>{html.escape(result.get('test_name', 'Unknown'))}</h2>
        <span class="badge" style="background:{color}">{label}</span>
        <span class="phase-badge">{html.escape(result.get('failure_phase', 'call'))}</span>
    </div>
    <div class="meta">
        <span>Node ID: <code>{html.escape(result.get('test_nodeid', ''))}</code></span>
        <span>Cost: ${result.get('cost_usd', 0):.4f}</span>
        <span>Turns: {result.get('num_turns', 0)}</span>
        <span>Duration: {result.get('duration_seconds', 0):.1f}s</span>
    </div>

    {safety_html}

    <h3>Root Cause</h3>
    <p class="root-cause">{html.escape(result.get('root_cause', 'Not determined'))}</p>

    <h3>Evidence</h3>
    <ul class="evidence">{evidence_html}</ul>

    <h3>Recommended Action</h3>
    <p>{html.escape(result.get('recommended_action', 'None'))}</p>

    <details>
        <summary>Full Investigation Narrative</summary>
        <div class="investigation">{investigation_html}</div>
    </details>

    {self._render_error(result)}
</div>
</body>
</html>"""

    def build_session_report(self, all_results, log_dir):
        """
        Generate aggregated HTML report for all investigated tests in the session.

        Args:
            all_results: List of investigation result dicts
            log_dir: Directory to write the report to

        Returns:
            str: Path to the generated report file
        """
        if not all_results:
            logger.info("No live debug results to report")
            return None

        # Compute summary stats
        total_cost = sum(r.get("cost_usd", 0) for r in all_results)
        total_duration = sum(r.get("duration_seconds", 0) for r in all_results)
        category_counts = {}
        for r in all_results:
            cat = r.get("category", "unknown")
            category_counts[cat] = category_counts.get(cat, 0) + 1

        # Build summary table rows
        summary_rows = []
        for r in all_results:
            cat = r.get("category", "unknown")
            color = CATEGORY_COLORS.get(cat, CATEGORY_COLORS["unknown"])
            label = CATEGORY_LABELS.get(cat, "Unknown")
            test_name = html.escape(r.get("test_name", "Unknown"))
            root_cause = html.escape(
                self._truncate_text(r.get("root_cause", ""), 120)
            )
            summary_rows.append(f"""
                <tr>
                    <td><code>{test_name}</code></td>
                    <td><span class="badge" style="background:{color}">{label}</span></td>
                    <td class="phase">{html.escape(r.get('failure_phase', 'call'))}</td>
                    <td>{root_cause}</td>
                    <td>${r.get('cost_usd', 0):.4f}</td>
                    <td>{r.get('duration_seconds', 0):.0f}s</td>
                </tr>
            """)

        # Build per-test expandable sections
        detail_sections = []
        for i, r in enumerate(all_results):
            cat = r.get("category", "unknown")
            color = CATEGORY_COLORS.get(cat, CATEGORY_COLORS["unknown"])
            label = CATEGORY_LABELS.get(cat, "Unknown")
            test_name = html.escape(r.get("test_name", "Unknown"))
            investigation_html = self._markdown_to_html(
                r.get("investigation", "No investigation data")
            )
            evidence_items = r.get("evidence", [])
            evidence_html = "\n".join(
                f"<li>{html.escape(e)}</li>" for e in evidence_items
            ) if evidence_items else "<li>No evidence</li>"

            safety_violations = r.get("safety_violations", [])
            safety_html = ""
            if safety_violations:
                items = "\n".join(
                    f'<li class="violation">{html.escape(v)}</li>'
                    for v in safety_violations
                )
                safety_html = f"""
                <div class="safety-warning">
                    <h4>Safety Violations</h4>
                    <ul>{items}</ul>
                </div>
                """

            detail_sections.append(f"""
            <details class="test-detail" {"open" if i == 0 else ""}>
                <summary>
                    <span class="badge" style="background:{color}">{label}</span>
                    <code>{test_name}</code>
                    <span class="phase-badge">{html.escape(r.get('failure_phase', 'call'))}</span>
                </summary>
                <div class="detail-body">
                    {safety_html}
                    <h4>Root Cause</h4>
                    <p class="root-cause">{html.escape(r.get('root_cause', 'Not determined'))}</p>

                    <h4>Evidence</h4>
                    <ul class="evidence">{evidence_html}</ul>

                    <h4>Recommended Action</h4>
                    <p>{html.escape(r.get('recommended_action', 'None'))}</p>

                    <details>
                        <summary>Full Investigation ({r.get('num_turns', 0)} turns, ${r.get('cost_usd', 0):.4f})</summary>
                        <div class="investigation">{investigation_html}</div>
                    </details>

                    {self._render_error(r)}
                </div>
            </details>
            """)

        # Category summary badges
        category_badges = " ".join(
            f'<span class="badge" style="background:{CATEGORY_COLORS.get(cat, "#6c757d")}">'
            f'{CATEGORY_LABELS.get(cat, cat)}: {count}</span>'
            for cat, count in sorted(category_counts.items())
        )

        report_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Live Debug Session Report</title>
{self._css()}
</head>
<body>
<div class="container">
    <h1>Live Cluster Debug Report</h1>

    <div class="session-summary">
        <h2>Session Summary</h2>
        <div class="stats">
            <div class="stat">
                <span class="stat-value">{len(all_results)}</span>
                <span class="stat-label">Tests Investigated</span>
            </div>
            <div class="stat">
                <span class="stat-value">${total_cost:.4f}</span>
                <span class="stat-label">Total Cost</span>
            </div>
            <div class="stat">
                <span class="stat-value">{total_duration:.0f}s</span>
                <span class="stat-label">Total Duration</span>
            </div>
        </div>
        <div class="category-summary">{category_badges}</div>
    </div>

    <h2>Summary Table</h2>
    <table>
        <thead>
            <tr>
                <th>Test</th>
                <th>Category</th>
                <th>Phase</th>
                <th>Root Cause</th>
                <th>Cost</th>
                <th>Duration</th>
            </tr>
        </thead>
        <tbody>
            {"".join(summary_rows)}
        </tbody>
    </table>

    <h2>Detailed Investigations</h2>
    {"".join(detail_sections)}
</div>
</body>
</html>"""

        # Write the report
        os.makedirs(log_dir, exist_ok=True)
        report_path = os.path.join(log_dir, "live_debug_report.html")
        try:
            with open(report_path, "w") as f:
                f.write(report_html)
            logger.info(f"Live debug session report saved: {report_path}")
        except OSError as e:
            logger.error(f"Failed to save session report: {e}")
            return None

        return report_path

    def _render_error(self, result):
        """Render an error section if the investigation had errors."""
        error = result.get("error")
        if not error:
            return ""
        return f"""
        <div class="error-box">
            <h3>Investigation Error</h3>
            <p>{html.escape(error)}</p>
        </div>
        """

    def _markdown_to_html(self, text):
        """Simple markdown-to-HTML conversion for the investigation narrative."""
        if not text:
            return "<p>No data</p>"

        text = html.escape(text)

        # Code blocks
        text = re.sub(
            r"```(\w*)\n(.*?)```",
            r'<pre><code class="language-\1">\2</code></pre>',
            text,
            flags=re.DOTALL,
        )

        # Inline code
        text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)

        # Bold
        text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)

        # Headers
        text = re.sub(r"^### (.+)$", r"<h4>\1</h4>", text, flags=re.MULTILINE)
        text = re.sub(r"^## (.+)$", r"<h3>\1</h3>", text, flags=re.MULTILINE)

        # Bullet lists
        text = re.sub(r"^- (.+)$", r"<li>\1</li>", text, flags=re.MULTILINE)
        text = re.sub(
            r"(<li>.*?</li>\n?)+",
            lambda m: f"<ul>{m.group(0)}</ul>",
            text,
        )

        # Paragraphs (double newlines)
        paragraphs = text.split("\n\n")
        processed = []
        for p in paragraphs:
            p = p.strip()
            if p and not p.startswith("<"):
                p = f"<p>{p}</p>"
            processed.append(p)
        text = "\n".join(processed)

        return text

    @staticmethod
    def _truncate_text(text, max_len):
        """Truncate text for display in summary table."""
        if not text or len(text) <= max_len:
            return text or ""
        return text[:max_len] + "..."

    @staticmethod
    def _css():
        """Return the CSS styles for reports."""
        return """<style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
           background: #f5f5f5; color: #333; line-height: 1.6; }
    .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
    h1 { color: #1a1a2e; margin-bottom: 20px; border-bottom: 2px solid #e94560; padding-bottom: 10px; }
    h2 { color: #16213e; margin: 20px 0 10px; }
    h3 { color: #0f3460; margin: 15px 0 8px; }
    h4 { color: #0f3460; margin: 12px 0 6px; }
    .badge { display: inline-block; padding: 3px 10px; border-radius: 12px;
             color: white; font-size: 0.85em; font-weight: 600; margin-right: 6px; }
    .phase-badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
                   background: #e9ecef; color: #495057; font-size: 0.8em; margin-left: 6px; }
    .test-header { display: flex; align-items: center; gap: 10px; margin-bottom: 10px; }
    .test-header h2 { margin: 0; }
    .meta { display: flex; gap: 20px; color: #666; font-size: 0.9em; margin-bottom: 15px;
            flex-wrap: wrap; }
    .meta code { background: #e9ecef; padding: 2px 6px; border-radius: 3px; font-size: 0.85em; }
    .root-cause { background: #fff3cd; padding: 10px 15px; border-radius: 5px;
                  border-left: 4px solid #ffc107; margin: 5px 0 15px; }
    .evidence { margin: 5px 0 15px; padding-left: 20px; }
    .evidence li { margin: 4px 0; }
    table { width: 100%; border-collapse: collapse; margin: 10px 0 20px; background: white;
            border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    th { background: #1a1a2e; color: white; padding: 10px 12px; text-align: left;
         font-size: 0.9em; }
    td { padding: 8px 12px; border-bottom: 1px solid #eee; font-size: 0.9em; }
    td code { background: #f0f0f0; padding: 1px 4px; border-radius: 3px; font-size: 0.85em;
              word-break: break-all; }
    td.phase { text-align: center; }
    tr:hover { background: #f8f9fa; }
    details { margin: 10px 0; }
    summary { cursor: pointer; padding: 8px 12px; background: #e9ecef; border-radius: 5px;
              font-weight: 600; }
    summary:hover { background: #dee2e6; }
    .test-detail { background: white; border-radius: 8px; margin: 10px 0;
                   box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }
    .test-detail > summary { background: #f8f9fa; padding: 12px 16px; border-radius: 0;
                              display: flex; align-items: center; gap: 8px; }
    .detail-body { padding: 16px; }
    .investigation { background: #f8f9fa; padding: 15px; border-radius: 5px; margin-top: 10px;
                     max-height: 600px; overflow-y: auto; font-size: 0.9em; }
    .investigation pre { background: #1a1a2e; color: #e9ecef; padding: 12px; border-radius: 5px;
                         overflow-x: auto; margin: 8px 0; }
    .investigation code { font-family: "SF Mono", "Fira Code", monospace; font-size: 0.9em; }
    .session-summary { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px;
                       box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .stats { display: flex; gap: 30px; margin: 15px 0; }
    .stat { text-align: center; }
    .stat-value { display: block; font-size: 1.8em; font-weight: 700; color: #1a1a2e; }
    .stat-label { font-size: 0.85em; color: #666; }
    .category-summary { margin-top: 10px; }
    .error-box { background: #f8d7da; border: 1px solid #f5c6cb; padding: 12px 16px;
                 border-radius: 5px; margin-top: 10px; }
    .error-box h3 { color: #721c24; margin-bottom: 5px; }
    .error-box p { color: #721c24; }
    .safety-warning { background: #fff3cd; border: 1px solid #ffc107; padding: 12px 16px;
                      border-radius: 5px; margin-bottom: 15px; }
    .safety-warning h3, .safety-warning h4 { color: #856404; margin-bottom: 5px; }
    .violation { color: #856404; }
</style>"""
