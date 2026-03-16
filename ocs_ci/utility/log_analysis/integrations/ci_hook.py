"""
Pytest plugin for automated post-session log analysis.

When enabled via LOG_ANALYSIS.ci_post_hook_enabled, this plugin runs
AI-powered failure analysis after all tests complete and saves reports
to the test run's log directory.

Registration: Conditionally registered in ocs_ci/framework/main.py.
"""

import logging
import os

log = logging.getLogger(__name__)


def pytest_sessionfinish(session, exitstatus):
    """
    Run AI-powered log analysis after all tests complete.

    Generates analysis reports (JSON, Markdown) in the log directory.
    This hook runs after the existing reports.py sessionfinish hook.
    """
    try:
        from ocs_ci.framework import config as ocsci_config
    except ImportError:
        log.warning("Could not import ocs_ci framework config, skipping log analysis")
        return

    la_config = ocsci_config.LOG_ANALYSIS
    if not la_config.get("ci_post_hook_enabled", False):
        return

    # Only run if there were test failures
    if exitstatus == 0:
        log.info("All tests passed, skipping AI log analysis")
        return

    log_dir = ocsci_config.RUN.get("log_dir", "")
    if not log_dir:
        log.warning("No log_dir configured, skipping AI log analysis")
        return

    log_dir = os.path.expanduser(log_dir)

    # Find the JUnit XML in the log directory
    junit_xml = _find_junit_xml(log_dir)
    if not junit_xml:
        log.warning(f"No JUnit XML found in {log_dir}, skipping AI log analysis")
        return

    log.info("Running post-session AI log analysis...")

    try:
        from ocs_ci.utility.log_analysis import analyze_run
        from ocs_ci.utility.log_analysis.reporting.report_builder import ReportBuilder

        # Run analysis using the local log directory
        run_analysis = analyze_run(
            source=log_dir,
            ai_backend=la_config.get("ai_backend", "claude-code"),
            known_issues_only=la_config.get("ai_backend") == "none",
            model=la_config.get("model", "sonnet"),
            max_budget_usd=la_config.get("max_budget_usd", 0.50),
            max_failures=la_config.get("max_failures_to_analyze", 30),
            cache_dir=la_config.get("cache_dir", "~/.ocs-ci/analysis_cache"),
            cache_enabled=la_config.get("cache_enabled", True),
            no_jira=not la_config.get("jira_search_enabled", True),
            jira_projects=la_config.get("jira_projects"),
            record_history=True,
            history_dir=la_config.get("history_dir", "~/.ocs-ci/analysis_history"),
        )

        # Generate reports
        builder = ReportBuilder()
        report_format = la_config.get("ci_report_format", "all")

        if report_format in ("json", "both", "all"):
            json_path = os.path.join(log_dir, "ai_analysis_report.json")
            json_report = builder.build(run_analysis, fmt="json")
            with open(json_path, "w") as f:
                f.write(json_report)
            log.info(f"AI analysis JSON report: {json_path}")

        if report_format in ("markdown", "both", "all"):
            md_path = os.path.join(log_dir, "ai_analysis_report.md")
            md_report = builder.build(run_analysis, fmt="markdown")
            with open(md_path, "w") as f:
                f.write(md_report)
            log.info(f"AI analysis Markdown report: {md_path}")

        if report_format in ("html", "all"):
            html_path = os.path.join(log_dir, "ai_analysis_report.html")
            html_report = builder.build(run_analysis, fmt="html")
            with open(html_path, "w") as f:
                f.write(html_report)
            log.info(f"AI analysis HTML report: {html_path}")

        # Log summary stats
        classified = sum(
            1 for fa in run_analysis.failure_analyses if fa.category.value != "unknown"
        )
        total = len(run_analysis.failure_analyses)
        log.info(f"AI log analysis complete: {classified}/{total} failures classified")

    except Exception as e:
        # Never fail the test run because of analysis errors
        log.warning(f"Post-session AI log analysis failed (non-fatal): {e}")


def _find_junit_xml(log_dir: str) -> str:
    """Find JUnit XML file in the log directory."""
    if not os.path.isdir(log_dir):
        return ""

    for filename in os.listdir(log_dir):
        if filename.startswith("test_results") and filename.endswith(".xml"):
            return os.path.join(log_dir, filename)

    # Also check for junit.xml
    junit_path = os.path.join(log_dir, "junit.xml")
    if os.path.exists(junit_path):
        return junit_path

    return ""
