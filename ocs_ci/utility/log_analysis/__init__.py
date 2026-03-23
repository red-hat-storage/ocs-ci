"""
AI-powered log analysis module for OCS-CI.

Provides tools to analyze test run logs, classify failures,
detect patterns, and correlate with Jira issues.
"""

import logging
from datetime import datetime

from ocs_ci.utility.log_analysis.models import (
    RunAnalysis,
    RunMetadata,
    TestStatus,
)

logger = logging.getLogger(__name__)


def analyze_run(source, ai_backend="claude-code", known_issues_only=False, **kwargs):
    """
    Analyze a test run from a log directory URL or local path.

    Args:
        source (str): URL or local path to log directory
        ai_backend (str): AI backend to use ("claude-code", "anthropic", "none")
        known_issues_only (bool): Only run regex-based matching, skip AI
        **kwargs: Additional options passed to backends/classifiers:
            model (str): AI model to use (default: "sonnet")
            max_budget_usd (float): Max spend per AI call (default: 0.50)
            cache_dir (str): Cache directory (default: "~/.ocs-ci/analysis_cache")
            cache_enabled (bool): Enable caching (default: True)
            known_issues_file (str): Extra known issues YAML file
            max_failures (int): Max unique failures for AI analysis (default: 30)
            no_jira (bool): Skip Jira integration (default: False)
            jira_projects (list): Jira project keys to search (default: DFBUGS, RHSTOR, OCSQE)
            junit_xml (str): Path to specific JUnit XML file (overrides auto-discovery)

    Returns:
        RunAnalysis: Complete analysis of the test run
    """
    from ocs_ci.utility.log_analysis.parsers.artifact_fetcher import ArtifactFetcher
    from ocs_ci.utility.log_analysis.parsers.junit_parser import JUnitResultParser
    from ocs_ci.utility.log_analysis.parsers.config_parser import RunConfigParser

    # If known_issues_only, force no AI
    if known_issues_only:
        ai_backend = "none"

    fetcher = ArtifactFetcher(source)
    manifest = fetcher.discover()

    # Parse run metadata
    run_metadata = RunMetadata(logs_url=source)
    if manifest.config_yaml:
        try:
            config_content = fetcher.fetch_text(manifest.config_yaml)
            run_metadata = RunConfigParser.parse(config_content, source)
        except Exception as e:
            logger.warning(f"Failed to parse config YAML: {e}")

    # Override JUnit XML if explicitly specified
    junit_xml_override = kwargs.get("junit_xml")
    if junit_xml_override:
        manifest.junit_xml = junit_xml_override

    # Parse JUnit XML
    if not manifest.junit_xml:
        raise ValueError(f"No JUnit XML found at {source}")

    xml_content = fetcher.fetch_text(manifest.junit_xml)
    parser = JUnitResultParser()
    test_results = parser.parse_from_string(xml_content)

    # Enrich metadata from JUnit suite properties (they have fuller versions)
    sp = parser.suite_properties
    if sp:
        # Prefer full nightly OCP version from JUnit over short YAML version
        rp_ocp = sp.get("rp_ocp_version", "")
        if rp_ocp and len(rp_ocp) > len(run_metadata.ocp_version):
            run_metadata.ocp_version = rp_ocp
        # Prefer OCS build (e.g. "4.21.0-110") over bare version
        rp_build = sp.get("rp_ocs_build", "")
        if rp_build:
            run_metadata.ocs_build = rp_build
        # Use registry tag for OCS version if it's richer (e.g. "4.21.0-110.konflux")
        rp_tag = sp.get("rp_ocs_registry_tag", "")
        if rp_tag and len(rp_tag) > len(run_metadata.ocs_version):
            run_metadata.ocs_version = rp_tag
        # Prefer rp_launch_name — it includes the test tier (e.g. tier1)
        # while REPORTING.display_name only has the deployment flavour
        rp_launch = sp.get("rp_launch_name", "")
        if rp_launch:
            run_metadata.launch_name = rp_launch
        # Use logs-url from JUnit for the run URL (instead of local path)
        logs_url = sp.get("logs-url", "")
        if logs_url:
            run_metadata.logs_url = logs_url

    # Use logs-url from JUnit XML if source is a local path
    effective_url = source
    if not source.startswith("http") and run_metadata.logs_url.startswith("http"):
        effective_url = run_metadata.logs_url

    # Build run analysis
    run_analysis = RunAnalysis(
        run_url=effective_url,
        run_metadata=run_metadata,
        total_tests=len(test_results),
        passed=sum(1 for t in test_results if t.status == TestStatus.PASSED),
        failed=sum(1 for t in test_results if t.status == TestStatus.FAILED),
        skipped=sum(1 for t in test_results if t.status == TestStatus.SKIPPED),
        error=sum(1 for t in test_results if t.status == TestStatus.ERROR),
        timestamp=parser.suite_timestamp or datetime.utcnow().isoformat(),
    )

    # Filter to failures only
    failures = [
        t for t in test_results if t.status in (TestStatus.FAILED, TestStatus.ERROR)
    ]

    if not failures:
        run_analysis.summary = "All tests passed or were skipped."
        return run_analysis

    test_filter = kwargs.get("test_filter")
    if test_filter:
        filtered = [
            f
            for f in failures
            if any(sub.lower() in f.name.lower() for sub in test_filter)
        ]
        logger.info(
            f"Filtered by test name ({', '.join(test_filter)}): "
            f"{len(filtered)} of {len(failures)} failures"
        )
        failures = filtered

    squad_filter = kwargs.get("squad")
    if squad_filter:
        # Normalize: "brown_squad" -> "brown", "Brown" -> "brown"
        squad_name = squad_filter.replace("_squad", "").lower()
        failures = [f for f in failures if f.squad and f.squad.lower() == squad_name]
        logger.info(f"Filtered to squad '{squad_filter}': {len(failures)} failures")

    limit = kwargs.get("limit")
    if limit is not None:
        logger.info(f"Limiting analysis to {limit} of {len(failures)} failures")
        failures = failures[:limit]

    # Use the FailureClassifier for the full pipeline
    from ocs_ci.utility.log_analysis.ai.base import get_backend
    from ocs_ci.utility.log_analysis.analysis.failure_classifier import (
        FailureClassifier,
    )
    from ocs_ci.utility.log_analysis.analysis.known_issues import KnownIssuesMatcher
    from ocs_ci.utility.log_analysis.cache import AnalysisCache

    # Initialize AI backend
    backend_kwargs = {}
    if "model" in kwargs:
        backend_kwargs["model"] = kwargs["model"]
    if "max_budget_usd" in kwargs:
        backend_kwargs["max_budget_usd"] = kwargs["max_budget_usd"]
    if kwargs.get("save_prompts"):
        run_id = run_metadata.run_id or "unknown"
        backend_kwargs["save_prompts_dir"] = f"~/.ocs-ci/prompts/{run_id}"

    backend = get_backend(ai_backend, **backend_kwargs)

    # Check AI backend availability
    if ai_backend != "none" and not backend.is_available():
        logger.warning(
            f"AI backend '{ai_backend}' is not available. "
            f"Falling back to regex-only analysis."
        )
        backend = get_backend("none")

    # Initialize known issues matcher
    known_issues_file = kwargs.get("known_issues_file")
    matcher = KnownIssuesMatcher(extra_patterns_file=known_issues_file)

    # Initialize cache
    cache = None
    if kwargs.get("cache_enabled", True) and ai_backend != "none":
        cache_dir = kwargs.get("cache_dir", "~/.ocs-ci/analysis_cache")
        cache_ttl = kwargs.get("cache_ttl", 720)
        cache = AnalysisCache(cache_dir=cache_dir, ttl_hours=cache_ttl)

    # Run classification pipeline
    classifier = FailureClassifier(
        ai_backend=backend,
        known_issues_matcher=matcher,
        cache=cache,
        skip_ai_for_known=True,
        max_failures=kwargs.get("max_failures", 30),
        failed_logs_dir=manifest.failed_logs_dir if ai_backend != "none" else None,
        test_logs_dir=manifest.test_logs_dir if ai_backend != "none" else None,
        ui_logs_dir=manifest.ui_logs_dir if ai_backend != "none" else None,
        run_id=run_metadata.run_id,
        sessions_dir=kwargs.get("sessions_dir"),
        sessions_url=kwargs.get("sessions_url"),
        run_metadata=run_metadata.to_dict() if ai_backend != "none" else None,
        bug_details_dir=kwargs.get("bug_details_dir"),
        ocs_ci_repo=kwargs.get("ocs_ci_repo"),
    )

    failure_analyses = classifier.classify_failures(
        failures=failures,
        fetcher=fetcher if ai_backend != "none" else None,
    )

    run_analysis.failure_analyses = failure_analyses

    # Jira integration: search for matching bugs and enrich analyses
    if not kwargs.get("no_jira", False) and failure_analyses:
        try:
            from ocs_ci.utility.log_analysis.integrations.jira_search import (
                JiraSearchIntegration,
            )

            jira_integration = JiraSearchIntegration(
                projects=kwargs.get("jira_projects"),
            )
            jira_integration.enrich_analyses(failure_analyses)
        except Exception as e:
            logger.debug(f"Jira integration failed (non-fatal): {e}")

    # Generate AI-powered run summary if using an AI backend
    no_summary = kwargs.get("no_summary", False)
    if ai_backend != "none" and failure_analyses and not no_summary:
        try:
            failure_summaries = [
                {
                    "test_name": fa.test_result.name,
                    "squad": fa.test_result.squad or "Unknown",
                    "category": fa.category.value,
                    "root_cause_summary": fa.root_cause_summary,
                }
                for fa in failure_analyses
            ]
            metadata_dict = run_metadata.to_dict()
            metadata_dict.update(
                {
                    "total_tests": run_analysis.total_tests,
                    "passed": run_analysis.passed,
                    "failed": run_analysis.failed,
                    "error": run_analysis.error,
                    "skipped": run_analysis.skipped,
                }
            )
            run_analysis.summary = backend.generate_run_summary(
                run_metadata=metadata_dict,
                failure_summaries=failure_summaries,
            )
        except Exception as e:
            logger.warning(f"Failed to generate AI run summary: {e}")

    # Fallback summary if AI didn't produce one
    if not run_analysis.summary:
        category_counts = {}
        for fa in failure_analyses:
            cat = fa.category.value
            category_counts[cat] = category_counts.get(cat, 0) + 1

        parts = []
        for cat, count in sorted(category_counts.items()):
            parts.append(f"{count} {cat}")
        breakdown = ", ".join(parts)

        run_analysis.summary = (
            f"{len(failures)} failures ({breakdown}). "
            f"{run_analysis.passed} passed, {run_analysis.skipped} skipped."
        )

    # Cross-run history: record and annotate
    record_history = kwargs.get("record_history", False)
    history_dir = kwargs.get("history_dir", "~/.ocs-ci/analysis_history")

    if record_history:
        try:
            from ocs_ci.utility.log_analysis.analysis.history_store import (
                RunHistoryStore,
            )
            from ocs_ci.utility.log_analysis.analysis.pattern_detector import (
                PatternDetector,
            )

            store = RunHistoryStore(history_dir=history_dir)
            store.record(run_analysis, all_test_results=test_results)
            logger.info(f"Run recorded to history ({store.count()} runs stored)")

            # Annotate with cross-run context if enough history exists
            history = store.get_history()
            if len(history) >= 2:
                detector = PatternDetector(history)
                detector.annotate_run(run_analysis)
        except Exception as e:
            logger.warning(f"History recording/annotation failed (non-fatal): {e}")

    return run_analysis
