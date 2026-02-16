"""
Cross-run pattern detection engine.

Analyzes historical run records to detect:
- Flaky tests (intermittent pass/fail)
- Regressions (tests that recently started failing)
- Trends (pass rate, category distribution over time)
"""

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict


logger = logging.getLogger(__name__)


@dataclass
class TestFlakiness:
    """Flakiness analysis for a single test across runs."""

    test_name: str
    squad: str = ""
    total_runs: int = 0
    pass_count: int = 0
    fail_count: int = 0
    error_count: int = 0
    skip_count: int = 0
    flakiness_rate: float = 0.0
    recent_results: list = field(default_factory=list)
    first_seen_failing: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Regression:
    """A test that recently started failing after previously passing."""

    test_name: str
    squad: str = ""
    first_failure_timestamp: str = ""
    first_failure_run_url: str = ""
    consecutive_failures: int = 0
    previous_pass_count: int = 0
    category: str = ""
    ocs_version_changed: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TrendReport:
    """Cross-run trend analysis report."""

    period: str = ""
    runs_analyzed: int = 0
    pass_rate_trend: list = field(default_factory=list)
    category_trend: dict = field(default_factory=dict)
    top_flaky_tests: list = field(default_factory=list)
    regressions: list = field(default_factory=list)
    most_failing_tests: list = field(default_factory=list)
    squad_health: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "period": self.period,
            "runs_analyzed": self.runs_analyzed,
            "pass_rate_trend": self.pass_rate_trend,
            "category_trend": self.category_trend,
            "top_flaky_tests": [t.to_dict() for t in self.top_flaky_tests],
            "regressions": [r.to_dict() for r in self.regressions],
            "most_failing_tests": [t.to_dict() for t in self.most_failing_tests],
            "squad_health": self.squad_health,
        }


class PatternDetector:
    """
    Analyze historical run records to detect cross-run patterns.

    Usage:
        history = store.get_history()
        detector = PatternDetector(history)
        flaky = detector.detect_flaky_tests()
        regressions = detector.detect_regressions()
        report = detector.build_trend_report()
    """

    def __init__(self, history: list):
        """
        Args:
            history: List of RunRecord objects, sorted newest-first
        """
        self.history = history
        # Build per-test timeline: {test_name: [(timestamp, status, category, run_url, squad), ...]}
        self._test_timeline = self._build_test_timeline()

    def _build_test_timeline(self) -> dict:
        """Build per-test timeline from history (oldest to newest)."""
        timeline = defaultdict(list)

        # Iterate oldest-first for chronological order
        for record in reversed(self.history):
            for test_name, outcome in record.test_outcomes.items():
                timeline[test_name].append(
                    {
                        "timestamp": record.timestamp,
                        "status": outcome.get("status", "unknown"),
                        "category": outcome.get("category", ""),
                        "run_url": record.run_url,
                        "squad": outcome.get("squad", ""),
                        "ocs_version": record.ocs_version,
                    }
                )

        return dict(timeline)

    def detect_flaky_tests(
        self,
        min_runs: int = 3,
        flakiness_threshold: float = 0.1,
    ) -> list:
        """
        Find tests that intermittently pass and fail across runs.

        A test is flaky if it has both passes and failures across runs,
        with a flakiness rate above the threshold.

        Args:
            min_runs: Minimum runs a test must appear in to be considered
            flakiness_threshold: Minimum flakiness rate to report (0.0-1.0)

        Returns:
            List of TestFlakiness, sorted by flakiness_rate descending
        """
        flaky_tests = []

        for test_name, entries in self._test_timeline.items():
            if len(entries) < min_runs:
                continue

            statuses = [e["status"] for e in entries]
            pass_count = statuses.count("passed")
            fail_count = statuses.count("failed")
            error_count = statuses.count("error")
            skip_count = statuses.count("skipped")

            # Flakiness requires both passes and failures
            total_decisive = pass_count + fail_count + error_count
            if (
                total_decisive == 0
                or pass_count == 0
                or (fail_count + error_count) == 0
            ):
                continue

            flakiness_rate = (fail_count + error_count) / total_decisive

            if flakiness_rate < flakiness_threshold:
                continue
            # Pure failures (100% fail rate) are not flaky — they're broken
            if flakiness_rate >= 1.0:
                continue

            # Get first failure timestamp
            first_failure = ""
            for e in entries:
                if e["status"] in ("failed", "error"):
                    first_failure = e["timestamp"]
                    break

            # Recent results (last 10)
            recent = [e["status"] for e in entries[-10:]]

            squad = entries[-1].get("squad", "") if entries else ""

            flaky_tests.append(
                TestFlakiness(
                    test_name=test_name,
                    squad=squad,
                    total_runs=len(entries),
                    pass_count=pass_count,
                    fail_count=fail_count,
                    error_count=error_count,
                    skip_count=skip_count,
                    flakiness_rate=round(flakiness_rate, 3),
                    recent_results=recent,
                    first_seen_failing=first_failure,
                )
            )

        flaky_tests.sort(key=lambda t: t.flakiness_rate, reverse=True)
        logger.info(f"Detected {len(flaky_tests)} flaky tests")
        return flaky_tests

    def detect_regressions(self, min_consecutive_failures: int = 2) -> list:
        """
        Find tests that recently started failing after passing consistently.

        A regression is defined as: a test that has N+ consecutive failures
        at the end of the timeline, preceded by at least one pass.

        Args:
            min_consecutive_failures: Minimum consecutive failures to flag

        Returns:
            List of Regression, sorted by consecutive_failures descending
        """
        regressions = []

        for test_name, entries in self._test_timeline.items():
            if len(entries) < 2:
                continue

            # Count consecutive failures from the end
            consecutive_failures = 0
            for e in reversed(entries):
                if e["status"] in ("failed", "error"):
                    consecutive_failures += 1
                else:
                    break

            if consecutive_failures < min_consecutive_failures:
                continue

            # Check that there were passes before the failures
            entries_before = entries[: len(entries) - consecutive_failures]
            previous_passes = sum(1 for e in entries_before if e["status"] == "passed")
            if previous_passes == 0:
                continue

            # Find the transition point
            failure_start_idx = len(entries) - consecutive_failures
            first_failure = entries[failure_start_idx]

            # Check if OCS version changed at the transition point
            version_changed = False
            if failure_start_idx > 0:
                prev_version = entries[failure_start_idx - 1].get("ocs_version", "")
                fail_version = first_failure.get("ocs_version", "")
                version_changed = (
                    prev_version != fail_version and prev_version and fail_version
                )

            # Most common failure category
            fail_categories = [
                e["category"] for e in entries[failure_start_idx:] if e.get("category")
            ]
            category = (
                Counter(fail_categories).most_common(1)[0][0] if fail_categories else ""
            )

            squad = entries[-1].get("squad", "")

            regressions.append(
                Regression(
                    test_name=test_name,
                    squad=squad,
                    first_failure_timestamp=first_failure["timestamp"],
                    first_failure_run_url=first_failure["run_url"],
                    consecutive_failures=consecutive_failures,
                    previous_pass_count=previous_passes,
                    category=category,
                    ocs_version_changed=version_changed,
                )
            )

        regressions.sort(key=lambda r: r.consecutive_failures, reverse=True)
        logger.info(f"Detected {len(regressions)} regressions")
        return regressions

    def build_trend_report(self) -> TrendReport:
        """
        Compute full trend analysis across all runs in history.

        Returns:
            TrendReport with pass rate trends, flaky tests, regressions, etc.
        """
        if not self.history:
            return TrendReport(period="no data", runs_analyzed=0)

        # Period description
        timestamps = [r.timestamp for r in self.history]
        period = f"{timestamps[-1][:10]} to {timestamps[0][:10]}"

        # Pass rate trend (newest first in self.history, reverse for chronological)
        pass_rate_trend = []
        for record in reversed(self.history):
            total_decisive = record.passed + record.failed + record.error
            rate = record.passed / total_decisive if total_decisive > 0 else 0.0
            pass_rate_trend.append(
                {
                    "timestamp": record.timestamp[:10],
                    "pass_rate": round(rate, 3),
                    "total": record.total_tests,
                    "failed": record.failed + record.error,
                }
            )

        # Category distribution trend
        category_trend = defaultdict(list)
        for record in reversed(self.history):
            cat_counts = Counter()
            for outcome in record.test_outcomes.values():
                cat = outcome.get("category", "")
                if cat:
                    cat_counts[cat] += 1
            for cat, count in cat_counts.items():
                category_trend[cat].append(
                    {"timestamp": record.timestamp[:10], "count": count}
                )

        # Detect patterns
        flaky_tests = self.detect_flaky_tests()
        regressions_list = self.detect_regressions()

        # Most failing tests (by total failure count)
        fail_counts = Counter()
        for test_name, entries in self._test_timeline.items():
            fails = sum(1 for e in entries if e["status"] in ("failed", "error"))
            if fails > 0:
                fail_counts[test_name] = fails

        most_failing = []
        for test_name, count in fail_counts.most_common(20):
            entries = self._test_timeline[test_name]
            total = len(entries)
            squad = entries[-1].get("squad", "") if entries else ""
            most_failing.append(
                TestFlakiness(
                    test_name=test_name,
                    squad=squad,
                    total_runs=total,
                    fail_count=count,
                    pass_count=sum(1 for e in entries if e["status"] == "passed"),
                    error_count=sum(1 for e in entries if e["status"] == "error"),
                    flakiness_rate=round(count / total, 3) if total > 0 else 0,
                )
            )

        # Squad health
        squad_stats = defaultdict(
            lambda: {
                "total_tests": 0,
                "pass_count": 0,
                "fail_count": 0,
                "flaky_count": 0,
            }
        )
        for test_name, entries in self._test_timeline.items():
            squad = entries[-1].get("squad", "Unknown") if entries else "Unknown"
            passes = sum(1 for e in entries if e["status"] == "passed")
            fails = sum(1 for e in entries if e["status"] in ("failed", "error"))
            squad_stats[squad]["total_tests"] += 1
            squad_stats[squad]["pass_count"] += passes
            squad_stats[squad]["fail_count"] += fails

        flaky_by_squad = Counter(t.squad or "Unknown" for t in flaky_tests)
        for squad in squad_stats:
            stats = squad_stats[squad]
            total = stats["pass_count"] + stats["fail_count"]
            stats["pass_rate"] = (
                round(stats["pass_count"] / total, 3) if total > 0 else 0
            )
            stats["flaky_count"] = flaky_by_squad.get(squad, 0)

        return TrendReport(
            period=period,
            runs_analyzed=len(self.history),
            pass_rate_trend=pass_rate_trend,
            category_trend=dict(category_trend),
            top_flaky_tests=flaky_tests[:20],
            regressions=regressions_list,
            most_failing_tests=most_failing,
            squad_health=dict(squad_stats),
        )

    def annotate_run(self, run_analysis) -> None:
        """
        Enrich a single-run analysis with cross-run context.

        Adds flakiness_info and regression_info to each FailureAnalysis
        via the evidence and recommended_action fields.

        Args:
            run_analysis: RunAnalysis object to annotate (modified in place)
        """
        if not self.history:
            return

        flaky_map = {t.test_name: t for t in self.detect_flaky_tests(min_runs=2)}
        regression_map = {r.test_name: r for r in self.detect_regressions()}

        for fa in run_analysis.failure_analyses:
            full_name = fa.test_result.full_name

            flaky = flaky_map.get(full_name)
            if flaky:
                fa.evidence.append(
                    f"Flaky test: {flaky.flakiness_rate:.0%} failure rate "
                    f"across {flaky.total_runs} runs "
                    f"(recent: {' '.join(r[0].upper() for r in flaky.recent_results[-5:])})"
                )

            regression = regression_map.get(full_name)
            if regression:
                version_note = ""
                if regression.ocs_version_changed:
                    version_note = " (OCS version changed at regression point)"
                fa.evidence.append(
                    f"Regression: failing for {regression.consecutive_failures} "
                    f"consecutive runs since {regression.first_failure_timestamp[:10]}, "
                    f"previously passed {regression.previous_pass_count} times"
                    f"{version_note}"
                )
