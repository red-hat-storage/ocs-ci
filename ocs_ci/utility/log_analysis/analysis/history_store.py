"""
File-based history store for tracking test run outcomes across runs.

Stores compact RunRecord objects as JSON files, one per run.
Used by PatternDetector to compute flakiness, regressions, and trends.
"""

import hashlib
import json
import logging
import os
from dataclasses import dataclass, field, asdict
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RunRecord:
    """Compact record of a single test run for historical tracking."""

    run_url: str
    timestamp: str
    platform: str = ""
    ocs_version: str = ""
    ocs_build: str = ""
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    error: int = 0
    skipped: int = 0
    # Per-test outcomes: {test_full_name: {"status": str, "category": str, "duration": float, "squad": str}}
    test_outcomes: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "RunRecord":
        return RunRecord(
            run_url=data.get("run_url", ""),
            timestamp=data.get("timestamp", ""),
            platform=data.get("platform", ""),
            ocs_version=data.get("ocs_version", ""),
            ocs_build=data.get("ocs_build", ""),
            total_tests=data.get("total_tests", 0),
            passed=data.get("passed", 0),
            failed=data.get("failed", 0),
            error=data.get("error", 0),
            skipped=data.get("skipped", 0),
            test_outcomes=data.get("test_outcomes", {}),
        )

    @staticmethod
    def from_run_analysis(run_analysis, all_test_results=None) -> "RunRecord":
        """
        Build a compact RunRecord from a RunAnalysis.

        Args:
            run_analysis: RunAnalysis object from the analysis pipeline
            all_test_results: Optional list of all TestResult objects
                (including passes). If provided, all tests are recorded
                for accurate flakiness detection.

        Returns:
            RunRecord with per-test outcomes
        """
        test_outcomes = {}

        # Build category map from failure analyses
        category_map = {}
        for fa in run_analysis.failure_analyses:
            tr = fa.test_result
            category_map[tr.full_name] = fa.category.value

        # Record all test results if available
        if all_test_results:
            for tr in all_test_results:
                test_outcomes[tr.full_name] = {
                    "status": tr.status.value,
                    "category": category_map.get(tr.full_name, ""),
                    "duration": tr.duration,
                    "squad": tr.squad or "",
                }
        else:
            # Fallback: only record failures from analyses
            for fa in run_analysis.failure_analyses:
                tr = fa.test_result
                test_outcomes[tr.full_name] = {
                    "status": tr.status.value,
                    "category": fa.category.value,
                    "duration": tr.duration,
                    "squad": tr.squad or "",
                }

        return RunRecord(
            run_url=run_analysis.run_url,
            timestamp=run_analysis.timestamp,
            platform=run_analysis.run_metadata.platform,
            ocs_version=run_analysis.run_metadata.ocs_version,
            ocs_build=run_analysis.run_metadata.ocs_build,
            total_tests=run_analysis.total_tests,
            passed=run_analysis.passed,
            failed=run_analysis.failed,
            error=run_analysis.error,
            skipped=run_analysis.skipped,
            test_outcomes=test_outcomes,
        )


class RunHistoryStore:
    """
    File-based store for historical test run records.

    Stores one JSON file per run in the history directory.
    Supports filtering by platform and OCS version.
    """

    def __init__(
        self,
        history_dir: str = "~/.ocs-ci/analysis_history",
        max_runs: int = 100,
    ):
        """
        Args:
            history_dir: Directory for history files (supports ~ expansion)
            max_runs: Maximum number of runs to keep (oldest are pruned)
        """
        self.history_dir = os.path.expanduser(history_dir)
        self.max_runs = max_runs
        os.makedirs(self.history_dir, exist_ok=True)

    def record(self, run_analysis, all_test_results=None) -> None:
        """
        Save a compact RunRecord from a RunAnalysis.

        Args:
            run_analysis: RunAnalysis object to record
            all_test_results: Optional list of all TestResult objects
        """
        record = RunRecord.from_run_analysis(run_analysis, all_test_results)
        url_hash = hashlib.sha256(record.run_url.encode()).hexdigest()[:8]
        ts_clean = record.timestamp.replace(":", "-").replace(".", "-")[:19]
        filename = f"{ts_clean}_{url_hash}.json"
        filepath = os.path.join(self.history_dir, filename)

        try:
            with open(filepath, "w") as f:
                json.dump(record.to_dict(), f, indent=2)
            logger.info(f"Recorded run history: {filepath}")
        except IOError as e:
            logger.warning(f"Failed to record run history: {e}")
            return

        self._prune_old_runs()

    def get_history(
        self,
        max_runs: Optional[int] = None,
        platform: Optional[str] = None,
        ocs_version: Optional[str] = None,
    ) -> list:
        """
        Load historical records, optionally filtered.

        Args:
            max_runs: Max records to return (None = use self.max_runs)
            platform: Filter by platform (e.g., "baremetal", "aws")
            ocs_version: Filter by OCS version (e.g., "4.21")

        Returns:
            List of RunRecord, sorted by timestamp (newest first)
        """
        limit = max_runs or self.max_runs
        records = []

        json_files = sorted(
            [f for f in os.listdir(self.history_dir) if f.endswith(".json")],
            reverse=True,
        )

        for filename in json_files:
            if len(records) >= limit:
                break

            filepath = os.path.join(self.history_dir, filename)
            try:
                with open(filepath) as f:
                    data = json.load(f)
                record = RunRecord.from_dict(data)

                # Apply filters
                if platform and record.platform.lower() != platform.lower():
                    continue
                if ocs_version and record.ocs_version != ocs_version:
                    continue

                records.append(record)
            except (json.JSONDecodeError, IOError, KeyError) as e:
                logger.debug(f"Skipping corrupt history file {filename}: {e}")

        logger.debug(f"Loaded {len(records)} historical run records")
        return records

    def count(self) -> int:
        """Return the number of stored runs."""
        return len([f for f in os.listdir(self.history_dir) if f.endswith(".json")])

    def clear(self) -> None:
        """Remove all history files."""
        count = 0
        for filename in os.listdir(self.history_dir):
            if filename.endswith(".json"):
                try:
                    os.remove(os.path.join(self.history_dir, filename))
                    count += 1
                except OSError:
                    pass
        logger.info(f"Cleared {count} history records")

    def _prune_old_runs(self):
        """Remove oldest runs if over max_runs limit."""
        json_files = sorted(
            [f for f in os.listdir(self.history_dir) if f.endswith(".json")]
        )

        while len(json_files) > self.max_runs:
            oldest = json_files.pop(0)
            try:
                os.remove(os.path.join(self.history_dir, oldest))
                logger.debug(f"Pruned old history: {oldest}")
            except OSError:
                pass
