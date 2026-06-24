"""
Run record management for z-stream agent pipeline.

Each run creates a timestamped directory under run_record/ containing:
  - <run_id>.log          — run log file
  - <run_id>_issues.json  — shared issue list; all stages read/update this file

Later stages load the same run via --run-id and append stage results per issue.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_WORKFLOW_DIR = Path(__file__).resolve().parents[1]
if str(_WORKFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_DIR))

from workflow_lib.claude_session import promote_session_from_stage_data

log = logging.getLogger(__name__)

RUN_RECORD_DIR = Path(__file__).resolve().parent / "run_record"
STAGE_JIRA_INTAKE = "jira_intake"
STAGE_REPRO_STEPS = "repro_steps"
STAGE_LIVE_CLUSTER_VERIFICATION = "live_cluster_verification"
STAGE_TEST_MATCHING = "test_matching"
STAGE_OCS_CI_EXECUTION = "ocs_ci_execution"


def normalize_odf_version(odf_version: str) -> str:
    """Normalize ODF version to JIRA target-version format (e.g. odf-4.21.7)."""
    version = odf_version.strip()
    if not version.startswith("odf-"):
        version = f"odf-{version}"
    return version


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_run_id() -> str:
    """Return a unique run id based on current timestamp."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _find_run_dir(run_id: str) -> Path:
    """Resolve run directory by run_id prefix."""
    if not RUN_RECORD_DIR.exists():
        raise FileNotFoundError(f"Run record directory not found: {RUN_RECORD_DIR}")

    matches = sorted(RUN_RECORD_DIR.glob(f"{run_id}*"))
    if not matches:
        raise FileNotFoundError(f"No run record found for run_id: {run_id}")

    run_dir = matches[0]
    if not run_dir.is_dir():
        raise FileNotFoundError(f"Run record path is not a directory: {run_dir}")
    return run_dir


class RunRecord:
    """Timestamped run record with log file and shared issues JSON."""

    def __init__(self, run_dir: Path, run_id: str):
        self.run_dir = run_dir
        self.run_id = run_id
        self.log_file = run_dir / f"{run_id}.log"
        self.issues_file = run_dir / f"{run_id}_issues.json"
        self._data: dict[str, Any] = {}

    @classmethod
    def create(cls, odf_version: str) -> "RunRecord":
        """
        Create a new run record directory and empty issues file scaffold.

        Args:
            odf_version (str): ODF z-stream version for this run

        Returns:
            RunRecord: New run record instance

        """
        run_id = generate_run_id()
        target_version = normalize_odf_version(odf_version)
        run_dir = RUN_RECORD_DIR / f"{run_id}_{target_version}"
        run_dir.mkdir(parents=True, exist_ok=True)

        record = cls(run_dir=run_dir, run_id=run_id)
        record._data = {
            "run_id": run_id,
            "odf_version": target_version,
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "jql": None,
            "stages_completed": [],
            "issues": [],
        }
        record.save()
        log.info("Created run record: %s", record.run_dir)
        return record

    @classmethod
    def load(cls, run_id: str) -> "RunRecord":
        """
        Load an existing run record for a follow-on pipeline stage.

        Args:
            run_id (str): Run id or prefix (e.g. 20250614_143022)

        Returns:
            RunRecord: Loaded run record

        """
        run_dir = _find_run_dir(run_id)
        issues_files = sorted(run_dir.glob("*_issues.json"))
        if not issues_files:
            raise FileNotFoundError(f"Issues file not found in {run_dir}")

        issues_file = issues_files[0]
        with issues_file.open(encoding="utf-8") as handle:
            data = json.load(handle)

        loaded_run_id = data.get("run_id") or issues_file.stem.replace("_issues", "")
        record = cls(run_dir=run_dir, run_id=loaded_run_id)
        record.issues_file = issues_file
        record.log_file = run_dir / f"{loaded_run_id}.log"
        record._data = data
        log.info("Loaded run record: %s", record.run_dir)
        return record

    def setup_file_logging(self) -> None:
        """Attach a file handler so all logs for this run go to the run log file."""
        root = logging.getLogger()
        if any(
            isinstance(handler, logging.FileHandler)
            and getattr(handler, "baseFilename", "") == str(self.log_file)
            for handler in root.handlers
        ):
            return

        file_handler = logging.FileHandler(self.log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        root.addHandler(file_handler)
        log.info("Run log file: %s", self.log_file)

    def get_issues(self) -> list[dict[str, Any]]:
        """Return the current issue list from the run record."""
        return list(self._data.get("issues", []))

    def get_issue(self, issue_key: str) -> dict[str, Any] | None:
        """Return a single issue by JIRA key."""
        for issue in self._data.get("issues", []):
            if issue.get("key") == issue_key:
                return issue
        return None

    def init_jira_intake(
        self,
        issues: list[dict[str, Any]],
        jql: str,
        odf_version: str,
    ) -> None:
        """
        Initialize the shared issues file after JIRA intake (stage 1).

        Args:
            issues (list): Parsed JIRA issue details
            jql (str): JQL used for the fetch
            odf_version (str): ODF z-stream version

        """
        now = _utc_now()
        self._data["odf_version"] = normalize_odf_version(odf_version)
        self._data["jql"] = jql
        self._data["updated_at"] = now
        self._data["issues"] = []

        for issue in issues:
            entry = dict(issue)
            entry["stages"] = {
                STAGE_JIRA_INTAKE: {
                    "status": "completed",
                    "completed_at": now,
                }
            }
            self._data["issues"].append(entry)

        if STAGE_JIRA_INTAKE not in self._data["stages_completed"]:
            self._data["stages_completed"].append(STAGE_JIRA_INTAKE)

        self.save()
        log.info("Initialized %d issues in run record", len(self._data["issues"]))

    def append_stage(
        self,
        stage_name: str,
        issue_key: str,
        stage_data: dict[str, Any],
        *,
        status: str = "completed",
    ) -> None:
        """
        Append or update stage results for a single issue.

        Args:
            stage_name (str): Pipeline stage name
            issue_key (str): JIRA issue key
            stage_data (dict): Stage-specific data to store
            status (str): Stage status (completed, failed, skipped)

        """
        issue = self.get_issue(issue_key)
        if issue is None:
            raise KeyError(f"Issue {issue_key} not found in run record {self.run_id}")

        stages = issue.setdefault("stages", {})
        stages[stage_name] = {
            "status": status,
            "completed_at": _utc_now(),
            "data": stage_data,
        }
        promote_session_from_stage_data(issue, stage_data)
        self._data["updated_at"] = _utc_now()

        if status == "completed" and stage_name not in self._data.setdefault(
            "stages_completed", []
        ):
            self._data["stages_completed"].append(stage_name)

        self.save()

    def append_stage_bulk(
        self,
        stage_name: str,
        per_issue: dict[str, dict[str, Any]],
        *,
        status: str = "completed",
    ) -> None:
        """
        Append stage results for multiple issues at once.

        Args:
            stage_name (str): Pipeline stage name
            per_issue (dict): issue_key -> stage data
            status (str): Stage status for all issues in this batch

        """
        now = _utc_now()
        for issue_key, stage_data in per_issue.items():
            issue = self.get_issue(issue_key)
            if issue is None:
                log.warning(
                    "Skipping unknown issue %s in stage %s", issue_key, stage_name
                )
                continue
            issue.setdefault("stages", {})[stage_name] = {
                "status": status,
                "completed_at": now,
                "data": stage_data,
            }
            promote_session_from_stage_data(issue, stage_data)

        self._data["updated_at"] = now
        stages_completed = self._data.setdefault("stages_completed", [])
        if status == "completed" and stage_name not in stages_completed:
            stages_completed.append(stage_name)
        self.save()
        log.info(
            "Stage '%s' appended for %d issues in run %s",
            stage_name,
            len(per_issue),
            self.run_id,
        )

    def mark_stage_completed(self, stage_name: str) -> None:
        """Mark a pipeline stage complete even when all per-issue entries were skipped."""
        stages_completed = self._data.setdefault("stages_completed", [])
        if stage_name not in stages_completed:
            stages_completed.append(stage_name)
            self._data["updated_at"] = _utc_now()
            self.save()
            log.info("Marked stage '%s' completed for run %s", stage_name, self.run_id)

    def save(self) -> None:
        """Persist the issues JSON file."""
        self._data["updated_at"] = _utc_now()
        with self.issues_file.open("w", encoding="utf-8") as handle:
            json.dump(self._data, handle, indent=2)
        log.debug("Saved run record: %s", self.issues_file)

    def to_summary(self) -> dict[str, Any]:
        """Return a compact summary of the run record."""
        return {
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "log_file": str(self.log_file),
            "issues_file": str(self.issues_file),
            "odf_version": self._data.get("odf_version"),
            "issue_count": len(self._data.get("issues", [])),
            "stages_completed": self._data.get("stages_completed", []),
        }
