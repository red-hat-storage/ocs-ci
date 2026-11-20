"""Issue verification RunContext wrapping RunRecord."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_ISSUE_VERIFICATION_DIR = Path(__file__).resolve().parent
if str(_ISSUE_VERIFICATION_DIR) not in sys.path:
    sys.path.insert(0, str(_ISSUE_VERIFICATION_DIR))

from run_record import RunRecord, normalize_odf_version


class IssueVerificationRunContext:
    """RunContext implementation for issue verification workflows."""

    create_run_stage = "jira_intake"

    def __init__(self, run_record: RunRecord):
        self.run_record = run_record

    @property
    def run_id(self) -> str:
        return self.run_record.run_id

    @property
    def run_dir(self) -> Path:
        return self.run_record.run_dir

    @property
    def issues_file(self) -> Path:
        return self.run_record.issues_file

    def stages_completed(self) -> list[str]:
        return list(self.run_record._data.get("stages_completed", []))

    def setup_logging(self) -> None:
        self.run_record.setup_file_logging()

    def to_ref_dict(self, parameters: dict[str, Any]) -> dict[str, Any]:
        return {
            "run_id": self.run_record.run_id,
            "run_dir": str(self.run_record.run_dir),
            "issues_file": str(self.run_record.issues_file),
            "odf_version": self.run_record._data.get("odf_version"),
            "odf_version_norm": normalize_odf_version(
                parameters.get("odf_version", "")
            ),
        }


class IssueVerificationContextFactory:
    """Create and load issue verification run contexts."""

    create_run_stage = "jira_intake"

    def load(self, run_id: str) -> IssueVerificationRunContext:
        return IssueVerificationRunContext(RunRecord.load(run_id))

    def create(self, parameters: dict[str, Any]) -> IssueVerificationRunContext:
        odf_version = parameters.get("odf_version")
        if not odf_version:
            raise ValueError("odf_version is required to create a new run record")
        return IssueVerificationRunContext(RunRecord.create(odf_version))


def seed_issue_verification_stage_outputs(
    context: IssueVerificationRunContext,
    pipeline: dict[str, Any],
    stage_outputs: dict[str, dict[str, Any]],
    get_record_stage: Any,
) -> None:
    """Populate stage_outputs for stages already completed in the run record."""
    run_record = context.run_record
    completed = set(run_record._data.get("stages_completed", []))
    issues = run_record.get_issues()
    base = {
        "issues": issues,
        "issues_file": str(run_record.issues_file),
        "issue_count": len(issues),
    }
    for stage_name, stage_cfg in pipeline.get("stages", {}).items():
        record_stage = get_record_stage(stage_cfg["agent"])
        if record_stage and record_stage in completed:
            stage_outputs[stage_name] = dict(base)
            jenkins = run_record._data.get("jenkins_execution")
            if jenkins and stage_name == "ocs_ci_execution":
                stage_outputs[stage_name]["jenkins_results"] = jenkins
                stage_outputs[stage_name]["jenkins_file"] = str(
                    run_record.run_dir / f"{run_record.run_id}_jenkins.json"
                )


IssueVerificationContextFactory.seed_stage_outputs = staticmethod(
    seed_issue_verification_stage_outputs
)
