"""
Data models for the z-stream test generator.
"""

import enum
from dataclasses import dataclass, field
from typing import Optional


class BugClassification(enum.Enum):
    """Classification of a bug for test generation."""

    AUTOMATABLE = "automatable"
    NEEDS_DR_SETUP = "needs-dr-setup"
    MANUAL_ONLY = "manual-only"
    ALREADY_COVERED = "already-covered"
    HAS_OPEN_PR = "has-open-pr"
    GENERATION_FAILED = "generation-failed"


class Component(enum.Enum):
    """ODF component that a bug belongs to."""

    CEPH_CSI_RBD = "ceph-csi-rbd"
    CEPH_CSI_CEPHFS = "ceph-csi-cephfs"
    CEPH_CSI = "ceph-csi"
    NOOBAA = "noobaa"
    RAMEN = "ramen"
    OCS_OPERATOR = "ocs-operator"
    ODF_OPERATOR = "odf-operator"
    UNKNOWN = "unknown"


# Maps components to their test directories, base classes, and squad markers
COMPONENT_CONFIG = {
    Component.CEPH_CSI_RBD: {
        "test_dirs": [
            "tests/functional/pv/pvc_resize",
            "tests/functional/pv/pvc_snapshot",
            "tests/functional/pv/pvc_clone",
            "tests/functional/pv/pv_encryption",
            "tests/functional/pv",
        ],
        "base_class": "ManageTest",
        "squad_marker": "green_squad",
        "base_import": "from ocs_ci.framework.testlib import ManageTest",
        "squad_import": "from ocs_ci.framework.pytest_customization.marks import green_squad",
    },
    Component.CEPH_CSI_CEPHFS: {
        "test_dirs": [
            "tests/functional/pv/pvc_resize",
            "tests/functional/pv/pvc_snapshot",
            "tests/functional/pv",
        ],
        "base_class": "ManageTest",
        "squad_marker": "green_squad",
        "base_import": "from ocs_ci.framework.testlib import ManageTest",
        "squad_import": "from ocs_ci.framework.pytest_customization.marks import green_squad",
    },
    Component.CEPH_CSI: {
        "test_dirs": ["tests/functional/pv"],
        "base_class": "ManageTest",
        "squad_marker": "green_squad",
        "base_import": "from ocs_ci.framework.testlib import ManageTest",
        "squad_import": "from ocs_ci.framework.pytest_customization.marks import green_squad",
    },
    Component.NOOBAA: {
        "test_dirs": ["tests/functional/object/mcg"],
        "base_class": "MCGTest",
        "squad_marker": "red_squad",
        "base_import": "from ocs_ci.framework.testlib import MCGTest",
        "squad_import": "from ocs_ci.framework.pytest_customization.marks import red_squad",
    },
    Component.RAMEN: {
        "test_dirs": [
            "tests/functional/disaster-recovery/regional-dr",
            "tests/functional/disaster-recovery/metro-dr",
        ],
        "base_class": "ManageTest",
        "squad_marker": "turquoise_squad",
        "base_import": "from ocs_ci.framework.testlib import ManageTest",
        "squad_import": "from ocs_ci.framework.pytest_customization.marks import turquoise_squad",
    },
    Component.OCS_OPERATOR: {
        "test_dirs": ["tests/functional"],
        "base_class": "ManageTest",
        "squad_marker": "green_squad",
        "base_import": "from ocs_ci.framework.testlib import ManageTest",
        "squad_import": "from ocs_ci.framework.pytest_customization.marks import green_squad",
    },
    Component.ODF_OPERATOR: {
        "test_dirs": ["tests/functional"],
        "base_class": "ManageTest",
        "squad_marker": "green_squad",
        "base_import": "from ocs_ci.framework.testlib import ManageTest",
        "squad_import": "from ocs_ci.framework.pytest_customization.marks import green_squad",
    },
    Component.UNKNOWN: {
        "test_dirs": ["tests/functional"],
        "base_class": "ManageTest",
        "squad_marker": "green_squad",
        "base_import": "from ocs_ci.framework.testlib import ManageTest",
        "squad_import": "from ocs_ci.framework.pytest_customization.marks import green_squad",
    },
}


@dataclass
class ExecutionSpec:
    """Execution context for a generated test — platforms, versions, deploy modes."""

    platforms: list = field(default_factory=list)
    odf_versions: list = field(default_factory=list)
    ocp_versions: str = ""
    acm_version: str = ""
    deploy_modes: list = field(default_factory=list)
    platform_notes: str = ""

    def to_text(self) -> str:
        """Format as a human-readable block."""
        lines = []
        if self.odf_versions:
            lines.append(f"  ODF versions: {', '.join(self.odf_versions)}")
        if self.ocp_versions:
            lines.append(f"  OCP versions: {self.ocp_versions}")
        if self.acm_version:
            lines.append(f"  ACM version:  {self.acm_version}")
        if self.platforms:
            lines.append(f"  Platforms:    {', '.join(self.platforms)}")
        if self.deploy_modes:
            lines.append(f"  Deploy modes: {', '.join(self.deploy_modes)}")
        if self.platform_notes:
            lines.append(f"  Notes:        {self.platform_notes}")
        return "\n".join(lines) if lines else "  (no execution spec)"

    def to_dict(self) -> dict:
        """Serialize to a dictionary."""
        return {
            "platforms": self.platforms,
            "odf_versions": self.odf_versions,
            "ocp_versions": self.ocp_versions,
            "acm_version": self.acm_version,
            "deploy_modes": self.deploy_modes,
            "platform_notes": self.platform_notes,
        }


@dataclass
class ConfidenceScore:
    """Signal-quality score for a bug's test generation potential."""

    score: int = 0
    max_score: int = 0
    signals: list = field(default_factory=list)

    @property
    def level(self) -> str:
        """Map numeric score to a confidence level."""
        if self.max_score == 0:
            return "unknown"
        ratio = self.score / self.max_score
        if ratio >= 0.7:
            return "high"
        if ratio >= 0.4:
            return "medium"
        return "low"

    def to_text(self) -> str:
        """Format as a one-line summary."""
        return f"{self.level} ({self.score}/{self.max_score})"

    def to_dict(self) -> dict:
        """Serialize to a dictionary."""
        return {
            "score": self.score,
            "max_score": self.max_score,
            "level": self.level,
            "signals": self.signals,
        }


@dataclass
class RovoTestSpec:
    """Test specification extracted from Rovo analysis of a bug."""

    root_cause: str = ""
    code_changes: str = ""
    verification_steps: str = ""
    files_modified: list = field(default_factory=list)
    reproduce_steps: str = ""
    test_name: str = ""
    raw_response: str = ""


@dataclass
class BugInfo:
    """Complete information about a z-stream bug."""

    bug_id: str
    summary: str
    description: str = ""
    component: Component = Component.UNKNOWN
    fix_versions: list = field(default_factory=list)
    original_bug_id: Optional[str] = None
    upstream_pr_url: Optional[str] = None
    upstream_pr_diff: Optional[str] = None
    qa_contact: Optional[str] = None
    labels: list = field(default_factory=list)
    comments: list = field(default_factory=list)
    web_links: list = field(default_factory=list)
    linked_issues: list = field(default_factory=list)
    classification: BugClassification = BugClassification.AUTOMATABLE
    classification_reason: str = ""
    rovo_spec: Optional[RovoTestSpec] = None
    execution_spec: Optional[ExecutionSpec] = None
    confidence: Optional[ConfidenceScore] = None
    existing_test_path: Optional[str] = None
    existing_pr_url: Optional[str] = None

    @property
    def is_backport(self):
        """Check if this bug is a backport."""
        return "backport" in self.summary.lower()

    @property
    def short_slug(self):
        """
        Generate a short slug for branch naming.

        Strips Jira prefixes like [Backport to odf-X.Y.z], [GSS], etc.
        and extracts the core scenario description.

        """
        import re

        summary = self.summary

        # Strip common Jira prefixes
        # Strip bracketed prefixes
        summary = re.sub(r"\[Backport to [^\]]*\]\s*-?\s*", "", summary)
        summary = re.sub(r"\[[^\]]*\]\s*-?\s*", "", summary)
        summary = summary.strip("- ")

        # Strip version patterns (odf-4.22.z, 4.22z, v4.22, etc.)
        summary = re.sub(
            r"\bodf[-_]?\d+\.\d+\.?\w*\b", "", summary, flags=re.IGNORECASE
        )
        summary = re.sub(r"\b\d+\.\d+\.?\w*\b", "", summary)
        summary = re.sub(r"\bv\d+\b", "", summary, flags=re.IGNORECASE)

        # Extract meaningful words (skip metadata, versions, short connectors)
        skip_words = {
            "the",
            "a",
            "an",
            "in",
            "on",
            "to",
            "for",
            "with",
            "when",
            "and",
            "or",
            "is",
            "are",
            "was",
            "not",
            "from",
            "by",
            "of",
            "at",
            "it",
            "its",
            "has",
            "had",
            "odf",
            "ocp",
            "ocs",
            "backport",
            "gss",
            "rfe",
            "cve",
            "bug",
            "fix",
            "verify",
        }
        words = []
        for word in summary.lower().split():
            clean = re.sub(r"[^a-z_]", "", word.replace("-", "_"))
            if clean and clean not in skip_words and len(clean) > 1:
                words.append(clean)
            if len(words) >= 5:
                break

        return "_".join(words) if words else self.bug_id.lower().replace("-", "_")


@dataclass
class HelperSpec:
    """A helper function to be added to an existing ocs-ci module."""

    target_file: str
    function_name: str
    function_code: str
    description: str = ""
    insertion_point: str = ""

    def to_dict(self) -> dict:
        """Serialize to a dictionary."""
        return {
            "target_file": self.target_file,
            "function_name": self.function_name,
            "description": self.description,
            "insertion_point": self.insertion_point,
        }


@dataclass
class GeneratedTest:
    """A generated test file."""

    bug: BugInfo
    file_path: str
    code: str
    similar_tests_used: list = field(default_factory=list)
    validation_passed: bool = False
    validation_errors: list = field(default_factory=list)
    needs_review: bool = False
    helper_specs: list = field(default_factory=list)


@dataclass
class PublishedPR:
    """A published pull request."""

    bug_id: str
    branch: str
    target_branch: str
    pr_url: str
    pr_number: int


@dataclass
class ZStreamReport:
    """Summary report for a z-stream test generation run."""

    fix_version: str
    total_bugs: int = 0
    generated: list = field(default_factory=list)
    already_covered: list = field(default_factory=list)
    manual_only: list = field(default_factory=list)
    needs_dr_setup: list = field(default_factory=list)
    generation_failed: list = field(default_factory=list)
    pr_already_open: list = field(default_factory=list)
    published_prs: list = field(default_factory=list)

    def to_text(self):
        """Generate a human-readable summary report."""
        lines = [
            f"Z-Stream Test Generation Report: {self.fix_version}",
            "=" * 60,
            "",
            f"Total bugs: {self.total_bugs}",
            "",
        ]

        all_generated = list(self.generated) + list(self.needs_dr_setup)
        if all_generated:
            # Split into high-confidence and needs-review
            auto_tests = [t for t in all_generated if not t.needs_review]
            review_tests = [t for t in all_generated if t.needs_review]

            if auto_tests:
                lines.append(f"Generated tests ({len(auto_tests)}):")
                for item in auto_tests:
                    self._format_test_entry(lines, item)
                lines.append("")

            if review_tests:
                lines.append(f"Generated tests - needs review ({len(review_tests)}):")
                for item in review_tests:
                    self._format_test_entry(lines, item, show_missing=True)
                lines.append("")

        if self.already_covered:
            lines.append(f"Already covered ({len(self.already_covered)}):")
            for bug in self.already_covered:
                lines.append(f"  {bug.bug_id}  {bug.summary}")
                lines.append(f"    Existing: {bug.existing_test_path or 'unknown'}")
            lines.append("")

        if self.manual_only:
            lines.append(f"Manual only ({len(self.manual_only)}):")
            for bug in self.manual_only:
                lines.append(f"  {bug.bug_id}  {bug.summary}")
                lines.append(f"    Reason: {bug.classification_reason}")
            lines.append("")

        if self.pr_already_open:
            lines.append(f"PR already open ({len(self.pr_already_open)}):")
            for bug in self.pr_already_open:
                lines.append(f"  {bug.bug_id}  {bug.summary}")
                pr_url = getattr(bug, "existing_pr_url", None)
                if pr_url:
                    lines.append(f"    PR: {pr_url}")
            lines.append("")

        if self.generation_failed:
            lines.append(f"Generation failed ({len(self.generation_failed)}):")
            for bug in self.generation_failed:
                lines.append(f"  {bug.bug_id}  {bug.summary}")
                lines.append(f"    Reason: {bug.classification_reason}")
            lines.append("")

        if self.published_prs:
            lines.append(f"Published PRs ({len(self.published_prs)}):")
            by_bug = {}
            for pr in self.published_prs:
                by_bug.setdefault(pr.bug_id, []).append(pr)
            for bug_id, prs in by_bug.items():
                lines.append(f"  {bug_id}:")
                for pr in prs:
                    lines.append(f"    {pr.target_branch}: {pr.pr_url}")
            lines.append("")

        return "\n".join(lines)

    def _format_test_entry(self, lines: list, item, show_missing: bool = False):
        """Append a single generated test entry to the report lines."""
        bug = item.bug if hasattr(item, "bug") else item
        conf = ""
        if bug.confidence:
            conf = f"  [{bug.confidence.to_text()}]"
        dr_tag = ""
        if bug.classification == BugClassification.NEEDS_DR_SETUP:
            dr_tag = "  [DR]"
        lines.append(f"  {bug.bug_id}  {bug.summary}{conf}{dr_tag}")
        if hasattr(item, "file_path"):
            lines.append(f"    Test: {item.file_path}")
        if hasattr(item, "helper_specs") and item.helper_specs:
            lines.append(f"    Helpers ({len(item.helper_specs)}):")
            for h in item.helper_specs:
                lines.append(f"      + {h.function_name} -> {h.target_file}")
                if h.description:
                    lines.append(f"        {h.description}")
        if show_missing and bug.confidence and bug.confidence.signals:
            missing = [
                s.lstrip("- ") for s in bug.confidence.signals if s.startswith("-")
            ]
            if missing:
                lines.append(f"    Missing: {', '.join(missing)}")
        if bug.execution_spec:
            lines.append(bug.execution_spec.to_text())
