"""
Data models for log analysis results.
"""

import hashlib
import json
import re
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


class FailureCategory(Enum):
    PRODUCT_BUG = "product_bug"
    TEST_BUG = "test_bug"
    INFRA_ISSUE = "infra_issue"
    FLAKY_TEST = "flaky_test"
    KNOWN_ISSUE = "known_issue"
    UNKNOWN = "unknown"


class TestStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class TestResult:
    """Parsed from JUnit XML + per-test logs."""

    classname: str
    name: str
    status: TestStatus
    duration: float
    traceback: Optional[str] = None
    skip_reason: Optional[str] = None
    squad: Optional[str] = None
    polarion_id: Optional[str] = None
    log_path: Optional[str] = None
    log_summary: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.classname}::{self.name}"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class FailureSignature:
    """Normalized fingerprint of a failure for caching and dedup."""

    test_name: str
    exception_type: str
    exception_message_hash: str
    traceback_hash: str

    @property
    def cache_key(self) -> str:
        combined = (
            f"{self.exception_type}:{self.exception_message_hash}:{self.traceback_hash}"
        )
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    @staticmethod
    def from_test_result(test_result: "TestResult") -> "FailureSignature":
        traceback = test_result.traceback or ""

        exception_type = "unknown"
        exception_message = ""
        lines = traceback.strip().splitlines()
        if lines:
            last_line = lines[-1].strip()
            match = re.match(
                r"^([\w.]+(?:Error|Exception|Failure)?)\s*:?\s*(.*)", last_line
            )
            if match:
                exception_type = match.group(1)
                exception_message = match.group(2)

        # Normalize traceback by removing line numbers and memory addresses
        normalized_tb = re.sub(r"0x[0-9a-fA-F]+", "0xADDR", traceback)
        normalized_tb = re.sub(r"line \d+", "line N", normalized_tb)

        msg_hash = hashlib.sha256(exception_message.encode()).hexdigest()[:12]
        tb_hash = hashlib.sha256(normalized_tb.encode()).hexdigest()[:12]

        return FailureSignature(
            test_name=test_result.name,
            exception_type=exception_type,
            exception_message_hash=msg_hash,
            traceback_hash=tb_hash,
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FailureAnalysis:
    """Analysis result for a single test failure."""

    test_result: TestResult
    category: FailureCategory
    confidence: float = 0.0
    root_cause_summary: str = ""
    evidence: list = field(default_factory=list)
    matched_known_issues: list = field(default_factory=list)
    suggested_jira_issues: list = field(default_factory=list)
    recommended_action: str = ""
    session_id: str = ""
    session_file: str = ""
    must_gather_url: str = ""
    mg_data_url: str = ""
    bug_details: dict = field(default_factory=dict)
    suggested_fix: dict = field(default_factory=dict)
    cache_file: str = ""
    cache_test: str = ""

    def to_dict(self) -> dict:
        d = {
            "test_name": self.test_result.full_name,
            "status": self.test_result.status.value,
            "duration": self.test_result.duration,
            "squad": self.test_result.squad,
            "polarion_id": self.test_result.polarion_id,
            "category": self.category.value,
            "confidence": self.confidence,
            "root_cause_summary": self.root_cause_summary,
            "evidence": self.evidence,
            "matched_known_issues": self.matched_known_issues,
            "suggested_jira_issues": self.suggested_jira_issues,
            "recommended_action": self.recommended_action,
            "session_id": self.session_id,
            "session_file": self.session_file,
            "must_gather_url": self.must_gather_url,
            "mg_data_url": self.mg_data_url,
            "cache_file": self.cache_file,
        }
        if self.cache_test:
            d["cache_test"] = self.cache_test
        if self.bug_details:
            d["bug_details"] = self.bug_details
        if self.suggested_fix:
            d["suggested_fix"] = self.suggested_fix
        return d

    @staticmethod
    def from_dict(data: dict, test_result: "TestResult") -> "FailureAnalysis":
        return FailureAnalysis(
            test_result=test_result,
            category=FailureCategory(data.get("category", "unknown")),
            confidence=data.get("confidence", 0.0),
            root_cause_summary=data.get("root_cause_summary", ""),
            evidence=data.get("evidence", []),
            matched_known_issues=data.get("matched_known_issues", []),
            suggested_jira_issues=data.get("suggested_jira_issues", []),
            recommended_action=data.get("recommended_action", ""),
            mg_data_url=data.get("mg_data_url", ""),
            bug_details=data.get("bug_details", {}),
            suggested_fix=data.get("suggested_fix", {}),
            cache_file=data.get("cache_file", ""),
            cache_test=data.get("cache_test", ""),
        )


@dataclass
class RunMetadata:
    """Environment metadata extracted from run config."""

    platform: str = ""
    deployment_type: str = ""
    ocp_version: str = ""
    ocs_version: str = ""
    ocs_build: str = ""
    run_id: str = ""
    logs_url: str = ""
    jenkins_url: str = ""
    launch_name: str = ""
    run_timestamp: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RunAnalysis:
    """Complete analysis of one test run."""

    run_url: str
    run_metadata: RunMetadata
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    error: int = 0
    failure_analyses: list = field(default_factory=list)
    summary: str = ""
    timestamp: str = ""

    def to_dict(self) -> dict:
        return {
            "run_url": self.run_url,
            "run_metadata": self.run_metadata.to_dict(),
            "total_tests": self.total_tests,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "error": self.error,
            "failure_analyses": [fa.to_dict() for fa in self.failure_analyses],
            "summary": self.summary,
            "timestamp": self.timestamp,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)
