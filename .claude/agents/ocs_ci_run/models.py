"""Data models for the OCS-CI run agent."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class JobRef:
    """Parsed Jenkins job or build URL."""

    base_url: str
    job_name: str
    build_number: int | None
    url: str

    @property
    def full_path(self) -> str:
        """Jenkins MCP full_path (e.g. job/qe-deploy-ocs-cluster)."""
        return f"job/{self.job_name}"

    @property
    def api_path(self) -> str:
        """REST API path segment after base_url."""
        if self.build_number is not None:
            return f"job/{self.job_name}/{self.build_number}"
        return f"job/{self.job_name}"


@dataclass
class ClusterProfile:
    """Resolved cluster metadata from a Jenkins deploy build."""

    source_job: JobRef
    cluster_name: str
    ocs_version: str
    ocp_version: str
    platform: str | None
    topology_hints: dict[str, Any]
    magna_dir_url: str | None
    kubeconfig_url: str | None
    kubeconfig_path: str | None
    console_url: str | None
    jenkins_result: str | None
    building: bool
    parameters: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_job_url": self.source_job.url,
            "cluster_name": self.cluster_name,
            "ocs_version": self.ocs_version,
            "ocp_version": self.ocp_version,
            "platform": self.platform,
            "topology_hints": self.topology_hints,
            "magna_dir_url": self.magna_dir_url,
            "kubeconfig_url": self.kubeconfig_url,
            "kubeconfig_path": self.kubeconfig_path,
            "console_url": self.console_url,
            "jenkins_result": self.jenkins_result,
            "building": self.building,
            "parameters": self.parameters,
        }


@dataclass
class TriggerResult:
    """Result of triggering a parameterized Jenkins test run."""

    dry_run: bool
    source_job_url: str
    new_job_url: str | None
    build_number: int | None
    parameters: dict[str, Any]
    parameter_overrides: dict[str, Any]
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "source_job_url": self.source_job_url,
            "new_job_url": self.new_job_url,
            "build_number": self.build_number,
            "parameters": self.parameters,
            "parameter_overrides": self.parameter_overrides,
            "message": self.message,
        }


@dataclass
class RunStatus:
    """Status of a Jenkins build."""

    job_url: str
    result: str | None
    building: bool
    duration_ms: int | None
    cluster_profile: ClusterProfile | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_url": self.job_url,
            "result": self.result,
            "building": self.building,
            "duration_ms": self.duration_ms,
            "cluster_profile": (
                self.cluster_profile.to_dict() if self.cluster_profile else None
            ),
        }
