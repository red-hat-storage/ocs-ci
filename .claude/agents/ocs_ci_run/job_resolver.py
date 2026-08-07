"""Resolve Jenkins deploy builds into cluster profiles."""

import logging
import os
import ssl
import urllib.request
from pathlib import Path

from description_parser import infer_topology_hints, parse_build_description
from jenkins.client import get_read_client
from jenkins.rest_client import extract_build_parameters
from jenkins.url_parser import parse_jenkins_url
from models import ClusterProfile, JobRef

log = logging.getLogger(__name__)


def _download_file(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    verify = os.environ.get("JENKINS_SSL_VERIFY", "true").strip().lower()
    context = None
    if verify in {"0", "false", "no", "off"}:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(url, timeout=120, context=context) as resp:
        dest.write_bytes(resp.read())
    return dest


def build_cluster_profile(
    job_ref: JobRef,
    build_data: dict,
    *,
    kubeconfig_path: str | None = None,
) -> ClusterProfile:
    """Build ClusterProfile from Jenkins build API payload."""
    parameters = extract_build_parameters(build_data)
    description_links = parse_build_description(build_data.get("description") or "")

    return ClusterProfile(
        source_job=job_ref,
        cluster_name=str(parameters.get("CLUSTER_NAME") or ""),
        ocs_version=str(
            parameters.get("OCS_VERSION") or parameters.get("ODF_VERSION") or ""
        ),
        ocp_version=str(parameters.get("OCP_VERSION") or ""),
        platform=str(
            parameters.get("PLATFORM") or parameters.get("CREDENTIALS_CONF") or ""
        )
        or None,
        topology_hints=infer_topology_hints(parameters),
        magna_dir_url=description_links.get("magna_dir_url"),
        kubeconfig_url=description_links.get("kubeconfig_url"),
        kubeconfig_path=kubeconfig_path,
        console_url=description_links.get("console_url"),
        jenkins_result=build_data.get("result"),
        building=bool(build_data.get("building")),
        parameters=parameters,
    )


def resolve_job(
    job_url: str,
    *,
    download_kubeconfig: bool = True,
    work_dir: Path | None = None,
    prefer_mcp: bool = False,
) -> ClusterProfile:
    """
    Resolve a Jenkins deploy build URL to a ClusterProfile.

    Args:
        job_url (str): Jenkins build URL
        download_kubeconfig (bool): Download kubeconfig from Magna when available
        work_dir (Path | None): Directory for kubeconfig
        prefer_mcp (bool): Use Jenkins MCP for getBuild when caller is configured

    Returns:
        ClusterProfile: Resolved cluster metadata

    """
    job_ref = parse_jenkins_url(job_url)
    if job_ref.build_number is None:
        raise ValueError(f"Build URL must include build number: {job_url}")

    read_client = get_read_client(prefer_mcp=prefer_mcp)
    build_data = read_client.get_build(job_ref)

    profile = build_cluster_profile(job_ref, build_data)

    if download_kubeconfig and profile.kubeconfig_url:
        root = work_dir or Path.cwd() / "_ocs_ci_run_cluster" / job_ref.job_name
        dest = root / str(job_ref.build_number) / "auth" / "kubeconfig"
        try:
            _download_file(profile.kubeconfig_url, dest)
            profile.kubeconfig_path = str(dest.resolve())
            log.info("Downloaded kubeconfig to %s", profile.kubeconfig_path)
        except OSError as exc:
            log.warning("Failed to download kubeconfig: %s", exc)

    return profile
