"""Poll and abort Jenkins builds."""

import logging
import time

from jenkins.client import get_read_client
from jenkins.rest_client import RestJenkinsClient
from jenkins.url_parser import parse_jenkins_url
from job_resolver import build_cluster_profile
from models import RunStatus

log = logging.getLogger(__name__)


def wait_for_job(
    job_url: str,
    *,
    timeout_sec: int = 14400,
    poll_sec: int = 60,
    resolve_on_complete: bool = False,
    prefer_mcp: bool = False,
) -> RunStatus:
    """Poll Jenkins until build completes or timeout."""
    job_ref = parse_jenkins_url(job_url)
    if job_ref.build_number is None:
        raise ValueError(f"Build URL required: {job_url}")

    read_client = get_read_client(prefer_mcp=prefer_mcp)
    deadline = time.time() + timeout_sec
    last_data: dict | None = None

    while time.time() < deadline:
        last_data = read_client.get_build(job_ref)
        building = bool(last_data.get("building"))
        result = last_data.get("result")
        if not building:
            profile = None
            if resolve_on_complete:
                profile = build_cluster_profile(job_ref, last_data)
            return RunStatus(
                job_url=job_ref.url,
                result=result,
                building=False,
                duration_ms=last_data.get("duration"),
                cluster_profile=profile,
            )
        log.info("Build %s still running; sleeping %ss", job_ref.url, poll_sec)
        time.sleep(poll_sec)

    building = bool((last_data or {}).get("building"))
    return RunStatus(
        job_url=job_ref.url,
        result=(last_data or {}).get("result"),
        building=building,
        duration_ms=(last_data or {}).get("duration"),
        cluster_profile=None,
    )


def abort_job(
    job_url: str,
    *,
    dry_run: bool = False,
    client: RestJenkinsClient | None = None,
) -> dict:
    """Abort a running Jenkins build."""
    job_ref = parse_jenkins_url(job_url)
    if job_ref.build_number is None:
        raise ValueError(f"Build URL required: {job_url}")

    if dry_run:
        return {
            "dry_run": True,
            "job_url": job_ref.url,
            "message": "Would abort build",
        }

    rest = client or RestJenkinsClient()
    rest.stop_build(job_ref)
    return {
        "dry_run": False,
        "job_url": job_ref.url,
        "message": "Abort requested",
    }
