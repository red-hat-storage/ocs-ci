"""Parse Jenkins job and build URLs."""

import re
from urllib.parse import urlparse

from models import JobRef

_BUILD_URL_RE = re.compile(
    r"^(?P<base>https?://[^/]+)/job/(?P<job>[^/]+)/(?P<build>\d+)/?",
    re.IGNORECASE,
)
_JOB_URL_RE = re.compile(
    r"^(?P<base>https?://[^/]+)/job/(?P<job>[^/]+)/?",
    re.IGNORECASE,
)


def parse_jenkins_url(url: str) -> JobRef:
    """
    Parse a Jenkins job or build URL into a JobRef.

    Args:
        url (str): e.g. https://host/job/qe-deploy-ocs-cluster/69391/

    Returns:
        JobRef: Parsed reference

    """
    cleaned = url.strip().rstrip("/")
    if cleaned.endswith("/console"):
        cleaned = cleaned[: -len("/console")].rstrip("/")

    match = _BUILD_URL_RE.match(cleaned)
    if match:
        base = match.group("base").rstrip("/")
        job_name = match.group("job")
        build_number = int(match.group("build"))
        return JobRef(
            base_url=base,
            job_name=job_name,
            build_number=build_number,
            url=f"{base}/job/{job_name}/{build_number}/",
        )

    match = _JOB_URL_RE.match(cleaned)
    if match:
        base = match.group("base").rstrip("/")
        job_name = match.group("job")
        return JobRef(
            base_url=base,
            job_name=job_name,
            build_number=None,
            url=f"{base}/job/{job_name}/",
        )

    parsed = urlparse(cleaned)
    if parsed.scheme and parsed.netloc:
        raise ValueError(f"Unrecognized Jenkins URL format: {url}")

    raise ValueError(f"Invalid Jenkins URL: {url}")
