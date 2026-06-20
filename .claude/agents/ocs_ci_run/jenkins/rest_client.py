"""Jenkins REST API client (CLI and parameterized triggers)."""

import json
import logging
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from typing import Any

from auth import load_jenkins_auth
from jenkins.url_parser import parse_jenkins_url
from models import JobRef

log = logging.getLogger(__name__)

BUILD_JSON_TREE = (
    "result,building,url,description,duration,timestamp,"
    "actions[parameters[name,value]]"
)


def _ssl_context() -> ssl.SSLContext | None:
    """Return SSL context; None uses default verification."""
    verify = os.environ.get("JENKINS_SSL_VERIFY", "true").strip().lower()
    if verify in {"0", "false", "no", "off"}:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        log.warning("Jenkins SSL verification disabled (JENKINS_SSL_VERIFY)")
        return ctx
    return None


def _urlopen(req: urllib.request.Request, *, timeout: int = 120):
    return urllib.request.urlopen(req, timeout=timeout, context=_ssl_context())


def extract_build_parameters(build_data: dict[str, Any]) -> dict[str, Any]:
    """Extract parameter name/value pairs from a Jenkins build API payload."""
    params: dict[str, Any] = {}
    for action in build_data.get("actions") or []:
        for param in action.get("parameters") or []:
            name = param.get("name")
            if name is not None:
                params[name] = param.get("value")
    return params


class RestJenkinsClient:
    """Jenkins HTTP API client using Basic auth and CSRF crumbs."""

    def __init__(
        self,
        *,
        username: str | None = None,
        token: str | None = None,
    ):
        self.username, self.token = load_jenkins_auth(username=username, token=token)
        self._auth_header = b64encode(f"{self.username}:{self.token}".encode()).decode()

    def _request(
        self,
        url: str,
        *,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        crumb: tuple[str, str] | None = None,
    ) -> dict[str, Any] | None:
        headers = {"Authorization": f"Basic {self._auth_header}"}
        body = None
        if data is not None:
            body = urllib.parse.urlencode(data).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        if crumb:
            headers[crumb[0]] = crumb[1]

        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with _urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8")
                if not raw.strip():
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")[:500]
            raise RuntimeError(
                f"Jenkins API {method} {url} failed: HTTP {exc.code} {detail}"
            ) from exc

    def get_crumb(self, base_url: str) -> tuple[str, str] | None:
        """Return (header_name, crumb_value) or None if not required."""
        url = f"{base_url.rstrip('/')}/crumbIssuer/api/json"
        try:
            data = self._request(url)
        except RuntimeError as exc:
            log.debug("Crumb issuer unavailable: %s", exc)
            return None
        if not data:
            return None
        field = data.get("crumbRequestField")
        value = data.get("crumb")
        if field and value:
            return str(field), str(value)
        return None

    def get_build(self, job_ref: JobRef) -> dict[str, Any]:
        """Fetch build metadata from Jenkins API."""
        if job_ref.build_number is None:
            raise ValueError("JobRef must include build_number for get_build")
        url = (
            f"{job_ref.base_url}/{job_ref.api_path}/api/json"
            f"?tree={urllib.parse.quote(BUILD_JSON_TREE)}"
        )
        data = self._request(url)
        if not data:
            raise RuntimeError(f"Empty response for build {job_ref.url}")
        return data

    def get_queue_item(self, queue_url: str) -> dict[str, Any]:
        """Fetch a queue item (after triggering a build)."""
        api_url = queue_url.rstrip("/") + "/api/json"
        data = self._request(api_url)
        if not data:
            raise RuntimeError(f"Empty queue response: {queue_url}")
        return data

    def trigger_build_with_parameters(
        self,
        job_ref: JobRef,
        parameters: dict[str, Any],
    ) -> str:
        """
        Trigger a parameterized build.

        Returns:
            str: Queue item URL from Location header or JSON

        """
        if job_ref.build_number is not None:
            job_ref = JobRef(
                base_url=job_ref.base_url,
                job_name=job_ref.job_name,
                build_number=None,
                url=f"{job_ref.base_url}/job/{job_ref.job_name}/",
            )

        url = f"{job_ref.base_url}/{job_ref.api_path}/buildWithParameters"
        crumb = self.get_crumb(job_ref.base_url)

        encoded: dict[str, str] = {}
        for key, value in parameters.items():
            if value is None:
                continue
            if isinstance(value, bool):
                encoded[key] = "true" if value else "false"
            else:
                encoded[key] = str(value)

        headers = {"Authorization": f"Basic {self._auth_header}"}
        if crumb:
            headers[crumb[0]] = crumb[1]
        body = urllib.parse.urlencode(encoded).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        req = urllib.request.Request(url, data=body, method="POST", headers=headers)
        try:
            with _urlopen(req, timeout=120) as resp:
                location = resp.headers.get("Location")
                if location:
                    return location
                raw = resp.read().decode("utf-8")
                if raw.strip():
                    payload = json.loads(raw)
                    if "queue_item" in payload:
                        return str(payload["queue_item"])
                return url
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")[:500]
            raise RuntimeError(
                f"Jenkins trigger failed: HTTP {exc.code} {detail}"
            ) from exc

    def stop_build(self, job_ref: JobRef) -> None:
        """Abort a running build."""
        if job_ref.build_number is None:
            raise ValueError("build_number required to stop build")
        url = f"{job_ref.base_url}/{job_ref.api_path}/stop"
        crumb = self.get_crumb(job_ref.base_url)
        self._request(url, method="POST", data={}, crumb=crumb)


def job_ref_from_url(url: str) -> JobRef:
    """Parse URL and return JobRef."""
    return parse_jenkins_url(url)
