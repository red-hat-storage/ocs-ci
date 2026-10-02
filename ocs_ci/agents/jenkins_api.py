"""Shared Jenkins client for ODF agents.

Any agent that reads a deploy or starts a job uses JenkinsClient. Credentials
are agents_credentials.jenkins, then the top-level jenkins section of
data/auth.yaml. trigger_build is refused during --dry-run.
"""

import logging
import re

import requests
from requests.auth import HTTPBasicAuth

from ocs_ci.agents.mcp.registry import _load_auth_config

logger = logging.getLogger(__name__)

DEFAULT_JENKINS_URL = "https://jenkins-csb-odf-qe-ocs4.dno.corp.redhat.com"
_DEPLOY_JOBS = (
    "qe-deploy-ocs-cluster",
    "eng-deploy-ocs-cluster",
    "qe-simple-deploy-odf-cluster",
    "eng-simple-deploy-odf-cluster",
    "qe-deploy-ocs-cluster-prod",
)
_PAGE = 100
_MAX_BUILDS = 500
_DETAIL_PARAMS = (
    "OCP_VERSION",
    "OCS_VERSION",
    "UPGRADE_OCS_VERSION",
    "PLATFORM",
    "FULL_PLATFORM_CONF",
    "PLATFORM_CONF",
    "CLUSTER_CONF",
    "ENCRYPTION_AT_REST",
    "FIPS",
    "MCG_ONLY",
    "RUN_TEARDOWN",
    "UPGRADE",
)


class JenkinsClient:
    """
    Client for the OCS QE Jenkins server.

    Credentials come from agents_credentials.jenkins, then the top-level
    jenkins section. email and token are the API user and token. The username
    is the email local-part unless username is set. trigger_build starts a
    job and is refused during --dry-run.
    """

    def __init__(self, auth=None):
        """
        Args:
            auth (dict): url, email or username, and token. Resolved from
                data/auth.yaml when omitted.
        """
        resolved = auth or resolve_jenkins_auth()
        self.url = resolved["url"].rstrip("/")
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(resolved["username"], resolved["token"])
        self.session.headers["Accept"] = "application/json"
        self._verify = True

    def latest_deploy(self, cluster_name):
        """
        Return the newest deploy build whose CLUSTER_NAME equals cluster_name.

        Args:
            cluster_name (str): Jenkins CLUSTER_NAME.

        Returns:
            dict: Build number, url, result, and selected parameters. None
                when no recent build matches.
        """
        logger.info(f"Looking up the newest Jenkins deploy for {cluster_name}")
        newest = None
        for job in _DEPLOY_JOBS:
            build = self._newest_in_job(job, cluster_name)
            if build and (newest is None or build["timestamp"] > newest["timestamp"]):
                newest = build
        return newest

    def agent_online(self, cluster_name):
        """
        Return whether the temporary Jenkins agent for the cluster is online.

        Args:
            cluster_name (str): Jenkins label, which is the cluster name.

        Returns:
            bool: True when a node with that label is online.
        """
        payload = self._get(
            f"/label/{cluster_name}/api/json",
            params={"tree": "nodes[nodeName,offline]"},
        )
        if not isinstance(payload, dict):
            return False
        for node in payload.get("nodes") or []:
            if isinstance(node, dict) and node.get("offline") is False:
                return True
        return False

    def get_build(self, job, number):
        """
        Return one build and its non-secret parameters.

        Args:
            job (str): Jenkins job name, for example qe-deploy-ocs-cluster.
            number (int): Build number.

        Returns:
            dict: number, url, result, building, and parameters. Empty when
                the build does not exist.
        """
        payload = self._get(
            f"/job/{job}/{int(number)}/api/json",
            params={
                "tree": (
                    "number,url,result,timestamp,building,"
                    "actions[parameters[name,value]]"
                )
            },
        )
        if not payload:
            return {}
        return {
            "job": job,
            "number": payload.get("number"),
            "url": payload.get("url") or "",
            "result": payload.get("result") or "",
            "building": bool(payload.get("building")),
            "parameters": _public_parameters(payload),
        }

    def trigger_build(self, job, parameters):
        """
        Start a job with buildWithParameters.

        Args:
            job (str): Jenkins job name.
            parameters (dict): Form parameters, matching the job's parameter
                names. Empty values are omitted so Jenkins keeps its defaults.

        Returns:
            dict: job, queued, and the queue URL from the Location header.

        Raises:
            RuntimeError: This is a dry run, or Jenkins rejects the request.
            ValueError: No parameters were given.
        """
        from ocs_ci.agents.runtime.dry_run import dry_run_enabled

        if dry_run_enabled():
            raise RuntimeError(
                "dry-run: the Jenkins job was not started and no cluster was changed"
            )
        if not isinstance(parameters, dict) or not parameters:
            raise ValueError("Jenkins parameters are required")
        form = {}
        for key, value in parameters.items():
            if value is None or value == "":
                continue
            if isinstance(value, bool):
                form[str(key)] = "true" if value else "false"
            else:
                form[str(key)] = str(value)
        if not form:
            raise ValueError("Jenkins parameters are required")
        logger.info(f"Starting Jenkins job {job}")
        response = self._request(
            "POST",
            f"/job/{job}/buildWithParameters",
            data=form,
            headers=self._crumb_headers(),
        )
        if response.status_code not in {200, 201, 302}:
            raise RuntimeError(f"Jenkins returned HTTP {response.status_code}")
        return {
            "job": job,
            "queued": True,
            "queue_url": response.headers.get("Location") or "",
        }

    def _crumb_headers(self):
        """
        Return the Jenkins crumb header when the server issues one.

        Returns:
            dict: Crumb header, or an empty dict when crumbs are disabled.
        """
        payload = self._get("/crumbIssuer/api/json")
        crumb = payload.get("crumb") if isinstance(payload, dict) else None
        if not crumb:
            return {}
        field = payload.get("crumbRequestField") or "Jenkins-Crumb"
        return {field: crumb}

    def _newest_in_job(self, job, cluster_name):
        """
        Scan recent builds of one job for the cluster.

        Args:
            job (str): Jenkins job name.
            cluster_name (str): CLUSTER_NAME to match.

        Returns:
            dict: Newest matching build, or None.
        """
        for start in range(0, _MAX_BUILDS, _PAGE):
            payload = self._get(
                f"/job/{job}/api/json",
                params={
                    "tree": (
                        "builds[number,url,result,timestamp,"
                        f"actions[parameters[name,value]]]{{{start},{start + _PAGE}}}"
                    )
                },
            )
            builds = payload.get("builds") if isinstance(payload, dict) else None
            if not builds:
                return None
            for build in builds:
                if _parameter(build, "CLUSTER_NAME") == cluster_name:
                    return _build_record(job, build)
        return None

    def _get(self, path, params=None):
        """
        GET a Jenkins JSON API path.

        Args:
            path (str): Path beginning with /.
            params (dict): Query parameters.

        Returns:
            dict: Parsed JSON, or an empty dict when the path is missing.

        Raises:
            RuntimeError: Jenkins rejects the credentials or the request fails.
        """
        response = self._request("GET", path, params=params)
        if response.status_code == 404:
            return {}
        if not response.content:
            return {}
        return response.json()

    def _request(self, method, path, params=None, data=None, headers=None):
        """
        Send one Jenkins HTTP request.

        The corporate certificate is accepted when the default trust store
        rejects it.

        Args:
            method (str): GET or POST.
            path (str): Path beginning with /.
            params (dict): Query parameters.
            data (dict): Form body.
            headers (dict): Extra headers.

        Returns:
            Response: Jenkins response.

        Raises:
            RuntimeError: Jenkins rejects the credentials or the request fails.
        """
        try:
            response = self.session.request(
                method,
                self.url + path,
                params=params,
                data=data,
                headers=headers,
                timeout=60,
                verify=self._verify,
            )
        except requests.exceptions.SSLError:
            if not self._verify:
                raise
            logger.warning(
                "Jenkins certificate is not in the trust store; "
                "retrying without verification"
            )
            self._verify = False
            requests.packages.urllib3.disable_warnings()
            response = self.session.request(
                method,
                self.url + path,
                params=params,
                data=data,
                headers=headers,
                timeout=60,
                verify=False,
            )
        if response.status_code in {401, 403}:
            raise RuntimeError("Jenkins rejected the API credentials")
        if method == "GET" and not response.ok and response.status_code != 404:
            raise RuntimeError(f"Jenkins returned HTTP {response.status_code}")
        return response


def resolve_jenkins_auth():
    """
    Find the Jenkins URL, username, and API token.

    agents_credentials.jenkins is preferred. The top-level jenkins section is
    used when the agent section is absent.

    Returns:
        dict: url, username, and token.

    Raises:
        ValueError: No email and token were configured.
    """
    loaded = _load_auth_config()
    agents = loaded.get("agents_credentials") or {}
    raw = agents.get("jenkins") if isinstance(agents, dict) else None
    if not isinstance(raw, dict):
        raw = loaded.get("jenkins")
    if not isinstance(raw, dict):
        raw = {}
    email = str(raw.get("email") or "").strip()
    username = str(raw.get("username") or "").strip() or email.split("@")[0]
    token = str(raw.get("token") or "").strip()
    url = str(raw.get("url") or DEFAULT_JENKINS_URL).strip()
    if not username or not token:
        raise ValueError(
            "Set agents_credentials.jenkins.email and "
            "agents_credentials.jenkins.token in data/auth.yaml."
        )
    return {"url": url, "username": username, "token": token}


def _build_record(job, build):
    """
    Keep the build fields needed for a cluster check.

    Args:
        job (str): Jenkins job name.
        build (dict): One build from the job API.

    Returns:
        dict: number, url, result, timestamp, job, and selected parameters.
    """
    parameters = {}
    for name in _DETAIL_PARAMS:
        value = _parameter(build, name)
        if value != "":
            parameters[name] = value
    return {
        "job": job,
        "number": build.get("number") or 0,
        "url": build.get("url") or "",
        "result": build.get("result") or "",
        "timestamp": build.get("timestamp") or 0,
        "parameters": parameters,
    }


def _parameter(build, name):
    """
    Return one build parameter value.

    Args:
        build (dict): Jenkins build.
        name (str): Parameter name.

    Returns:
        str: Value, or an empty string.
    """
    for action in build.get("actions") or []:
        if not isinstance(action, dict):
            continue
        for param in action.get("parameters") or []:
            if isinstance(param, dict) and param.get("name") == name:
                return str(param.get("value") or "")
    return ""


_SECRET_PARAMETER = re.compile(
    r"PASSWORD|SECRET|TOKEN|PRIVATE|API_KEY|YAML_TEXT",
    re.IGNORECASE,
)


def _public_parameters(build):
    """
    Return build parameters that are safe to hand to an agent.

    Args:
        build (dict): Jenkins build.

    Returns:
        dict: Parameter names to values, without passwords, tokens, or
            inline YAML config.
    """
    parameters = {}
    for action in build.get("actions") or []:
        if not isinstance(action, dict):
            continue
        for param in action.get("parameters") or []:
            if not isinstance(param, dict):
                continue
            name = str(param.get("name") or "")
            if not name or _SECRET_PARAMETER.search(name):
                continue
            parameters[name] = str(param.get("value") or "")
    return parameters
