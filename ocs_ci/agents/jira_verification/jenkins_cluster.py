"""Read an OCS Jenkins deploy build and decide if that cluster can verify a bug."""

import logging
import re

from ocs_ci.agents.jenkins_api import JenkinsClient

logger = logging.getLogger(__name__)

_PLATFORM_ALIASES = {
    "vmware": "vsphere",
    "ibm_cloud": "ibmcloud",
    "ibm-cloud": "ibmcloud",
    "bare-metal": "baremetal",
    "bare_metal": "baremetal",
}
_PLATFORM_NAMES = {
    "aws",
    "vsphere",
    "baremetal",
    "ibmcloud",
    "gcp",
    "azure",
    "rosa",
    "nutanix",
    "rhv",
    "openstack",
}
_TOPOLOGY = re.compile(r"(\d+)m_(\d+)w")


def cluster_check(report):
    """
    Compare the named cluster with the bug's fix version and environment.

    Args:
        report (dict): Verification report. cluster is the Jenkins CLUSTER_NAME.

    Returns:
        dict: fits, reasons, and the Jenkins deploy details. None when the
            report does not name a cluster.
    """
    name = str((report or {}).get("cluster") or "").strip()
    if not name:
        return None
    try:
        client = JenkinsClient()
        build = client.latest_deploy(name)
        agent_online = client.agent_online(name)
    except Exception as error:
        logger.warning(f"Could not read Jenkins cluster {name}: {error}")
        return {
            "cluster": name,
            "fits": False,
            "reasons": [f"Jenkins lookup failed: {error}"],
            "jenkins": {},
            "details": {},
        }
    if not build:
        logger.warning(
            f"No deploy build for {name} was found in the recent Jenkins jobs"
        )
        return {
            "cluster": name,
            "fits": False,
            "reasons": [
                f"No deploy build for {name} was found in the recent Jenkins jobs."
            ],
            "jenkins": {},
            "details": {"agent_online": agent_online},
        }
    checked = _fitness(name, build, agent_online, report)
    jenkins = checked.get("jenkins") or {}
    where = f"{jenkins.get('job') or 'unknown'} #{jenkins.get('build') or ''}".strip()
    if checked["fits"]:
        logger.info(f"Cluster {name} can run verification on {where}")
    else:
        logger.warning(
            f"Cluster {name} cannot run verification on {where}: "
            + "; ".join(checked["reasons"])
        )
    return checked


def _fitness(name, build, agent_online, report):
    """
    Decide whether the deploy build can run this bug's verification.

    Args:
        name (str): Cluster name.
        build (dict): Deploy build record.
        agent_online (bool): Temporary Jenkins agent is online.
        report (dict): Verification report.

    Returns:
        dict: Cluster check stored on the report.
    """
    params = build.get("parameters") or {}
    platform = _platform(params)
    reasons = []
    if not agent_online:
        reasons.append(
            f"The Jenkins agent for {name} is offline, so verification "
            "commands cannot run on that cluster now."
        )
    if _truthy(params.get("RUN_TEARDOWN")) and build.get("result"):
        reasons.append("The deploy build requested teardown, so the cluster is gone.")
    version_reason = _version_reason(
        params.get("OCS_VERSION"), report.get("fix_version")
    )
    if version_reason:
        reasons.append(version_reason)
    platform_reason = _platform_reason(platform, report)
    if platform_reason:
        reasons.append(platform_reason)
    if _truthy(params.get("UPGRADE")) and report.get("upgrade_scenario") is False:
        reasons.append(
            "The Jenkins cluster was deployed as an upgrade, and this bug is not "
            "an upgrade scenario."
        )
    details = _details(params, platform, agent_online)
    return {
        "cluster": name,
        "fits": not reasons,
        "reasons": reasons,
        "jenkins": {
            "job": build.get("job"),
            "build": build.get("number"),
            "result": build.get("result") or "",
            "url": build.get("url") or "",
        },
        "details": details,
    }


def _details(params, platform, agent_online):
    """
    Return the cluster facts that belong on the report.

    Args:
        params (dict): Selected build parameters.
        platform (str): Platform name derived from the build.
        agent_online (bool): Temporary Jenkins agent is online.

    Returns:
        dict: Versions, platform, topology, and flags.
    """
    conf = params.get("FULL_PLATFORM_CONF") or params.get("PLATFORM_CONF") or ""
    topology = _TOPOLOGY.search(conf)
    details = {
        "ocp_version": params.get("OCP_VERSION") or "",
        "ocs_version": params.get("OCS_VERSION") or "",
        "platform": platform,
        "platform_conf": conf,
        "cluster_conf": params.get("CLUSTER_CONF") or "",
        "encryption_at_rest": _truthy(params.get("ENCRYPTION_AT_REST")),
        "fips": _truthy(params.get("FIPS")),
        "mcg_only": _truthy(params.get("MCG_ONLY")),
        "teardown_requested": _truthy(params.get("RUN_TEARDOWN")),
        "agent_online": agent_online,
    }
    if topology:
        details["masters"] = int(topology.group(1))
        details["workers"] = int(topology.group(2))
    return details


def _version_reason(cluster_version, fix_versions):
    """
    Return a mismatch sentence when the cluster is not on the fix stream.

    Args:
        cluster_version (str): OCS_VERSION from Jenkins.
        fix_versions (list): Fix versions from the bug.

    Returns:
        str: Reason, or an empty string when the streams match or are unknown.
    """
    cluster_stream = _stream(cluster_version)
    if not cluster_stream:
        return ""
    streams = [_stream(version) for version in fix_versions or []]
    streams = [stream for stream in streams if stream]
    if not streams or cluster_stream in streams:
        return ""
    listed = ", ".join(str(version) for version in fix_versions)
    return (
        f"The cluster ODF version is {cluster_version}; "
        f"the fix version is {listed}."
    )


def _platform_reason(platform, report):
    """
    Return a mismatch sentence when the bug names a different platform.

    Args:
        platform (str): Cluster platform.
        report (dict): Verification report.

    Returns:
        str: Reason, or an empty string when the platform is compatible.
    """
    if not platform:
        return ""
    text = " ".join(
        [
            str(report.get("environment_reported") or ""),
            str(report.get("environment_verify") or ""),
        ]
    ).lower()
    named = []
    for token in re.split(r"[^a-z0-9_+-]+", text):
        token = _PLATFORM_ALIASES.get(token, token)
        if token in _PLATFORM_NAMES and token not in named:
            named.append(token)
    if not named or platform in named:
        return ""
    return (
        f"The cluster platform is {platform}; the bug was reported on "
        + ", ".join(named)
        + "."
    )


def _platform(params):
    """
    Read the platform from PLATFORM or the platform config path.

    Args:
        params (dict): Build parameters.

    Returns:
        str: Platform name, or an empty string.
    """
    explicit = _PLATFORM_ALIASES.get(
        str(params.get("PLATFORM") or "").strip().lower(),
        str(params.get("PLATFORM") or "").strip().lower(),
    )
    if explicit:
        return explicit
    conf = str(params.get("FULL_PLATFORM_CONF") or params.get("PLATFORM_CONF") or "")
    parts = [part for part in conf.split("/") if part]
    if len(parts) >= 2:
        return _PLATFORM_ALIASES.get(parts[1].lower(), parts[1].lower())
    return ""


def _stream(version):
    """
    Return the major.minor stream of an ODF version.

    Args:
        version (str): Version such as odf-4.22.6 or 5.0.

    Returns:
        str: Stream such as 4.22, or an empty string.
    """
    text = str(version or "").lower().replace("odf-", "").replace("ocs-", "").strip()
    match = re.match(r"(\d+)\.(\d+)", text)
    if not match:
        return ""
    return f"{match.group(1)}.{match.group(2)}"


def _truthy(value):
    """
    Return whether a Jenkins checkbox parameter is enabled.

    Args:
        value: Parameter value.

    Returns:
        bool: True for true, yes, or 1.
    """
    return str(value or "").strip().lower() in {"1", "true", "yes"}
