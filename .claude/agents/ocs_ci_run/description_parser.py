"""Parse Jenkins build description HTML for cluster links."""

import re
from typing import Any

_HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)
_MAGNA_RE = re.compile(
    r"(https?://magna[^\"'<>\s]+/openshift-clusters/[^\"'<>\s]+)",
    re.IGNORECASE,
)
_CONSOLE_RE = re.compile(
    r"https://console-openshift-console\.apps\.[^\"'<>\s]+",
    re.IGNORECASE,
)
_KUBECONFIG_SUFFIX = "/openshift-cluster-dir/auth/kubeconfig"


def _first_href_matching(description: str, predicate) -> str | None:
    for match in _HREF_RE.finditer(description or ""):
        href = match.group(1)
        if predicate(href):
            return href
    return None


def parse_build_description(description: str) -> dict[str, Any]:
    """
    Extract Magna, kubeconfig, and console URLs from Jenkins description HTML.

    Passwords and login lines are not returned.
    """
    text = description or ""
    magna_dir = _first_href_matching(
        text,
        lambda u: "magna" in u and "openshift-clusters" in u and "kubeconfig" not in u,
    )
    if not magna_dir:
        magna_match = _MAGNA_RE.search(text)
        magna_dir = magna_match.group(1) if magna_match else None

    kubeconfig_url = _first_href_matching(
        text, lambda u: "kubeconfig" in u and "magna" in u
    )
    if not kubeconfig_url and magna_dir:
        kubeconfig_url = magna_dir.rstrip("/") + _KUBECONFIG_SUFFIX

    console_match = _CONSOLE_RE.search(text)
    console_url = console_match.group(0) if console_match else None

    return {
        "magna_dir_url": magna_dir,
        "kubeconfig_url": kubeconfig_url,
        "console_url": console_url,
    }


def infer_topology_hints(parameters: dict[str, Any]) -> dict[str, Any]:
    """Infer topology signals from Jenkins job parameters."""
    cluster_conf = str(parameters.get("CLUSTER_CONF") or "").lower()
    yaml_config = str(parameters.get("YAML_TEXT_CONFIG") or "").lower()
    combined = f"{cluster_conf} {yaml_config}"

    hints: dict[str, Any] = {
        "deploy_edr": _as_bool(parameters.get("DEPLOY_EDR")),
        "mcg_only": _as_bool(parameters.get("MCG_ONLY")),
        "upgrade": _as_bool(parameters.get("UPGRADE")),
        "external_mode": "external" in combined,
        "regional_dr": "regional" in combined or "disaster-recovery" in combined,
        "lso": "lso" in combined or "local_storage" in combined,
        "provider_client": "provider" in combined and "client" in combined,
    }
    return hints


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes"}
