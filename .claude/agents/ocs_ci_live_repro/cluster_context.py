"""Resolve cluster metadata via ocs_ci_run."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

_AGENT_DIR = Path(__file__).resolve().parent
_OCS_CI_RUN_DIR = _AGENT_DIR.parent / "ocs_ci_run"

if str(_OCS_CI_RUN_DIR) not in sys.path:
    sys.path.insert(0, str(_OCS_CI_RUN_DIR))

log = logging.getLogger(__name__)


def resolve_cluster_profile(
    deploy_job_url: str,
    *,
    dry_run: bool = True,
    download_kubeconfig: bool | None = None,
) -> dict[str, Any]:
    """
    Resolve Jenkins deploy build URL to cluster metadata.

    Phase A (dry_run) fetches Jenkins build metadata only — no kubeconfig download.
    """
    from job_resolver import resolve_job

    if not deploy_job_url:
        raise ValueError("deploy_job_url is required to resolve cluster profile")

    fetch_kubeconfig = (
        not dry_run if download_kubeconfig is None else download_kubeconfig
    )
    profile = resolve_job(
        deploy_job_url,
        download_kubeconfig=fetch_kubeconfig,
    )
    log.info(
        "Resolved cluster %s (ocs=%s, ocp=%s)",
        profile.cluster_name,
        profile.ocs_version,
        profile.ocp_version,
    )
    return profile.to_dict()
