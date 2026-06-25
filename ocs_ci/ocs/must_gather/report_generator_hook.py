import importlib.util
import logging
import os
import re
import subprocess
import sys

from ocs_ci.framework import config

logger = logging.getLogger(__name__)

# JUnit testcase property consumed by Data Router / Report Portal (property_filter: ".*")
MUST_GATHER_ANALYSIS_URL_PROPERTY = "must-gather-analysis-url"

# Magna HTTP base for NFS paths when logs_url is not configured (see ocs4-jenkins)
MAGNA_BASE_URL = "http://magna002.ceph.redhat.com"


def local_path_to_logs_url(local_path: str, cluster_config) -> str | None:
    """
    Convert a local log file path to a remote logs URL (Magna) when possible.

    Prefers ``cluster_config.RUN['logs_url']`` (set by Jenkins). Falls back to
    replacing the ``/mnt`` NFS prefix with :data:`MAGNA_BASE_URL`.
    """
    local_path = os.path.abspath(local_path)
    if not os.path.isfile(local_path):
        return None

    logs_url = cluster_config.RUN.get("logs_url")
    log_dir = os.path.abspath(os.path.expanduser(cluster_config.RUN["log_dir"]))
    if logs_url:
        if local_path == log_dir or local_path.startswith(log_dir + os.sep):
            rel = os.path.relpath(local_path, log_dir).replace(os.sep, "/")
            base = logs_url if logs_url.endswith("/") else f"{logs_url}/"
            return f"{base}{rel}"

    if local_path.startswith("/mnt"):
        return local_path.replace("/mnt", MAGNA_BASE_URL, 1)

    return None


def run_report_generator(mg_dir_path: str, report_dir: str, prefix: str) -> str | None:
    """
    Run the ``must_gather_report_generator`` package after must-gather is complete
    (``python -m must_gather_report_generator``, same as the ``must-gather-report``
    console script). The generator handles tarballs, symlinks, etc.; this hook only
    triggers it after log collection.

    Requires the ocs-ci environment where ``must_gather_report_generator`` is
    installed (editable or otherwise).

    Returns:
        str | None: Absolute path to the generated text report, or None on failure.
    """
    if not config.REPORTING.get("generate_must_gather_report", False):
        return None

    if importlib.util.find_spec("must_gather_report_generator") is None:
        return None

    mg_dir_path = os.path.abspath(mg_dir_path)
    tarball_path = f"{mg_dir_path}.tar.gz"
    if not (os.path.isdir(mg_dir_path) or os.path.isfile(tarball_path)):
        return None

    # Use tarball path if directory doesn't exist (happens when tarball_mg_logs + delete_packed_mg_logs are enabled)
    path_to_analyze = mg_dir_path if os.path.isdir(mg_dir_path) else tarball_path

    os.makedirs(report_dir, exist_ok=True)

    safe_prefix = re.sub(r"[^A-Za-z0-9_.-]+", "_", prefix).strip("_")
    text_out = os.path.join(report_dir, f"{safe_prefix}_mg_analysis.txt")
    xml_out = os.path.join(report_dir, f"{safe_prefix}_mg_analysis.xml")

    cmd = [
        sys.executable,
        "-m",
        "must_gather_report_generator",
        path_to_analyze,
        "--output-file",
        text_out,
        "--xml-output",
        xml_out,
    ]
    try:
        subprocess.run(
            cmd,
            check=True,
            timeout=300,
            capture_output=True,
            text=True,
        )
        if not os.path.isfile(text_out):
            logger.error(
                "Must-gather text report missing after generation: %s", text_out
            )
            return None
        logger.info(
            "Must-gather report generated for %s (text=%s xml=%s)",
            path_to_analyze,
            text_out,
            xml_out,
        )
        return text_out
    except subprocess.TimeoutExpired:
        logger.error(
            "Must-gather report generation timed out after 300s for %s",
            path_to_analyze,
        )
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        stdout = (e.stdout or "").strip()
        logger.error(
            "Must-gather report generation failed for %s (exit %s): stderr=%s stdout=%s",
            path_to_analyze,
            e.returncode,
            stderr,
            stdout[:500] if len(stdout) > 500 else stdout,
        )
    except Exception as e:
        logger.error(
            "Unexpected error running must-gather report for %s: %s",
            path_to_analyze,
            e,
            exc_info=True,
        )
    return None


def trigger_reports_after_collect_ocs_logs(
    dir_name: str,
    status_failure: bool,
    cluster_configs,
) -> list[str]:
    """
    Trigger report generation for the OCS must-gather directory per cluster.
    Should be called only AFTER collect_ocs_logs() returns.

    Returns:
        list[str]: Remote (Magna) URLs for generated text reports, one per cluster
            when conversion succeeds.
    """
    if not config.REPORTING.get("generate_must_gather_report", False):
        return []

    report_urls = []
    for cluster in cluster_configs:
        if status_failure:
            base = os.path.join(
                os.path.expanduser(cluster.RUN["log_dir"]),
                f"failed_testcase_ocs_logs_{cluster.RUN['run_id']}",
                f"{dir_name}_ocs_logs",
                f"{cluster.ENV_DATA['cluster_name']}",
            )
        else:
            base = os.path.join(
                os.path.expanduser(cluster.RUN["log_dir"]),
                f"{dir_name}_{cluster.RUN['run_id']}",
                f"{cluster.ENV_DATA['cluster_name']}",
            )

        mg_dir = os.path.join(base, "ocs_must_gather")
        report_dir = os.path.join(base, "must_gather_report")
        text_out = run_report_generator(mg_dir, report_dir, prefix=dir_name)
        if not text_out:
            continue

        report_url = local_path_to_logs_url(text_out, cluster)
        if report_url:
            report_urls.append(report_url)
            logger.info("Must-gather analysis report URL: %s", report_url)
        else:
            logger.info(
                "Must-gather analysis report saved locally (no remote URL): %s",
                text_out,
            )

    return report_urls
