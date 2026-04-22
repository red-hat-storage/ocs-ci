import importlib.util
import logging
import os
import re
import subprocess
import sys

from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def run_report_generator(mg_dir_path: str, report_dir: str, prefix: str) -> None:
    """
    Run the ``must_gather_report_generator`` package after must-gather is complete
    (``python -m must_gather_report_generator``, same as the ``must-gather-report``
    console script). The generator handles tarballs, symlinks, etc.; this hook only
    triggers it after log collection.

    Requires the ocs-ci environment where ``must_gather_report_generator`` is
    installed (editable or otherwise).
    """
    if not config.REPORTING.get("generate_must_gather_report", False):
        return

    if importlib.util.find_spec("must_gather_report_generator") is None:
        return

    mg_dir_path = os.path.abspath(mg_dir_path)
    tarball_path = f"{mg_dir_path}.tar.gz"
    if not (os.path.isdir(mg_dir_path) or os.path.isfile(tarball_path)):
        return

    os.makedirs(report_dir, exist_ok=True)

    safe_prefix = re.sub(r"[^A-Za-z0-9_.-]+", "_", prefix).strip("_")
    text_out = os.path.join(report_dir, f"{safe_prefix}_mg_analysis.txt")
    xml_out = os.path.join(report_dir, f"{safe_prefix}_mg_analysis.xml")

    cmd = [
        sys.executable,
        "-m",
        "must_gather_report_generator",
        mg_dir_path,
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
        logger.info(
            "Must-gather report generated for %s (text=%s xml=%s)",
            mg_dir_path,
            text_out,
            xml_out,
        )
    except subprocess.TimeoutExpired:
        logger.error(
            "Must-gather report generation timed out after 300s for %s",
            mg_dir_path,
        )
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        stdout = (e.stdout or "").strip()
        logger.error(
            "Must-gather report generation failed for %s (exit %s): stderr=%s stdout=%s",
            mg_dir_path,
            e.returncode,
            stderr,
            stdout[:500] if len(stdout) > 500 else stdout,
        )
    except Exception as e:
        logger.error(
            "Unexpected error running must-gather report for %s: %s",
            mg_dir_path,
            e,
            exc_info=True,
        )


def trigger_reports_after_collect_ocs_logs(
    dir_name: str,
    status_failure: bool,
    cluster_configs,
) -> None:
    """
    Trigger report generation for the OCS must-gather directory per cluster.
    Should be called only AFTER collect_ocs_logs() returns.
    """
    if not config.REPORTING.get("generate_must_gather_report", False):
        return

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
        run_report_generator(mg_dir, report_dir, prefix=dir_name)
