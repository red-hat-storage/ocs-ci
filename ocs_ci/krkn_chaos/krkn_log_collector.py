"""Copy Krkn run artifacts into the collected ocs-ci log directories.

Krkn writes scenario YAMLs and runner logs under ``data/krkn_scenarios`` and
``data/krkn_output``. Jenkins archives ``RUN.log_dir`` (pytest logs and
must-gather), so those trees are copied into the collected log path.
"""

import logging
import os
import shutil

from ocs_ci.framework import config
from ocs_ci.ocs.constants import KRKN_CHAOS_SCENARIO_DIR, KRKN_OUTPUT_DIR

log = logging.getLogger(__name__)

KRKN_RUN_LOGS_SUBDIR = "krkn_run_logs"


def _copy_tree_if_populated(src, dest):
    """Copy ``src`` to ``dest`` when the source exists and is not empty."""
    if not os.path.isdir(src):
        log.info("Krkn run logs: source %s not found; skipping", src)
        return False
    try:
        entries = os.listdir(src)
    except OSError:
        log.warning("Krkn run logs: cannot list %s; skipping", src, exc_info=True)
        return False
    if not entries:
        log.info("Krkn run logs: source %s is empty; skipping", src)
        return False
    parent = os.path.dirname(dest)
    if parent:
        os.makedirs(parent, exist_ok=True)
    log.info("Krkn run logs: copying %s to %s", src, dest)
    shutil.copytree(src, dest, dirs_exist_ok=True)
    return True


def collect_krkn_run_logs(destination_dir):
    """
    Copy Krkn scenario and output directories into ``destination_dir``.

    Creates ``destination_dir/krkn_scenarios`` and
    ``destination_dir/krkn_output`` when those source trees exist and contain
    files. Missing or empty sources are skipped.

    Args:
        destination_dir (str): Directory that should receive the copies.

    Returns:
        bool: True if at least one tree was copied.
    """
    if not destination_dir:
        log.warning("Krkn run logs: no destination directory provided")
        return False

    copied = False
    trees = (
        ("krkn_scenarios", KRKN_CHAOS_SCENARIO_DIR),
        ("krkn_output", KRKN_OUTPUT_DIR),
    )
    for name, src in trees:
        dest = os.path.join(destination_dir, name)
        try:
            if _copy_tree_if_populated(src, dest):
                copied = True
        except Exception:
            log.exception("Krkn run logs: failed to copy %s to %s", src, dest)
    return copied


def get_krkn_logs_collection_dir(dir_name, status_failure=True):
    """
    Return the Krkn log path used by :func:`ocs_ci.ocs.utils.collect_ocs_logs`.

    Args:
        dir_name (str): Test / directory name passed to collect_ocs_logs.
        status_failure (bool): True for the failed-test must-gather layout.

    Returns:
        str: Absolute path of the ``krkn_run_logs`` directory.
    """
    log_dir = os.path.expanduser(config.RUN["log_dir"])
    run_id = config.RUN["run_id"]
    if status_failure:
        return os.path.join(
            log_dir,
            f"failed_testcase_ocs_logs_{run_id}",
            f"{dir_name}_ocs_logs",
            KRKN_RUN_LOGS_SUBDIR,
        )
    return os.path.join(log_dir, f"{dir_name}_{run_id}", KRKN_RUN_LOGS_SUBDIR)


def collect_krkn_run_logs_for_current_test():
    """
    Copy Krkn run logs under ``ocs-ci-logs-<run_id>/<test_name>/krkn_run_logs``.

    Returns:
        bool: True if at least one tree was copied.
    """
    from ocs_ci.utility.utils import ocsci_log_path

    pytest_name = os.environ.get("PYTEST_CURRENT_TEST")
    if pytest_name:
        test_name = pytest_name.split(":")[-1].split(" ")[0]
    else:
        test_name = "unknown_test"
    dest = os.path.join(ocsci_log_path(), test_name, KRKN_RUN_LOGS_SUBDIR)
    log.info("Krkn run logs: collecting into %s", dest)
    return collect_krkn_run_logs(dest)
