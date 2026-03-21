"""
Periodic scanner for completed OCS-CI test runs with failures.

Scans /mnt/ocsci-jenkins/openshift-clusters/ for j-prefixed directories
containing JUnit XML results with failures, and triggers log analysis
via the CLI for each unprocessed run.

Designed to run as a cron job on the Jenkins agent:
    */5 * * * * /opt/ocs-ci-analysis/venv/bin/python \
        -m ocs_ci.utility.log_analysis.integrations.scanner \
        >> /mnt/ocsci-jenkins/log_analysis/scanner.log 2>&1
"""

import argparse
import fcntl
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)

DEFAULT_SCAN_DIR = "/mnt/ocsci-jenkins/openshift-clusters/"
DEFAULT_STATE_FILE = "/mnt/ocsci-jenkins/log_analysis/session_manage/scanner_state.json"
DEFAULT_OCS_CI_PATH = "/opt/ocs-ci-analysis/ocs-ci"
DEFAULT_HISTORY_BASE = "/mnt/ocsci-jenkins/log_analysis/history_dir"
DEFAULT_CACHE_BASE = "/mnt/ocsci-jenkins/log_analysis/cache_dir"
DEFAULT_MAX_BUDGET = 2.00
DEFAULT_MAX_FAILURES = 70
DEFAULT_MAX_AGE_DAYS = 7
DEFAULT_MAX_RUNS_PER_CYCLE = 5
DEFAULT_PARALLEL = 3
DEFAULT_SESSIONS_BASE = "/mnt/ocsci-jenkins/log_analysis/sessions_dir"
DEFAULT_LOCK_FILE = "/mnt/ocsci-jenkins/log_analysis/session_manage/scanner.lock"
DEFAULT_VERSION_FALLBACK = "4_21"

# Regex to detect failures/errors in JUnit XML without full parsing
_FAILURES_RE = re.compile(r'failures="(\d+)"')
_ERRORS_RE = re.compile(r'errors="(\d+)"')
# Extract timestamp from directory names like j-xxx_20260315T222624
_DIR_TIMESTAMP_RE = re.compile(r"_(\d{8}T\d{6})(?:/|$)")
# Extract rp_ocs_build property value
_OCS_BUILD_RE = re.compile(
    r'<property\s+name="rp_ocs_build"\s+value="([^"]*)"', re.IGNORECASE
)


def load_state(state_file: str) -> dict:
    """Load the scanner state file."""
    if os.path.exists(state_file):
        with open(state_file) as f:
            state = json.load(f)
        if "pending" not in state:
            state["pending"] = []
        return state
    return {"processed": {}, "pending": []}


def save_state(state_file: str, state: dict):
    """Save the scanner state file atomically."""
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    tmp = state_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, state_file)


def has_failures(xml_path: str) -> bool:
    """Quick check if a JUnit XML has failures or errors > 0.

    Reads only the first 4KB (the <testsuite> header) to avoid
    parsing the entire file.
    """
    try:
        with open(xml_path) as f:
            header = f.read(4096)
    except OSError:
        return False

    for match in _FAILURES_RE.finditer(header):
        if int(match.group(1)) > 0:
            return True
    for match in _ERRORS_RE.finditer(header):
        if int(match.group(1)) > 0:
            return True
    return False


def detect_odf_version(xml_path: str) -> str:
    """Extract ODF version from rp_ocs_build in JUnit XML properties.

    Returns version string like '4_17' for use in directory names.
    Falls back to DEFAULT_VERSION_FALLBACK if not found.
    """
    try:
        with open(xml_path) as f:
            # Properties are near the top; read enough to find them
            content = f.read(8192)
    except OSError:
        return DEFAULT_VERSION_FALLBACK

    match = _OCS_BUILD_RE.search(content)
    if not match:
        return DEFAULT_VERSION_FALLBACK

    build_str = match.group(1)  # e.g. "4.17.0-85"
    parts = build_str.split(".")
    if len(parts) >= 2:
        return f"{parts[0]}_{parts[1]}"

    return DEFAULT_VERSION_FALLBACK


def _get_run_sort_key(run: dict) -> str:
    """Extract timestamp from logs_dir path for oldest-first sorting.

    Parses timestamps like '20260315T222624' from directory names.
    Falls back to '0' so runs without timestamps sort first.
    """
    match = _DIR_TIMESTAMP_RE.search(run["logs_dir"])
    return match.group(1) if match else "0"


def find_runs_to_analyze(
    scan_dir: str, state: dict, max_age_days: int = 0
) -> list[dict]:
    """Scan for completed test runs with failures that haven't been processed.

    Directory structure:
        scan_dir/j-*/j-*_TIMESTAMP/logs/test_results_*.xml

    Returns list of dicts with keys: logs_dir, xml_path, version
    """
    runs = []
    processed = state.get("processed", {})
    pending_dirs = {r["logs_dir"] for r in state.get("pending", [])}
    cutoff_time = 0.0
    if max_age_days > 0:
        cutoff_time = time.time() - (max_age_days * 86400)

    try:
        top_entries = os.listdir(scan_dir)
    except OSError as e:
        log.error(f"Cannot list scan directory {scan_dir}: {e}")
        return runs

    for cluster_dir_name in top_entries:
        if not cluster_dir_name.startswith("j"):
            continue

        cluster_path = os.path.join(scan_dir, cluster_dir_name)
        if not os.path.isdir(cluster_path):
            continue

        try:
            run_entries = os.listdir(cluster_path)
        except OSError:
            continue

        for run_dir_name in run_entries:
            run_path = os.path.join(cluster_path, run_dir_name)
            logs_dir = os.path.join(run_path, "logs")
            if not os.path.isdir(logs_dir):
                continue

            # Skip old runs
            if cutoff_time > 0:
                try:
                    if os.path.getmtime(run_path) < cutoff_time:
                        continue
                except OSError:
                    continue

            # Already processed or pending?
            if logs_dir in processed or logs_dir in pending_dirs:
                continue

            # Find test_results XML files
            xml_path = _find_latest_xml(logs_dir)
            if not xml_path:
                continue

            if not has_failures(xml_path):
                continue

            version = detect_odf_version(xml_path)
            runs.append(
                {
                    "logs_dir": logs_dir,
                    "xml_path": xml_path,
                    "version": version,
                }
            )

    return runs


def _find_latest_xml(logs_dir: str) -> str:
    """Find the latest test_results_*.xml in a logs directory."""
    try:
        candidates = [
            f
            for f in os.listdir(logs_dir)
            if f.startswith("test_results") and f.endswith(".xml")
        ]
    except OSError:
        return ""

    if not candidates:
        return ""

    latest = sorted(candidates)[-1]
    return os.path.join(logs_dir, latest)


def git_pull(ocs_ci_path: str):
    """Update the persistent ocs-ci clone."""
    log.info(f"Updating ocs-ci clone at {ocs_ci_path}")
    try:
        result = subprocess.run(
            ["git", "pull", "--ff-only"],
            cwd=ocs_ci_path,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            log.warning(f"git pull failed: {result.stderr.strip()}")
        else:
            log.info(f"git pull: {result.stdout.strip()}")
    except subprocess.TimeoutExpired:
        log.warning("git pull timed out")
    except FileNotFoundError:
        log.warning(f"ocs-ci path does not exist: {ocs_ci_path}")


def _build_analysis_cmd(run: dict, args) -> tuple[list[str], str]:
    """Build the CLI command for a run. Returns (cmd, output_path)."""
    logs_dir = run["logs_dir"]
    version = run["version"]

    history_dir = os.path.join(args.history_base, f"{version}_history_dir")
    cache_dir = os.path.join(args.cache_base, f"{version}_cache_dir")
    sessions_dir = os.path.join(args.sessions_base, f"{version}_sessions_dir")
    output_path = os.path.join(logs_dir, "ai_analysis_report.html")

    cmd = [
        sys.executable,
        "-m",
        "ocs_ci.utility.log_analysis.cli",
        logs_dir,
        "--model",
        "sonnet",
        "--jslave",
        "--record-history",
        "-v",
        "-f",
        "html",
        "-o",
        output_path,
        "--history-dir",
        history_dir,
        "--cache-dir",
        cache_dir,
        "--sessions-dir",
        sessions_dir,
        "--max-budget-usd",
        str(args.max_budget),
        "--max-failures",
        str(args.max_failures),
    ]

    if args.jira_config:
        cmd.extend(["--jira-config", args.jira_config])

    return cmd, output_path


def _log_process_result(proc, run: dict, output_path: str, log_path: str):
    """Log the result of a completed analysis subprocess."""
    logs_dir = run["logs_dir"]
    if proc.returncode == 0:
        log.info(f"  Analysis complete: {output_path} (log: {log_path})")
    else:
        log.error(
            f"  [{logs_dir}] Analysis failed (exit {proc.returncode}), see {log_path}"
        )


def scan(args):
    """Main scan cycle."""
    now = datetime.now(timezone.utc).isoformat()

    # Step 1: Update ocs-ci clone
    if not args.no_git_pull:
        git_pull(args.ocs_ci_path)

    # Step 2: Load state
    state = load_state(args.state_file)

    # Step 3: Discover new runs and merge into pending queue
    new_runs = find_runs_to_analyze(
        args.scan_dir, state, max_age_days=args.max_age_days
    )

    # Drop pending entries whose directories no longer exist
    valid_pending = [r for r in state["pending"] if os.path.isdir(r["logs_dir"])]
    if len(valid_pending) < len(state["pending"]):
        dropped = len(state["pending"]) - len(valid_pending)
        log.info(f"Dropped {dropped} pending run(s) whose directories no longer exist")

    # Merge new discoveries into pending
    if new_runs:
        log.info(f"Discovered {len(new_runs)} new run(s) with failures")
        valid_pending.extend(new_runs)

    # Sort oldest first by timestamp in directory name
    valid_pending.sort(key=_get_run_sort_key)
    state["pending"] = valid_pending
    save_state(args.state_file, state)

    if not valid_pending:
        log.info("No pending runs to analyze")
        return

    log.info(f"{len(valid_pending)} pending run(s) in queue")

    if args.dry_run:
        for run in valid_pending:
            version = run["version"]
            history_dir = os.path.join(args.history_base, f"{version}_history_dir")
            cache_dir = os.path.join(args.cache_base, f"{version}_cache_dir")
            sessions_dir = os.path.join(args.sessions_base, f"{version}_sessions_dir")
            print(f"  {run['logs_dir']}")
            print(f"    ODF version:   {version}")
            print(f"    history-dir:   {history_dir}")
            print(f"    cache-dir:     {cache_dir}")
            print(f"    sessions-dir:  {sessions_dir}")
        return

    # Step 4: Analyze runs (limited per cycle, parallel)
    limit = args.max_runs_per_cycle
    to_process = valid_pending[:limit] if limit > 0 else list(valid_pending)
    if limit > 0 and len(valid_pending) > limit:
        log.info(f"Processing {limit} of {len(valid_pending)} pending runs this cycle")

    parallel = max(1, args.parallel)
    active = {}  # pid -> (proc, run, output_path, log_path, log_fd)
    work_queue = list(to_process)
    total = len(work_queue)
    completed = 0

    while work_queue or active:
        # Launch new processes up to parallel limit
        while work_queue and len(active) < parallel:
            run = work_queue.pop(0)
            cmd, output_path = _build_analysis_cmd(run, args)
            log_path = os.path.join(run["logs_dir"], "ai_analysis.log")
            log.info(
                f"Launching [{completed + len(active) + 1}/{total}]: "
                f"{run['logs_dir']} (ODF {run['version']})"
            )
            log.info(f"  Live log: {log_path}")
            log_fd = open(log_path, "w")
            proc = subprocess.Popen(
                cmd,
                cwd=args.ocs_ci_path,
                stdout=log_fd,
                stderr=log_fd,
            )
            active[proc.pid] = (proc, run, output_path, log_path, log_fd)

        if not active:
            break

        # Wait for any child to finish
        try:
            finished_pid, wait_status = os.waitpid(-1, 0)
        except ChildProcessError:
            break

        if finished_pid in active:
            proc, run, output_path, log_path, log_fd = active.pop(finished_pid)
            log_fd.close()
            # Extract exit code from waitpid status (Popen.returncode stays None
            # when we use os.waitpid directly instead of proc.wait)
            exit_code = os.waitstatus_to_exitcode(wait_status)
            proc.returncode = exit_code
            _log_process_result(proc, run, output_path, log_path)
            success = exit_code == 0
            # Move from pending to processed
            state["processed"][run["logs_dir"]] = {
                "timestamp": now,
                "status": "done" if success else "failed",
                "version": run["version"],
            }
            state["pending"] = [
                r for r in state["pending"] if r["logs_dir"] != run["logs_dir"]
            ]
            save_state(args.state_file, state)
            completed += 1

    remaining = len(state["pending"])
    log.info(f"Cycle complete: {completed} runs processed, {remaining} pending")


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="scanner",
        description="Periodic scanner for OCS-CI test runs with failures",
    )
    parser.add_argument(
        "--scan-dir",
        default=DEFAULT_SCAN_DIR,
        help=f"Base directory to scan (default: {DEFAULT_SCAN_DIR})",
    )
    parser.add_argument(
        "--state-file",
        default=DEFAULT_STATE_FILE,
        help=f"Path to state JSON (default: {DEFAULT_STATE_FILE})",
    )
    parser.add_argument(
        "--ocs-ci-path",
        default=DEFAULT_OCS_CI_PATH,
        help=f"Path to persistent ocs-ci clone (default: {DEFAULT_OCS_CI_PATH})",
    )
    parser.add_argument(
        "--history-base",
        default=DEFAULT_HISTORY_BASE,
        help=f"Base dir for version-specific history (default: {DEFAULT_HISTORY_BASE})",
    )
    parser.add_argument(
        "--cache-base",
        default=DEFAULT_CACHE_BASE,
        help=f"Base dir for version-specific cache (default: {DEFAULT_CACHE_BASE})",
    )
    parser.add_argument(
        "--sessions-base",
        default=DEFAULT_SESSIONS_BASE,
        help=f"Base dir for version-specific sessions (default: {DEFAULT_SESSIONS_BASE})",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=DEFAULT_MAX_AGE_DAYS,
        help=f"Only process runs newer than N days (0=no limit, default: {DEFAULT_MAX_AGE_DAYS})",
    )
    parser.add_argument(
        "--max-runs-per-cycle",
        type=int,
        default=DEFAULT_MAX_RUNS_PER_CYCLE,
        help=f"Max runs to analyze per cycle (0=no limit, default: {DEFAULT_MAX_RUNS_PER_CYCLE})",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=DEFAULT_PARALLEL,
        help=f"Number of parallel analysis runs (default: {DEFAULT_PARALLEL})",
    )
    parser.add_argument(
        "--lock-file",
        default=DEFAULT_LOCK_FILE,
        help=f"Lock file to prevent concurrent runs (default: {DEFAULT_LOCK_FILE})",
    )
    parser.add_argument(
        "--jira-config",
        default=None,
        help="Path to Jira INI config file (passed through to analyze-logs)",
    )
    parser.add_argument(
        "--max-budget",
        type=float,
        default=DEFAULT_MAX_BUDGET,
        help=f"Max spend per analysis in USD (default: {DEFAULT_MAX_BUDGET})",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=DEFAULT_MAX_FAILURES,
        help=f"Max failures to analyze per run (default: {DEFAULT_MAX_FAILURES})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be analyzed without running",
    )
    parser.add_argument(
        "--no-git-pull",
        action="store_true",
        help="Skip the git pull step (useful for development)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    log.info(f"Scanner starting at {datetime.now(timezone.utc).isoformat()}")
    start = time.monotonic()

    # Acquire exclusive lock to prevent concurrent scanner instances
    os.makedirs(os.path.dirname(args.lock_file), exist_ok=True)
    lock_fd = open(args.lock_file, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.info("Another scanner instance is running, exiting")
        lock_fd.close()
        sys.exit(0)

    try:
        lock_fd.write(f"{os.getpid()}\n")
        lock_fd.flush()
        scan(args)
    except Exception:
        log.exception("Scanner failed")
        sys.exit(1)
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()
        elapsed = time.monotonic() - start
        log.info(f"Scanner finished in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
