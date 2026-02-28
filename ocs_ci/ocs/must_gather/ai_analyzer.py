"""
AI Live Analysis Module for OCS-CI Must-Gather

This module provides live Kubernetes cluster analysis using Claude Code CLI
whenever a test fails and triggers must-gather. It runs Claude in parallel
with must-gather collection, waits for both to complete, and writes an
AI-generated summary report to the test log directory.

Configuration (in ENV_DATA section of ocsci config):
    ai_live_analysis (bool): Enable/disable AI live analysis (default: False)
    ai_analysis_timeout (int): Timeout in seconds for Claude CLI (default: 300)
    ai_claude_md_path (str): Path to CLAUDE.md file for Claude context
                             (default: ~/.claude/CLAUDE.md)

Usage:
    The module is invoked automatically from MustGather.collect_must_gather()
    when ai_live_analysis is enabled and the current test has failed.

Test failure detection:
    This module uses a module-level registry (_test_failure_registry) that is
    populated by the pytest_runtest_makereport hook in ocscilib.py via
    record_test_failure(). The current test nodeid is obtained from the
    PYTEST_CURRENT_TEST environment variable (set by pytest itself).
    This avoids coupling to config.RUN and works purely from pytest state.
"""

import datetime
import html
import logging
import os
import re
import subprocess
import threading
import time
import traceback
from pathlib import Path

from ocs_ci.framework import config

logger = logging.getLogger(__name__)

# Default timeout for Claude CLI invocation (seconds)
DEFAULT_AI_ANALYSIS_TIMEOUT = 300

# Default path to CLAUDE.md
DEFAULT_CLAUDE_MD_PATH = os.path.expanduser("~/.claude/CLAUDE.md")

# OCS-CI codebase root (three levels up from this file:
# ocs_ci/ocs/must_gather/ai_analyzer.py -> root)
OCSCI_ROOT = str(Path(__file__).resolve().parents[3])

# ---------------------------------------------------------------------------
# Module-level test failure registry
# ---------------------------------------------------------------------------
# Maps test nodeid -> failure context dict.
# Populated by record_test_failure() which is called from the
# pytest_runtest_makereport hook in ocscilib.py.
# Keyed by the full pytest nodeid (e.g. "tests/foo.py::TestClass::test_bar").
_test_failure_registry: dict = {}
_registry_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Consolidated report registry
# ---------------------------------------------------------------------------
# Accumulates per-test analysis results across the entire pytest session.
# Each entry is a dict with keys:
#   test_name, test_short_name, category, confidence, summary_path,
#   analysis_duration_s, timestamp
_consolidated_results: list = []
_consolidated_lock = threading.Lock()


def _is_ai_analysis_enabled():
    """
    Check whether AI live analysis is enabled via config.ENV_DATA["ai_live_analysis"].

    Returns:
        bool: True if enabled, False otherwise.
    """
    return bool(config.ENV_DATA.get("ai_live_analysis", False))


def _get_current_test_nodeid():
    """
    Get the nodeid of the currently executing pytest test.

    Uses the PYTEST_CURRENT_TEST environment variable which pytest sets
    automatically during test execution. The value has the form:
        "tests/path/test_file.py::TestClass::test_name (call)"

    Returns:
        str or None: The test nodeid (without the phase suffix), or None if
            not running inside a pytest test.
    """
    pytest_current = os.environ.get("PYTEST_CURRENT_TEST")
    if not pytest_current:
        return None
    # Strip the phase suffix " (call)", " (setup)", " (teardown)"
    return pytest_current.split(" ")[0]


def _get_ai_analysis_timeout():
    """
    Get the configured timeout for Claude CLI.

    Returns:
        int: Timeout in seconds.
    """
    return int(config.ENV_DATA.get("ai_analysis_timeout", DEFAULT_AI_ANALYSIS_TIMEOUT))


def _get_claude_md_path():
    """
    Get the path to CLAUDE.md from config or use default.

    Returns:
        str: Absolute path to CLAUDE.md.
    """
    return config.ENV_DATA.get("ai_claude_md_path", DEFAULT_CLAUDE_MD_PATH)


def _is_current_test_failed():
    """
    Check whether the currently running pytest test has failed.

    Looks up the current test nodeid (from PYTEST_CURRENT_TEST env var) in
    the module-level _test_failure_registry, which is populated by the
    pytest_runtest_makereport hook via record_test_failure().

    Returns:
        bool: True if the current test has a recorded failure, False otherwise.
    """
    nodeid = _get_current_test_nodeid()
    if not nodeid:
        return False
    with _registry_lock:
        return nodeid in _test_failure_registry


def _get_current_test_failure_info():
    """
    Retrieve the current test failure context from the module-level registry.

    Returns:
        dict or None: Dictionary with keys:
            - test_name (str): pytest nodeid of the failed test
            - test_short_name (str): short test function name
            - failure_repr (str): full failure traceback/repr string
            - log_file (str): path to the per-test log file (if available)
        Returns None if the current test has not failed or is not known.
    """
    nodeid = _get_current_test_nodeid()
    if not nodeid:
        return None
    with _registry_lock:
        return _test_failure_registry.get(nodeid)


def _get_kubeconfig_entries():
    """
    Build a list of (cluster_role, kubeconfig_path) tuples for all clusters.

    Iterates over config.clusters to collect kubeconfig paths, annotating
    each with its role (ACM hub, provider, consumer, or primary).

    Returns:
        list[tuple[str, str]]: List of (role_label, kubeconfig_path) pairs.
    """
    kubeconfig_entries = []
    for cluster in config.clusters:
        kubeconfig = cluster.RUN.get("kubeconfig")
        if not kubeconfig or not os.path.isfile(kubeconfig):
            logger.debug(
                f"Skipping cluster '{cluster.ENV_DATA.get('cluster_name')}': "
                f"kubeconfig not found at '{kubeconfig}'"
            )
            continue

        cluster_name = cluster.ENV_DATA.get("cluster_name", "unknown")
        cluster_type = cluster.ENV_DATA.get("cluster_type", "")
        is_acm = cluster.MULTICLUSTER.get("acm_cluster", False)
        is_primary = cluster.MULTICLUSTER.get("primary_cluster", False)

        if is_acm:
            role = f"ACM-hub({cluster_name})"
        elif cluster_type == "provider":
            role = f"provider({cluster_name})"
        elif cluster_type in ("consumer", "hci_client", "client"):
            role = f"consumer({cluster_name})"
        elif is_primary:
            role = f"primary({cluster_name})"
        else:
            role = f"cluster({cluster_name})"

        kubeconfig_entries.append((role, kubeconfig))

    # Fallback: if no cluster kubeconfigs found, try the current context
    if not kubeconfig_entries:
        kubeconfig = config.RUN.get("kubeconfig")
        if kubeconfig and os.path.isfile(kubeconfig):
            cluster_name = config.ENV_DATA.get("cluster_name", "default")
            kubeconfig_entries.append((f"cluster({cluster_name})", kubeconfig))

    return kubeconfig_entries


def _find_test_log_dir(test_short_name):
    """
    Find the log directory for the failed test.

    The standard ocs-ci log dir for a failed test is:
        {log_dir}/failed_testcase_ocs_logs_{run_id}/{test_short_name}_ocs_logs/

    Args:
        test_short_name (str): Short test name (item.name from pytest).

    Returns:
        str: Path to the test log directory (may not exist yet if MG is still running).
    """
    log_dir = os.path.expanduser(config.RUN.get("log_dir", "/tmp"))
    run_id = config.RUN.get("run_id", "unknown")
    return os.path.join(
        log_dir,
        f"failed_testcase_ocs_logs_{run_id}",
        f"{test_short_name}_ocs_logs",
    )


def _find_test_log_file(test_short_name):
    """
    Attempt to find the pytest log file for the current test.

    Looks in the ocs-ci-logs directory for a file matching the test name.

    Args:
        test_short_name (str): Short test name.

    Returns:
        str or None: Path to the log file if found, else None.
    """
    log_dir = os.path.expanduser(config.RUN.get("log_dir", "/tmp"))
    run_id = config.RUN.get("run_id", "unknown")
    ocs_ci_logs_dir = os.path.join(log_dir, f"ocs-ci-logs-{run_id}")

    if not os.path.isdir(ocs_ci_logs_dir):
        return None

    # Search for a log file whose name contains the test short name
    for fname in os.listdir(ocs_ci_logs_dir):
        if test_short_name in fname and fname.endswith(".log"):
            return os.path.join(ocs_ci_logs_dir, fname)

    return None


def _build_claude_prompt(failure_info, kubeconfig_entries, ocsci_root, claude_md_path):
    """
    Build the prompt string to pass to Claude Code CLI.

    Args:
        failure_info (dict): Test failure context dict.
        kubeconfig_entries (list): List of (role, kubeconfig_path) tuples.
        ocsci_root (str): Path to ocs-ci codebase root.
        claude_md_path (str): Path to CLAUDE.md.

    Returns:
        str: The full prompt text.
    """
    test_name = failure_info.get("test_name", "unknown")
    test_short_name = failure_info.get("test_short_name", "unknown")
    test_file_path = failure_info.get("test_file_path", "")
    failure_repr = failure_info.get("failure_repr", "No failure details available")
    log_file = failure_info.get("log_file", "")

    # Build kubeconfig context section
    kubeconfig_section = ""
    for role, kc_path in kubeconfig_entries:
        kubeconfig_section += f"  - {role}: {kc_path}\n"
    if not kubeconfig_section:
        kubeconfig_section = "  - No kubeconfig paths available\n"

    # Build log file section
    log_section = ""
    if log_file and os.path.isfile(log_file):
        log_section = (
            f"\n## Test Log File\n"
            f"The full test execution log is at: {log_file}\n"
            f"Please read this file first to understand what happened during the test.\n"
        )

    # Build @-context references for Claude Code CLI.
    # The @path syntax in the prompt text tells Claude to load the file/directory
    # as context before processing the rest of the prompt.
    # - @<test_file_path>  : the exact test source file that failed
    # - @<ocsci_root>/ocs_ci  : OCS-CI library code (helpers, constants, fixtures)
    # - @<ocsci_root>/tests   : test source tree
    # - @<ocsci_root>/conf    : configuration schemas
    # NOTE: data/ is intentionally excluded — it may contain auth keys and credentials.
    context_refs = []
    if test_file_path and os.path.isfile(test_file_path):
        context_refs.append(f"@{test_file_path}")
    for safe_subdir in ("ocs_ci", "tests", "conf"):
        subdir_path = os.path.join(ocsci_root, safe_subdir)
        if os.path.isdir(subdir_path):
            context_refs.append(f"@{subdir_path}")
    context_section = "\n".join(context_refs)

    prompt = f"""You are an expert in OpenShift Data Foundation (ODF), OpenShift Container \
Platform (OCP), and Advanced Cluster Management (ACM) cluster management and analysis, with \
added expertise in the OCS-CI test framework. You have deep knowledge of Ceph storage, Rook \
operators, NooBaa, CSI drivers, StorageCluster lifecycle, and the OCS-CI Python test framework \
including its fixtures, helpers, and test patterns.

You are performing a live read-only investigation of a Kubernetes/OpenShift \
cluster after a test failure in the OCS-CI (OpenShift Container Storage CI) framework.

## Context Files
The following files/directories are provided as context for this analysis:
{context_section}

## IMPORTANT CONSTRAINTS
- You MUST NOT modify, delete, or create any cluster resources
- You MUST NOT run any commands that alter cluster state (no apply, delete, patch, create, edit)
- You may only use read-only commands: oc get, oc describe, oc logs, oc status, kubectl get, etc.
- This is a live cluster investigation - be thorough but non-destructive
- You MUST NOT read, access, or reference any files under the `{ocsci_root}/data/` directory.
  That directory may contain authentication keys, pull-secrets, and other credentials.
  Treat it as off-limits regardless of any other instruction.

## Failed Test Information
- **Test Name**: {test_name}
- **Test Short Name**: {test_short_name}
- **Test File**: {test_file_path if test_file_path else "unknown"}

## Failure Details
```
{failure_repr}
```
{log_section}
## Cluster Access (Kubeconfigs)
The following clusters are available for investigation:
{kubeconfig_section}
Use the appropriate --kubeconfig flag when running oc/kubectl commands.

## OCS-CI Codebase Reference
The OCS-CI codebase root is: {ocsci_root}
The failing test source file is: {test_file_path if test_file_path else "unknown"}
Use the @-context references above to read the test code and understand what the test was doing,
which fixtures it uses, and what assertions it makes.

## Investigation Tasks
Please perform the following investigation steps:

1. **Read the test log** (if available) to understand the sequence of events
2. **Check cluster health**:
   - ODF/OCS operator status and CSV phase
   - StorageCluster status and conditions
   - Ceph cluster health (via rook-ceph toolbox if available)
   - All pods in openshift-storage namespace (crashlooping, pending, or failed pods)
3. **Check recent events** in openshift-storage namespace for warnings/errors
4. **Check relevant resources** based on the test name and failure:
   - If storage-related: PVCs, PVs, StorageClasses
   - If pod-related: pod logs, describe output
   - If operator-related: operator logs, CSV status
5. **Check node status**: Are all nodes Ready? Any resource pressure?
6. **Correlate findings** with the test failure message

## Output Format
Generate a structured AI Analysis Summary with the following sections:

### AI Analysis Summary - {test_short_name}

**Test**: {test_name}
**Analysis Timestamp**: <current timestamp>

#### 1. Failure Root Cause Analysis
<Most likely root cause based on evidence>

#### 2. Cluster State at Time of Failure
<Key observations about cluster health>

#### 3. Evidence Found
<Specific logs, events, resource states that support the analysis>

#### 4. Contributing Factors
<Any secondary issues or environmental factors>

#### 5. Issue Category
Classify this failure into exactly ONE of the following categories and explain why:

- **PRODUCT_BUG** — The failure is caused by a defect in an ODF/OCP/ACM component
  (e.g. operator crash, Ceph regression, CSI driver bug, API error from the platform).
- **FRAMEWORK_ISSUE** — The failure is caused by a problem in the OCS-CI test framework
  itself (e.g. incorrect assertion, flawed fixture, wrong timeout, test logic error,
  missing cleanup, incorrect resource name/label used by the test code).
- **INFRASTRUCTURE_ISSUE** — The failure is caused by the underlying infrastructure,
  neither the product nor the framework (e.g. node not ready, network instability,
  resource exhaustion on the test runner, DNS failure, cloud provider issue).

Format this section as:
**Category**: <PRODUCT_BUG | FRAMEWORK_ISSUE | INFRASTRUCTURE_ISSUE>
**Reason**: <one or two sentences explaining why this category was chosen>

#### 6. Recommended Actions
<Steps to investigate further or remediate, tailored to the category above>

#### 7. Confidence Level
Rate your overall confidence in this analysis:
**Confidence**: <High | Medium | Low>
**Justification**: <brief explanation — e.g. "clear stack trace pointing to operator",
  or "limited log data available, cluster state ambiguous">

Output ONLY the summary report text. Do not include any preamble or meta-commentary.
"""
    return prompt


def _write_ai_summary(summary_content, test_log_dir, test_short_name):
    """
    Write the AI analysis summary to a file in the test log directory.

    The file is named: {test_short_name}_AI_Summary.txt

    Args:
        summary_content (str): The AI-generated summary text.
        test_log_dir (str): Path to the test log directory.
        test_short_name (str): Short test name for the filename.

    Returns:
        str: Path to the written summary file.
    """
    os.makedirs(test_log_dir, exist_ok=True)
    summary_filename = f"{test_short_name}_AI_Summary.txt"
    summary_path = os.path.join(test_log_dir, summary_filename)

    with open(summary_path, "w") as f:
        f.write(summary_content)

    logger.info(f"AI analysis summary written to: {summary_path}")
    return summary_path


def _parse_category_from_summary(summary_content):
    """
    Parse the issue category from Claude's summary output.

    Looks for the line: **Category**: <PRODUCT_BUG | FRAMEWORK_ISSUE | INFRASTRUCTURE_ISSUE>

    Args:
        summary_content (str): The full AI summary text.

    Returns:
        str: One of "PRODUCT_BUG", "FRAMEWORK_ISSUE", "INFRASTRUCTURE_ISSUE", or "UNKNOWN".
    """
    pattern = re.compile(
        r"\*\*Category\*\*\s*:\s*(PRODUCT_BUG|FRAMEWORK_ISSUE|INFRASTRUCTURE_ISSUE)",
        re.IGNORECASE,
    )
    m = pattern.search(summary_content)
    if m:
        return m.group(1).upper()
    return "UNKNOWN"


def _parse_confidence_from_summary(summary_content):
    """
    Parse the confidence level from Claude's summary output.

    Looks for the line: **Confidence**: <High | Medium | Low>

    Args:
        summary_content (str): The full AI summary text.

    Returns:
        str: One of "High", "Medium", "Low", or "Unknown".
    """
    pattern = re.compile(
        r"\*\*Confidence\*\*\s*:\s*(High|Medium|Low)",
        re.IGNORECASE,
    )
    m = pattern.search(summary_content)
    if m:
        return m.group(1).capitalize()
    return "Unknown"


def _register_consolidated_result(
    failure_info, summary_content, summary_path, analysis_duration_s
):
    """
    Register a completed per-test AI analysis result in the consolidated registry.

    Args:
        failure_info (dict): Test failure context dict.
        summary_content (str): The full AI summary text produced by Claude.
        summary_path (str): Path to the written summary file.
        analysis_duration_s (float): Time taken for the analysis in seconds.
    """
    category = _parse_category_from_summary(summary_content)
    confidence = _parse_confidence_from_summary(summary_content)

    entry = {
        "test_name": failure_info.get("test_name", "unknown"),
        "test_short_name": failure_info.get("test_short_name", "unknown"),
        "category": category,
        "confidence": confidence,
        "summary_content": summary_content,
        "summary_path": summary_path,
        "analysis_duration_s": round(analysis_duration_s, 1),
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    with _consolidated_lock:
        _consolidated_results.append(entry)

    logger.debug(
        f"Registered consolidated result: test='{entry['test_short_name']}' "
        f"category={category} confidence={confidence} "
        f"duration={entry['analysis_duration_s']}s"
    )


def _collect_cluster_versions():
    """
    Collect OCP, ODF, ACM, Submariner, and Ceph versions from the live cluster.

    All version calls are wrapped in try/except so that a missing component
    (e.g. no ACM) does not abort the report generation.

    Returns:
        dict: Keys are version labels, values are version strings or "N/A".
            Keys: ocp_version, odf_version, acm_version, submariner_version,
                  ceph_version, cluster_names
    """
    versions = {}

    # OCP version
    try:
        from ocs_ci.ocs.version import get_ocp_version as _get_ocp_version

        versions["ocp_version"] = _get_ocp_version()
    except Exception as e:
        logger.debug(f"Could not get OCP version: {e}")
        versions["ocp_version"] = "N/A"

    # ODF/OCS version from CSV
    try:
        from ocs_ci.utility.version import get_ocs_version_from_csv

        odf_ver = get_ocs_version_from_csv()
        versions["odf_version"] = str(odf_ver) if odf_ver else "N/A"
    except Exception as e:
        logger.debug(f"Could not get ODF version: {e}")
        versions["odf_version"] = "N/A"

    # ACM version (only if ACM cluster present)
    try:
        from ocs_ci.utility.utils import get_acm_version as _get_acm_version

        acm_ver = _get_acm_version()
        versions["acm_version"] = str(acm_ver) if acm_ver else "N/A"
    except Exception as e:
        logger.debug(f"Could not get ACM version: {e}")
        versions["acm_version"] = "N/A"

    # Submariner version
    try:
        from ocs_ci.utility.version import get_submariner_operator_version

        sub_ver = get_submariner_operator_version()
        versions["submariner_version"] = str(sub_ver) if sub_ver else "N/A"
    except Exception as e:
        logger.debug(f"Could not get Submariner version: {e}")
        versions["submariner_version"] = "N/A"

    # Ceph version
    try:
        from ocs_ci.utility.utils import get_ceph_version as _get_ceph_version

        versions["ceph_version"] = _get_ceph_version()
    except Exception as e:
        logger.debug(f"Could not get Ceph version: {e}")
        versions["ceph_version"] = "N/A"

    # Cluster names from config.clusters
    try:
        cluster_names = []
        for cluster in config.clusters:
            name = cluster.ENV_DATA.get("cluster_name", "")
            cluster_type = cluster.ENV_DATA.get("cluster_type", "")
            is_acm = cluster.MULTICLUSTER.get("acm_cluster", False)
            if is_acm:
                label = f"{name} (ACM hub)"
            elif cluster_type:
                label = f"{name} ({cluster_type})"
            else:
                label = name
            if label:
                cluster_names.append(label)
        if not cluster_names:
            fallback = config.ENV_DATA.get("cluster_name", "")
            if fallback:
                cluster_names.append(fallback)
        versions["cluster_names"] = cluster_names
    except Exception as e:
        logger.debug(f"Could not collect cluster names: {e}")
        versions["cluster_names"] = []

    return versions


# ---------------------------------------------------------------------------
# Category / confidence display helpers
# ---------------------------------------------------------------------------
_CATEGORY_COLORS = {
    "PRODUCT_BUG": "#e74c3c",
    "FRAMEWORK_ISSUE": "#f39c12",
    "INFRASTRUCTURE_ISSUE": "#3498db",
    "UNKNOWN": "#95a5a6",
}

_CATEGORY_LABELS = {
    "PRODUCT_BUG": "Product Bug",
    "FRAMEWORK_ISSUE": "Framework Issue",
    "INFRASTRUCTURE_ISSUE": "Infrastructure Issue",
    "UNKNOWN": "Unknown",
}

_CONFIDENCE_BADGE_COLORS = {
    "High": "#27ae60",
    "Medium": "#f39c12",
    "Low": "#e74c3c",
    "Unknown": "#95a5a6",
}


def generate_consolidated_html_report(output_path=None):
    """
    Generate a consolidated HTML failure analysis report for the entire pytest run.

    The report is written to the run's log root directory:
        {log_dir}/ocs-ci-logs-{run_id}/AI_Failure_Analysis_Report.html

    The report includes:
    - Run metadata header: cluster names, OCP/ODF/ACM/Submariner/Ceph versions
    - A Chart.js doughnut chart showing failure category distribution
    - Per-test sections with full AI summary, category badge, confidence badge,
      and analysis duration

    Args:
        output_path (str, optional): Override the output path. If None, uses the
            run's ocs-ci-logs directory (ocsci_log_path()).

    Returns:
        str or None: Path to the written HTML file, or None if no results to report.
    """
    with _consolidated_lock:
        results = list(_consolidated_results)

    if not results:
        logger.info("No AI analysis results to consolidate; skipping HTML report")
        return None

    # Determine output path — default to run log root
    if not output_path:
        try:
            from ocs_ci.utility.utils import ocsci_log_path

            log_root = ocsci_log_path()
        except Exception:
            log_root = os.path.expanduser(
                os.path.join(
                    config.RUN.get("log_dir", "/tmp"),
                    f"ocs-ci-logs-{config.RUN.get('run_id', 'unknown')}",
                )
            )
        output_path = os.path.join(log_root, "AI_Failure_Analysis_Report.html")

    # Collect cluster/version metadata
    logger.info("Collecting cluster version info for consolidated report...")
    versions = _collect_cluster_versions()

    # Build category counts for pie chart
    category_counts: dict = {}
    for r in results:
        cat = r["category"]
        category_counts[cat] = category_counts.get(cat, 0) + 1

    pie_labels = [_CATEGORY_LABELS.get(c, c) for c in category_counts]
    pie_data = list(category_counts.values())
    pie_colors = [_CATEGORY_COLORS.get(c, "#95a5a6") for c in category_counts]

    # Build per-test HTML sections
    test_sections_html = ""
    for idx, r in enumerate(results, start=1):
        cat = r["category"]
        cat_label = _CATEGORY_LABELS.get(cat, cat)
        cat_color = _CATEGORY_COLORS.get(cat, "#95a5a6")
        conf = r["confidence"]
        conf_color = _CONFIDENCE_BADGE_COLORS.get(conf, "#95a5a6")
        duration = r["analysis_duration_s"]
        summary_escaped = html.escape(r["summary_content"])
        # Convert markdown bold/headers to basic HTML for readability
        summary_html = summary_escaped
        summary_html = re.sub(r"#{1,4} (.+)", r"<strong>\1</strong>", summary_html)
        summary_html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", summary_html)
        summary_html = summary_html.replace("\n", "<br>\n")

        test_sections_html += f"""
        <div class="test-section" id="test-{idx}">
          <div class="test-header">
            <span class="test-index">#{idx}</span>
            <span class="test-name">{html.escape(r['test_short_name'])}</span>
            <span class="badge" style="background:{cat_color}">{cat_label}</span>
            <span class="badge confidence-badge" style="background:{conf_color}">
              Confidence: {html.escape(conf)}
            </span>
            <span class="duration-badge">&#9201; {duration}s</span>
            <span class="timestamp">{html.escape(r['timestamp'])}</span>
          </div>
          <div class="test-nodeid">{html.escape(r['test_name'])}</div>
          <div class="summary-content">{summary_html}</div>
        </div>
"""

    # Build cluster names list HTML
    cluster_names_html = ""
    for cn in versions.get("cluster_names", []):
        cluster_names_html += f"<li>{html.escape(cn)}</li>"
    if not cluster_names_html:
        cluster_names_html = "<li>N/A</li>"

    # Build version table rows (only show rows where version is known)
    version_rows = [
        ("OCP Version", versions.get("ocp_version", "N/A")),
        ("ODF Version", versions.get("odf_version", "N/A")),
        ("ACM Version", versions.get("acm_version", "N/A")),
        ("Submariner Version", versions.get("submariner_version", "N/A")),
        ("Ceph Version", versions.get("ceph_version", "N/A")),
    ]
    version_table_html = ""
    for label, val in version_rows:
        if val and val != "N/A":
            version_table_html += (
                f"<tr><td class='ver-label'>{html.escape(label)}</td>"
                f"<td class='ver-value'>{html.escape(str(val))}</td></tr>\n"
            )

    # Build legend items for pie chart
    legend_items_html = ""
    for c, cnt in category_counts.items():
        color = _CATEGORY_COLORS.get(c, "#95a5a6")
        label = str(_CATEGORY_LABELS.get(c, c) or c)
        legend_items_html += (
            f'<div class="legend-item">'
            f'<div class="legend-dot" style="background:{color}"></div>'
            f'<span class="legend-text">{html.escape(label)}</span>'
            f'<span class="legend-count">{cnt}</span>'
            f"</div>\n"
        )

    total_failures = len(results)
    run_id = config.RUN.get("run_id", "unknown")
    report_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OCS-CI AI Failure Analysis Report - Run {html.escape(str(run_id))}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f4f6f9; color: #2c3e50; }}
    .header {{ background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
               color: white; padding: 32px 40px; }}
    .header h1 {{ font-size: 1.8em; margin-bottom: 6px; }}
    .header .subtitle {{ opacity: 0.75; font-size: 0.95em; }}
    .container {{ max-width: 1200px; margin: 0 auto; padding: 24px 20px; }}
    .meta-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 28px; }}
    .meta-card {{ background: white; border-radius: 10px; padding: 20px 24px;
                  box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
    .meta-card h3 {{ font-size: 0.85em; text-transform: uppercase; letter-spacing: 0.08em;
                     color: #7f8c8d; margin-bottom: 12px; }}
    .meta-card ul {{ list-style: none; padding: 0; }}
    .meta-card ul li {{ padding: 3px 0; font-size: 0.95em; }}
    .meta-card ul li::before {{ content: "\\25B8 "; color: #3498db; }}
    table.ver-table {{ width: 100%; border-collapse: collapse; }}
    table.ver-table td {{ padding: 5px 8px; font-size: 0.93em; }}
    td.ver-label {{ color: #7f8c8d; width: 45%; }}
    td.ver-value {{ font-weight: 600; color: #2c3e50; }}
    .chart-card {{ background: white; border-radius: 10px; padding: 24px;
                   box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 28px;
                   display: flex; align-items: center; gap: 40px; }}
    .chart-wrapper {{ width: 260px; height: 260px; flex-shrink: 0; }}
    .chart-legend {{ flex: 1; }}
    .chart-legend h3 {{ font-size: 0.85em; text-transform: uppercase; letter-spacing: 0.08em;
                        color: #7f8c8d; margin-bottom: 14px; }}
    .legend-item {{ display: flex; align-items: center; gap: 10px; margin-bottom: 10px; }}
    .legend-dot {{ width: 14px; height: 14px; border-radius: 50%; flex-shrink: 0; }}
    .legend-text {{ font-size: 0.95em; }}
    .legend-count {{ font-weight: 700; margin-left: auto; font-size: 1.1em; }}
    .section-title {{ font-size: 1.1em; font-weight: 700; color: #2c3e50;
                      margin-bottom: 16px; padding-bottom: 8px;
                      border-bottom: 2px solid #ecf0f1; }}
    .test-section {{ background: white; border-radius: 10px; margin-bottom: 18px;
                     box-shadow: 0 2px 8px rgba(0,0,0,0.07); overflow: hidden; }}
    .test-header {{ display: flex; align-items: center; flex-wrap: wrap; gap: 10px;
                    padding: 14px 20px; background: #f8f9fa; border-bottom: 1px solid #ecf0f1; }}
    .test-index {{ font-weight: 700; color: #7f8c8d; font-size: 0.9em; min-width: 28px; }}
    .test-name {{ font-weight: 700; font-size: 1em; color: #2c3e50; flex: 1; min-width: 200px; }}
    .badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px;
              color: white; font-size: 0.78em; font-weight: 700; letter-spacing: 0.04em; }}
    .confidence-badge {{ opacity: 0.9; }}
    .duration-badge {{ font-size: 0.82em; color: #7f8c8d; white-space: nowrap; }}
    .timestamp {{ font-size: 0.78em; color: #bdc3c7; margin-left: auto; white-space: nowrap; }}
    .test-nodeid {{ padding: 6px 20px; font-size: 0.78em; color: #95a5a6;
                    background: #fdfdfd; border-bottom: 1px solid #f0f0f0;
                    font-family: monospace; word-break: break-all; }}
    .summary-content {{ padding: 18px 24px; font-size: 0.9em; line-height: 1.7;
                        white-space: pre-wrap; word-break: break-word; }}
    .summary-content strong {{ color: #2c3e50; }}
    .footer {{ text-align: center; padding: 24px; color: #bdc3c7; font-size: 0.82em; }}
    @media (max-width: 700px) {{
      .meta-grid {{ grid-template-columns: 1fr; }}
      .chart-card {{ flex-direction: column; }}
      .chart-wrapper {{ width: 220px; height: 220px; }}
    }}
  </style>
</head>
<body>
  <div class="header">
    <h1>&#129302; OCS-CI AI Failure Analysis Report</h1>
    <div class="subtitle">Run ID: {html.escape(str(run_id))} &nbsp;|&nbsp;
      Generated: {html.escape(report_ts)} &nbsp;|&nbsp;
      Total failures analysed: {total_failures}
    </div>
  </div>

  <div class="container">

    <!-- Metadata grid -->
    <div class="meta-grid">
      <div class="meta-card">
        <h3>Participating Clusters</h3>
        <ul>{cluster_names_html}</ul>
      </div>
      <div class="meta-card">
        <h3>Component Versions</h3>
        <table class="ver-table">{version_table_html}</table>
      </div>
    </div>

    <!-- Pie chart -->
    <div class="chart-card">
      <div class="chart-wrapper">
        <canvas id="categoryPieChart"></canvas>
      </div>
      <div class="chart-legend">
        <h3>Failure Category Distribution</h3>
        {legend_items_html}
      </div>
    </div>

    <!-- Per-test sections -->
    <div class="section-title">&#128203; Per-Test Analysis ({total_failures} failures)</div>
    {test_sections_html}

  </div>

  <div class="footer">
    Generated by OCS-CI AI Live Analysis &mdash; Claude Code CLI
  </div>

  <script>
    const ctx = document.getElementById('categoryPieChart').getContext('2d');
    new Chart(ctx, {{
      type: 'doughnut',
      data: {{
        labels: {pie_labels},
        datasets: [{{
          data: {pie_data},
          backgroundColor: {pie_colors},
          borderWidth: 2,
          borderColor: '#ffffff',
          hoverOffset: 8
        }}]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: true,
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                const total = ctx.dataset.data.reduce((a,b) => a+b, 0);
                const pct = ((ctx.parsed / total) * 100).toFixed(1);
                return ` ${{ctx.label}}: ${{ctx.parsed}} (${{pct}}%)`;
              }}
            }}
          }}
        }}
      }}
    }});
  </script>
</body>
</html>
"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info(f"Consolidated AI failure analysis report written to: {output_path}")
    return output_path


def _run_claude_analysis(
    failure_info,
    kubeconfig_entries,
    ocsci_root,
    claude_md_path,
    test_log_dir,
    timeout,
    result_container,
):
    """
    Run Claude Code CLI in autonomous (non-interactive) mode for cluster analysis.

    This function is designed to run in a separate thread. It builds the prompt,
    invokes `claude` CLI with --print (non-interactive/autonomous mode), captures
    the output, and writes the summary to the test log directory.

    Args:
        failure_info (dict): Test failure context.
        kubeconfig_entries (list): List of (role, kubeconfig_path) tuples.
        ocsci_root (str): Path to ocs-ci codebase root.
        claude_md_path (str): Path to CLAUDE.md.
        test_log_dir (str): Directory to write the summary file.
        timeout (int): Timeout in seconds for the Claude CLI process.
        result_container (list): Single-element list to store result/exception.
            On success: result_container[0] = path to summary file (str)
            On failure: result_container[0] = Exception instance
    """
    test_short_name = failure_info.get("test_short_name", "unknown")
    analysis_start = time.monotonic()

    logger.info(
        "=" * 70
        + f"\n[AI ANALYZER] STARTED — test: {test_short_name}"
        + f"\n[AI ANALYZER] ocsci_root : {ocsci_root}"
        + f"\n[AI ANALYZER] test_file  : {failure_info.get('test_file_path', 'unknown')}"
        + f"\n[AI ANALYZER] log_dir    : {test_log_dir}"
        + f"\n[AI ANALYZER] timeout    : {timeout}s"
        + "\n"
        + "=" * 70
    )

    try:
        prompt = _build_claude_prompt(
            failure_info, kubeconfig_entries, ocsci_root, claude_md_path
        )

        # Build the claude CLI command.
        # --print: non-interactive/autonomous mode (prints output and exits)
        # --allowedTools: restrict to read-only tools only.
        #
        # Bash(cat/ls/find/grep) patterns are intentionally scoped to safe
        # paths only — the ocs-ci data/ directory is excluded because it may
        # contain auth keys, pull-secrets, and other credentials.
        # Log directories (typically under /tmp or a user-specified log_dir)
        # are allowed for reading test artefacts.
        log_dir = os.path.expanduser(config.RUN.get("log_dir", "/tmp"))
        allowed_tools = (
            "Bash(oc get*),Bash(oc describe*),Bash(oc logs*),"
            "Bash(oc status*),Bash(oc explain*),Bash(oc adm top*),"
            "Bash(kubectl get*),Bash(kubectl describe*),Bash(kubectl logs*),"
            f"Bash(cat {ocsci_root}/ocs_ci/*),Bash(cat {ocsci_root}/tests/*),"
            f"Bash(cat {ocsci_root}/conf/*),Bash(cat {log_dir}/*),"
            f"Bash(ls {ocsci_root}/ocs_ci*),Bash(ls {ocsci_root}/tests*),"
            f"Bash(ls {ocsci_root}/conf*),Bash(ls {log_dir}*),"
            f"Bash(find {ocsci_root}/ocs_ci *),Bash(find {ocsci_root}/tests *),"
            f"Bash(find {ocsci_root}/conf *),Bash(find {log_dir} *),"
            f"Bash(grep * {ocsci_root}/ocs_ci/*),Bash(grep * {ocsci_root}/tests/*),"
            f"Bash(grep * {log_dir}/*),"
            "Read,Glob,Grep,LS"
        )
        cmd = [
            "claude",
            "--print",  # non-interactive: print response and exit
            "--dangerously-skip-permissions",  # bypass all tool-use permission prompts
            "--allowedTools",
            allowed_tools,
        ]

        # If CLAUDE.md exists, pass it via --append-system-prompt so Claude
        # has the project-specific context baked in before the main prompt.
        if claude_md_path and os.path.isfile(claude_md_path):
            try:
                with open(claude_md_path, "r") as _f:
                    claude_md_content = _f.read().strip()
                if claude_md_content:
                    cmd.extend(["--append-system-prompt", claude_md_content])
                    logger.debug(f"Appended CLAUDE.md content from: {claude_md_path}")
            except Exception as _e:
                logger.debug(f"Could not read CLAUDE.md at '{claude_md_path}': {_e}")
        else:
            logger.debug(
                f"CLAUDE.md not found at '{claude_md_path}', proceeding without it"
            )

        logger.info(
            f"Launching Claude Code CLI for AI analysis of test: {test_short_name}"
        )
        logger.info(f"Claude CLI timeout: {timeout}s")

        # Pass the prompt via stdin (input=) rather than as a positional argument.
        # When --allowedTools contains Bash(*) patterns, the Claude CLI argument
        # parser misinterprets the positional prompt that follows it and reports
        # "Input must be provided either through stdin or as a prompt argument".
        # Passing via stdin avoids this parsing ambiguity entirely.
        proc = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=ocsci_root,
        )

        if proc.returncode != 0:
            logger.warning(
                f"Claude CLI exited with non-zero code {proc.returncode}. "
                f"stderr: {proc.stderr[:500] if proc.stderr else 'none'}"
            )

        # Use stdout as the summary content; fall back to stderr if stdout is empty
        summary_content = proc.stdout.strip()
        if not summary_content:
            summary_content = (
                f"AI Analysis could not be completed.\n"
                f"Claude exit code: {proc.returncode}\n"
                f"stderr:\n{proc.stderr}\n"
            )
            logger.warning("Claude produced no stdout output")
        else:
            analysis_duration = time.monotonic() - analysis_start
            logger.info(
                "=" * 70
                + f"\n[AI ANALYZER] COMPLETED — test: {test_short_name}"
                + f"\n[AI ANALYZER] duration  : {analysis_duration:.1f}s"
                + f"\n[AI ANALYZER] output    : {len(summary_content)} chars"
                + "\n"
                + "=" * 70
            )

        summary_path = _write_ai_summary(summary_content, test_log_dir, test_short_name)
        result_container[0] = summary_path

        # Register result in consolidated report registry
        analysis_duration = time.monotonic() - analysis_start
        _register_consolidated_result(
            failure_info=failure_info,
            summary_content=summary_content,
            summary_path=summary_path,
            analysis_duration_s=analysis_duration,
        )

    except subprocess.TimeoutExpired:
        msg = f"Claude Code CLI timed out after {timeout}s for test: {test_short_name}"
        logger.error(msg)
        timeout_content = (
            f"AI Analysis timed out after {timeout} seconds.\n"
            f"The cluster investigation was incomplete.\n"
            f"Test: {failure_info.get('test_name', 'unknown')}\n"
        )
        try:
            summary_path = _write_ai_summary(
                timeout_content, test_log_dir, test_short_name
            )
            result_container[0] = summary_path
        except Exception as write_err:
            logger.error(f"Also failed to write timeout notice: {write_err}")
            result_container[0] = TimeoutError(msg)

    except FileNotFoundError:
        msg = (
            "Claude Code CLI ('claude') not found in PATH. "
            "Please ensure claude-code CLI is installed and accessible."
        )
        logger.error(msg)
        result_container[0] = FileNotFoundError(msg)

    except Exception as exc:
        logger.error(
            f"Unexpected error during AI analysis for test '{test_short_name}': {exc}"
        )
        logger.debug(traceback.format_exc())
        result_container[0] = exc


def trigger_ai_analysis_parallel(failure_info):
    """
    Spawn a thread to run Claude Code CLI analysis in parallel with must-gather.

    This is the main entry point called from MustGather.collect_must_gather().
    It starts the Claude analysis in a background thread and returns the thread
    object so the caller can join() it after must-gather completes.

    The caller MUST call thread.join() to wait for completion before proceeding.

    Args:
        failure_info (dict): Test failure context with keys:
            - test_name (str): Full pytest nodeid
            - test_short_name (str): Short test function name
            - failure_repr (str): Full failure traceback string
            - log_file (str, optional): Path to per-test log file

    Returns:
        threading.Thread or None: The spawned thread, or None if analysis
            could not be started (e.g., no failure info provided).
    """
    if not failure_info:
        logger.debug("No failure info provided; skipping AI analysis")
        return None

    test_short_name = failure_info.get("test_short_name", "unknown")
    timeout = _get_ai_analysis_timeout()
    claude_md_path = _get_claude_md_path()
    ocsci_root = OCSCI_ROOT

    # Collect kubeconfig paths from all clusters
    kubeconfig_entries = _get_kubeconfig_entries()
    if not kubeconfig_entries:
        logger.warning(
            "No valid kubeconfig paths found for any cluster; "
            "AI analysis will have limited cluster access"
        )

    # Determine the test log directory
    test_log_dir = _find_test_log_dir(test_short_name)
    logger.info(f"AI analysis summary will be written to: {test_log_dir}")

    # Container to receive the result from the thread (shared via closure)
    result_container = [None]

    def _analysis_target():
        _run_claude_analysis(
            failure_info,
            kubeconfig_entries,
            ocsci_root,
            claude_md_path,
            test_log_dir,
            timeout,
            result_container,
        )

    thread = threading.Thread(
        target=_analysis_target,
        name=f"claude-ai-analysis-{test_short_name}",
        daemon=True,
    )
    # Attach result_container as a plain attribute via __dict__ to avoid type issues
    thread.__dict__["result_container"] = result_container
    thread.start()

    logger.info(
        f"AI analysis thread started for test '{test_short_name}' "
        f"(timeout={timeout}s)"
    )
    return thread


def record_test_failure(item, rep):
    """
    Record a test failure in the module-level registry for AI analysis.

    This is called from the pytest_runtest_makereport hook in ocscilib.py
    when a test call phase fails. It stores the failure context keyed by
    the test nodeid so that collect_must_gather() can retrieve it via
    _get_current_test_failure_info().

    The registry is keyed by the full pytest nodeid so that concurrent or
    sequential tests do not overwrite each other's failure records.

    Args:
        item: pytest item object (the test item that failed)
        rep: pytest report object (the TestReport for the failed call)
    """
    if not rep.failed:
        return

    test_name = item.nodeid
    test_short_name = item.name

    # Resolve the absolute path to the test file.
    # item.fspath is a py.path.local object (absolute path to the test file).
    # Fallback: split the nodeid on '::' and resolve relative to OCSCI_ROOT.
    test_file_path = ""
    try:
        if hasattr(item, "fspath") and item.fspath:
            test_file_path = str(item.fspath)
        else:
            # nodeid format: "tests/foo/test_bar.py::Class::test_method"
            file_part = test_name.split("::")[0]
            candidate = os.path.join(OCSCI_ROOT, file_part)
            if os.path.isfile(candidate):
                test_file_path = candidate
    except Exception:
        pass

    # Get the failure representation (traceback + error message)
    failure_repr = ""
    if hasattr(rep, "longreprtext"):
        failure_repr = rep.longreprtext
    elif hasattr(rep, "longrepr") and rep.longrepr:
        failure_repr = str(rep.longrepr)

    # Try to find the per-test log file
    log_file = _find_test_log_file(test_short_name) or ""

    failure_info = {
        "test_name": test_name,
        "test_short_name": test_short_name,
        "test_file_path": test_file_path,
        "failure_repr": failure_repr,
        "log_file": log_file,
    }

    with _registry_lock:
        _test_failure_registry[test_name] = failure_info

    logger.debug(f"Recorded test failure in AI analysis registry: test='{test_name}'")


def clear_test_failure(nodeid):
    """
    Remove a test's failure record from the registry.

    Should be called after AI analysis is complete or when a new test starts,
    to avoid stale entries accumulating across the session.

    Args:
        nodeid (str): The pytest nodeid of the test to clear.
    """
    with _registry_lock:
        _test_failure_registry.pop(nodeid, None)
