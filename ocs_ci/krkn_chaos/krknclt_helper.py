"""
Krkctl plan generation for chaos testing.

The plan template (plan.json.j2) matches the workable krknctl plan format: each
scenario key is "scenario_name_{{ suffix }}" and depends_on is "root_{{ suffix }}".
PlanGenerator parses the Jinja template, fills parameters, and writes the plan file;
the instance holds the plan file path after generation.
"""

import json
import logging
import os
import random
import string
import copy
import subprocess
import time

from jinja2 import Template

from ocs_ci.ocs.constants import (
    KRKN_CLOUD_IBM,
    KRKN_CLOUD_VMWARE,
    KRKN_OUTPUT_DIR,
    KRKNCTL_PLAN_TEMPLATE,
    OPENSHIFT_STORAGE_NAMESPACE,
    OSD_APP_LABEL,
    MON_APP_LABEL,
    MGR_APP_LABEL,
    MDS_APP_LABEL,
    RGW_APP_LABEL,
    OPERATOR_LABEL,
    NOOBAA_APP_LABEL,
    RBD_NODEPLUGIN_LABEL,
    CEPHFS_NODEPLUGIN_LABEL,
)
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.krkn_chaos.krkn_chaos import KrKnctlRunner
from ocs_ci.krkn_chaos.krkn_helpers import (
    CephHealthHelper,
    ValidationHelper,
    krknctl_random_test_exit_criteria,
    vsphere_creds_for_krkn_from_ocs_config,
)

log = logging.getLogger(__name__)

# Default interval (seconds) for polling krknctl process and running Ceph crash check.
POLL_INTERVAL = 180


def _normalize_krknctl_cloud_type(cloud_type):
    """
    Map legacy krkn names to values accepted by krknctl ``random run`` validation.

    krknctl allows ``aws,azure,gcp,vmware,ibmcloud,ibmcloudpower,bm`` (not ``ibm``).
    """
    if cloud_type is None:
        return cloud_type
    s = str(cloud_type).strip()
    if not s:
        return cloud_type
    if s.lower() == "ibm":
        return "ibmcloud"
    return s


KRKN_APP_LABEL_CONSTANTS = (
    OSD_APP_LABEL,
    MON_APP_LABEL,
    MGR_APP_LABEL,
    MDS_APP_LABEL,
    RGW_APP_LABEL,
    OPERATOR_LABEL,
    NOOBAA_APP_LABEL,
)
CEPH_APP_SELECTORS = [label.split("=", 1)[1] for label in KRKN_APP_LABEL_CONSTANTS]

# Base scenario names in plan.json.j2 (JSON keys are "<base>_{{ suffix }}"; variants
# use hyphenated bases e.g. network-chaos-ingress-latency_<suffix>). Keep in sync with
# the template when adding scenarios. Used for documentation and PlanGenerator.SCENARIO_NAMES.
KRKNCTL_PLAN_SCENARIO_KEYS = (
    "root",
    "application-outages",
    "application-outages-egress-only",
    "application-outages-ingress-only",
    "application-outages-mds",
    "application-outages-noobaa",
    "application-outages-operator",
    "application-outages-osd",
    "application-outages-rgw",
    "container-scenarios",
    "container-scenarios-mds",
    "container-scenarios-mgr",
    "container-scenarios-mon",
    "container-scenarios-osd",
    "container-scenarios-rgw",
    "kubevirt-outage",
    "network-chaos",
    "network-chaos-egress-loss",
    "network-chaos-egress-latency",
    "network-chaos-egress-serial",
    "network-chaos-ingress-latency",
    "node-cpu-hog",
    "node-cpu-hog-mild",
    "node-io-hog",
    "node-io-hog-heavy",
    "node-memory-hog",
    "node-memory-hog-moderate",
    "node-network-filter",
    "node-scenarios",
    "pod-network-chaos",
    "pod-network-chaos-egress-only",
    "pod-network-chaos-ingress-only",
    "pod-network-filter",
    "pod-scenarios",
    "pod-scenarios-cephfs-plugin",
    "pod-scenarios-mgr",
    "pod-scenarios-mds",
    "pod-scenarios-mon",
    "pod-scenarios-multi-disruption",
    "pod-scenarios-noobaa",
    "pod-scenarios-operator",
    "pod-scenarios-rbd-plugin",
    "pod-scenarios-rgw",
    "service-disruption-scenarios",
    "service-disruption-scenarios-rook",
    "syn-flood",
    "syn-flood-mds",
    "syn-flood-mgr",
    "syn-flood-noobaa",
    "syn-flood-rgw",
    "time-scenarios",
    "time-scenarios-mds",
    "time-scenarios-mgr",
    "time-scenarios-noobaa",
    "time-scenarios-osd",
    "time-scenarios-rgw",
)

ROOT_SCENARIO_KEY = "root"

# Plan with only root + service-disruption-scenarios (for test_random_service_disruption).
SERVICE_DISRUPTION_INCLUDE_SCENARIOS = (
    ROOT_SCENARIO_KEY,
    "service-disruption-scenarios",
)

# Plan with only root + application-outages (expanded per label).
APPLICATION_OUTAGES_INCLUDE_SCENARIOS = (
    ROOT_SCENARIO_KEY,
    "application-outages",
)

# Comprehensive service disruption: root + application-outages, pod-scenarios,
# container-scenarios (expanded per label), plus a single service-disruption-scenarios
# node (namespace targeting only — not expanded per pod label).
COMPREHENSIVE_SERVICE_DISRUPTION_INCLUDE_SCENARIOS = (
    ROOT_SCENARIO_KEY,
    "application-outages",
    "pod-scenarios",
    "container-scenarios",
    "service-disruption-scenarios",
)

# App labels used when expanding application-outages (one node per label).
# Use KRKN_APP_LABEL_CONSTANTS so application-outage covers OSD, MON, MGR, MDS, RGW, operator, Noobaa.
APPLICATION_OUTAGES_APP_LABELS = KRKN_APP_LABEL_CONSTANTS

# krknctl random: base scenario names for a minimal plan (dummy root + node-scenarios only).
KRKNCTL_RANDOM_NODE_SCENARIO_BASES = (
    ROOT_SCENARIO_KEY,
    "node-scenarios",
)

# krknctl random: base scenario names for kubevirt-outage (dummy root + krkn-hub kubevirt-outage).
KRKNCTL_RANDOM_KUBEVIRT_OUTAGE_SCENARIO_BASES = (
    ROOT_SCENARIO_KEY,
    "kubevirt-outage",
)

# krknctl random: base scenario names for time-scenarios (dummy root + krkn-hub time-scenarios).
KRKNCTL_RANDOM_TIME_SCENARIO_BASES = (
    ROOT_SCENARIO_KEY,
    "time-scenarios",
)


def _apply_ibmcloud_node_scenario_auth_from_config(template_vars):
    """
    Populate ``ibm_url``, ``ibm_apikey``, ``ibm_power_url``, ``ibm_power_crn`` for
    ``plan.json.j2`` from ``config.AUTH['ibmcloud']`` (loaded from ``data/auth.yaml``),
    matching :meth:`ocs_ci.krkn_chaos.krkn_chaos.KrKnRunner._run_krkn_command`.

    Only runs when ``cloud_type`` is IBM (``KRKN_CLOUD_IBM``). Does not overwrite keys
    already set (caller should apply env overrides after this).

    Recognized optional keys under ``ibmcloud`` besides ``api_key`` / ``api_endpoint``:
    ``ibm_power_url`` or ``power_url``, ``ibm_power_crn`` or ``power_crn``.
    """
    from ocs_ci.framework import config
    from ocs_ci.utility.ibmcloud import get_ibmcloud_cluster_region

    if template_vars.get("cloud_type") != KRKN_CLOUD_IBM:
        return

    ibmcloud_auth = config.AUTH.get("ibmcloud") or {}
    if not ibmcloud_auth:
        return

    api_key = ibmcloud_auth.get("api_key")
    if api_key:
        template_vars.setdefault("ibm_apikey", api_key)

    api_endpoint = ibmcloud_auth.get("api_endpoint")
    if api_endpoint:
        template_vars.setdefault("ibm_url", api_endpoint)
    elif "ibm_url" not in template_vars:
        try:
            region = get_ibmcloud_cluster_region()
            template_vars["ibm_url"] = f"https://{region}.iaas.cloud.ibm.com/v1"
            log.info(
                "Set krknctl node-scenarios ibm_url from cluster region (no api_endpoint in AUTH)"
            )
        except (CommandFailed, Exception) as e:
            log.warning(
                "Could not derive ibm_url from IBM Cloud region (%s); "
                "set api_endpoint in AUTH['ibmcloud'] or IBMC_URL in the environment",
                e,
            )

    power_url = ibmcloud_auth.get("ibm_power_url") or ibmcloud_auth.get("power_url")
    if power_url:
        template_vars.setdefault("ibm_power_url", power_url)

    power_crn = ibmcloud_auth.get("ibm_power_crn") or ibmcloud_auth.get("power_crn")
    if power_crn:
        template_vars.setdefault("ibm_power_crn", power_crn)


def _apply_vsphere_node_scenario_auth_from_config(template_vars):
    """
    Populate ``vsphere_ip``, ``vsphere_user``, ``vsphere_pass`` for ``plan.json.j2``
    from ``config.AUTH['vmware']`` / ``config.AUTH['vsphere']`` (merged from
    ``data/auth.yaml`` under the ``AUTH`` section), with fallback to ``ENV_DATA``
    keys ``vsphere_server``, ``vsphere_user``, ``vsphere_password`` — same sources as
    :class:`~ocs_ci.ocs.platform_nodes.VMWare` / ``deployment/vmware.py``.

    Only runs when ``cloud_type`` is VMware (``KRKN_CLOUD_VMWARE``). Does not overwrite
    keys already set (caller applies env overrides after this).
    """
    if template_vars.get("cloud_type") != KRKN_CLOUD_VMWARE:
        return

    server, user, password = vsphere_creds_for_krkn_from_ocs_config()
    if server:
        template_vars.setdefault("vsphere_ip", server)
    if user:
        template_vars.setdefault("vsphere_user", user)
    if password:
        template_vars.setdefault("vsphere_pass", password)


def build_krknctl_node_scenario_template_vars():
    """
    Build Jinja context for ``plan.json.j2`` when the plan includes ``node-scenarios``.

    Sets ``cloud_type`` from :func:`~ocs_ci.krkn_chaos.krkn_helpers.get_krkn_cloud_type`
    (calls ``pytest.skip`` on unsupported platforms). For IBM Cloud, fills ``IBMC_*``
    template fields from ``config.AUTH['ibmcloud']`` (``data/auth.yaml``), same as
    :class:`~ocs_ci.krkn_chaos.krkn_chaos.KrKnRunner`. For VMware, fills ``VSPHERE_*``
    template fields from ``config.AUTH['vmware']`` / ``['vsphere']`` when present, else
    from ``ENV_DATA`` (``vsphere_server``, ``vsphere_user``, ``vsphere_password``).
    Environment variables override config for the same keys. Other optional tuning knobs
    come from the process environment (same names as krkn-hub ``node-scenarios`` env keys).

    **Do not** read generic ``NODE_NAME`` for targeting nodes: Jenkins sets ``NODE_NAME``
    to the **agent** hostname, which is not a Kubernetes node name and makes krknctl
    validation fail. Use ``KRKN_NODE_NAME`` to pin cluster node(s) (comma-separated);
    it is mapped to the plan's ``NODE_NAME`` field for krkn-hub.

    Returns:
        dict: Template variables passed to :class:`PlanGenerator`.
    """
    import os

    from ocs_ci.krkn_chaos.krkn_helpers import get_krkn_cloud_type

    cloud_override = os.environ.get("CLOUD_TYPE")
    raw_cloud = cloud_override if cloud_override else get_krkn_cloud_type()
    template_vars = {"cloud_type": _normalize_krknctl_cloud_type(raw_cloud)}
    _apply_ibmcloud_node_scenario_auth_from_config(template_vars)
    _apply_vsphere_node_scenario_auth_from_config(template_vars)
    # Optional: env vars map to Jinja names in plan.json.j2 ``node-scenarios`` block
    env_map = (
        ("ACTION", "node_scenario_action"),
        ("LABEL_SELECTOR", "node_scenario_label"),
        ("EXCLUDE_LABEL", "exclude_label"),
        ("INSTANCE_COUNT", "node_instance_count"),
        ("RUNS", "node_scenario_runs"),
        ("KUBE_CHECK", "kube_check"),
        ("PARALLEL", "parallel"),
        ("TIMEOUT", "node_scenario_timeout"),
        ("DURATION", "node_scenario_duration"),
        ("DISABLE_SSL_VERIFICATION", "disable_ssl_verification"),
        ("VSPHERE_IP", "vsphere_ip"),
        ("VSPHERE_USERNAME", "vsphere_user"),
        ("VSPHERE_PASSWORD", "vsphere_pass"),
        ("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
        ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
        ("AWS_DEFAULT_REGION", "aws_region"),
        ("BMC_USER", "bmc_user"),
        ("BMC_PASSWORD", "bmc_password"),
        ("BMC_ADDR", "bmc_addr"),
        ("DISKS", "disks"),
        ("IBMC_URL", "ibm_url"),
        ("IBMC_POWER_URL", "ibm_power_url"),
        ("IBMC_POWER_CRN", "ibm_power_crn"),
        ("IBMC_APIKEY", "ibm_apikey"),
        ("AZURE_TENANT_ID", "azure_tenant_id"),
        ("AZURE_CLIENT_ID", "azure_client_id"),
        ("AZURE_CLIENT_SECRET", "azure_client_secret"),
        ("AZURE_SUBSCRIPTION_ID", "azure_subscription_id"),
    )
    for env_key, tmpl_key in env_map:
        val = os.environ.get(env_key)
        if val:
            template_vars[tmpl_key] = val
    # Pin cluster node(s) in the plan (krkn-hub env NODE_NAME). Use KRKN_NODE_NAME only —
    # do not use process env NODE_NAME (Jenkins sets that to the agent hostname).
    krkn_node = os.environ.get("KRKN_NODE_NAME", "").strip()
    if krkn_node:
        template_vars["node_name"] = krkn_node
    return template_vars


def build_krknctl_kubevirt_outage_template_vars():
    """
    Build Jinja context for ``plan.json.j2`` when the plan includes ``kubevirt-outage``.

    Target VM is read from ``krkn_config.kubevirt_outage`` in merged ocsci config, or from
    environment variables ``KRKN_KUBEVIRT_NAMESPACE`` / ``KRKN_KUBEVIRT_VM_NAME`` (env wins).
    Calls ``pytest.skip`` if ``vm_name`` is unset (scenario needs a concrete VM).

    Returns:
        dict: Template variables passed to :class:`PlanGenerator`.
    """
    import os

    import pytest

    from ocs_ci.framework import config

    krkn_config = config.ENV_DATA.get("krkn_config", {})
    ko = krkn_config.get("kubevirt_outage", {}) or {}

    namespace = (
        os.environ.get("KRKN_KUBEVIRT_NAMESPACE") or ko.get("namespace") or "default"
    )
    vm_name = (
        os.environ.get("KRKN_KUBEVIRT_VM_NAME") or ko.get("vm_name") or ""
    ).strip()
    if not vm_name:
        pytest.skip(
            "kubevirt-outage requires a target VM: set krkn_config.kubevirt_outage.vm_name "
            "in ocsci config or export KRKN_KUBEVIRT_VM_NAME"
        )

    timeout = ko.get("timeout", 60)
    kill_count = ko.get("kill_count", 1)

    return {
        "kubevirt_namespace": namespace,
        "kubevirt_vm_name": vm_name,
        "kubevirt_timeout": str(timeout),
        "kubevirt_kill_count": str(kill_count),
    }


def build_krknctl_time_scenario_template_vars():
    """
    Build Jinja context for ``plan.json.j2`` when the plan includes ``time-scenarios``.

    Values come from ``krkn_config.time_scenarios`` in merged ocsci config, with optional
    environment overrides (``KRKN_TIME_ACTION``, ``KRKN_TIME_CONTAINER``,
    ``KRKN_TIME_LABEL_SELECTOR``, ``KRKN_TIME_OBJECT_NAME``, ``KRKN_TIME_OBJECT_TYPE``).
    Storage namespace for the scenario comes from :class:`PlanGenerator` ``namespace``.

    Returns:
        dict: Template variables passed to :class:`PlanGenerator`.
    """
    import os

    from ocs_ci.framework import config

    krkn_config = config.ENV_DATA.get("krkn_config", {})
    ts = krkn_config.get("time_scenarios", {}) or {}

    def _pick(cfg_key: str, env_key: str, default: str) -> str:
        v = os.environ.get(env_key)
        if v is not None and v != "":
            return v
        v = ts.get(cfg_key)
        if v is not None and v != "":
            return str(v)
        return default

    return {
        "time_action": _pick("action", "KRKN_TIME_ACTION", "skew_date"),
        "time_container": _pick("container_name", "KRKN_TIME_CONTAINER", ""),
        "time_label_selector": _pick(
            "label_selector", "KRKN_TIME_LABEL_SELECTOR", "app=rook-ceph-mon"
        ),
        "time_object_name": _pick("object_name", "KRKN_TIME_OBJECT_NAME", "[]"),
        "time_object_type": _pick("object_type", "KRKN_TIME_OBJECT_TYPE", "pod"),
    }


def run_krknctl_chaos_and_validate(
    plan_path,
    workload_ops,
    max_parallel,
    run_name,
    failure_context,
    validation_failure_context,
    poll_interval=None,
    namespace=None,
):
    """
    Shared flow: start krknctl in background, poll until exit (with Ceph crash check),
    handle returncode, validate/cleanup workloads, run exit criteria.

    Use this from tests that run krknctl random chaos or service disruption so
    the run, poll, cleanup, and exit-criteria logic stay in one place.

    Args:
        plan_path: Path to the krknctl plan file.
        workload_ops: WorkloadOps fixture instance (or any object with
            setup_workloads(), validate_and_cleanup()).
        max_parallel: Max parallel scenarios for krknctl.
        run_name: Name for poll logs and exit criteria (e.g. "krknctl", "krknctl service disruption").
        failure_context: Short name for CommandFailed message (e.g. "krknctl random run", "krknctl service disruption").
        validation_failure_context: Context for ValidationHelper (e.g. "krknctl-random", "krknctl-service-disruption").
        poll_interval (int, optional): Seconds between polls; default POLL_INTERVAL.
        namespace (str, optional): OpenShift namespace for Ceph check; default OPENSHIFT_STORAGE_NAMESPACE.

    Raises:
        CommandFailed: If krknctl exits with non-zero returncode.
        AssertionError: From exit criteria or ValidationHelper as appropriate.
    """
    if poll_interval is None:
        poll_interval = POLL_INTERVAL
    if namespace is None:
        namespace = OPENSHIFT_STORAGE_NAMESPACE

    log_path = os.path.join(os.path.dirname(plan_path), "krknctl.log")
    runner = KrKnctlRunner()
    process, _ = runner.random_background(
        plan_path,
        log_path=log_path,
        max_parallel=max_parallel,
    )
    log.info(
        "krknctl started in background; log file: %s",
        log_path,
    )

    returncode = poll_krknctl_until_exit(
        process,
        log_path=log_path,
        poll_interval=poll_interval,
        namespace=namespace,
        run_name=run_name,
        workload_ops=workload_ops,
    )
    log.info("krknctl process ended with returncode=%s", returncode)

    if returncode != 0:
        log.error(
            "%s failed (returncode=%s). Check log: %s",
            failure_context,
            returncode,
            log_path,
        )
        try:
            workload_ops.validate_and_cleanup()
        except (UnexpectedBehaviour, CommandFailed) as cleanup_ex:
            log.warning("Workload cleanup after chaos failure: %s", cleanup_ex)
        raise CommandFailed(
            f"{failure_context} failed with returncode={returncode}. Log: {log_path}"
        )

    try:
        workload_ops.validate_and_cleanup()
        log.info("Workloads validated and cleaned up successfully")
    except (UnexpectedBehaviour, CommandFailed) as e:
        ValidationHelper().handle_workload_validation_failure(
            e,
            validation_failure_context,
            run_name,
        )

    krknctl_random_test_exit_criteria(run_name)


def _terminate_krknctl_process(process):
    """
    Stop the krknctl subprocess (SIGTERM, then SIGKILL on timeout).

    Called when aborting early so heartbeat / poll loops do not see a live process.
    """
    if process is None or process.poll() is not None:
        return
    log.warning("Terminating krknctl subprocess after Ceph crash detection")
    try:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            log.warning("krknctl did not exit after SIGTERM; sending SIGKILL")
            process.kill()
            process.wait(timeout=15)
    except Exception as ex:
        log.warning("Error terminating krknctl process: %s", ex)


def _shutdown_chaos_on_ceph_crash(process, workload_ops):
    """
    Best-effort shutdown when periodic Ceph crash check fails: stop bg cluster ops,
    then terminate krknctl so child and heartbeat threads wind down before pytest
    failure hooks (e.g. must-gather) run.
    """
    if workload_ops is not None:
        stop_fn = getattr(
            workload_ops, "stop_background_operations_on_chaos_failure", None
        )
        if callable(stop_fn):
            try:
                stop_fn()
            except Exception as ex:
                log.warning(
                    "Could not stop background cluster operations after Ceph crash: %s",
                    ex,
                )
    _terminate_krknctl_process(process)


def poll_krknctl_until_exit(
    process,
    log_path=None,
    poll_interval=180,
    namespace=None,
    run_name="krknctl",
    workload_ops=None,
):
    """
    Poll a krknctl subprocess until it exits, checking for Ceph crashes every
    poll_interval seconds. Use this in tests that run krknctl in the background
    so that any Ceph crash during the run fails the test immediately and
    generates evidence.

    Args:
        process: Subprocess with .poll() and .returncode (e.g. subprocess.Popen).
        log_path (str, optional): Path to krknctl log file for log messages.
        poll_interval (int): Seconds between polls and Ceph crash checks (default 180).
        namespace (str, optional): OpenShift namespace for Ceph check.
            Defaults to OPENSHIFT_STORAGE_NAMESPACE.
        run_name (str): Short name for log messages (e.g. "krknctl", "krknctl service disruption").
        workload_ops: Optional workload ops object with stop_background_operations_on_chaos_failure();
            used to stop BgOp threads when failing fast on Ceph crash.

    Returns:
        int: Process return code when the process has exited.

    Raises:
        AssertionError: If Ceph crash(es) are detected during the poll loop.
    """
    if namespace is None:
        namespace = OPENSHIFT_STORAGE_NAMESPACE
    health_helper = CephHealthHelper(namespace=namespace)
    while process.poll() is None:
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, f"chaos (periodic check every {poll_interval} s)"
        )
        if not no_crashes and crash_details:
            _shutdown_chaos_on_ceph_crash(process, workload_ops)
            raise AssertionError(
                f"Periodic Ceph crash check failed (every {poll_interval} s). "
                f"Ceph crash detected during chaos; failing test to generate evidence.\n{crash_details}"
            )
        log_msg = f"{run_name} still running"
        if log_path:
            log_msg += f" (log: {log_path})"
        log_msg += f"; next check in {poll_interval} s"
        log.info(log_msg)
        time.sleep(poll_interval)
    return process.returncode


def _full_key(base_name, suffix):
    """Plan key for a scenario: base_name_suffix (e.g. application-outages_5j6t5)."""
    return f"{base_name}_{suffix}"


def _label_to_slug(label_selector):
    """Convert label_selector like 'app=rook-ceph-osd' to a slug 'rook-ceph-osd'."""
    if "=" in label_selector:
        return label_selector.split("=", 1)[1].replace(".", "-")
    return label_selector.replace(".", "-")


def _label_to_short_slug(label_selector):
    """Convert label_selector to short slug for plan keys: 'app=rook-ceph-osd' -> 'osd'."""
    app_value = _label_to_slug(label_selector)
    if app_value.startswith("rook-ceph-"):
        return app_value.split("rook-ceph-", 1)[1]
    return app_value


def _label_to_pod_selector(label_selector):
    """Convert label_selector 'app=rook-ceph-osd' to POD_SELECTOR value '{app: rook-ceph-osd}'."""
    app_value = _label_to_slug(label_selector)
    return f"{{app: {app_value}}}"


def get_worker_instance_count_for_krknctl_plan():
    """
    String worker-node count for ``INSTANCE_COUNT`` defaults in ``plan.json.j2``.

    Uses :func:`~ocs_ci.ocs.node.get_worker_nodes`. Returns ``"1"`` if the list is
    empty or the cluster cannot be queried (e.g. missing kubeconfig).

    Returns:
        str: Number of workers as a decimal string, at least ``"1"``.
    """
    try:
        from ocs_ci.ocs.node import get_worker_nodes

        n = len(get_worker_nodes())
        return str(n) if n > 0 else "1"
    except Exception as e:
        log.warning(
            "Could not get worker node count for krknctl plan; defaulting INSTANCE_COUNT to 1: %s",
            e,
        )
        return "1"


class PlanGenerator:
    """
    Generates krknctl plan JSON files from the Jinja template.

    Holds scenario names and exposes one method that parses the template,
    fills parameters, applies exclusions/overrides, and writes the plan file.
    After generate() is called, plan_path is set to the written file location.
    """

    # Scenario names defined in the plan template (same as KRKNCTL_PLAN_SCENARIO_KEYS).
    SCENARIO_NAMES = KRKNCTL_PLAN_SCENARIO_KEYS

    def __init__(
        self,
        namespace="openshift-storage",
        include_scenarios=None,
        exclude_scenarios=None,
        exclude_scenario_bases_exact=None,
        scenario_overrides=None,
        use_random_selectors=True,
        label_selectors=None,
        **template_vars,
    ):
        self.namespace = namespace
        self.include_scenarios = (
            include_scenarios  # None or list; takes precedence over exclude
        )
        self.exclude_scenarios = exclude_scenarios or []
        # Plan keys "base" before _<suffix>); match == only (no prefix variants), e.g.
        # "service-disruption-scenarios" drops that node but not "service-disruption-scenarios-rook".
        self.exclude_scenario_bases_exact = exclude_scenario_bases_exact or []
        self.scenario_overrides = scenario_overrides or {}
        self.use_random_selectors = use_random_selectors
        self.label_selectors = label_selectors  # list of pod label strings; expands app-outage/pod/container only
        self.template_vars = template_vars
        self.plan_path = None
        self._suffix = None

    def generate(self):
        """
        Parse the Jinja template, fill parameters, apply exclusions and overrides,
        write the plan file, and set self.plan_path to the written path.

        Injects ``worker_instance_count`` (cluster worker count via
        :func:`get_worker_instance_count_for_krknctl_plan`) unless overridden in
        ``template_vars``.

        Returns:
            str: Absolute path to the generated plan JSON file.
        """
        if not os.path.isfile(KRKNCTL_PLAN_TEMPLATE):
            raise FileNotFoundError(
                f"krknctl plan template not found at {KRKNCTL_PLAN_TEMPLATE}"
            )

        self._suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=5)
        )

        worker_instance_count = (
            self.template_vars.get("worker_instance_count")
            or get_worker_instance_count_for_krknctl_plan()
        )

        if self.use_random_selectors:
            pod_app = random.choice(CEPH_APP_SELECTORS)
            label_app = random.choice(CEPH_APP_SELECTORS)
            pod_selector = f"{{app: {pod_app}}}"
            label_selector = f"app={label_app}"
            workers = str(random.randint(1, 6))
        else:
            pod_selector = self.template_vars.get("pod_selector", "")
            label_selector = self.template_vars.get("label_selector", "")
            workers = self.template_vars.get("workers", "1")

        # Component-specific labels must not use global `pod_label` (same as
        # label_selector / random Ceph pick); named blocks in plan.json.j2 now use
        # literal selectors per scenario so random globals do not override them.
        # Generic pod-scenarios / multi-disruption OSD label is also literal; overrides
        # remain available via scenario_overrides / template_vars for custom plans.
        context = {
            "suffix": self._suffix,
            "namespace": self.namespace,
            "pod_selector": pod_selector,
            "label_selector": label_selector,
            "pod_label": label_selector,
            "noobaa_pod_label": self.template_vars.get(
                "noobaa_pod_label", NOOBAA_APP_LABEL
            ),
            "mon_pod_label": self.template_vars.get("mon_pod_label", MON_APP_LABEL),
            "mgr_pod_label": self.template_vars.get("mgr_pod_label", MGR_APP_LABEL),
            "rbd_plugin_pod_label": self.template_vars.get(
                "rbd_plugin_pod_label", RBD_NODEPLUGIN_LABEL
            ),
            "mds_pod_label": self.template_vars.get("mds_pod_label", MDS_APP_LABEL),
            "rgw_pod_label": self.template_vars.get("rgw_pod_label", RGW_APP_LABEL),
            "operator_pod_label": self.template_vars.get(
                "operator_pod_label", OPERATOR_LABEL
            ),
            "cephfs_plugin_pod_label": self.template_vars.get(
                "cephfs_plugin_pod_label", CEPHFS_NODEPLUGIN_LABEL
            ),
            "workers": workers,
            "worker_instance_count": worker_instance_count,
        }
        context.update(self.template_vars)

        with open(KRKNCTL_PLAN_TEMPLATE, "r") as f:
            template = Template(f.read())
        rendered = template.render(**context)
        rendered_stripped = rendered.strip() if rendered else ""
        if not rendered_stripped:
            raise ValueError(
                f"Plan template rendered to empty. Path: {KRKNCTL_PLAN_TEMPLATE}"
            )
        plan_data = json.loads(rendered)

        if self.include_scenarios:
            self._keep_only_included(plan_data)
            if self.label_selectors:
                if "application-outages" in self.include_scenarios:
                    self._expand_application_outages_by_labels(plan_data)
                if "pod-scenarios" in self.include_scenarios:
                    self._expand_pod_scenarios_by_labels(plan_data)
                if "container-scenarios" in self.include_scenarios:
                    self._expand_container_scenarios_by_labels(plan_data)
            if "service-disruption-scenarios" in self.include_scenarios:
                self._normalize_service_disruption_scenario(plan_data)
        else:
            self._remove_excluded(plan_data)
            self._warn_if_root_excluded(plan_data)
        self._apply_overrides(plan_data)

        os.makedirs(KRKN_OUTPUT_DIR, exist_ok=True)
        dir_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        plan_dir = os.path.join(KRKN_OUTPUT_DIR, dir_suffix)
        os.makedirs(plan_dir, exist_ok=True)
        file_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        self.plan_path = os.path.join(plan_dir, f"plan_{file_suffix}.json")
        with open(self.plan_path, "w") as f:
            json.dump(plan_data, f, indent=2)

        log.info(
            "Generated krknctl plan: %s (namespace=%s, suffix=%s, include=%s, "
            "exclude=%s, exclude_bases_exact=%s)",
            self.plan_path,
            self.namespace,
            self._suffix,
            self.include_scenarios,
            self.exclude_scenarios,
            self.exclude_scenario_bases_exact,
        )
        return os.path.abspath(self.plan_path)

    def _keep_only_included(self, plan_data):
        """Keep only scenarios in include_scenarios (and _comment keys). Root must be in include_scenarios if needed."""
        include_set = set(self.include_scenarios)
        for k in list(plan_data.keys()):
            if k.startswith("_"):
                continue
            base = k.rsplit("_", 1)[0] if "_" in k else k
            if base not in include_set:
                del plan_data[k]
                log.debug("Removed scenario (not in include list): %s", base)

    def _expand_application_outages_by_labels(self, plan_data):
        """
        Replace the single application-outages node with one node per label in
        label_selectors, each with its own POD_SELECTOR. Keys become
        application-outages_<short_slug>_<suffix> (e.g. application-outages_osd_xxyyzz).
        """
        base_key = _full_key("application-outages", self._suffix)
        if base_key not in plan_data:
            log.warning(
                "application-outages key %s not in plan; skip expand by labels",
                base_key,
            )
            return
        template_node = plan_data.pop(base_key)
        root_key = _full_key(ROOT_SCENARIO_KEY, self._suffix)
        for label in self.label_selectors:
            node = copy.deepcopy(template_node)
            node.setdefault("env", {})["POD_SELECTOR"] = _label_to_pod_selector(label)
            if "depends_on" in node:
                node["depends_on"] = root_key
            short_slug = _label_to_short_slug(label)
            new_key = f"application-outages_{short_slug}_{self._suffix}"
            plan_data[new_key] = node
            log.debug("Added application-outages node for label: %s", label)

    def _expand_pod_scenarios_by_labels(self, plan_data):
        """
        Replace the single pod-scenarios node with one node per label in
        label_selectors, each with its own POD_LABEL. Keys become
        pod-scenarios_<short_slug>_<suffix>.
        """
        base_key = _full_key("pod-scenarios", self._suffix)
        if base_key not in plan_data:
            log.warning(
                "pod-scenarios key %s not in plan; skip expand by labels",
                base_key,
            )
            return
        template_node = plan_data.pop(base_key)
        root_key = _full_key(ROOT_SCENARIO_KEY, self._suffix)
        for label in self.label_selectors:
            node = copy.deepcopy(template_node)
            node.setdefault("env", {})["POD_LABEL"] = label
            if "depends_on" in node:
                node["depends_on"] = root_key
            short_slug = _label_to_short_slug(label)
            new_key = f"pod-scenarios_{short_slug}_{self._suffix}"
            plan_data[new_key] = node
            log.debug("Added pod-scenarios node for label: %s", label)

    def _expand_container_scenarios_by_labels(self, plan_data):
        """
        Replace the single container-scenarios node with one node per label in
        label_selectors, each with its own LABEL_SELECTOR. Keys become
        container-scenarios_<short_slug>_<suffix>.
        """
        base_key = _full_key("container-scenarios", self._suffix)
        if base_key not in plan_data:
            log.warning(
                "container-scenarios key %s not in plan; skip expand by labels",
                base_key,
            )
            return
        template_node = plan_data.pop(base_key)
        root_key = _full_key(ROOT_SCENARIO_KEY, self._suffix)
        for label in self.label_selectors:
            node = copy.deepcopy(template_node)
            node.setdefault("env", {})["LABEL_SELECTOR"] = label
            if "depends_on" in node:
                node["depends_on"] = root_key
            short_slug = _label_to_short_slug(label)
            new_key = f"container-scenarios_{short_slug}_{self._suffix}"
            plan_data[new_key] = node
            log.debug("Added container-scenarios node for label: %s", label)

    def _normalize_service_disruption_scenario(self, plan_data):
        """
        Ensure service-disruption-scenarios targets a namespace only.

        Krkn-hub's service-disruption plugin disrupts namespaces (not pods).
        LABEL_SELECTOR selects namespace labels when NAMESPACE is unset; it is not
        a pod label. Mutually exclusive with NAMESPACE — use storage namespace name
        and empty LABEL_SELECTOR. Do not expand per pod label (unlike pod/container
        scenarios).
        """
        base_key = _full_key("service-disruption-scenarios", self._suffix)
        if base_key not in plan_data:
            log.warning(
                "service-disruption-scenarios key %s not in plan; skip normalize",
                base_key,
            )
            return
        node = plan_data[base_key]
        env = node.setdefault("env", {})
        env["NAMESPACE"] = self.namespace
        env["LABEL_SELECTOR"] = ""
        log.debug(
            "service-disruption-scenarios: namespace=%s, LABEL_SELECTOR cleared (namespace-only)",
            self.namespace,
        )

    def _remove_excluded(self, plan_data):
        """
        Drop excluded scenario nodes.

        ``exclude_scenario_bases_exact``: plan key base must match exactly (no
        ``base.startswith(ex + "-")``), e.g. remove ``service-disruption-scenarios``
        (namespace + ``LABEL_SELECTOR: ""`` in plan.json.j2) but keep
        ``service-disruption-scenarios-rook``.

        ``exclude_scenarios``: a base like ``network-chaos`` also removes hyphenated
        variants (``network-chaos-ingress-latency``, etc.) so exclude lists match
        krkn-hub scenario families in plan.json.j2.
        """
        exact_set = set(self.exclude_scenario_bases_exact)
        for k in list(plan_data.keys()):
            if k.startswith("_"):
                continue
            base = k.rsplit("_", 1)[0] if "_" in k else k
            for ex in exact_set:
                if base == ex:
                    del plan_data[k]
                    log.debug(
                        "Excluded scenario from plan: %s (exact base, matched %s)",
                        k,
                        ex,
                    )
                    break

        excluded_set = set(self.exclude_scenarios)
        for k in list(plan_data.keys()):
            if k.startswith("_"):
                continue
            base = k.rsplit("_", 1)[0] if "_" in k else k
            for ex in excluded_set:
                if base == ex or base.startswith(ex + "-"):
                    del plan_data[k]
                    log.debug(
                        "Excluded scenario from plan: %s (matched exclude %s)",
                        base,
                        ex,
                    )
                    break

    def _warn_if_root_excluded(self, plan_data):
        if ROOT_SCENARIO_KEY not in self.exclude_scenarios:
            return
        root_key = _full_key(ROOT_SCENARIO_KEY, self._suffix)
        remaining = [k for k in plan_data if not k.startswith("_") and k != root_key]
        if remaining:
            log.warning(
                "Root scenario is excluded but %s remain; DAG may be invalid (depends_on root).",
                remaining,
            )

    def _apply_overrides(self, plan_data):
        if not self.scenario_overrides:
            return
        for base_name, overrides in self.scenario_overrides.items():
            key = _full_key(base_name, self._suffix)
            if key not in plan_data or not isinstance(plan_data[key], dict):
                log.warning(
                    "scenario_overrides key %s not in plan, skipping", base_name
                )
                continue
            scenario = plan_data[key]
            if "env" in overrides and isinstance(overrides["env"], dict):
                env = scenario.setdefault("env", {})
                for k, v in overrides["env"].items():
                    env[k] = str(v)
                    log.debug("Override %s env %s = %s", base_name, k, v)


def generate_plan_file(
    namespace="openshift-storage",
    include_scenarios=None,
    exclude_scenarios=None,
    exclude_scenario_bases_exact=None,
    scenario_overrides=None,
    use_random_selectors=True,
    **template_vars,
):
    """
    Generate a krknctl plan JSON file from the Jinja template.

    Uses PlanGenerator: parses template, fills parameters, writes file.
    When include_scenarios is set, only those scenarios (plus root if listed) are kept;
    otherwise exclude_scenarios is used to remove scenarios.
    ``exclude_scenario_bases_exact`` removes only exact plan key bases (see PlanGenerator).
    Returns the plan file path.
    """
    generator = PlanGenerator(
        namespace=namespace,
        include_scenarios=include_scenarios,
        exclude_scenarios=exclude_scenarios,
        exclude_scenario_bases_exact=exclude_scenario_bases_exact,
        scenario_overrides=scenario_overrides,
        use_random_selectors=use_random_selectors,
        **template_vars,
    )
    return generator.generate()


def generate_random_plan_file(
    namespace="openshift-storage",
    include_scenarios=None,
    exclude_scenarios=None,
    exclude_scenario_bases_exact=None,
    scenario_overrides=None,
    **kwargs,
):
    """
    Generate a plan file with random pod/label selectors.

    Convenience wrapper: creates PlanGenerator with use_random_selectors=True
    and returns the plan path.
    """
    return generate_plan_file(
        namespace=namespace,
        include_scenarios=include_scenarios,
        exclude_scenarios=exclude_scenarios,
        exclude_scenario_bases_exact=exclude_scenario_bases_exact,
        scenario_overrides=scenario_overrides,
        use_random_selectors=True,
        **kwargs,
    )
