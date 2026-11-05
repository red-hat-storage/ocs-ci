"""
Helpers for the MCG (NooBaa) performance profiles feature.

A performance profile is selected through
spec.multiCloudGateway.performanceProfile on the StorageCluster CR. The
ocs-operator copies it to spec.performanceProfile on the NooBaa CR, and the
NooBaa operator resolves the profile into concrete CPU/memory requests and
limits, endpoint min/max count, DB instance count and PV pool agent resources.

This module holds the expected profile values and the assertions shared by the
performance profile test modules.
"""

import json
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


# StorageCluster keys under spec.resources that override a profile for a single
# component. Note that these live in the top level spec.resources map and NOT
# under spec.multiCloudGateway - there is no multiCloudGateway.coreResources or
# multiCloudGateway.dbResources field in the StorageCluster CRD. See
# setNooBaaDesiredState in
# ocs-operator/internal/controller/storagecluster/noobaa_system_reconciler.go
SC_RESOURCE_KEY_CORE = "noobaa-core"
SC_RESOURCE_KEY_DB = "noobaa-db"
SC_RESOURCE_KEY_ENDPOINT = "noobaa-endpoint"

# Import MCG profile constants from constants.py
SC_PROFILES = constants.MCG_PROFILES
NOOBAA_ONLY_PROFILES = constants.MCG_ONLY_PROFILES

# Binary memory suffixes, the only ones the profile specs and the NooBaa CRs use
MEMORY_UNITS = {
    "Ki": constants.BYTES_IN_KB,
    "Mi": constants.BYTES_IN_MB,
    "Gi": constants.BYTES_IN_GB,
}

QOS_BURSTABLE = "Burstable"
QOS_GUARANTEED = "Guaranteed"


def _resources(req_cpu, lim_cpu, req_mem, lim_mem, qos):
    """
    Build one component entry of a profile specification.

    Args:
        req_cpu (str): Expected CPU request
        lim_cpu (str): Expected CPU limit
        req_mem (str): Expected memory request
        lim_mem (str): Expected memory limit
        qos (str): Expected pod QoS class

    Returns:
        dict: Component specification
    """
    return {
        "req_cpu": req_cpu,
        "lim_cpu": lim_cpu,
        "req_mem": req_mem,
        "lim_mem": lim_mem,
        "qos": qos,
    }


# Common defaults for non-default profiles to eliminate duplication
_PROFILE_DEFAULTS = {
    "endpoint_count": {"min": 2, "max": 4},
    "db_instances": 2,
    "pv_pool": {"cpu": "1", "mem": "2Gi"},
}

# Profile specifications as per RHSTOR-9144 and NooBaa operator source code
# (https://github.com/noobaa/noobaa-operator/blob/master/pkg/system/performance_profiles.go)
PROFILE_SPECS = {
    "default": {
        "core": _resources("500m", "1", "1Gi", "4Gi", QOS_BURSTABLE),
        "db": _resources("1", "1", "2Gi", "2Gi", QOS_GUARANTEED),
        "endpoint": _resources("500m", "2", "1Gi", "3Gi", QOS_BURSTABLE),
        "endpoint_count": {"min": 1, "max": 2},
        "db_instances": 2,
        "pv_pool": {"cpu": "400m", "mem": "800Mi"},
    },
    "mixed-workload": {
        "core": _resources("1", "2", "2Gi", "4Gi", QOS_BURSTABLE),
        "db": _resources("4", "4", "8Gi", "8Gi", QOS_GUARANTEED),
        "endpoint": _resources("2", "4", "2Gi", "4Gi", QOS_BURSTABLE),
        **_PROFILE_DEFAULTS,
    },
    "small-objects": {
        "core": _resources("1", "2", "2Gi", "6Gi", QOS_BURSTABLE),
        "db": _resources("6", "6", "16Gi", "16Gi", QOS_GUARANTEED),
        # Endpoint CPU request is lower than mixed-workload on purpose: a single
        # endpoint process saturates before it can use more.
        "endpoint": _resources("1", "4", "2Gi", "4Gi", QOS_BURSTABLE),
        **_PROFILE_DEFAULTS,
    },
}

# Import PV pool volume count from constants.py
PV_POOL_NUM_VOLUMES = constants.MCG_PV_POOL_NUM_VOLUMES

# Expected noobaa-core resources for all five profiles - the three in
# PROFILE_SPECS plus dev-env and mini-env, which only the NooBaa CR accepts.
# The core StatefulSet is rolled on every profile change, so comparing the
# running core pod against this table tells us a profile change took effect.
# Only core is listed because that is all the "did it apply?" check needs.
CORE_SPECS = dict(
    {profile: PROFILE_SPECS[profile]["core"] for profile in SC_PROFILES},
    **{
        "dev-env": _resources("500m", "500m", "1Gi", "1Gi", QOS_GUARANTEED),
        "mini-env": _resources("100m", "100m", "1Gi", "1Gi", QOS_GUARANTEED),
    },
)


def normalize_quantity(value):
    """
    Normalize a Kubernetes resource quantity so equivalent notations compare
    equal: CPU to cores ("500m" == 0.5, "1" == "1000m") and memory to bytes
    ("1Gi" == "1024Mi"). The "m" (milli) suffix is CPU-only and the binary
    suffixes are memory-only, so one function covers both.

    Args:
        value: Quantity as a string (e.g. "500m", "1", "800Mi") or a number

    Returns:
        float or None: Cores for a CPU quantity, bytes for a memory quantity,
            or None if value is None
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return float(value)
    value = value.strip()
    if value.endswith("m"):
        return float(value[:-1]) / 1000
    for suffix, multiplier in MEMORY_UNITS.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * multiplier
    return float(value)


def verify_resources(
    actual,
    expected_req_cpu,
    expected_lim_cpu,
    expected_req_mem,
    expected_lim_mem,
    component_name,
):
    """
    Verify that actual resources match expected values, logging every
    mismatch. CPU and memory quantities are compared normalized, so
    equivalent notations (e.g. "1" and "1000m", "1Gi" and "1024Mi") match.

    Args:
        actual (dict): Actual resource dict from pod
        expected_req_cpu (str): Expected CPU request
        expected_lim_cpu (str): Expected CPU limit
        expected_req_mem (str): Expected memory request
        expected_lim_mem (str): Expected memory limit
        component_name (str): Name of component being verified

    Returns:
        bool: True if all values match, False otherwise
    """
    requests = actual.get("requests", {})
    limits = actual.get("limits", {})

    req_cpu = requests.get("cpu")
    lim_cpu = limits.get("cpu")
    req_mem = requests.get("memory")
    lim_mem = limits.get("memory")

    errors = []

    if normalize_quantity(req_cpu) != normalize_quantity(expected_req_cpu):
        errors.append(
            f"{component_name} CPU request: expected {expected_req_cpu}, got {req_cpu}"
        )

    if normalize_quantity(lim_cpu) != normalize_quantity(expected_lim_cpu):
        errors.append(
            f"{component_name} CPU limit: expected {expected_lim_cpu}, got {lim_cpu}"
        )

    if normalize_quantity(req_mem) != normalize_quantity(expected_req_mem):
        errors.append(
            f"{component_name} Memory request: expected {expected_req_mem}, got {req_mem}"
        )

    if normalize_quantity(lim_mem) != normalize_quantity(expected_lim_mem):
        errors.append(
            f"{component_name} Memory limit: expected {expected_lim_mem}, got {lim_mem}"
        )

    if errors:
        for error in errors:
            logger.error(error)
        return False

    logger.info(f"{component_name} resources verified successfully")
    return True


def resources_match(actual, req_cpu, lim_cpu, req_mem, lim_mem):
    """
    Return whether a pod's resource dict matches the expected requests/limits.

    A quiet, non-logging counterpart to verify_resources, intended for polling
    while the operator recreates pods after a profile change.

    Args:
        actual (dict): Actual resource dict from a pod
        req_cpu (str): Expected CPU request
        lim_cpu (str): Expected CPU limit
        req_mem (str): Expected memory request
        lim_mem (str): Expected memory limit

    Returns:
        bool: True if requests and limits match (compared normalized)
    """
    requests = actual.get("requests", {})
    limits = actual.get("limits", {})
    return (
        normalize_quantity(requests.get("cpu")) == normalize_quantity(req_cpu)
        and normalize_quantity(limits.get("cpu")) == normalize_quantity(lim_cpu)
        and normalize_quantity(requests.get("memory")) == normalize_quantity(req_mem)
        and normalize_quantity(limits.get("memory")) == normalize_quantity(lim_mem)
    )


def get_storagecluster_ocp():
    """
    Returns:
        ocs_ci.ocs.resources.storage_cluster.StorageCluster: Handle for the
            default StorageCluster CR
    """
    return StorageCluster(
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace=config.ENV_DATA["cluster_namespace"],
    )


def get_noobaa_ocp():
    """
    Returns:
        ocs_ci.ocs.ocp.OCP: Handle for the NooBaa CR
    """
    return OCP(
        kind=constants.NOOBAA_KIND,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.NOOBAA_RESOURCE_NAME,
    )


def set_storagecluster_profile(profile):
    """
    Set spec.multiCloudGateway.performanceProfile on the StorageCluster CR.

    Args:
        profile (str or None): Profile name. None removes the field, because a
            JSON merge patch deletes a key whose value is null.
    """
    logger.info(f"Setting MCG performance profile to '{profile}'")
    patched = get_storagecluster_ocp().patch(
        params=json.dumps(
            {"spec": {"multiCloudGateway": {"performanceProfile": profile}}}
        ),
        format_type="merge",
    )
    # patch() returns False when nothing changed, which is a valid no-op when
    # the field already holds the requested value.
    if not patched:
        logger.warning(f"Profile patch to '{profile}' reported no change")


def set_storagecluster_endpoint_override(min_replicas=None, max_replicas=None):
    """
    Set explicit min/max endpoint replica count on the StorageCluster CR.

    Endpoint replica overrides take precedence over the count the active
    profile would otherwise apply.

    Args:
        min_replicas (int or None): Min endpoint replicas, or None to clear
        max_replicas (int or None): Max endpoint replicas, or None to clear
    """
    endpoints_spec = None
    if min_replicas is not None and max_replicas is not None:
        endpoints_spec = {"minCount": min_replicas, "maxCount": max_replicas}
        logger.info(
            f"Setting endpoint replica override: "
            f"min={min_replicas}, max={max_replicas}"
        )
    else:
        logger.info("Clearing endpoint replica override")

    get_storagecluster_ocp().patch(
        params=json.dumps(
            {"spec": {"multiCloudGateway": {"endpoints": endpoints_spec}}}
        ),
        format_type="merge",
    )


def remove_storagecluster_profile():
    """
    Delete the performanceProfile field from the StorageCluster CR and verify
    the key is really gone, so a fall-back-to-default check cannot silently
    pass against a field that is still present.
    """
    logger.info("Removing performanceProfile field from the StorageCluster CR")
    set_storagecluster_profile(None)
    mcg_spec = (
        get_storagecluster_ocp().get().get("spec", {}).get("multiCloudGateway", {})
    )
    assert "performanceProfile" not in mcg_spec, (
        "performanceProfile key is still present on the StorageCluster CR "
        f"after removal: {mcg_spec}"
    )


def wait_for_profile_settled(profile, timeout=900):
    """
    Wait until a performance-profile change has fully reconciled: NooBaa is
    back to the Ready phase and the running noobaa-core pod has been recreated
    with the target profile's resources.

    The operator recreates the NooBaa pods asynchronously, so both reading pod
    resources and starting the next test must wait for this; otherwise the
    previous profile's pods (or a NooBaa still in the Creating phase) are
    observed. TimeoutSampler swallows transient errors raised while pods churn
    and retries until the timeout.

    Args:
        profile (str or None): Target profile; None means the default spec
        timeout (int): Seconds to wait before giving up
    """
    core_spec = PROFILE_SPECS[profile or "default"]["core"]
    noobaa_ocp = get_noobaa_ocp()

    def _settled():
        phase = noobaa_ocp.get().get("status", {}).get("phase")
        if phase != constants.STATUS_READY:
            return False
        core_pods = get_pods_having_label(
            label=constants.NOOBAA_CORE_POD_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
            statuses=[constants.STATUS_RUNNING],
        )
        return bool(core_pods) and resources_match(
            core_pods[0]["spec"]["containers"][0].get("resources", {}),
            core_spec["req_cpu"],
            core_spec["lim_cpu"],
            core_spec["req_mem"],
            core_spec["lim_mem"],
        )

    for settled in TimeoutSampler(timeout=timeout, sleep=20, func=_settled):
        if settled:
            break
        logger.info(
            f"Waiting for NooBaa to settle on profile '{profile}' "
            "(Ready phase and core pods recreated with new resources)"
        )


def apply_profile(profile, timeout=900):
    """
    Set a profile on the StorageCluster CR and wait until it has reconciled.

    Args:
        profile (str or None): Profile name, or None to remove the field
        timeout (int): Seconds to wait for the rollout to settle
    """
    if profile is None:
        remove_storagecluster_profile()
    else:
        set_storagecluster_profile(profile)
    wait_for_profile_settled(profile, timeout=timeout)
    logger.info(f"Profile '{profile}' successfully applied and reconciled")


def verify_profile_propagated(expected_profile):
    """
    Verify that the profile set on the StorageCluster CR reached the NooBaa CR
    and that ocs-operator did not additionally write hardcoded resource values
    onto the NooBaa CR. When a profile is active the NooBaa operator resolves
    the resources internally, so coreResources, dbResources and
    endpoints.resources must stay unset.

    Args:
        expected_profile (str): Profile expected on the NooBaa CR
    """
    noobaa_spec = get_noobaa_ocp().get().get("spec", {})

    actual_profile = noobaa_spec.get("performanceProfile")
    assert actual_profile == expected_profile, (
        f"NooBaa CR spec.performanceProfile: expected '{expected_profile}', "
        f"got '{actual_profile}'"
    )

    unexpected = {
        "spec.coreResources": noobaa_spec.get("coreResources"),
        "spec.dbResources": noobaa_spec.get("dbResources"),
        "spec.endpoints.resources": noobaa_spec.get("endpoints", {}).get("resources"),
    }
    populated = {field: value for field, value in unexpected.items() if value}
    assert not populated, (
        "ocs-operator wrote hardcoded resource values onto the NooBaa CR while "
        f"profile '{expected_profile}' is active, these must stay unset: {populated}"
    )
    logger.info(
        f"Profile '{expected_profile}' propagated to the NooBaa CR with no "
        "hardcoded resource values ✓"
    )


def verify_pods_against_spec(pods, component_spec, label, profile, check_qos=True):
    """
    Verify every pod's resources and QoS class against a component spec.

    Args:
        pods (list): Pod dicts as returned by get_pods_having_label
        component_spec (dict): Expected requests, limits and QoS class
        label (str): Component name used in log and assertion messages
        profile (str): Profile name being verified, for the messages
        check_qos (bool): Whether to also assert the pod QoS class
    """
    for i, pod in enumerate(pods, start=1):
        resources = pod["spec"]["containers"][0].get("resources", {})
        assert verify_resources(
            resources,
            component_spec["req_cpu"],
            component_spec["lim_cpu"],
            component_spec["req_mem"],
            component_spec["lim_mem"],
            f"{label}-{i}",
        ), f"{label} pod {i} resources do not match '{profile}' profile"

        if not check_qos:
            continue
        # Use the pod-reported QoS class, which Kubernetes computes across all
        # (app and init) containers, rather than deriving it from a single
        # container's resources.
        qos = pod["status"]["qosClass"]
        assert qos == component_spec["qos"], (
            f"{label} pod {i} QoS class: "
            f"expected {component_spec['qos']}, got {qos}"
        )
    logger.info(f"All {len(pods)} {label} pods verified against '{profile}' ✓")


def get_running_pods(label):
    """
    Args:
        label (str): Label selector

    Returns:
        list: Running pod dicts carrying the label
    """
    return get_pods_having_label(
        label=label,
        namespace=config.ENV_DATA["cluster_namespace"],
        statuses=[constants.STATUS_RUNNING],
    )


def wait_for_pods_with_resources(
    label, component_spec, expected_count=None, timeout=600
):
    """
    Wait until the running pods carrying a label all report the expected
    resources, and optionally until there is an exact number of them.

    A resource change recreates pods one at a time, so a snapshot taken
    mid-rollout legitimately mixes old and new values. Polling here keeps the
    callers free of sleeps.

    Args:
        label (str): Label selector
        component_spec (dict): Expected req_cpu, lim_cpu, req_mem, lim_mem
        expected_count (int): Exact number of running pods to wait for, or
            None to accept any non-zero number
        timeout (int): Seconds to wait before giving up

    Returns:
        list: The running pod dicts once they match
    """
    pods = []
    for pods in TimeoutSampler(
        timeout=timeout, sleep=15, func=get_running_pods, label=label
    ):
        count_ok = bool(pods) and (
            expected_count is None or len(pods) == expected_count
        )
        resources_ok = bool(pods) and all(
            resources_match(
                pod["spec"]["containers"][0].get("resources", {}),
                component_spec["req_cpu"],
                component_spec["lim_cpu"],
                component_spec["req_mem"],
                component_spec["lim_mem"],
            )
            for pod in pods
        )
        if count_ok and resources_ok:
            return pods
        logger.info(
            f"Waiting for pods '{label}' to report the expected resources "
            f"(running: {len(pods)}, wanted: {expected_count or 'any'})"
        )
    return pods


def verify_core(spec, profile):
    """
    Verify noobaa-core pod resources and QoS class.

    Args:
        spec (dict): Profile specification from PROFILE_SPECS
        profile (str): Profile name being verified
    """
    logger.info("Verifying noobaa-core pod resources")
    core_pods = get_running_pods(constants.NOOBAA_CORE_POD_LABEL)
    assert core_pods, "No running noobaa-core pods found"
    verify_pods_against_spec(core_pods[:1], spec["core"], "noobaa-core", profile)


def verify_db(spec, profile):
    """
    Verify noobaa-db pod resources and QoS class (per instance) and the
    expected DB instance count.

    Args:
        spec (dict): Profile specification from PROFILE_SPECS
        profile (str): Profile name being verified
    """
    logger.info("Verifying noobaa-db pod resources")
    # CNPG rolls the DB instances one at a time, so poll until every running
    # instance carries the new resources rather than reading mid-rollout.
    db_pods = wait_for_pods_with_resources(
        constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
        spec["db"],
        expected_count=spec["db_instances"],
    )
    assert db_pods, "No running noobaa-db pods found"

    verify_pods_against_spec(db_pods, spec["db"], "noobaa-db", profile)

    expected_db_instances = spec["db_instances"]
    assert (
        len(db_pods) == expected_db_instances
    ), f"DB instance count: expected {expected_db_instances}, got {len(db_pods)}"
    logger.info(f"DB instances: {len(db_pods)} ✓")


def get_endpoint_label():
    """
    Returns:
        str: Label selector matching noobaa-endpoint pods
    """
    return f"{constants.NOOBAA_APP_LABEL},{constants.NOOBAA_ENDPOINT_POD_LABEL}"


def verify_endpoint_hpa(expected_min, expected_max):
    """
    Verify the noobaa endpoint HPA min/max replicas, when an HPA exists.

    Args:
        expected_min (int): Expected minReplicas
        expected_max (int): Expected maxReplicas
    """
    hpa_ocp = OCP(kind=constants.HPA, namespace=config.ENV_DATA["cluster_namespace"])
    hpas = hpa_ocp.get(selector=constants.NOOBAA_ENDPOINT_POD_LABEL).get("items", [])
    if not hpas:
        logger.info("No HPA found (static replica count)")
        return

    def _hpa_matches():
        hpa = hpa_ocp.get(selector=constants.NOOBAA_ENDPOINT_POD_LABEL)["items"][0]
        return (
            hpa["spec"]["minReplicas"] == expected_min
            and hpa["spec"]["maxReplicas"] == expected_max
        )

    # The HPA spec is rewritten by the operator shortly after the profile or an
    # explicit count override changes, so give it a moment rather than reading
    # the pre-update values.
    for matches in TimeoutSampler(timeout=300, sleep=10, func=_hpa_matches):
        if matches:
            break
        logger.info(f"Waiting for HPA to report min={expected_min} max={expected_max}")
    logger.info(f"HPA configured: min={expected_min}, max={expected_max} ✓")


def verify_endpoints(spec, profile):
    """
    Verify noobaa-endpoint pod resources, QoS class, and pod count (min/max via
    the HPA).

    Args:
        spec (dict): Profile specification from PROFILE_SPECS
        profile (str): Profile name being verified
    """
    logger.info("Verifying noobaa-endpoint pod resources")
    expected_min = spec["endpoint_count"]["min"]
    expected_max = spec["endpoint_count"]["max"]

    # A profile change recreates/rescales the endpoint pods, so wait until the
    # running endpoint count settles within the expected range AND every
    # running endpoint already carries the target profile's resources. The
    # count alone can be valid mid-transition while a terminating pod (still
    # phase Running) still has the previous profile's resources.
    endpoint_pods = []
    for endpoint_pods in TimeoutSampler(
        timeout=300, sleep=10, func=get_running_pods, label=get_endpoint_label()
    ):
        count_ok = expected_min <= len(endpoint_pods) <= expected_max
        resources_ok = bool(endpoint_pods) and all(
            resources_match(
                pod["spec"]["containers"][0].get("resources", {}),
                spec["endpoint"]["req_cpu"],
                spec["endpoint"]["lim_cpu"],
                spec["endpoint"]["req_mem"],
                spec["endpoint"]["lim_mem"],
            )
            for pod in endpoint_pods
        )
        if count_ok and resources_ok:
            break
        logger.info(
            f"Waiting for {expected_min}-{expected_max} running endpoint pods "
            f"with '{profile}' resources, current running: {len(endpoint_pods)}"
        )
    assert endpoint_pods, "No running noobaa-endpoint pods found"

    # Verify every endpoint pod, not just the first one, so a pod with stale or
    # incorrect resources cannot slip through.
    verify_pods_against_spec(
        endpoint_pods, spec["endpoint"], "noobaa-endpoint", profile
    )

    current_count = len(endpoint_pods)
    assert expected_min <= current_count <= expected_max, (
        f"Endpoint pod count {current_count} not within expected range "
        f"[{expected_min}, {expected_max}]"
    )
    logger.info(
        f"Endpoint pod count: {current_count} "
        f"(within range [{expected_min}, {expected_max}]) ✓"
    )

    verify_endpoint_hpa(expected_min, expected_max)


def get_backingstore(name):
    """
    Args:
        name (str): BackingStore name

    Returns:
        dict or None: The BackingStore resource, or None if it does not exist
    """
    bs_ocp = OCP(
        kind=constants.BACKINGSTORE, namespace=config.ENV_DATA["cluster_namespace"]
    )
    try:
        return bs_ocp.get(resource_name=name)
    except CommandFailed as e:
        # Only a genuine "not found" means the backingstore is absent.
        # Authorization, connectivity or other API errors must propagate.
        if "not found" in str(e).lower() or "notfound" in str(e).lower():
            logger.info(f"BackingStore '{name}' not found: {e}")
            return None
        raise


def get_pv_pool_agent_pods(backingstore_name):
    """
    Args:
        backingstore_name (str): PV pool backingstore name

    Returns:
        list: Running agent pod dicts belonging to the backingstore
    """
    return get_running_pods(f"pool={backingstore_name}")


def verify_pv_pool(spec, profile):
    """
    Verify the resources of every default backingstore PV pool agent pod
    (vSphere/on-prem only). Skipped on cloud platforms where the default
    backingstore is not a pv-pool.

    Note that a profile switch does not recreate existing PV pool agent pods -
    the profile's CPU/memory is stamped into the pod template only when a pod
    is created. A backingstore that predates the current profile therefore
    keeps its original values, so this check is only meaningful for agent pods
    created after the profile was set.

    Args:
        spec (dict): Profile specification from PROFILE_SPECS
        profile (str): Profile name being verified
    """
    logger.info("Checking for PV pool backingstore")
    default_bs = get_backingstore(constants.DEFAULT_NOOBAA_BACKINGSTORE)
    if default_bs is None:
        logger.info("Default backingstore not found, skipping PV pool verification")
        return

    bs_type = default_bs.get("spec", {}).get("type")
    if bs_type != constants.BACKINGSTORE_TYPE_PV_POOL:
        logger.info(
            f"Backingstore type is '{bs_type}' (cloud storage), "
            "skipping PV pool verification (N/A for cloud platforms)"
        )
        return

    logger.info("PV pool backingstore detected, verifying agent pod resources")
    pv_pool_pods = get_running_pods(constants.NOOBAA_DEFAULT_BACKINGSTORE_LABEL)
    assert pv_pool_pods, "PV pool backingstore exists but no running agent pods found"

    verify_pv_pool_agent_resources(
        pv_pool_pods, spec["pv_pool"]["cpu"], spec["pv_pool"]["mem"], profile
    )


def verify_pv_pool_agent_resources(agent_pods, expected_cpu, expected_mem, context):
    """
    Verify PV pool agent pod resources. PV pool agents are created with equal
    requests and limits, so a single cpu/memory pair describes both.

    Args:
        agent_pods (list): Agent pod dicts
        expected_cpu (str): Expected CPU request and limit
        expected_mem (str): Expected memory request and limit
        context (str): Description used in assertion messages
    """
    for i, agent_pod in enumerate(agent_pods, start=1):
        resources = agent_pod["spec"]["containers"][0].get("resources", {})
        assert verify_resources(
            resources,
            expected_cpu,
            expected_cpu,  # limits == requests for PV pool
            expected_mem,
            expected_mem,  # limits == requests for PV pool
            f"PV pool agent-{i}",
        ), f"PV pool agent {i} resources do not match {context}"
    logger.info(f"All {len(agent_pods)} PV pool agent pod resources verified ✓")


def verify_all_components(spec, profile, check_pv_pool=True):
    """
    Run every per-component profile verification.

    Args:
        spec (dict): Profile specification from PROFILE_SPECS
        profile (str): Profile name being verified
        check_pv_pool (bool): Whether to verify the default backingstore PV
            pool agent pods. Pass False when the profile was applied to an
            already running system - the agent pods keep the resources they
            were created with, so they legitimately lag behind the profile
            (see verify_pv_pool)
    """
    verify_core(spec, profile)
    verify_db(spec, profile)
    verify_endpoints(spec, profile)
    if check_pv_pool:
        verify_pv_pool(spec, profile)


def verify_noobaa_pods_healthy(settle_checks=2, sleep=20):
    """
    Verify that no NooBaa pod is stuck. A profile switch legitimately produces
    short-lived Pending pods and scheduling or volume-attach warnings while the
    scheduler moves workloads around, so require the namespace to look healthy
    over consecutive checks rather than in a single instantaneous sample.

    The noobaa-core standby pod stays 1/2 Ready by design (leader election), so
    container readiness is deliberately not asserted here.

    Args:
        settle_checks (int): Number of consecutive healthy samples required
        sleep (int): Seconds between samples
    """
    bad_reasons = (
        "CrashLoopBackOff",
        "ImagePullBackOff",
        "ErrImagePull",
        "CreateContainerError",
    )
    consecutive = 0

    def _unhealthy():
        problems = []
        pods = get_pods_having_label(
            label=constants.NOOBAA_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        for pod in pods:
            name = pod["metadata"]["name"]
            phase = pod["status"].get("phase")
            if phase in ("Pending", "Failed", "Unknown"):
                problems.append(f"{name} phase={phase}")
            for container in pod["status"].get("containerStatuses") or []:
                waiting = (container.get("state") or {}).get("waiting") or {}
                if waiting.get("reason") in bad_reasons:
                    problems.append(
                        f"{name}/{container['name']} {waiting.get('reason')}"
                    )
        return problems

    for problems in TimeoutSampler(timeout=600, sleep=sleep, func=_unhealthy):
        if problems:
            consecutive = 0
            logger.info(f"NooBaa pods not healthy yet: {problems}")
            continue
        consecutive += 1
        if consecutive >= settle_checks:
            logger.info("No stuck NooBaa pods ✓")
            return
