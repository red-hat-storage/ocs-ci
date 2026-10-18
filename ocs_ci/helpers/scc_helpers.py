import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


# Expected values from the RHSTOR-8757 operator changes. Optional components
# are checked when present. Jobs and optional topologies need separate runs.
SCC_BY_CATEGORY = {
    "Rook-Ceph": {
        constants.MON_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.OSD_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.MGR_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.MDS_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.RGW_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.CRASHCOLLECTOR_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.EXPORTER_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.TOOL_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.OPERATOR_LABEL: constants.SCC_ROOK_CEPH,
        constants.OSD_PREPARE_APP_LABEL: constants.SCC_ROOK_CEPH,
        f"app={constants.OSD_KEY_ROTATION_POD_NAME}": constants.SCC_ROOK_CEPH,
        constants.RBD_MIRROR_APP_LABEL: constants.SCC_ROOK_CEPH,
        "app=rook-ceph-nfs": constants.SCC_ROOK_CEPH,
    },
    "CSI": {
        constants.CSI_CEPHFSPLUGIN_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.CSI_RBDPLUGIN_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420: constants.SCC_CEPH_CSI_OP,
        constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420: constants.SCC_CEPH_CSI_OP,
        constants.NFS_CSI_NODEPLUGIN_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.NFS_CSI_CTRLPLUGIN_LABEL_419: constants.SCC_CEPH_CSI_OP,
    },
    "NooBaa": {
        constants.NOOBAA_CORE_POD_LABEL: constants.SCC_NOOBAA_CORE,
        constants.NOOBAA_DB_LABEL_419_AND_ABOVE: constants.SCC_RESTRICTED_V2,
        constants.NOOBAA_ENDPOINT_POD_LABEL: constants.SCC_NOOBAA_ENDPOINT,
        constants.NOOBAA_OPERATOR_POD_LABEL: constants.SCC_RESTRICTED_V2,
        constants.NOOBAA_CNPG_POD_LABEL: constants.SCC_RESTRICTED_V2,
        constants.NOOBAA_PROMETHEUS_ADAPTER_LABEL: constants.SCC_RESTRICTED_V2,
    },
    "ODF operators": {
        constants.OCS_OPERATOR_LABEL: constants.SCC_RESTRICTED_V2,
        constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL: constants.SCC_RESTRICTED_V2,
        constants.ODF_EXTERNAL_SNAPSHOTTER: constants.SCC_RESTRICTED_V2,
        constants.CEPH_CSI_CONTROLLER_MANAGER_LABEL: constants.SCC_RESTRICTED_V2,
        constants.CSI_ADDONS_CONTROLLER_MANAGER_LABEL: constants.SCC_RESTRICTED_V2,
        constants.OCS_METRICS_EXPORTER: constants.SCC_RESTRICTED_V2,
        constants.ODF_CONSOLE: constants.SCC_RESTRICTED_V2,
        constants.UX_BACKEND_APP_LABEL: constants.SCC_RESTRICTED_V2,
        constants.PROVIDER_SERVER_LABEL: constants.SCC_RESTRICTED_V2,
        "app=vault-agent-rgw": constants.SCC_RESTRICTED_V2,
        constants.PROMETHEUS_OPERATOR_LABEL: constants.SCC_RESTRICTED_V2,
        constants.MANAGED_PROMETHEUS_LABEL: constants.SCC_NONROOT_V2,
        constants.MANAGED_ALERTMANAGER_LABEL: constants.SCC_NONROOT_V2,
        constants.MANAGED_FUSION_PROMETHEUS_LABEL: constants.SCC_NONROOT_V2,
        constants.MANAGED_FUSION_ALERTMANAGER_LABEL: constants.SCC_NONROOT_V2,
        constants.TOKEN_EXCHANGE_AGENT_LABEL: constants.SCC_RESTRICTED_V2,
    },
    "blackbox exporter": {
        constants.BLACKBOX_POD_LABEL_422_AND_ABOVE: constants.SCC_ODF_BLACKBOX,
    },
}

REQUIRED_CEPH_LABELS = {
    constants.MON_APP_LABEL,
    constants.OSD_APP_LABEL,
    constants.MGR_APP_LABEL,
    constants.OPERATOR_LABEL,
    constants.CSI_CEPHFSPLUGIN_LABEL_419,
    constants.CSI_RBDPLUGIN_LABEL_419,
    constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419,
    constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419,
    constants.CEPH_CSI_CONTROLLER_MANAGER_LABEL,
}
REQUIRED_LABELS = REQUIRED_CEPH_LABELS | {
    constants.NOOBAA_CORE_POD_LABEL,
    constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
    constants.NOOBAA_ENDPOINT_POD_LABEL,
    constants.NOOBAA_OPERATOR_POD_LABEL,
    constants.OCS_OPERATOR_LABEL,
    constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL,
    constants.ODF_CONSOLE,
}
HOST_NETWORK_LABELS = {constants.OPERATOR_LABEL, constants.OCS_METRICS_EXPORTER}

# Some operator templates do not have a unique label in the current mapping.
SCC_BY_POD_PREFIX = {
    "devicefinder-discovery": constants.PRIVILEGED,
    constants.OCS_CLIENT_OPERATOR_CONTROLLER_MANAGER_PREFIX: constants.SCC_RESTRICTED_V2,
    constants.OCS_CLIENT_OPERATOR_CONSOLE: constants.SCC_RESTRICTED_V2,
    constants.ODF_MULTICLUSTER_ORCHESTRATOR_CONTROLLER_MANAGER: constants.SCC_RESTRICTED_V2,
    "odf-multicluster-console": constants.SCC_RESTRICTED_V2,
    "ocs-extend-cluster": constants.SCC_RESTRICTED_V2,
    "ocs-osd-removal": constants.SCC_ROOK_CEPH,
    "onboarding-validation-keys-generator": constants.SCC_RESTRICTED_V2,
}
SCC_BY_NAME_FRAGMENT = {
    "status-reporter": constants.SCC_RESTRICTED_V2,
    "analyze-resource": constants.SCC_RESTRICTED_V2,
}
REQUIRED_PREFIXES_BY_CSV = {
    "ocs-client-operator": (
        constants.OCS_CLIENT_OPERATOR_CONTROLLER_MANAGER_PREFIX,
        constants.OCS_CLIENT_OPERATOR_CONSOLE,
    ),
    "odf-multicluster-orchestrator": (
        constants.ODF_MULTICLUSTER_ORCHESTRATOR_CONTROLLER_MANAGER,
        "odf-multicluster-console",
    ),
}


def get_required_scc_labels() -> set[str]:
    """Return required SCC component labels for the current deployment."""
    required = REQUIRED_LABELS
    if config.ENV_DATA.get("mcg_only_deployment"):
        required = required - REQUIRED_CEPH_LABELS
    if config.ENV_DATA.get("noobaa_external_pgsql"):
        required = required - {constants.NOOBAA_DB_LABEL_419_AND_ABOVE}
    return required


def get_active_scc_pods(namespace: str) -> list[dict]:
    """Return Running or Pending pods without a deletion timestamp."""
    pods = OCP(kind=constants.POD, namespace=namespace).get().get("items", [])
    return [
        pod
        for pod in pods
        if pod.get("status", {}).get("phase") in ("Running", "Pending")
        and not pod.get("metadata", {}).get("deletionTimestamp")
    ]


def pod_matches_scc_label(pod: dict, selector: str) -> bool:
    """Check whether a pod matches a single key=value label selector."""
    key, _, value = selector.partition("=")
    return (pod.get("metadata", {}).get("labels") or {}).get(key) == value


def get_expected_scc_for_label(pod: dict, selector: str, default_scc: str) -> str:
    """Return the expected SCC, including any host-network override."""
    if selector in HOST_NETWORK_LABELS and pod.get("spec", {}).get("hostNetwork"):
        return constants.SCC_HOSTNETWORK_V2
    return default_scc


def get_matching_scc_components(pod: dict) -> list[tuple[str, str]]:
    """Return all matching SCC entries so callers can detect ambiguity."""
    name = pod.get("metadata", {}).get("name", "")
    matches = []
    for components in SCC_BY_CATEGORY.values():
        for selector, expected_scc in components.items():
            if pod_matches_scc_label(pod, selector):
                matches.append(
                    (selector, get_expected_scc_for_label(pod, selector, expected_scc))
                )

    matches.extend(
        (prefix, expected_scc)
        for prefix, expected_scc in SCC_BY_POD_PREFIX.items()
        if name.startswith(prefix)
    )
    matches.extend(
        (fragment, expected_scc)
        for fragment, expected_scc in SCC_BY_NAME_FRAGMENT.items()
        if fragment in name
    )
    if any(
        container.get("name") == "noobaa-agent"
        for container in pod.get("spec", {}).get("containers", [])
    ):
        matches.append(("noobaa-agent", constants.SCC_NOOBAA_AGENT))
    return matches


def get_scc_pinning_errors(pod: dict, component: str, expected_scc: str) -> list[str]:
    """Return required and admitted SCC mismatches for a component pod."""
    result = verify_pod_scc_pinning(pod, expected_scc)
    errors = []
    if not result["required_match"]:
        errors.append(
            f"Pod '{result['pod_name']}' ({component}) requests "
            f"'{result['required_scc']}', expected '{expected_scc}'"
        )
    if not result["admission_match"]:
        errors.append(
            f"Pod '{result['pod_name']}' ({component}) was admitted under "
            f"'{result['admission_scc']}', expected '{expected_scc}'"
        )
    return errors


def get_pod_scc_annotations(pod_data: dict) -> tuple:
    """
    Extract SCC annotation values from a pod's data dict.

    Args:
        pod_data (dict): Raw pod dictionary from Kubernetes API.

    Returns:
        tuple: (required_scc, admission_scc) where either may be None
            if the annotation is not present.

    """
    annotations = pod_data.get("metadata", {}).get("annotations") or {}
    required_scc = annotations.get(constants.SCC_REQUIRED_ANNOTATION)
    admission_scc = annotations.get(constants.SCC_ADMISSION_ANNOTATION)
    return required_scc, admission_scc


def verify_pod_scc_pinning(pod_data: dict, expected_scc: str) -> dict:
    """
    Verify a pod has the correct required-scc annotation and that admission
    enforced it.

    Args:
        pod_data (dict): Raw pod dictionary from Kubernetes API.
        expected_scc (str): The SCC name the pod should be pinned to.

    Returns:
        dict: Result with keys:
            - pod_name (str): Name of the pod.
            - required_scc (str or None): Value of openshift.io/required-scc.
            - admission_scc (str or None): Value of openshift.io/scc.
            - required_match (bool): Whether required_scc equals expected_scc.
            - admission_match (bool): Whether admission_scc equals expected_scc.

    """
    pod_name = pod_data.get("metadata", {}).get("name", "unknown")
    required_scc, admission_scc = get_pod_scc_annotations(pod_data)
    return {
        "pod_name": pod_name,
        "required_scc": required_scc,
        "admission_scc": admission_scc,
        "required_match": required_scc == expected_scc,
        "admission_match": admission_scc == expected_scc,
    }


def get_pods_missing_required_scc(namespace: str) -> list:
    """
    Find all running pods in a namespace that lack the required-scc annotation.

    Scans every pod in the namespace and returns those in Running or Pending
    phase that do not have the openshift.io/required-scc annotation set.
    Completed (Succeeded/Failed) pods are excluded since they are transient
    Job artifacts.

    Args:
        namespace (str): The namespace to scan.

    Returns:
        list: List of dicts with keys "name" and "phase" for each pod
            missing the annotation.

    """
    ocp_pod = OCP(kind=constants.POD, namespace=namespace)
    all_pods = ocp_pod.get().get("items", [])

    missing = []
    for pod_data in all_pods:
        phase = pod_data.get("status", {}).get("phase", "Unknown")
        if phase not in ("Running", "Pending"):
            continue

        required_scc, _ = get_pod_scc_annotations(pod_data)
        if not required_scc:
            pod_name = pod_data.get("metadata", {}).get("name", "unknown")
            logger.warning(
                "Pod %s in phase %s is missing %s annotation",
                pod_name,
                phase,
                constants.SCC_REQUIRED_ANNOTATION,
            )
            missing.append({"name": pod_name, "phase": phase})

    return missing
