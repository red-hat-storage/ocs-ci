import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


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
