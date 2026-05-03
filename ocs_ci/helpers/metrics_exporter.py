import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pods_having_label

log = logging.getLogger(__name__)


def get_metrics_exporter_pod(namespace=None):
    """
    Return the ocs-metrics-exporter Pod object.

    Args:
        namespace (str): Namespace to search in. Defaults to cluster_namespace.

    Returns:
        OCS: Pod object for the metrics exporter.

    Raises:
        AssertionError: If no (or more than one) exporter pod is found.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    pods = get_pods_having_label(
        label=constants.OCS_METRICS_EXPORTER,
        namespace=namespace,
    )
    assert pods, (
        f"No ocs-metrics-exporter pod found in namespace {namespace} "
        f"(label: {constants.OCS_METRICS_EXPORTER})"
    )
    assert (
        len(pods) == 1
    ), f"Expected exactly 1 ocs-metrics-exporter pod, found {len(pods)}"
    pod_name = pods[0]["metadata"]["name"]
    log.info(f"Found ocs-metrics-exporter pod: {pod_name}")
    return pods[0]


def verify_metrics_exporter_running(namespace=None, expected_container_count=1):
    """
    Verify the ocs-metrics-exporter pod is Running with the expected number
    of containers ready.

    After RHSTOR-7964, kube-rbac-proxy is removed, so the pod should have
    exactly one container. Pass expected_container_count=2 only when testing
    a pre-RHSTOR-7964 deployment.

    Args:
        namespace (str): Namespace to search in. Defaults to cluster_namespace.
        expected_container_count (int): Expected number of containers in the
            pod spec (1 after kube-rbac-proxy removal).

    Returns:
        dict: The raw pod dict.

    Raises:
        AssertionError: If the pod is not Running or container count mismatches.
    """
    pod = get_metrics_exporter_pod(namespace=namespace)
    phase = pod["status"]["phase"]
    assert (
        phase == "Running"
    ), f"ocs-metrics-exporter pod phase is '{phase}', expected 'Running'"

    container_names = [c["name"] for c in pod["spec"]["containers"]]
    assert len(container_names) == expected_container_count, (
        f"Expected {expected_container_count} container(s), "
        f"found {len(container_names)}: {container_names}"
    )
    assert "kube-rbac-proxy" not in container_names, (
        f"kube-rbac-proxy container should have been removed but is present: "
        f"{container_names}"
    )

    ready = pod["status"]["containerStatuses"][0]["ready"]
    assert ready, (
        f"ocs-metrics-exporter container is not ready. "
        f"Containers: {container_names}"
    )

    log.info(f"ocs-metrics-exporter pod is Running with containers: {container_names}")
    return pod


def query_metrics_exporter_endpoint(pod_name, namespace=None, metric_filter=None):
    """
    Execute a curl against the metrics exporter's /metrics endpoint from
    inside the pod and return the output.

    Args:
        pod_name (str): Name of the ocs-metrics-exporter pod.
        namespace (str): Namespace of the pod. Defaults to cluster_namespace.
        metric_filter (str): Optional substring to grep for in the output.
            If None, the full /metrics output is returned.

    Returns:
        str: Raw metrics text (optionally filtered to lines matching
            metric_filter).

    Raises:
        AssertionError: If the curl command returns empty output.
    """
    from ocs_ci.utility.utils import exec_cmd

    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    curl_cmd = (
        f"oc exec {pod_name} -n {namespace} -- "
        f"curl -sk http://localhost:8443/metrics"
    )
    result = exec_cmd(curl_cmd)
    output = result.stdout.decode()

    assert output, (
        f"Empty response from ocs-metrics-exporter /metrics endpoint "
        f"on pod {pod_name}"
    )

    if metric_filter:
        lines = [l for l in output.splitlines() if metric_filter in l]
        log.info(
            f"Filtered {len(lines)} line(s) matching '{metric_filter}' "
            f"from /metrics on pod {pod_name}"
        )
        return "\n".join(lines)

    log.info(
        f"Retrieved {len(output.splitlines())} lines from /metrics "
        f"on pod {pod_name}"
    )
    return output
