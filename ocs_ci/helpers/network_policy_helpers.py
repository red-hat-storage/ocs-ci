import logging
import shlex

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pod_obj


logger = logging.getLogger(__name__)


def get_all_network_policies(namespace=None):
    """
    Get all NetworkPolicies in a namespace.

    Args:
        namespace (str): Namespace to query. Defaults to cluster_namespace.

    Returns:
        list: List of NetworkPolicy resource dicts.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind=constants.NETWORK_POLICY, namespace=namespace)
    policies = ocp_obj.get().get("items", [])
    logger.info(
        f"Found {len(policies)} NetworkPolicies in {namespace}: "
        f"{[p['metadata']['name'] for p in policies]}"
    )
    return policies


def verify_network_policies_exist(expected_policies, namespace=None):
    """
    Verify that expected NetworkPolicy CRs exist in the namespace.

    Args:
        expected_policies (list): List of expected policy names.
        namespace (str): Namespace to check. Defaults to cluster_namespace.

    Raises:
        AssertionError: If any expected policy is missing.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind=constants.NETWORK_POLICY, namespace=namespace)
    missing = []
    for policy_name in expected_policies:
        if not ocp_obj.is_exist(resource_name=policy_name):
            missing.append(policy_name)
    assert not missing, f"Missing NetworkPolicies in {namespace}: {missing}"
    logger.info(
        f"All {len(expected_policies)} expected NetworkPolicies "
        f"verified in {namespace}"
    )


def _matches_api_group(api_groups):
    return "networking.k8s.io" in api_groups or "*" in api_groups


def _matches_resource(resources):
    return "networkpolicies" in resources or "*" in resources


def _expand_verbs(verbs):
    if "*" in verbs:
        return {"create", "delete", "get", "list", "patch", "update", "watch"}
    return set(verbs)


def get_csv_network_policy_rbac(csv_name, namespace=None):
    """
    Extract NetworkPolicy RBAC rules from a CSV.

    Args:
        csv_name (str): Full CSV name (e.g. 'rook-ceph-operator.v4.23.0-41.stable').
        namespace (str): Namespace. Defaults to cluster_namespace.

    Returns:
        list: List of matching RBAC rule dicts that reference
              networking.k8s.io networkpolicies.

    Raises:
        AssertionError: If the CSV cannot be retrieved.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(
        kind="ClusterServiceVersion",
        namespace=namespace,
    )
    try:
        csv_data = ocp_obj.get(resource_name=csv_name)
    except CommandFailed as ex:
        raise AssertionError(f"Failed to retrieve CSV {csv_name}: {ex}") from ex
    spec = csv_data.get("spec", {})
    install_spec = spec.get("install", {}).get("spec", {})

    matching_rules = []
    for perm_block in ("permissions", "clusterPermissions"):
        for perm in install_spec.get(perm_block, []):
            for rule in perm.get("rules", []):
                api_groups = rule.get("apiGroups", [])
                resources = rule.get("resources", [])
                if _matches_api_group(api_groups) and _matches_resource(resources):
                    matching_rules.append(
                        {
                            "perm_type": perm_block,
                            "service_account": perm.get("serviceAccountName", ""),
                            "verbs": _expand_verbs(rule.get("verbs", [])),
                        }
                    )
    return matching_rules


def verify_csv_network_policy_rbac(csv_prefix, namespace=None):
    """
    Verify a CSV declares NetworkPolicy RBAC per Conforma requirements.

    Looks up the CSV by prefix, then checks that its permissions include
    networking.k8s.io/networkpolicies with verbs create, delete, and
    update or patch.

    Args:
        csv_prefix (str): CSV name prefix (e.g. 'rook-ceph-operator').
        namespace (str): Namespace. Defaults to cluster_namespace.

    Raises:
        AssertionError: If RBAC is missing or insufficient.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    csv_name = get_csv_name_by_prefix(csv_prefix, namespace)
    rules = get_csv_network_policy_rbac(csv_name, namespace)

    assert rules, (
        f"CSV {csv_name} has no RBAC rules for " f"networking.k8s.io/networkpolicies"
    )

    all_verbs = set()
    for rule in rules:
        all_verbs.update(rule["verbs"])

    missing_required = constants.NETWORK_POLICY_REQUIRED_VERBS - all_verbs
    assert not missing_required, (
        f"CSV {csv_name} missing required verbs {missing_required}. "
        f"Has: {all_verbs}"
    )

    has_update = all_verbs & constants.NETWORK_POLICY_REQUIRED_UPDATE_VERBS
    assert has_update, f"CSV {csv_name} missing update/patch verb. Has: {all_verbs}"

    logger.info(f"CSV {csv_name} has valid NetworkPolicy RBAC: {all_verbs}")


def get_csv_name_by_prefix(csv_prefix, namespace=None):
    """
    Find the full CSV name matching a prefix.

    Args:
        csv_prefix (str): Prefix to match (e.g. 'rook-ceph-operator').
        namespace (str): Namespace. Defaults to cluster_namespace.

    Returns:
        str: Full CSV name.

    Raises:
        AssertionError: If no CSV matches the prefix.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind="ClusterServiceVersion", namespace=namespace)
    csvs = ocp_obj.get().get("items", [])
    for csv in csvs:
        name = csv["metadata"]["name"]
        if name.startswith(csv_prefix):
            return name
    raise AssertionError(f"No CSV found with prefix '{csv_prefix}' in {namespace}")


def verify_sa_can_manage_network_policies(sa_name, namespace=None):
    """
    Verify a service account has effective permissions to manage
    NetworkPolicies using 'oc auth can-i'.

    Args:
        sa_name (str): Service account name.
        namespace (str): Namespace. Defaults to cluster_namespace.

    Raises:
        AssertionError: If any required permission is missing.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind=constants.NETWORK_POLICY, namespace=namespace)
    required_verbs = list(constants.NETWORK_POLICY_REQUIRED_VERBS)
    failed_verbs = []
    for verb in required_verbs:
        cmd = (
            f"auth can-i {verb} networkpolicies.networking.k8s.io "
            f"--as=system:serviceaccount:{namespace}:{sa_name} "
            f"-n {namespace}"
        )
        try:
            result = ocp_obj.exec_oc_cmd(cmd, out_yaml_format=False)
            if "yes" not in str(result).lower():
                failed_verbs.append(verb)
        except CommandFailed:
            failed_verbs.append(verb)

    update_ok = False
    for verb in constants.NETWORK_POLICY_REQUIRED_UPDATE_VERBS:
        cmd = (
            f"auth can-i {verb} networkpolicies.networking.k8s.io "
            f"--as=system:serviceaccount:{namespace}:{sa_name} "
            f"-n {namespace}"
        )
        try:
            result = ocp_obj.exec_oc_cmd(cmd, out_yaml_format=False)
            if "yes" in str(result).lower():
                update_ok = True
                break
        except CommandFailed:
            continue
    if not update_ok:
        failed_verbs.append("update/patch")

    assert not failed_verbs, (
        f"SA {sa_name} cannot {failed_verbs} NetworkPolicies " f"in {namespace}"
    )
    logger.info(
        f"SA {sa_name} has create/delete/update permissions for "
        f"NetworkPolicies in {namespace}"
    )


def check_pod_connectivity(
    source_pod_name,
    target_ip,
    port,
    namespace=None,
    should_succeed=True,
    timeout=10,
):
    """
    Check TCP connectivity from a pod to a target IP:port.

    Uses bash /dev/tcp for connectivity testing.

    Args:
        source_pod_name (str): Name of the source pod.
        target_ip (str): Target IP address.
        port (int): Target port.
        namespace (str): Namespace of the source pod.
        should_succeed (bool): If True, assert connection succeeds.
            If False, assert it fails.
        timeout (int): Connection timeout in seconds.

    Raises:
        AssertionError: If result doesn't match should_succeed.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    pod_obj = get_pod_obj(source_pod_name, namespace=namespace)
    cmd = (
        f"bash -c 'timeout {timeout} "
        f'bash -c "echo > /dev/tcp/$1/$2" '
        f"&& echo CONNECTED || echo FAILED' "
        f"_ {shlex.quote(str(target_ip))} {shlex.quote(str(port))}"
    )
    try:
        result = pod_obj.exec_cmd_on_pod(
            cmd, out_yaml_format=False, timeout=timeout + 10
        )
        connected = "CONNECTED" in str(result)
    except CommandFailed:
        connected = False

    if should_succeed:
        assert connected, (
            f"Pod {source_pod_name} could not connect to "
            f"{target_ip}:{port} (expected success)"
        )
        logger.info(
            f"Pod {source_pod_name} -> {target_ip}:{port}: connected (expected)"
        )
    else:
        assert not connected, (
            f"Pod {source_pod_name} connected to {target_ip}:{port} "
            f"but should have been blocked"
        )
        logger.info(f"Pod {source_pod_name} -> {target_ip}:{port}: blocked (expected)")


def verify_dns_from_pod(pod_name, namespace=None, hostname=None):
    """
    Verify DNS resolution works from a pod.

    Args:
        pod_name (str): Pod name.
        namespace (str): Pod namespace. Defaults to cluster_namespace.
        hostname (str): Hostname to resolve.
            Defaults to kubernetes.default.svc.cluster.local.

    Raises:
        AssertionError: If DNS resolution fails.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    hostname = hostname or "kubernetes.default.svc.cluster.local"
    pod_obj = get_pod_obj(pod_name, namespace=namespace)
    cmd = f'python3 -c "import socket; ' f"print(socket.gethostbyname('{hostname}'))\""
    try:
        result = pod_obj.exec_cmd_on_pod(cmd, out_yaml_format=False, timeout=30)
        assert result and result.strip(), (
            f"DNS resolution returned empty result for {hostname} "
            f"from pod {pod_name}"
        )
        logger.info(f"DNS from pod {pod_name}: {hostname} -> {result.strip()}")
    except CommandFailed as ex:
        raise AssertionError(
            f"DNS resolution failed for {hostname} from pod " f"{pod_name}: {ex}"
        ) from ex


def get_service_ip(service_name, namespace=None):
    """
    Get the ClusterIP of a service.

    Args:
        service_name (str): Service name.
        namespace (str): Namespace. Defaults to cluster_namespace.

    Returns:
        str: ClusterIP address, or None if service not found.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind="Service", namespace=namespace)
    try:
        svc = ocp_obj.get(resource_name=service_name)
        return svc.get("spec", {}).get("clusterIP")
    except CommandFailed:
        logger.warning(f"Service {service_name} not found in {namespace}")
        return None


def get_route_url(route_name, namespace=None):
    """
    Get the URL for an OpenShift route.

    Args:
        route_name (str): Route name.
        namespace (str): Namespace. Defaults to cluster_namespace.

    Returns:
        str: Full URL (https://host), or None if not found.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_obj = OCP(kind="Route", namespace=namespace)
    try:
        route = ocp_obj.get(resource_name=route_name)
        host = route.get("spec", {}).get("host", "")
        tls = route.get("spec", {}).get("tls")
        scheme = "https" if tls else "http"
        return f"{scheme}://{host}"
    except CommandFailed:
        logger.warning(f"Route {route_name} not found in {namespace}")
        return None


def verify_policy_structure(policy_data):
    """
    Validate that a NetworkPolicy follows ODF design principles.

    Checks:
    - Has podSelector
    - Has policyTypes defined
    - If Egress type: has egress rules or is allow-all for operators

    Args:
        policy_data (dict): NetworkPolicy resource dict.

    Returns:
        dict: Validation result with 'valid' bool and 'issues' list.
    """
    issues = []
    name = policy_data.get("metadata", {}).get("name", "unknown")
    spec = policy_data.get("spec", {})

    if "podSelector" not in spec:
        issues.append(f"{name}: missing podSelector")

    policy_types = spec.get("policyTypes", [])
    if not policy_types:
        issues.append(f"{name}: missing policyTypes")

    if "Ingress" in policy_types and not spec.get("ingress"):
        logger.info(
            f"{name}: Ingress type with no ingress rules " f"(deny-all ingress)"
        )

    if "Egress" in policy_types and not spec.get("egress"):
        logger.info(f"{name}: Egress type with no egress rules " f"(deny-all egress)")

    return {"valid": len(issues) == 0, "issues": issues}
