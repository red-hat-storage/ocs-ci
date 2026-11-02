"""
Helper functions, fixtures, and constants for RHSTOR-8082 VM IP Translation
DR tests.

Provides utilities for managing network mapping ConfigMaps, polling DRPolicy
network mapping status, reading IPAMClaim IP addresses, and pytest fixtures
for setting up UDN/NAD prerequisites.

API schema confirmed from manual testing on ammahapa-21a RDR (Aug 2026):
  - DR discovery label: ramendr.openshift.io/dr-network=true on the UDN
    (NOT the NAD — UDN controller owns the NAD and strips labels applied to it)
  - ConfigMap namespace: same namespace as the DRPC (openshift-dr-ops for
    discovered apps), referenced via DRPolicy.spec.networkMappingRef.name
  - ConfigMap data key: mappings.yaml (not patternMappings/explicitMappings)
  - ConfigMap schema: networkRef / cidr / explicitMappings / regexMappings
    with explicitMappings taking precedence over regexMappings
  - IPAMClaim name: <vm-name>.<interface-name> (e.g. vm-client.primary-udn)
  - IPAMClaim status field: .status.ips (list of CIDR strings, e.g. ["192.168.10.10/24"])
  - DRClusterConfig: lives on managed clusters (not hub); field status.networkAttachments
  - DRPolicy conditions: NetworkAttachmentsValidated; status.networkPeers lists discovered pairs
  - VRG condition: NetworkMappingLoaded=True once mapping is active
"""

import logging
import tempfile

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.utils import get_primary_cluster_config
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feature constants (confirmed from manual testing, Aug 2026)
# ---------------------------------------------------------------------------

# ConfigMap namespace: same as the DRPC namespace on the hub (openshift-dr-ops
# for discovered apps). The DRPolicy references it by name via networkMappingRef.
VM_IP_TRANSLATION_NS = constants.DR_OPS_NAMESPACE

# ConfigMap data key (the whole mapping document lives under this single key)
CM_KEY_MAPPINGS = "mappings.yaml"

# DRPolicy status condition names (confirmed)
DRPOLICY_NADS_SYNCED_CONDITION = "NetworkAttachmentsValidated"

# DRPolicy status path for network mapping
# TODO: confirm exact field name once status schema is finalized by developer
DRPOLICY_NM_FIELD = "networkMapping"
DRPOLICY_NM_STATUS_VALID = "Valid"
DRPOLICY_NM_STATUS_INVALID = "Invalid"
DRPOLICY_NM_METHOD_PATTERN = "pattern"
DRPOLICY_NM_METHOD_EXPLICIT = "explicit"
DRPOLICY_NM_METHOD_PATTERN_OVERRIDES = "pattern-with-overrides"

# VRG condition name for IP translation (confirmed working in manual test)
VRG_CONDITION_NM_LOADED = "NetworkMappingLoaded"

# VRG status fields for static IP discovery and translation (RHSTOR-8082).
# On the PRIMARY cluster VRG, Ramen populates staticIPDiscovery with the
# source addresses it found in each VM's IPAMClaim.
# On the SECONDARY cluster VRG, Ramen populates staticIPTranslationSpec with
# the translated targetIPs it will write into the destination IPAMClaims.
#
# Schema (confirmed from manual testing, Aug 2026):
#   VRG.status.staticIPDiscovery:
#     resources:
#       - resourceRef: {name: <vm>, namespace: <ns>}
#         networks:
#           - networkName: <udn-name>
#             addresses: ["<ip>"]
#
#   VRG.status.staticIPTranslationSpec:
#     ipTranslations:
#       - resourceRef: {name: <vm>, namespace: <ns>}
#         networks:
#           - networkName: <udn-name>
#             addresses:
#               - targetIP: "<translated-ip>"
VRG_STATIC_IP_DISCOVERY_FIELD = "staticIPDiscovery"
VRG_STATIC_IP_TRANSLATION_FIELD = "staticIPTranslationSpec"
VRG_STATIC_IP_DISCOVERY_RESOURCES_KEY = "resources"
VRG_STATIC_IP_TRANSLATION_KEY = "ipTranslations"

# DRPC status condition type set when a network mapping ConfigMap is loaded.
# True  → ConfigMap present, valid, and loaded by Ramen
# False → ConfigMap missing, empty, invalid regex, or networkMappingRef unset
DRPC_CONDITION_NM_LOADED = "NetworkMappingLoaded"

# Label applied to the UDN (propagates to NAD) to enable Ramen discovery.
# Value MUST be "true" — empty string does not match the discovery selector.
# Apply to the UserDefinedNetwork, NOT the NAD (UDN controller owns the NAD
# and will strip any label applied directly to it).
UDN_DR_LABEL = "ramendr.openshift.io/dr-network"
UDN_DR_LABEL_VALUE = "true"

# DRClusterConfig status field listing discovered NADs (confirmed)
DRCLUSTERCONFIG_NAD_STATUS_FIELD = "networkAttachments"

# UDN and NAD names used across IP translation tests
VM_NETWORK_UDN_NAME = "vm-network"
VM_NETWORK_NAD_NAME = "vm-network"

# UDN subnets, assigned by DR role (see setup_udn_nad). The static-IP VM
# workloads in ocs-workloads hardcode addresses inside PRIMARY_SUBNET
# (vm-static-ip-workload-1 -> .11, vm-static-ip-workload-2 -> .12); the network
# mapping ConfigMap translates them into SECONDARY_SUBNET on failover.
PRIMARY_SUBNET = "192.168.1.0/24"
SECONDARY_SUBNET = "192.168.2.0/24"
PRIMARY_SUBNET_NS2 = "192.168.10.0/24"
SECONDARY_SUBNET_NS2 = "192.168.20.0/24"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def build_network_mapping_cm_data(
    cluster1_name=None,
    cluster2_name=None,
    nad_namespace=None,
    nad_name=None,
    cluster1_cidr=None,
    cluster2_cidr=None,
    explicit_mappings=None,
    regex_mappings=None,
    network_mappings=None,
):
    """
    Build the ConfigMap data dict using the confirmed mappings.yaml schema.
    Supports single or multiple network mapping entries.

    Schema (confirmed from manual testing):
        version: "v1"
        networkMappings:
          - networkRef:
              nadNamespace: <nad_namespace>
              nadName: <nad_name>
            cidr:
              <cluster1>: <cidr1>
              <cluster2>: <cidr2>
            explicitMappings:            # optional; takes precedence over regexMappings
              - <cluster1>: <ip>
                <cluster2>: <ip>
            regexMappings:               # optional; bidirectional pattern rules
              <cluster1>-to-<cluster2>:
                - pattern: "..."
                  replacement: "..."
              <cluster2>-to-<cluster1>:
                - pattern: "..."
                  replacement: "..."

    Args:
        cluster1_name (str): first cluster name (e.g. "ammahapa-21a-c1")
        cluster2_name (str): second cluster name (e.g. "ammahapa-21a-c2")
        nad_namespace (str): namespace of the NAD (e.g. "vm-ip-translation-test")
        nad_name (str): name of the NAD / VM interface (e.g. "vm-network")
        cluster1_cidr (str): subnet on cluster1 (e.g. "192.168.1.0/24")
        cluster2_cidr (str): subnet on cluster2 (e.g. "192.168.2.0/24")
        explicit_mappings (list[dict]): optional pinned IP pairs, each a dict
            {cluster1_name: ip, cluster2_name: ip}; take precedence over regex
        regex_mappings (dict): optional {direction_key: [{pattern, replacement}]};
            if None, a simple host-preserving bidirectional regex is generated
            from the cluster CIDRs
        network_mappings (list[dict]): optional list of network mapping dicts.
            Each dict should define:
            - nad_namespace (or namespace)
            - nad_name (or name, default: VM_NETWORK_NAD_NAME)
            - cluster1_cidr
            - cluster2_cidr
            - explicit_mappings (optional)
            - regex_mappings (optional)
            - cluster1_name (optional, defaults to cluster1_name)
            - cluster2_name (optional, defaults to cluster2_name)

    Returns:
        dict: ConfigMap.data contents with a single key "mappings.yaml"
    """
    mapping_entries = []

    if network_mappings:
        for net in network_mappings:
            c1_name = net.get("cluster1_name", cluster1_name)
            c2_name = net.get("cluster2_name", cluster2_name)
            ns = net.get("nad_namespace") or net.get("namespace")
            name = net.get("nad_name") or net.get("name", VM_NETWORK_NAD_NAME)
            c1_cidr = net.get("cluster1_cidr")
            c2_cidr = net.get("cluster2_cidr")
            exp_maps = net.get("explicit_mappings")
            reg_maps = net.get("regex_mappings")

            entry = {
                "networkRef": {
                    "nadNamespace": ns,
                    "nadName": name,
                },
                "cidr": {
                    c1_name: c1_cidr,
                    c2_name: c2_cidr,
                },
            }
            if exp_maps:
                entry["explicitMappings"] = exp_maps

            if reg_maps is not None:
                if reg_maps:
                    entry["regexMappings"] = reg_maps
            elif not exp_maps:
                c1_prefix = c1_cidr.rsplit(".", 1)[0]
                c2_prefix = c2_cidr.rsplit(".", 1)[0]
                c1_escaped = c1_prefix.replace(".", "\\.")
                c2_escaped = c2_prefix.replace(".", "\\.")
                entry["regexMappings"] = {
                    f"{c1_name}-to-{c2_name}": [
                        {
                            "pattern": f"^{c1_escaped}\\.(\\d+)$",
                            "replacement": f"{c2_prefix}.$1",
                        }
                    ],
                    f"{c2_name}-to-{c1_name}": [
                        {
                            "pattern": f"^{c2_escaped}\\.(\\d+)$",
                            "replacement": f"{c1_prefix}.$1",
                        }
                    ],
                }
            mapping_entries.append(entry)
    else:
        mapping_entry = {
            "networkRef": {
                "nadNamespace": nad_namespace,
                "nadName": nad_name,
            },
            "cidr": {
                cluster1_name: cluster1_cidr,
                cluster2_name: cluster2_cidr,
            },
        }
        if explicit_mappings:
            mapping_entry["explicitMappings"] = explicit_mappings

        if regex_mappings is not None:
            if regex_mappings:
                mapping_entry["regexMappings"] = regex_mappings
        else:
            # Auto-generate simple host-preserving bidirectional regex rules
            c1_prefix = cluster1_cidr.rsplit(".", 1)[0]  # e.g. "192.168.10"
            c2_prefix = cluster2_cidr.rsplit(".", 1)[0]  # e.g. "192.168.20"
            c1_escaped = c1_prefix.replace(".", "\\.")
            c2_escaped = c2_prefix.replace(".", "\\.")
            mapping_entry["regexMappings"] = {
                f"{cluster1_name}-to-{cluster2_name}": [
                    {
                        "pattern": f"^{c1_escaped}\\.(\\d+)$",
                        "replacement": f"{c2_prefix}.$1",
                    }
                ],
                f"{cluster2_name}-to-{cluster1_name}": [
                    {
                        "pattern": f"^{c2_escaped}\\.(\\d+)$",
                        "replacement": f"{c1_prefix}.$1",
                    }
                ],
            }
        mapping_entries.append(mapping_entry)

    document = {
        "version": "v1",
        "networkMappings": mapping_entries,
    }
    return {CM_KEY_MAPPINGS: yaml.dump(document, default_flow_style=False)}


def get_drpolicy_name():
    """
    Return the name of the first DRPolicy found on the hub cluster.

    This is the standard 3-line pattern repeated across every test that needs
    the DRPolicy name:
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies, "No DRPolicy found on hub"
        drpolicy_name = existing_policies[0]["metadata"]["name"]

    Raises:
        AssertionError: if no DRPolicy exists on the hub

    Returns:
        str: DRPolicy name
    """
    config.switch_acm_ctx()
    existing_policies = dr_helpers.get_all_drpolicy()
    assert existing_policies, "No DRPolicy found on hub"
    return existing_policies[0]["metadata"]["name"]


def verify_vm_ip_and_ipamclaim(cluster_name, namespace, vm_name, interface_name):
    """
    Read the VMI IP and IPAMClaim IP for a VM and assert they match.

    This is the standard pair of calls repeated across IP translation tests:
        vm_ip   = get_vm_ip_from_vmi(cluster, ns, vm, iface)
        ipam_ip = get_ipamclaim_ip(cluster, ns, vm, iface)
        assert ipam_ip == vm_ip

    Args:
        cluster_name (str):   managed cluster to query
        namespace (str):      workload namespace
        vm_name (str):        VirtualMachine/VMI name
        interface_name (str): network interface name (e.g. "vm-network")

    Returns:
        str: the verified IP address (same value from both VMI and IPAMClaim)

    Raises:
        AssertionError: if the IP is empty or the two sources disagree
    """
    vm_ip = get_vm_ip_from_vmi(cluster_name, namespace, vm_name, interface_name)
    assert vm_ip, f"Could not read IP from VMI {vm_name} on {cluster_name}"
    ipam_ip = get_ipamclaim_ip(cluster_name, namespace, vm_name, interface_name)
    assert (
        ipam_ip == vm_ip
    ), f"{vm_name}: IPAMClaim IP {ipam_ip} != VMI IP {vm_ip} on {cluster_name}"
    return vm_ip


def delete_udn_ssh_helper_pod(cluster_names, namespaces, pod_name="udn-ssh-helper"):
    """
    Delete the UDN SSH helper pod from the given namespaces on each cluster.

    The helper pod is created dynamically during the test and is not part of the
    workload kustomize manifests, so the workload teardown's
    wait_for_all_resources_deletion would block waiting for it forever. Call this
    before the workload teardown to avoid that.

    Args:
        cluster_names (list): cluster names to clean up on.
        namespaces (list): workload namespaces to clean up in.
        pod_name (str): name of the SSH helper pod (default: "udn-ssh-helper").
    """
    for cluster_name in cluster_names:
        config.switch_to_cluster_by_name(cluster_name)
        for namespace in namespaces:
            pod_ocp = ocp.OCP(kind="Pod", namespace=namespace)
            try:
                pod_ocp.delete(resource_name=pod_name, wait=False)
                logger.info(
                    f"Deleted helper pod '{pod_name}' in {namespace} on {cluster_name}"
                )
            except Exception as exc:
                logger.warning(
                    f"Could not delete helper pod '{pod_name}' in {namespace} on {cluster_name}: {exc}"
                )


def ensure_udn_ssh_helper_pod(cluster_name, namespace, pod_name="udn-ssh-helper"):
    """
    Ensure a helper pod is running in the UDN namespace with sshpass and openssh-clients.

    Args:
        cluster_name (str): Managed cluster name where the pod should run.
        namespace (str): UDN workload namespace.
        pod_name (str): Name of the helper pod.

    Returns:
        str: pod name
    """
    config.switch_to_cluster_by_name(cluster_name)
    pod_ocp = ocp.OCP(kind="pod", namespace=namespace)
    existing = pod_ocp.get(resource_name=pod_name, dont_raise=True)
    if existing:
        phase = existing.get("status", {}).get("phase")
        container_statuses = existing.get("status", {}).get("containerStatuses", [])
        # ContainerStatusUnknown: kubelet lost contact with node; the container
        # state dict becomes empty (no running/terminated/waiting key) or the
        # terminated reason is explicitly "ContainerStatusUnknown".
        has_unknown_container = any(
            not cs.get("state")
            or cs.get("state", {}).get("terminated", {}).get("reason")
            == "ContainerStatusUnknown"
            for cs in container_statuses
        )
        if phase != "Running" or has_unknown_container:
            logger.info(
                f"Deleting stale helper pod '{pod_name}' "
                f"(phase: {phase}, container_issue: {has_unknown_container}) "
                f"in namespace '{namespace}' on {cluster_name}"
            )
            pod_ocp.delete(resource_name=pod_name, wait=True)
            existing = None

    if not existing:
        logger.info(
            f"Creating UDN SSH helper pod '{pod_name}' in namespace '{namespace}' on {cluster_name}"
        )
        exec_cmd(
            f"oc run {pod_name} --image=registry.access.redhat.com/ubi9/ubi "
            f"-n {namespace} --restart=Never -- sleep 3600"
        )
        pod_ocp.wait_for_resource(
            resource_name=pod_name,
            condition=constants.STATUS_RUNNING,
            timeout=120,
            sleep=5,
        )

    check_installed = pod_ocp.exec_oc_cmd(
        f"exec {pod_name} -- which sshpass", ignore_error=True, out_yaml_format=False
    )
    if not check_installed:
        logger.info(
            f"Installing openssh-clients and sshpass in helper pod '{pod_name}' on {cluster_name}"
        )
        pod_ocp.exec_oc_cmd(
            f"exec {pod_name} -- dnf install -y openssh-clients sshpass",
            out_yaml_format=False,
        )
    return pod_name


def wait_for_vm_ssh_via_udn_pod(
    cluster_name,
    namespace,
    vm_ip,
    username="fedora",
    password="fedora",
    pod_name="udn-ssh-helper",
    timeout=360,
    sleep_interval=5,
):
    """
    Wait until SSH connectivity to VM IP is established from the UDN helper pod.

    Args:
        cluster_name (str): Managed cluster name where VM and helper pod live.
        namespace (str): UDN workload namespace.
        vm_ip (str): IP address of the VM within the UDN.
        username (str): SSH username.
        password (str): SSH password.
        pod_name (str): Helper pod name.
        timeout (int): Timeout in seconds to wait for SSH readiness.
        sleep_interval (int): Sleep interval between attempts.

    Returns:
        bool: True if SSH is reachable and responds.
    """
    ensure_udn_ssh_helper_pod(cluster_name, namespace, pod_name)
    pod_ocp = ocp.OCP(kind="pod", namespace=namespace)
    test_cmd = (
        f"exec {pod_name} -- sshpass -p {password} ssh "
        f"-o StrictHostKeyChecking=no -o ConnectTimeout=5 "
        f"{username}@{vm_ip} 'echo ssh_ready'"
    )
    logger.info(
        f"Waiting for SSH to be ready on VM {vm_ip} via UDN helper pod '{pod_name}' (timeout: {timeout}s)"
    )

    def _check_ssh():
        try:
            res = pod_ocp.exec_oc_cmd(
                test_cmd, out_yaml_format=False, ignore_error=True
            )
            if res and "ssh_ready" in res:
                return True
        except Exception:
            pass
        return False

    for sample in TimeoutSampler(
        timeout=timeout, sleep=sleep_interval, func=_check_ssh
    ):
        if sample:
            logger.info(f"SSH to VM {vm_ip} via helper pod '{pod_name}' is ready.")
            return True
    raise TimeoutError(
        f"Timed out waiting for SSH connectivity to VM {vm_ip} via UDN helper pod '{pod_name}'"
    )


def run_vm_cmd_via_udn_pod(
    cluster_name,
    namespace,
    vm_ip,
    command,
    username="fedora",
    password="fedora",
    pod_name="udn-ssh-helper",
    timeout=360,
):
    """
    Execute a shell command inside a VM on a Primary UDN via a helper pod in the same namespace.

    Args:
        cluster_name (str): Managed cluster name where VM and helper pod live.
        namespace (str): UDN workload namespace.
        vm_ip (str): IP address of the VM within the UDN.
        command (str): Command to execute on the VM.
        username (str): SSH username (defaults to "fedora").
        password (str): SSH password (defaults to "fedora").
        pod_name (str): Helper pod name.
        timeout (int): Timeout to wait for SSH readiness before executing command.

    Returns:
        str: stdout of the command
    """
    wait_for_vm_ssh_via_udn_pod(
        cluster_name=cluster_name,
        namespace=namespace,
        vm_ip=vm_ip,
        username=username,
        password=password,
        pod_name=pod_name,
        timeout=timeout,
    )
    pod_ocp = ocp.OCP(kind="pod", namespace=namespace)
    ssh_cmd = (
        f"exec {pod_name} -- sshpass -p {password} ssh "
        f"-o StrictHostKeyChecking=no -o ConnectTimeout=15 "
        f"{username}@{vm_ip} '{command}'"
    )
    return pod_ocp.exec_oc_cmd(ssh_cmd, out_yaml_format=False)


def run_dd_io_via_udn_pod(
    cluster_name,
    namespace,
    vm_ip,
    file_path,
    size="10240",
    username="fedora",
    password="fedora",
    verify=False,
):
    """
    Perform dd I/O and optional MD5 checksum calculation on a VM via UDN helper pod.

    Args:
        cluster_name (str): Managed cluster name where VM lives.
        namespace (str): UDN workload namespace.
        vm_ip (str): IP address of the VM within the UDN.
        file_path (str): Destination file path on the VM.
        size (str): Size in blocks of 1KB (default "10240" for 10MB).
        username (str): SSH username (default "fedora").
        password (str): SSH password (default "fedora").
        verify (bool): Whether to calculate and return MD5 checksum.

    Returns:
        str or None: MD5 checksum if verify=True, else None
    """
    bs = 1024
    logger.info(
        f"Executing dd I/O on VM {vm_ip}:{file_path} via UDN helper pod in {namespace} on {cluster_name}"
    )
    run_vm_cmd_via_udn_pod(
        cluster_name=cluster_name,
        namespace=namespace,
        vm_ip=vm_ip,
        command=f"sudo dd if=/dev/urandom of={file_path} bs={bs} count={size}",
        username=username,
        password=password,
    )
    if verify:
        return cal_md5sum_vm_via_udn_pod(
            cluster_name=cluster_name,
            namespace=namespace,
            vm_ip=vm_ip,
            file_path=file_path,
            username=username,
            password=password,
        )


def cal_md5sum_vm_via_udn_pod(
    cluster_name,
    namespace,
    vm_ip,
    file_path,
    username="fedora",
    password="fedora",
):
    """
    Calculate MD5 checksum of a file on a VM via UDN helper pod.

    Args:
        cluster_name (str): Managed cluster name where VM lives.
        namespace (str): UDN workload namespace.
        vm_ip (str): IP address of the VM within the UDN.
        file_path (str): File path on the VM.
        username (str): SSH username.
        password (str): SSH password.

    Returns:
        str: MD5 checksum
    """
    out = run_vm_cmd_via_udn_pod(
        cluster_name=cluster_name,
        namespace=namespace,
        vm_ip=vm_ip,
        command=f"md5sum {file_path}",
        username=username,
        password=password,
    )
    return out.strip().split()[0]


def validate_data_integrity_vm_via_udn_pod(
    cluster_name,
    namespace,
    vm_ip,
    file_path,
    md5sum_original,
    app_state="Failover",
    username="fedora",
    password="fedora",
):
    """
    Validate data integrity (MD5 checksum) on a VM via UDN helper pod.

    Args:
        cluster_name (str): Managed cluster name where VM lives.
        namespace (str): UDN workload namespace.
        vm_ip (str): IP address of the VM within the UDN.
        file_path (str): File path on the VM.
        md5sum_original (str): Expected original MD5 checksum.
        app_state (str): App state (Failover/Relocate).
        username (str): SSH username.
        password (str): SSH password.
    """
    md5sum_new = cal_md5sum_vm_via_udn_pod(
        cluster_name=cluster_name,
        namespace=namespace,
        vm_ip=vm_ip,
        file_path=file_path,
        username=username,
        password=password,
    )
    logger.info(
        f"Comparing original checksum: {md5sum_original} of {file_path} with {md5sum_new} "
        f"on VM ({vm_ip}) after {app_state}"
    )
    assert (
        md5sum_original == md5sum_new
    ), f"Failed: MD5 comparison after {app_state} on VM ({vm_ip})"


def build_udn_manifest(name, namespace, subnet, dr_label=True):
    """
    Return a UserDefinedNetwork manifest dict for a Primary Layer2 UDN with
    persistent IPAM.

    Used by setup_udn_nad (creation) and test_missing_incorrectly_labeled_nad
    (restore after deletion). Centralising the dict avoids drift between the
    two sites.

    Args:
        name (str):      UDN resource name (e.g. ``VM_NETWORK_UDN_NAME``)
        namespace (str): namespace to create the UDN in
        subnet (str):    subnet CIDR (e.g. "192.168.1.0/24")
        dr_label (bool): when True, attach the DR discovery label
                         ``ramendr.openshift.io/dr-network=true`` (default True)

    Returns:
        dict: UDN manifest ready to ``oc apply``
    """
    manifest = {
        "apiVersion": "k8s.ovn.org/v1",
        "kind": "UserDefinedNetwork",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "topology": "Layer2",
            "layer2": {
                "role": "Primary",
                "subnets": [subnet],
                "ipam": {"lifecycle": "Persistent"},
            },
        },
    }
    if dr_label:
        manifest["metadata"]["labels"] = {UDN_DR_LABEL: UDN_DR_LABEL_VALUE}
    return manifest


def get_drpolicy_network_peers(drpolicy_name):
    """
    Return the networkPeers list from DRPolicy.status.

    Args:
        drpolicy_name (str): name of the DRPolicy resource on the hub cluster

    Returns:
        list: contents of status.networkPeers, or [] if absent
    """
    config.switch_acm_ctx()
    drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)
    return drpolicy_ocp.get().get("status", {}).get("networkPeers", [])


def verify_network_mapping_configmap_content(
    cm_name,
    expected_method,
    namespace=constants.DR_OPS_NAMESPACE,
    unmapped_ip=None,
):
    """
    Verify that the network mapping ConfigMap exists, contains valid YAML,
    and adheres to the expected mapping structure and counts.

    Args:
        cm_name (str): ConfigMap resource name.
        expected_method (str): Expected translation method ("pattern", "explicit", or "pattern-with-overrides").
        namespace (str): Namespace where the ConfigMap is located (default: openshift-dr-ops).
        unmapped_ip (str): Optional IP that should NOT appear in the ConfigMap content.

    Raises:
        AssertionError: If validation checks fail.
    """
    import yaml as _yaml

    config.switch_acm_ctx()
    logger.info(
        f"Verify ConfigMap '{cm_name}' content contains expected method={expected_method}"
    )
    cm_ocp = ocp.OCP(
        kind="ConfigMap",
        namespace=namespace,
        resource_name=cm_name,
    )
    cm_data_raw = cm_ocp.get().get("data", {}).get("mappings.yaml", "")
    assert cm_data_raw, f"ConfigMap '{cm_name}' has empty mappings.yaml"
    parsed = _yaml.safe_load(cm_data_raw)
    nm_list = parsed.get("networkMappings", [])
    assert nm_list, f"ConfigMap '{cm_name}' has no networkMappings entries"
    for nm_entry in nm_list:
        regex_keys = nm_entry.get("regexMappings", {})
        explicit_keys = nm_entry.get("explicitMappings", [])
        total_patterns = sum(len(v) for v in regex_keys.values())

        if (
            expected_method == DRPOLICY_NM_METHOD_PATTERN
            or expected_method == "pattern"
        ):
            assert (
                total_patterns > 0
            ), f"Expected pattern rules in '{cm_name}', found none"
            assert (
                not explicit_keys
            ), f"Expected zero explicit mappings in '{cm_name}', found some"
        elif (
            expected_method == DRPOLICY_NM_METHOD_EXPLICIT
            or expected_method == "explicit"
        ):
            assert explicit_keys, f"Expected explicit rules in '{cm_name}', found none"
            assert (
                total_patterns == 0
            ), f"Expected zero pattern mappings in '{cm_name}', found some"
        elif (
            expected_method == DRPOLICY_NM_METHOD_PATTERN_OVERRIDES
            or expected_method == "pattern-with-overrides"
        ):
            assert (
                total_patterns > 0
            ), f"Expected pattern rules in '{cm_name}', found none"
            assert explicit_keys, f"Expected explicit rules in '{cm_name}', found none"

    if unmapped_ip:
        logger.info(
            f"Verify the unmapped IP {unmapped_ip} is not listed in ConfigMap '{cm_name}'"
        )
        assert (
            unmapped_ip not in cm_data_raw
        ), f"Unmapped IP {unmapped_ip} should not appear in '{cm_name}' ConfigMap content"


def create_raw_network_mapping_configmap(
    name,
    data_dict,
    namespace=constants.DR_OPS_NAMESPACE,
    drpolicy_name=None,
):
    """
    Create a ConfigMap on the ACM hub cluster with the given data dictionary
    and optionally link it to a DRPolicy.

    Args:
        name (str): ConfigMap name.
        data_dict (dict): Data dictionary to include in the ConfigMap (e.g. {"mappings.yaml": "..."}).
        namespace (str): Namespace where the ConfigMap is created (default: openshift-dr-ops).
        drpolicy_name (str): Optional DRPolicy to patch with networkMappingRef.

    Returns:
        ocp.OCP: ConfigMap OCP resource wrapper.
    """
    config.switch_acm_ctx()
    manifest = {
        "apiVersion": "v1",
        "kind": constants.CONFIGMAP,
        "metadata": {"name": name, "namespace": namespace},
        "data": data_dict,
    }
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as f:
        templating.dump_data_to_temp_yaml(manifest, f.name)
        exec_cmd(f"oc apply -f {f.name}")
    logger.info(f"Created ConfigMap '{name}' in {namespace}")

    if drpolicy_name:
        set_drpolicy_network_mapping_ref(drpolicy_name, name)

    return ocp.OCP(
        kind=constants.CONFIGMAP,
        namespace=namespace,
        resource_name=name,
    )


def get_drpolicy_network_mapping_status(drpolicy_name):
    """
    Return the networkMapping sub-dict from DRPolicy.status.

    Args:
        drpolicy_name (str): name of the DRPolicy resource on the hub cluster

    Returns:
        dict: contents of status.networkMapping, or {} if absent
    """
    config.switch_acm_ctx()
    drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)
    return drpolicy_ocp.get().get("status", {}).get(DRPOLICY_NM_FIELD, {})


def get_ipamclaim_ip(cluster_name, namespace, vm_name, interface_name):
    """
    Return the IP address recorded in the IPAMClaim for the given VM interface.

    IPAMClaim name format (confirmed): <vm-name>.<interface-name>
    e.g. for VM "vm-client" on interface "primary-udn" → "vm-client.primary-udn"

    Args:
        cluster_name (str): managed cluster to query
        namespace (str): namespace where the IPAMClaim lives
        vm_name (str): VM name
        interface_name (str): VM network interface name (e.g. "primary-udn")

    Returns:
        str: IP address without prefix length, or "" if not found
    """
    config.switch_to_cluster_by_name(cluster_name)
    claim_name = f"{vm_name}.{interface_name}"
    ipamclaim_ocp = ocp.OCP(
        kind="IPAMClaim",
        namespace=namespace,
        resource_name=claim_name,
    )
    # status.ips is a list of CIDR strings e.g. ["192.168.10.10/24"]
    ips = ipamclaim_ocp.get().get("status", {}).get("ips", [])
    return ips[0].split("/")[0] if ips else ""


def get_vm_ip_from_vmi(cluster_name, namespace, vm_name, interface_name=None):
    """
    Return the IP address shown in the VMI's status.interfaces for the given VM.

    Reads from VirtualMachineInstance.status.interfaces[*].ipAddress — this is
    the IP as seen at the OVN-K8s layer, which is what IP translation affects.
    Use this to verify the translated IP after failover and the restored IP after
    relocate, alongside get_ipamclaim_ip() which reads the IPAM allocation.

    Args:
        cluster_name (str): managed cluster to query
        namespace (str): namespace of the VMI
        vm_name (str): VM name (VMI has the same name as the VM)
        interface_name (str): if given, return the IP for that specific interface
                             name; if None, return the first interface's IP

    Returns:
        str: IP address (without prefix length), or "" if not found
    """
    config.switch_to_cluster_by_name(cluster_name)
    vmi_ocp = ocp.OCP(
        kind="VirtualMachineInstance",
        namespace=namespace,
        resource_name=vm_name,
    )
    interfaces = vmi_ocp.get().get("status", {}).get("interfaces", [])
    if interface_name:
        for iface in interfaces:
            if iface.get("name") == interface_name:
                return iface.get("ipAddress", "")
        return ""
    return interfaces[0].get("ipAddress", "") if interfaces else ""


def wait_for_drpolicy_network_peers(drpolicy_name, timeout=120):
    """
    Poll until DRPolicy.status.networkPeers is non-empty.

    networkPeers is populated once DRPolicy.spec.networkMappingRef is set
    AND the NADs are discovered symmetrically on both clusters.

    Args:
        drpolicy_name (str): name of the DRPolicy resource
        timeout (int): seconds to wait

    Returns:
        list: the populated networkPeers list

    Raises:
        AssertionError: if networkPeers is not populated within timeout
    """
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=5,
        func=lambda: get_drpolicy_network_peers(drpolicy_name),
    ):
        if sample:
            return sample
    raise AssertionError(
        f"DRPolicy {drpolicy_name} status.networkPeers did not populate "
        f"within {timeout}s"
    )


def set_drpolicy_network_mapping_ref(drpolicy_name, configmap_name):
    """
    Patch DRPolicy.spec.networkMappingRef to reference the given ConfigMap.

    Args:
        drpolicy_name (str): name of the DRPolicy to patch
        configmap_name (str): name of the network-mapping ConfigMap
    """
    config.switch_acm_ctx()
    exec_cmd(
        f"oc patch drpolicy {drpolicy_name} --type=merge "
        f'-p \'{{"spec":{{"networkMappingRef":{{"name":"{configmap_name}"}}}}}}\''
    )
    logger.info(
        f"Patched DRPolicy {drpolicy_name} networkMappingRef → {configmap_name}"
    )


def wait_for_drpolicy_network_mapping_status(
    drpolicy_name, expected_status, timeout=120
):
    """
    Poll until DRPolicy networkMapping.status matches expected_status.

    Args:
        drpolicy_name (str): name of the DRPolicy resource
        expected_status (str): one of DRPOLICY_NM_STATUS_VALID / _INVALID
        timeout (int): seconds to wait before raising AssertionError

    Raises:
        AssertionError: if expected_status is not reached within timeout
    """
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=5,
        func=lambda: get_drpolicy_network_mapping_status(drpolicy_name).get(
            "status", ""
        ),
    ):
        if sample == expected_status:
            return
    raise AssertionError(
        f"DRPolicy {drpolicy_name} networkMapping.status did not reach "
        f"'{expected_status}' within {timeout}s"
    )


def wait_for_drpc_network_mapping_loaded(
    drpc_name,
    expected_status="True",
    expected_message_substring=None,
    timeout=300,
):
    """
    Poll until DRPlacementControl.status.conditions[NetworkMappingLoaded] matches expected_status
    and optionally contains expected_message_substring in its message.

    Args:
        drpc_name (str): name of the DRPC resource
        expected_status (str): "True" or "False"
        expected_message_substring (str, optional): Substring expected in condition message
        timeout (int): seconds to wait before raising AssertionError

    Raises:
        AssertionError: if expected_status is not reached within timeout
    """
    from ocs_ci.ocs.resources.drpc import DRPC

    def _get_drpc_nm_loaded_matched():
        config.switch_acm_ctx()
        drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESPACE, resource_name=drpc_name)
        drpc_data = drpc_obj.get()
        conds = drpc_data.get("status", {}).get("conditions", [])
        logger.info(f"DRPC '{drpc_name}' conditions: {conds}")
        cond = next(
            (c for c in conds if c.get("type") == DRPC_CONDITION_NM_LOADED),
            None,
        )
        if not cond:
            return False
        if cond.get("status") != expected_status:
            return False
        if expected_message_substring:
            msg = cond.get("message", "")
            return expected_message_substring.lower() in msg.lower()
        return True

    msg_info = (
        f" (message containing '{expected_message_substring}')"
        if expected_message_substring
        else ""
    )
    logger.info(
        f"Waiting for DRPC '{drpc_name}' condition {DRPC_CONDITION_NM_LOADED}={expected_status}"
        f"{msg_info} (timeout {timeout}s)"
    )
    for sample in TimeoutSampler(
        timeout=timeout, sleep=5, func=_get_drpc_nm_loaded_matched
    ):
        if sample:
            break
    else:
        config.switch_acm_ctx()
        drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESPACE, resource_name=drpc_name)
        actual_conditions = drpc_obj.get().get("status", {}).get("conditions", [])
        raise AssertionError(
            f"Timeout: DRPC '{drpc_name}' condition '{DRPC_CONDITION_NM_LOADED}' did not reach expected status '{expected_status}'"  # noqa: E501
            f"{msg_info} within {timeout}s. "
            f"Actual DRPC conditions are: {actual_conditions}"
        )

    config.switch_acm_ctx()
    drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESPACE, resource_name=drpc_name)
    conds = drpc_obj.get().get("status", {}).get("conditions", [])
    nm_cond = next(
        (c for c in conds if c.get("type") == DRPC_CONDITION_NM_LOADED),
        None,
    )
    assert (
        nm_cond is not None
    ), f"Validation Error: DRPC '{drpc_name}' is completely missing the expected status condition '{DRPC_CONDITION_NM_LOADED}'."  # noqa: E501
    assert nm_cond.get("status") == expected_status, (
        f"Validation Error: DRPC '{drpc_name}' condition '{DRPC_CONDITION_NM_LOADED}' status mismatch. "
        f"Expected: '{expected_status}', Actual condition block: {nm_cond}"
    )


def get_drpc_network_mapping_loaded_condition(drpc_name):
    """
    Fetch the NetworkMappingLoaded status condition dictionary from DRPC.

    Args:
        drpc_name (str): name of the DRPC resource

    Returns:
        dict: The condition block containing keys like "status", "reason", "message",
              or None if not found.
    """
    from ocs_ci.ocs.resources.drpc import DRPC

    config.switch_acm_ctx()
    drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESPACE, resource_name=drpc_name)
    conds = drpc_obj.get().get("status", {}).get("conditions", [])
    cond = next(
        (c for c in conds if c.get("type") == DRPC_CONDITION_NM_LOADED),
        None,
    )
    return cond


def verify_drpolicy_network_attachments_validated(drpolicy_name, timeout=300):
    """
    Poll until DRPolicy status condition NetworkAttachmentsValidated is True and has reason Validated.
    Also assert that the status.networkPeers displays NAD details.

    Args:
        drpolicy_name (str): name of the DRPolicy resource on ACM hub
        timeout (int): seconds to wait before raising AssertionError
    """

    def _get_validated_condition():
        config.switch_acm_ctx()
        drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)
        conditions = drpolicy_ocp.get().get("status", {}).get("conditions", [])
        cond = next(
            (c for c in conditions if c.get("type") == "NetworkAttachmentsValidated"),
            None,
        )
        return cond

    logger.info(
        f"Waiting for DRPolicy '{drpolicy_name}' NetworkAttachmentsValidated=True (timeout {timeout}s)"
    )
    for sample in TimeoutSampler(
        timeout=timeout, sleep=5, func=_get_validated_condition
    ):
        if (
            sample
            and sample.get("status") == "True"
            and sample.get("reason") == "Validated"
        ):
            if "symmetric" in sample.get("message", "").lower():
                break
    else:
        config.switch_acm_ctx()
        drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)
        actual_conditions = drpolicy_ocp.get().get("status", {}).get("conditions", [])
        raise AssertionError(
            f"Timeout: DRPolicy '{drpolicy_name}' condition 'NetworkAttachmentsValidated' did not reach status='True' with reason='Validated' within {timeout}s. "  # noqa: E501
            f"Actual DRPolicy conditions are: {actual_conditions}"
        )

    # Double check and assert the values
    cond = _get_validated_condition()
    assert (
        cond is not None
    ), f"Validation Error: DRPolicy '{drpolicy_name}' is missing status condition 'NetworkAttachmentsValidated'."
    assert cond.get("status") == "True", (
        f"Validation Error: DRPolicy '{drpolicy_name}' status condition 'NetworkAttachmentsValidated' status mismatch. "
        f"Expected: 'True', Actual: {cond}"
    )
    assert cond.get("reason") == "Validated", (
        f"Validation Error: DRPolicy '{drpolicy_name}' status condition 'NetworkAttachmentsValidated' reason mismatch. "
        f"Expected: 'Validated', Actual: {cond}"
    )
    assert "symmetric" in cond.get("message", "").lower(), (
        f"Validation Error: DRPolicy '{drpolicy_name}' status condition 'NetworkAttachmentsValidated' message mismatch. "  # noqa: E501
        f"Expected 'symmetric' message, Actual message: '{cond.get('message')}'"
    )

    # Verify networkPeers section is non-empty
    peers = get_drpolicy_network_peers(drpolicy_name)
    assert peers, (
        f"Validation Error: DRPolicy '{drpolicy_name}' has successfully validated network attachments, "
        f"but status.networkPeers is completely empty. Expected peer details to be displayed."
    )
    logger.info(
        f"Verified DRPolicy {drpolicy_name} NetworkAttachmentsValidated successfully. Peers: {peers}"
    )


def get_drpolicy_network_attachments_condition(drpolicy_name):
    """
    Return the status, reason and message for the NetworkAttachmentsValidated
    condition from a DRPolicy.

    Args:
        drpolicy_name (str): Name of the DRPolicy resource on the ACM hub.

    Returns:
        tuple: (status, reason, message) strings, or (None, None, None) if the
               condition is not present.
    """
    config.switch_acm_ctx()
    drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)
    conditions = drpolicy_ocp.get().get("status", {}).get("conditions", [])
    cond = next(
        (c for c in conditions if c.get("type") == DRPOLICY_NADS_SYNCED_CONDITION),
        None,
    )
    if cond is None:
        return None, None, None
    return cond.get("status"), cond.get("reason"), cond.get("message")


def wait_for_drclusterconfig_nad_discovery(cluster_name, timeout=120):
    """
    Poll until DRClusterConfig.status.networkAttachments is non-empty on the
    given managed cluster, indicating the DR discovery controller has picked up
    the labeled UDN/NAD.

    Args:
        cluster_name (str): name of the managed cluster to check
        timeout (int): seconds to wait before raising AssertionError

    Raises:
        AssertionError: if the NAD is not discovered within timeout
    """

    def _get_net_attachments():
        config.switch_to_cluster_by_name(cluster_name)
        return (
            ocp.OCP(kind=constants.DRCLUSTERCONFIG, resource_name=cluster_name)
            .get()
            .get("status", {})
            .get(DRCLUSTERCONFIG_NAD_STATUS_FIELD, [])
        )

    logger.info(
        f"Waiting for DRClusterConfig on '{cluster_name}' to discover "
        f"NAD '{VM_NETWORK_NAD_NAME}' in status.{DRCLUSTERCONFIG_NAD_STATUS_FIELD} "
        f"(timeout {timeout}s)"
    )
    for sample in TimeoutSampler(timeout=timeout, sleep=5, func=_get_net_attachments):
        if sample:
            logger.info(
                f"DRClusterConfig on '{cluster_name}' discovered NADs: {sample}"
            )
            return sample
    raise AssertionError(
        f"DRClusterConfig on '{cluster_name}' did not discover any NADs in "
        f"status.{DRCLUSTERCONFIG_NAD_STATUS_FIELD} within {timeout}s"
    )


def verify_vrg_static_ip_discovery(
    cluster_name, workload, expected_ip, network_name, timeout=120
):
    """
    Verify that the primary VRG status.staticIPDiscovery is correctly populated
    with the workload's source/primary IP details.

    Accepts both SimpleNamespace workloads (.namespace, .drpc_name) and
    CnvWorkloadDiscoveredApps objects (.workload_namespace,
    .discovered_apps_placement_name). The actual DRPC name on the hub is
    resolved via get_drpc_name() when .drpc_name is absent.

    Args:
        cluster_name (str): name of the managed cluster
        workload: workload object (SimpleNamespace or CnvWorkloadDiscoveredApps)
        expected_ip (str): expected source static IP (e.g. 192.168.1.11)
        network_name (str): network name (e.g. vm-network)
        timeout (int): seconds to wait for discovery details to populate
    """
    from ocs_ci.ocs.resources.drpc import get_drpc_name

    ns = getattr(workload, "namespace", None) or workload.workload_namespace
    vrg_name = (
        getattr(workload, "drpc_name", None)
        or getattr(workload, "discovered_apps_placement_name", None)
        or get_drpc_name(ns)
    )
    logger.info(
        f"Verifying VRG staticIPDiscovery on cluster '{cluster_name}' "
        f"for VM '{workload.vm_name}' (expected IP: {expected_ip})"
    )

    def _get_vrg_discovery():
        config.switch_to_cluster_by_name(cluster_name)
        # Discovered-app VRGs live in the DR ops namespace, not the workload namespace
        vrg_ocp = ocp.OCP(
            kind="VolumeReplicationGroup",
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=vrg_name,
        )
        data = vrg_ocp.get(dont_raise=True)
        if not data:
            return None
        return (
            data.get("status", {})
            .get(VRG_STATIC_IP_DISCOVERY_FIELD, {})
            .get(VRG_STATIC_IP_DISCOVERY_RESOURCES_KEY, [])
        )

    for sample in TimeoutSampler(timeout=timeout, sleep=5, func=_get_vrg_discovery):
        if sample:
            break
    else:
        raise AssertionError(
            f"Timeout: VRG '{vrg_name}' on '{cluster_name}' staticIPDiscovery was not populated within {timeout}s."
        )

    discovery_resources = _get_vrg_discovery()
    assert (
        discovery_resources
    ), f"VRG '{vrg_name}' has empty staticIPDiscovery resources on '{cluster_name}'"

    # Search for correct resource ref and network details
    found_ref = False
    for res in discovery_resources:
        ref = res.get("resourceRef", {})
        if ref.get("name") == workload.vm_name and ref.get("namespace") == ns:
            found_ref = True
            networks = res.get("networks", [])
            assert (
                networks
            ), f"VRG Discovery error: networks list is empty for VM '{workload.vm_name}'."
            found_net = False
            for net in networks:
                if net.get("networkName") == network_name:
                    found_net = True
                    addresses = net.get("addresses", [])
                    assert expected_ip in addresses, (
                        f"VRG Discovery error: expected IP '{expected_ip}' not found in discovery addresses. "
                        f"Actual addresses: {addresses}"
                    )
            assert (
                found_net
            ), f"VRG Discovery error: expected network '{network_name}' not found under discovered networks: {networks}"

    assert found_ref, (
        f"VRG Discovery error: did not find resourceRef matching VM '{workload.vm_name}' "
        f"and namespace '{ns}' in discovered resources: {discovery_resources}"
    )
    logger.info(
        f"Successfully verified VRG staticIPDiscovery on cluster '{cluster_name}' "
        f"for VM '{workload.vm_name}' with IP '{expected_ip}'."
    )


def verify_vrg_static_ip_translation(
    cluster_name, workload, expected_translated_ip, network_name, timeout=120
):
    """
    Verify that the secondary VRG status.staticIPTranslationSpec is correctly populated
    with the translated target IP details as per the translation rule.

    Accepts both SimpleNamespace workloads (.namespace, .drpc_name) and
    CnvWorkloadDiscoveredApps objects (.workload_namespace,
    .discovered_apps_placement_name). The actual DRPC name on the hub is
    resolved via get_drpc_name() when .drpc_name is absent.

    Args:
        cluster_name (str): name of the managed cluster
        workload: workload object (SimpleNamespace or CnvWorkloadDiscoveredApps)
        expected_translated_ip (str): expected translated static IP (e.g. 192.168.2.200)
        network_name (str): network name (e.g. vm-network)
        timeout (int): seconds to wait for translation details to populate
    """
    from ocs_ci.ocs.resources.drpc import get_drpc_name

    ns = getattr(workload, "namespace", None) or workload.workload_namespace
    vrg_name = (
        getattr(workload, "drpc_name", None)
        or getattr(workload, "discovered_apps_placement_name", None)
        or get_drpc_name(ns)
    )
    logger.info(
        f"Verifying VRG staticIPTranslationSpec on cluster '{cluster_name}' "
        f"for VM '{workload.vm_name}' (expected translated IP: {expected_translated_ip})"
    )

    def _get_vrg_translation():
        config.switch_to_cluster_by_name(cluster_name)
        # Discovered-app VRGs live in the DR ops namespace, not the workload namespace
        vrg_ocp = ocp.OCP(
            kind="VolumeReplicationGroup",
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=vrg_name,
        )
        data = vrg_ocp.get(dont_raise=True)
        if not data:
            return None
        # Support both spec.staticIPTranslationSpec and status.staticIPTranslationSpec
        trans_spec = data.get("status", {}).get(VRG_STATIC_IP_TRANSLATION_FIELD)
        if not trans_spec:
            trans_spec = data.get("spec", {}).get(VRG_STATIC_IP_TRANSLATION_FIELD)
        if not trans_spec:
            return None
        return trans_spec.get(VRG_STATIC_IP_TRANSLATION_KEY, [])

    for sample in TimeoutSampler(timeout=timeout, sleep=5, func=_get_vrg_translation):
        if sample:
            break
    else:
        raise AssertionError(
            f"Timeout: VRG '{vrg_name}' on '{cluster_name}' staticIPTranslationSpec was not populated within {timeout}s."  # noqa: E501
        )

    translations = _get_vrg_translation()
    assert (
        translations
    ), f"VRG '{vrg_name}' has empty staticIPTranslationSpec.ipTranslations on '{cluster_name}'"

    # Search for correct resource ref and translated network details
    found_ref = False
    for trans in translations:
        ref = trans.get("resourceRef", {})
        if ref.get("name") == workload.vm_name and ref.get("namespace") == ns:
            found_ref = True
            networks = trans.get("networks", [])
            assert (
                networks
            ), f"VRG Translation error: networks list is empty for VM '{workload.vm_name}'."
            found_net = False
            for net in networks:
                if net.get("networkName") == network_name:
                    found_net = True
                    addresses = net.get("addresses", [])
                    assert (
                        addresses
                    ), f"VRG Translation error: addresses list is empty under network '{network_name}'."
                    # Each address is a dictionary carrying 'targetIP'
                    target_ips = [
                        addr.get("targetIP")
                        for addr in addresses
                        if addr.get("targetIP")
                    ]
                    assert expected_translated_ip in target_ips, (
                        f"VRG Translation error: expected translated targetIP '{expected_translated_ip}' "
                        f"not found in translation targets. Actual targetIPs: {target_ips}"
                    )
            assert (
                found_net
            ), f"VRG Translation error: expected network '{network_name}' not found under translation networks: {networks}"  # noqa: E501

    assert found_ref, (
        f"VRG Translation error: did not find resourceRef matching VM '{workload.vm_name}' "
        f"and namespace '{ns}' in translation list: {translations}"
    )
    logger.info(
        f"Successfully verified VRG staticIPTranslationSpec on cluster '{cluster_name}' "
        f"for VM '{workload.vm_name}' with target IP '{expected_translated_ip}'."
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def setup_udn_nad():
    """
    Prerequisite fixture: creates the workload namespace and a Primary
    UserDefinedNetwork (with persistent IPAM) on each managed cluster, then
    tears them down after the test completes.

    Function scoped on purpose: the workload teardown deletes the project it
    ran in, which is this same namespace (and takes the UDN with it). A wider
    scope would leave later tests in the class without a namespace or UDN.

    Confirmed requirements from manual testing (Aug 2026):
      - Namespace must carry label k8s.ovn.org/primary-user-defined-network=""
        BEFORE the UDN is created (cannot be patched in afterwards).
      - UDN role: Primary, topology: Layer2, ipam.lifecycle: Persistent.
      - UDN controller auto-creates the NAD — do NOT create NAD separately.
      - DR discovery label ramendr.openshift.io/dr-network=true goes on the
        UDN (NOT the NAD). The UDN controller owns the NAD via ownerReference
        and strips any label applied directly to the NAD. Value must be "true";
        empty string does not match the discovery selector.
      - After labeling the UDN, verify DRClusterConfig.status.networkAttachments
        on each managed cluster shows the NAD discovered.

    The fixture yields a dict with keys "udn_name", "nad_name",
    "cluster_names", "cluster_subnets" so tests can reference them.
    """
    created_resources = []  # list of (cluster_name, kind, namespace, name)

    config.switch_acm_ctx()
    # get_all_drclusters() returns the DRCluster names, not the objects
    managed_cluster_names = dr_helpers.get_all_drclusters()

    # Per-cluster subnets, assigned by DR role rather than by drcluster list
    # order. The static-IP VM workloads hardcode an address in PRIMARY_SUBNET
    # (e.g. 192.168.1.11), so the primary must always own that subnet or the
    # VMI will fail to start with an address outside its UDN subnet.
    primary_cluster_name = get_primary_cluster_config().ENV_DATA["cluster_name"]
    secondary_cluster_name = next(
        name for name in managed_cluster_names if name != primary_cluster_name
    )
    cluster_subnets = {
        primary_cluster_name: PRIMARY_SUBNET,
        secondary_cluster_name: SECONDARY_SUBNET,
    }

    workload_namespace = constants.VM_IP_TRANSLATION_WORKLOAD_NS
    workload_namespace_2 = constants.VM_STATIC_IP_WORKLOAD_NS

    ns_configs = [
        {
            "namespace": workload_namespace,
            "subnets": {
                primary_cluster_name: PRIMARY_SUBNET,
                secondary_cluster_name: SECONDARY_SUBNET,
            },
        },
        {
            "namespace": workload_namespace_2,
            "subnets": {
                primary_cluster_name: PRIMARY_SUBNET_NS2,
                secondary_cluster_name: SECONDARY_SUBNET_NS2,
            },
        },
    ]

    for cluster_name in managed_cluster_names:
        config.switch_to_cluster_by_name(cluster_name)

        for ns_cfg in ns_configs:
            ns = ns_cfg["namespace"]
            subnet = ns_cfg["subnets"][cluster_name]

            # Clear any stale VMs from previous runs in the workload namespace
            exec_cmd(f"oc delete vm --all -n {ns} --ignore-not-found")

            # Step 1: namespace with the primary-UDN label
            ns_manifest = {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": ns,
                    "labels": {"k8s.ovn.org/primary-user-defined-network": ""},
                },
            }
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".yaml", delete=False
            ) as f:
                templating.dump_data_to_temp_yaml(ns_manifest, f.name)
                exec_cmd(f"oc apply -f {f.name}")
            created_resources.append((cluster_name, "Namespace", None, ns))

            # Step 2: network-config Secret (networkData DHCP) consumed by the VM's
            # cloudInitNoCloud.networkDataSecretRef; must exist before oc create -k.
            create_network_config_secret(ns)
            created_resources.append(
                (cluster_name, "Secret", ns, VM_NETWORK_CONFIG_SECRET_NAME)
            )

            # Step 3: Primary UDN with persistent IPAM; label triggers DR discovery
            udn_manifest = build_udn_manifest(
                name=VM_NETWORK_UDN_NAME,
                namespace=ns,
                subnet=subnet,
            )
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".yaml", delete=False
            ) as f:
                templating.dump_data_to_temp_yaml(udn_manifest, f.name)
                exec_cmd(f"oc apply -f {f.name}")
            created_resources.append(
                (
                    cluster_name,
                    "UserDefinedNetwork",
                    ns,
                    VM_NETWORK_UDN_NAME,
                )
            )
            logger.info(
                f"Created Primary UDN {VM_NETWORK_UDN_NAME} on {cluster_name} in namespace {ns} "
                f"(subnet {subnet}, DR label set)"
            )

    # Wait for DRClusterConfig to discover the NAD on each cluster before
    # yielding. Without this, tests that trigger failover immediately after
    # setup may hit "DRPolicy not valid: NADs absent on peer cluster".
    for cluster_name in managed_cluster_names:
        wait_for_drclusterconfig_nad_discovery(cluster_name)

    yield {
        "udn_name": VM_NETWORK_UDN_NAME,
        "nad_name": VM_NETWORK_NAD_NAME,
        "cluster_names": managed_cluster_names,
        # Convenience entries for primary and secondary, resolved by DR role not list order.
        # Use these instead of cluster_names[0]/[1] which is ordering-dependent.
        "primary_cluster": primary_cluster_name,
        "secondary_cluster": secondary_cluster_name,
        "cluster_subnets": cluster_subnets,
        "workload_namespace": workload_namespace,
        "workload_namespaces": [workload_namespace, workload_namespace_2],
        "workload_namespace_1": workload_namespace,
        "workload_namespace_2": workload_namespace_2,
        "subnets_ns1": {
            primary_cluster_name: PRIMARY_SUBNET,
            secondary_cluster_name: SECONDARY_SUBNET,
        },
        "subnets_ns2": {
            primary_cluster_name: PRIMARY_SUBNET_NS2,
            secondary_cluster_name: SECONDARY_SUBNET_NS2,
        },
    }

    # Teardown: delete UDN then namespace on each managed cluster
    for cluster_name, kind, namespace, name in reversed(created_resources):
        try:
            config.switch_to_cluster_by_name(cluster_name)
            ns_flag = f"-n {namespace}" if namespace else ""
            exec_cmd(f"oc delete {kind} {name} {ns_flag} --ignore-not-found")
            logger.info(f"Deleted {kind}/{name} on {cluster_name}")
        except Exception as exc:
            logger.warning(f"Failed to delete {kind}/{name} on {cluster_name}: {exc}")


@pytest.fixture
def setup_nad_in_namespace(setup_udn_nad):
    """
    Per-test fixture: creates the NAD in the given workload namespace on both
    managed clusters. Must be called with the namespace as a parameter after
    the workload namespace is known.

    Returns a factory: call it with the workload namespace to create the NAD.
    The NAD is deleted from all clusters on teardown.

    Depends on setup_udn_nad for the UDN to already exist.
    """
    created = []  # list of (cluster_name, namespace)

    def _factory(namespace):
        for cluster_name in setup_udn_nad["cluster_names"]:
            config.switch_to_cluster_by_name(cluster_name)
            nad_manifest = {
                "apiVersion": "k8s.cni.cncf.io/v1",
                "kind": constants.NETWORK_ATTACHMENT_DEFINITION,
                "metadata": {
                    "name": VM_NETWORK_NAD_NAME,
                    "namespace": namespace,
                },
                "spec": {
                    "config": (
                        '{"cniVersion":"0.3.1",'
                        f'"name":"{VM_NETWORK_NAD_NAME}",'
                        '"type":"ovn-k8s-cni-overlay",'
                        f'"netAttachDefName":"{namespace}/{VM_NETWORK_NAD_NAME}",'
                        '"allowPersistentIPs":true}'
                    )
                },
            }
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".yaml", delete=False
            ) as f:
                templating.dump_data_to_temp_yaml(nad_manifest, f.name)
                exec_cmd(f"oc apply -f {f.name}")
            created.append((cluster_name, namespace))
            logger.info(
                f"Created NAD {VM_NETWORK_NAD_NAME} in {namespace} on {cluster_name}"
            )

    yield _factory

    for cluster_name, namespace in created:
        try:
            config.switch_to_cluster_by_name(cluster_name)
            exec_cmd(
                f"oc delete net-attach-def "
                f"{VM_NETWORK_NAD_NAME} -n {namespace} --ignore-not-found"
            )
            logger.info(
                f"Deleted NAD {VM_NETWORK_NAD_NAME} from {namespace} on {cluster_name}"
            )
        except Exception as exc:
            logger.warning(
                f"Failed to delete NAD in {namespace} on {cluster_name}: {exc}"
            )


@pytest.fixture
def network_mapping_configmap():
    """
    Factory fixture that creates a network mapping ConfigMap on the hub cluster
    (in the DRPC namespace) and patches DRPolicy.spec.networkMappingRef to
    reference it, then cleans up on teardown.

    The ConfigMap must be in the same namespace as the DRPC (openshift-dr-ops
    for discovered apps) per the confirmed API schema. The linkage is via
    DRPolicy.spec.networkMappingRef.name, NOT auto-discovery by ConfigMap name.

    Usage::

        def test_foo(network_mapping_configmap, setup_udn_nad):
            network_mapping_configmap(
                "my-mapping",
                cluster1_name=..., cluster2_name=...,
                nad_namespace=..., nad_name=...,
                cluster1_cidr="192.168.1.0/24",
                cluster2_cidr="192.168.2.0/24",
            )
    """
    created_cms = []
    drpolicy_patched = []

    def _factory(
        name,
        cluster1_name=None,
        cluster2_name=None,
        nad_namespace=None,
        nad_name=None,
        cluster1_cidr=None,
        cluster2_cidr=None,
        explicit_mappings=None,
        regex_mappings=None,
        drpolicy_name=None,
        network_mappings=None,
    ):
        config.switch_acm_ctx()
        manifest = {
            "apiVersion": "v1",
            "kind": constants.CONFIGMAP,
            "metadata": {
                "name": name,
                "namespace": VM_IP_TRANSLATION_NS,
                "labels": {constants.ACM_BACKUP_LABEL: ""},
            },
            "data": build_network_mapping_cm_data(
                cluster1_name=cluster1_name,
                cluster2_name=cluster2_name,
                nad_namespace=nad_namespace,
                nad_name=nad_name,
                cluster1_cidr=cluster1_cidr,
                cluster2_cidr=cluster2_cidr,
                explicit_mappings=explicit_mappings,
                regex_mappings=regex_mappings,
                network_mappings=network_mappings,
            ),
        }
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".yaml", delete=False
        ) as cm_file:
            templating.dump_data_to_temp_yaml(manifest, cm_file.name)
            exec_cmd(f"oc apply -f {cm_file.name}")
        created_cms.append(name)
        logger.info(
            f"Created network mapping ConfigMap '{name}' in {VM_IP_TRANSLATION_NS}"
        )

        if drpolicy_name:
            set_drpolicy_network_mapping_ref(drpolicy_name, name)
            drpolicy_patched.append(drpolicy_name)

        return ocp.OCP(
            kind=constants.CONFIGMAP,
            namespace=VM_IP_TRANSLATION_NS,
            resource_name=name,
        )

    yield _factory

    # Teardown: remove networkMappingRef then delete ConfigMap.
    # A JSON-patch "remove" is required: a merge-patch with an empty object
    # leaves the existing reference in place.
    for drpolicy_name in drpolicy_patched:
        try:
            config.switch_acm_ctx()
            exec_cmd(
                f"oc patch drpolicy {drpolicy_name} --type=json "
                f'-p \'[{{"op":"remove","path":"/spec/networkMappingRef"}}]\''
            )
            logger.info(f"Removed networkMappingRef from DRPolicy {drpolicy_name}")
        except Exception as exc:
            logger.warning(
                f"Failed to clear networkMappingRef on {drpolicy_name}: {exc}"
            )
    for cm_name in created_cms:
        try:
            config.switch_acm_ctx()
            exec_cmd(
                f"oc delete configmap {cm_name} -n {VM_IP_TRANSLATION_NS} --ignore-not-found"
            )
            logger.info(f"Deleted network mapping ConfigMap '{cm_name}'")
        except Exception as exc:
            logger.warning(f"Failed to delete configmap '{cm_name}': {exc}")


# ---------------------------------------------------------------------------
# Runtime VM creation helpers and fixture (RHSTOR-8082)
# ---------------------------------------------------------------------------


# Name of the cloud-init networkData Secret created in the workload namespace
# on both managed clusters before the VM is deployed.  The VM template
# references it via cloudInitConfigDrive.networkDataSecretRef.
VM_NETWORK_CONFIG_SECRET_NAME = "vm-client-network-config"

_NETWORK_DATA_DHCP = "ethernets:\n  eth0:\n    dhcp4: true\n"


def create_network_config_secret(namespace, secret_name=VM_NETWORK_CONFIG_SECRET_NAME):
    """
    Create (or recreate) the cloud-init networkData Secret in *namespace*.

    The Secret stores the DHCP networkData that KubeVirt injects into the VM
    via ``cloudInitConfigDrive.networkDataSecretRef``.  Using a Secret instead
    of inlining ``networkData`` keeps the VM manifest cluster-agnostic and
    allows Ramen to replicate the Secret alongside the PVC.

    The Secret must exist on **both** managed clusters before the VM is
    applied.  Call this function once per cluster inside a
    ``config.switch_to_cluster_by_name`` context.

    Args:
        namespace (str):   Workload namespace.
        secret_name (str): Secret name; defaults to
                           ``VM_NETWORK_CONFIG_SECRET_NAME``.

    Returns:
        dict: The ``oc apply`` response (unused by callers but useful for
              debugging).
    """
    secret_manifest = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": secret_name,
            "namespace": namespace,
        },
        "stringData": {
            "networkData": _NETWORK_DATA_DHCP,
        },
    }
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as f:
        templating.dump_data_to_temp_yaml(secret_manifest, f.name)
        exec_cmd(f"oc apply -f {f.name} -n {namespace}")
    logger.info(
        f"Applied network-config secret '{secret_name}' in namespace '{namespace}'"
    )
