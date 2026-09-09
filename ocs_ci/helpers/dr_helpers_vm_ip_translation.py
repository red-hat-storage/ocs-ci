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


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def build_network_mapping_cm_data(
    cluster1_name,
    cluster2_name,
    nad_namespace,
    nad_name,
    cluster1_cidr,
    cluster2_cidr,
    explicit_mappings=None,
    regex_mappings=None,
):
    """
    Build the ConfigMap data dict using the confirmed mappings.yaml schema.

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
        nad_namespace (str): namespace of the NAD (e.g. "verizon-app")
        nad_name (str): name of the NAD / VM interface (e.g. "primary-udn")
        cluster1_cidr (str): subnet on cluster1 (e.g. "192.168.10.0/24")
        cluster2_cidr (str): subnet on cluster2 (e.g. "192.168.20.0/24")
        explicit_mappings (list[dict]): optional pinned IP pairs, each a dict
            {cluster1_name: ip, cluster2_name: ip}; take precedence over regex
        regex_mappings (dict): optional {direction_key: [{pattern, replacement}]};
            if None, a simple host-preserving bidirectional regex is generated
            from the cluster CIDRs

    Returns:
        dict: ConfigMap.data contents with a single key "mappings.yaml"
    """
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

    document = {
        "version": "v1",
        "networkMappings": [mapping_entry],
    }
    return {CM_KEY_MAPPINGS: yaml.dump(document, default_flow_style=False)}


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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def setup_udn_nad(request):
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
    managed_cluster_names = []
    created_resources = []  # list of (cluster_name, kind, namespace, name)

    config.switch_acm_ctx()
    for drcluster in dr_helpers.get_all_drclusters():
        managed_cluster_names.append(drcluster["metadata"]["name"])

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

    for cluster_name in managed_cluster_names:
        config.switch_to_cluster_by_name(cluster_name)

        # Step 1: namespace with the primary-UDN label
        ns_manifest = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": workload_namespace,
                "labels": {"k8s.ovn.org/primary-user-defined-network": ""},
            },
        }
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as f:
            templating.dump_data_to_temp_yaml(ns_manifest, f.name)
            exec_cmd(f"oc apply -f {f.name}")
        created_resources.append((cluster_name, "Namespace", None, workload_namespace))

        # Step 2: Primary UDN with persistent IPAM; label triggers DR discovery
        udn_manifest = {
            "apiVersion": "k8s.ovn.org/v1",
            "kind": "UserDefinedNetwork",
            "metadata": {
                "name": VM_NETWORK_UDN_NAME,
                "namespace": workload_namespace,
                "labels": {UDN_DR_LABEL: UDN_DR_LABEL_VALUE},
            },
            "spec": {
                "topology": "Layer2",
                "layer2": {
                    "role": "Primary",
                    "subnets": [cluster_subnets[cluster_name]],
                    "ipam": {"lifecycle": "Persistent"},
                },
            },
        }
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as f:
            templating.dump_data_to_temp_yaml(udn_manifest, f.name)
            exec_cmd(f"oc apply -f {f.name}")
        created_resources.append(
            (
                cluster_name,
                "UserDefinedNetwork",
                workload_namespace,
                VM_NETWORK_UDN_NAME,
            )
        )
        logger.info(
            f"Created Primary UDN {VM_NETWORK_UDN_NAME} on {cluster_name} "
            f"(subnet {cluster_subnets[cluster_name]}, DR label set)"
        )

    yield {
        "udn_name": VM_NETWORK_UDN_NAME,
        "nad_name": VM_NETWORK_NAD_NAME,
        "cluster_names": managed_cluster_names,
        "cluster_subnets": cluster_subnets,
        "workload_namespace": workload_namespace,
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
                f"oc delete {constants.NETWORK_ATTACHMENT_DEFINITION} "
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
        cluster1_name,
        cluster2_name,
        nad_namespace,
        nad_name,
        cluster1_cidr,
        cluster2_cidr,
        explicit_mappings=None,
        regex_mappings=None,
        drpolicy_name=None,
    ):
        config.switch_acm_ctx()
        manifest = {
            "apiVersion": "v1",
            "kind": constants.CONFIGMAP,
            "metadata": {"name": name, "namespace": VM_IP_TRANSLATION_NS},
            "data": build_network_mapping_cm_data(
                cluster1_name=cluster1_name,
                cluster2_name=cluster2_name,
                nad_namespace=nad_namespace,
                nad_name=nad_name,
                cluster1_cidr=cluster1_cidr,
                cluster2_cidr=cluster2_cidr,
                explicit_mappings=explicit_mappings,
                regex_mappings=regex_mappings,
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

    # Teardown: clear networkMappingRef then delete ConfigMap
    for drpolicy_name in drpolicy_patched:
        try:
            config.switch_acm_ctx()
            exec_cmd(
                f"oc patch drpolicy {drpolicy_name} --type=merge "
                f'-p \'{{"spec":{{"networkMappingRef":{{}}}}}}\''
            )
            logger.info(f"Cleared networkMappingRef on DRPolicy {drpolicy_name}")
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
