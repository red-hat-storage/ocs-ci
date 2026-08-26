"""
Helper functions, fixtures, and constants for RHSTOR-8082 VM IP Translation
DR tests.

Provides utilities for managing network mapping ConfigMaps, polling DRPolicy
network mapping status, reading IPAMClaim IP addresses, and pytest fixtures
for setting up UDN/NAD prerequisites.

NOTE: Several constants reference DRPolicy/VRG/DRClusterConfig status field
names that are not yet finalized. These are marked with TODO comments.
Update once the developer confirms the API schema (RHSTOR-8082 open questions).
"""

import logging
import tempfile

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feature constants
# TODO: Update field names below once the developer confirms the API schema.
# ---------------------------------------------------------------------------

# Namespace for the network mapping ConfigMap (hub cluster)
VM_IP_TRANSLATION_NS = constants.OPENSHIFT_DR_SYSTEM_NAMESPACE

# ConfigMap data keys — TODO: confirm with developer
CM_KEY_PATTERN_MAPPINGS = "patternMappings"
CM_KEY_EXPLICIT_MAPPINGS = "explicitMappings"
CM_KEY_ENABLED = "enabled"

# DRPolicy status path for network mapping — TODO: confirm with developer
DRPOLICY_NM_FIELD = "networkMapping"
DRPOLICY_NM_STATUS_VALID = "Valid"
DRPOLICY_NM_STATUS_INVALID = "Invalid"
DRPOLICY_NM_METHOD_PATTERN = "pattern"
DRPOLICY_NM_METHOD_EXPLICIT = "explicit"
DRPOLICY_NM_METHOD_PATTERN_OVERRIDES = "pattern-with-overrides"

# DRPolicy NADs synced condition — TODO: confirm with developer
DRPOLICY_NADS_SYNCED_CONDITION = "NADsSynced"

# VRG condition name for IP translation — TODO: confirm with developer
VRG_CONDITION_NM_LOADED = "NetworkMappingLoaded"

# NAD label required for Ramen network discovery
NAD_DR_LABEL = "ramendr.openshift.io/network-id"

# DRClusterConfig status field listing discovered NADs — TODO: confirm
DRCLUSTERCONFIG_NAD_STATUS_FIELD = "networkAttachments"

# UDN and NAD names used across IP translation tests
VM_NETWORK_UDN_NAME = "vm-network"
VM_NETWORK_NAD_NAME = "vm-network"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def build_network_mapping_cm_data(
    pattern_rules=None, explicit_rules=None, enabled=True
):
    """
    Build the ConfigMap data dict for a network mapping ConfigMap.

    Args:
        pattern_rules (list[dict]): list of source/destination subnet pairs,
            e.g. [{"source": "192.168.1.0/24", "destination": "192.168.2.0/24"}]
        explicit_rules (list[dict]): list of source/destination IP pairs,
            e.g. [{"source": "192.168.1.100", "destination": "192.168.2.200"}]
        enabled (bool): whether to include the enabled=true opt-in key

    Returns:
        dict: ConfigMap.data contents

    # TODO: Update key names and serialization format to match the final API.
    """
    data = {}
    if enabled:
        data[CM_KEY_ENABLED] = "true"
    if pattern_rules:
        data[CM_KEY_PATTERN_MAPPINGS] = yaml.dump(
            pattern_rules, default_flow_style=False
        )
    if explicit_rules:
        data[CM_KEY_EXPLICIT_MAPPINGS] = yaml.dump(
            explicit_rules, default_flow_style=False
        )
    return data


def get_drpolicy_network_mapping_status(drpolicy_name):
    """
    Return the networkMapping sub-dict from DRPolicy.status.

    Args:
        drpolicy_name (str): name of the DRPolicy resource on the hub cluster

    Returns:
        dict: contents of status.networkMapping, or {} if absent

    # TODO: Update field path once API is confirmed.
    """
    config.switch_acm_ctx()
    drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)
    return drpolicy_ocp.get().get("status", {}).get(DRPOLICY_NM_FIELD, {})


def get_ipamclaim_ip(cluster_name, namespace, vm_name):
    """
    Return the IP address recorded in the IPAMClaim for the given VM.
    IPAMClaim name is assumed to match the VM name.

    Args:
        cluster_name (str): managed cluster to query
        namespace (str): namespace where the IPAMClaim lives
        vm_name (str): VM name (used as the IPAMClaim resource name)

    Returns:
        str: IP address without prefix length, or "" if not found

    # TODO: Confirm IPAMClaim name convention with developer.
    """
    config.switch_to_cluster_by_name(cluster_name)
    ipamclaim_ocp = ocp.OCP(
        kind="IPAMClaim",  # TODO: add constant once available
        namespace=namespace,
        resource_name=vm_name,
    )
    addresses = ipamclaim_ocp.get().get("status", {}).get("addresses", [""])
    return addresses[0].split("/")[0] if addresses else ""


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


@pytest.fixture(scope="class")
def setup_udn_nad(request):
    """
    Prerequisite fixture: creates a UserDefinedNetwork and a matching
    NetworkAttachmentDefinition (NAD) on each managed cluster, then tears
    them down after the test class completes.

    UDN spec:
      - name: vm-network
      - ipam.lifecycle: Persistent   (required for IPAMClaim support)
      - layer: Layer2

    NAD spec:
      - allowPersistentIPs: true      (required for IPAMClaim creation)
      - label ramendr.openshift.io/network-id: <network-id>
                                      (required for DRClusterConfig discovery)

    The fixture yields a dict with keys "udn_name" and "nad_name" so tests
    can reference them without hardcoding.

    # TODO: Replace subnet/prefix values with environment-specific ones read
    # from config once the env schema for this feature is settled.
    """
    managed_cluster_names = []
    created_resources = []  # list of (cluster_name, kind, namespace, name)

    config.switch_acm_ctx()
    for drcluster in dr_helpers.get_all_drclusters():
        managed_cluster_names.append(drcluster["metadata"]["name"])

    # Network ID label value — unique per UDN to allow DRClusterConfig discovery
    network_id = VM_NETWORK_UDN_NAME

    for cluster_name in managed_cluster_names:
        config.switch_to_cluster_by_name(cluster_name)

        # Create UserDefinedNetwork (cluster-scoped)
        # TODO: confirm UDN API version and exact spec fields once available.
        udn_manifest = {
            "apiVersion": "k8s.ovn.org/v1",  # TODO: confirm GVK
            "kind": "UserDefinedNetwork",
            "metadata": {"name": VM_NETWORK_UDN_NAME},
            "spec": {
                "topology": "Layer2",
                "layer2": {
                    "ipam": {"lifecycle": "Persistent"},
                    "subnets": [
                        # TODO: make subnet per-cluster from env config
                        (
                            "192.168.1.0/24"
                            if cluster_name == managed_cluster_names[0]
                            else "192.168.2.0/24"
                        )
                    ],
                },
            },
        }
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as f:
            templating.dump_data_to_temp_yaml(udn_manifest, f.name)
            exec_cmd(f"oc apply -f {f.name}")
        created_resources.append(
            (cluster_name, "UserDefinedNetwork", None, VM_NETWORK_UDN_NAME)
        )
        logger.info(f"Created UDN {VM_NETWORK_UDN_NAME} on {cluster_name}")

    yield {
        "udn_name": VM_NETWORK_UDN_NAME,
        "nad_name": VM_NETWORK_NAD_NAME,
        "network_id": network_id,
        "cluster_names": managed_cluster_names,
    }

    # Teardown: delete UDN on each managed cluster
    for cluster_name, kind, namespace, name in created_resources:
        try:
            config.switch_to_cluster_by_name(cluster_name)
            ns_flag = f"-n {namespace}" if namespace else ""
            exec_cmd(f"oc delete {kind} {name} {ns_flag} --ignore-not-found")
            logger.info(f"Deleted {kind} {name} on {cluster_name}")
        except Exception as exc:
            logger.warning(f"Failed to delete {kind} {name} on {cluster_name}: {exc}")


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
                    "labels": {
                        NAD_DR_LABEL: setup_udn_nad["network_id"],
                    },
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
    and deletes it on teardown.

    Usage::

        def test_foo(network_mapping_configmap):
            cm = network_mapping_configmap(
                "my-mapping",
                pattern_rules=[{"source": "192.168.1.0/24", "destination": "192.168.2.0/24"}],
            )
    """
    created = []

    def _factory(name, pattern_rules=None, explicit_rules=None, enabled=True):
        config.switch_acm_ctx()
        manifest = {
            "apiVersion": "v1",
            "kind": constants.CONFIGMAP,
            "metadata": {"name": name, "namespace": VM_IP_TRANSLATION_NS},
            "data": build_network_mapping_cm_data(
                pattern_rules, explicit_rules, enabled
            ),
        }
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".yaml", delete=False
        ) as cm_file:
            templating.dump_data_to_temp_yaml(manifest, cm_file.name)
            exec_cmd(f"oc create -f {cm_file.name}")
        created.append(name)
        logger.info(
            f"Created network mapping ConfigMap '{name}' in {VM_IP_TRANSLATION_NS}"
        )
        return ocp.OCP(
            kind=constants.CONFIGMAP,
            namespace=VM_IP_TRANSLATION_NS,
            resource_name=name,
        )

    yield _factory

    for cm_name in created:
        try:
            config.switch_acm_ctx()
            exec_cmd(
                f"oc delete configmap {cm_name} -n {VM_IP_TRANSLATION_NS} --ignore-not-found"
            )
            logger.info(f"Deleted network mapping ConfigMap '{cm_name}'")
        except Exception as exc:
            logger.warning(f"Failed to delete configmap '{cm_name}': {exc}")
