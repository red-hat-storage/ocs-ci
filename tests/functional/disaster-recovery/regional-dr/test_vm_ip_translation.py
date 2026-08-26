"""
RHSTOR-8082: VM IP Translation During DR Failover/Failback

When VMs with static IPs fail over across sites with no shared L2 network,
Ramen reads the VM's IPAMClaim from the primary cluster, applies rules from a
network mapping ConfigMap in openshift-dr-system, and creates a translated
IPAMClaim at the secondary cluster. OVN-Kubernetes then assigns the translated
IP via the UDN — no guest OS modification needed.

Translation methods:
  pattern             — subnet prefix replaced, host octet preserved
  explicit            — IP-to-IP mapping; host octet may change
  pattern-with-overrides — explicit takes precedence over pattern

Test classes:
  TestVMIPTranslationConfigMap     — ConfigMap validation, NAD discovery,
                                     controller conditions (no failover needed)
  TestVMIPTranslationDRCycle       — Full failover + failback IP translation,
                                     backward compatibility, opt-in behavior,
                                     and edge cases
  TestVMIPTranslationPlatformSpecific — Hub recovery, post-upgrade, BM HCP

NOTE: Several assertions reference DRPolicy/VRG/DRClusterConfig status field
names that are not yet finalized. These are marked with # TODO comments.
Update the constants at the top of this file once the developer confirms the
API schema (RHSTOR-8082 open questions).
"""

import logging
import tempfile

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
    tier2,
)
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers import wait_for_all_resources_deletion
from ocs_ci.helpers.dr_helpers_vm_ip_translation import (
    DRCLUSTERCONFIG_NAD_STATUS_FIELD,
    DRPOLICY_NADS_SYNCED_CONDITION,
    DRPOLICY_NM_METHOD_EXPLICIT,
    DRPOLICY_NM_METHOD_PATTERN,
    DRPOLICY_NM_METHOD_PATTERN_OVERRIDES,
    DRPOLICY_NM_STATUS_INVALID,
    DRPOLICY_NM_STATUS_VALID,
    VM_IP_TRANSLATION_NS,
    VRG_CONDITION_NM_LOADED,
    build_network_mapping_cm_data,
    get_drpolicy_network_mapping_status,
    get_ipamclaim_ip,
    wait_for_drpolicy_network_mapping_status,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check, exec_cmd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Test IP addresses used across scenarios (dr1 = primary, dr2 = secondary)
# ---------------------------------------------------------------------------

DR1_SUBNET = "192.168.1.0/24"
DR2_SUBNET = "192.168.2.0/24"
VM1_IP_DR1 = "192.168.1.50"
VM1_IP_DR2_PATTERN = "192.168.2.50"  # host octet preserved by pattern rule
VM_DB_IP_DR1 = "192.168.1.100"
VM_DB_IP_DR2_EXPLICIT = "192.168.2.200"  # overridden by explicit rule
VM_DB_IP_DR2_PATTERN = "192.168.2.100"  # what pattern alone would give (NOT used)
UNMAPPED_IP_DR1 = "192.168.1.99"  # no explicit rule → untranslated

# Subnets for proportional translation scenario (different sizes)
PROP_SRC_SUBNET = "10.0.0.0/24"
PROP_DST_SUBNET = "172.16.0.0/16"


# ---------------------------------------------------------------------------
# Class 1: ConfigMap validation and controller condition tests
# (No failover required — hub-only or managed-cluster inspection)
# ---------------------------------------------------------------------------


@rdr
@tier2
@turquoise_squad
@skipif_ocs_version("<4.21")
class TestVMIPTranslationConfigMap:
    """
    Validate that the DRPolicy controller correctly classifies each translation
    ConfigMap type and that the DRClusterConfig NAD discovery works as expected.
    No VM failover is performed in this class.

    Prerequisites:
      - ODF >= 4.21 with RHSTOR-8082 feature enabled
      - ACM hub cluster with at least two registered DRClusters
      - UDN vm-network on both managed clusters (setup_udn_nad fixture)
      - NADs labeled with ramendr.openshift.io/network-id (setup_udn_nad fixture)
      - DRClusterConfig with NAD discovery enabled on both managed clusters
    """

    @pytest.mark.polarion_id("OCS-XXXX")
    @pytest.mark.parametrize(
        "cm_name,pattern_rules,explicit_rules,expected_method,expected_pattern_count,expected_explicit_count",
        [
            pytest.param(
                "vm-ip-map-pattern",
                [{"source": DR1_SUBNET, "destination": DR2_SUBNET}],
                None,
                DRPOLICY_NM_METHOD_PATTERN,
                1,
                0,
                id="pattern-only",
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
            pytest.param(
                "vm-ip-map-explicit",
                None,
                [
                    {"source": VM1_IP_DR1, "destination": VM1_IP_DR2_PATTERN},
                    {"source": VM_DB_IP_DR1, "destination": VM_DB_IP_DR2_EXPLICIT},
                ],
                DRPOLICY_NM_METHOD_EXPLICIT,
                0,
                2,
                id="explicit-only",
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
            pytest.param(
                "vm-ip-map-overrides",
                [{"source": DR1_SUBNET, "destination": DR2_SUBNET}],
                [{"source": VM_DB_IP_DR1, "destination": VM_DB_IP_DR2_EXPLICIT}],
                DRPOLICY_NM_METHOD_PATTERN_OVERRIDES,
                1,
                1,
                id="pattern-with-overrides",
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
        ],
    )
    def test_configmap_validation(
        self,
        setup_udn_nad,
        network_mapping_configmap,
        cm_name,
        pattern_rules,
        explicit_rules,
        expected_method,
        expected_pattern_count,
        expected_explicit_count,
    ):
        """
        TC-1 / TC-2 / TC-3 / TC-8: Verify that pattern-only, explicit-only,
        and pattern-with-overrides ConfigMaps are each accepted and classified
        correctly by the DRPolicy controller.

        For explicit-only (TC-2): also confirms that an IP not listed in any
        rule (UNMAPPED_IP_DR1) has no translation entry — i.e. the ConfigMap
        does not implicitly map unknown IPs.
        """
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies, "No DRPolicy found on hub"
        drpolicy_name = existing_policies[0]["metadata"]["name"]

        network_mapping_configmap(
            cm_name, pattern_rules=pattern_rules, explicit_rules=explicit_rules
        )

        # Poll until DRPolicy reports Valid status for the new ConfigMap
        # TODO: DRPolicy must reference the ConfigMap by name — confirm the
        # linkage mechanism (annotation, spec field, or auto-discovery by name).
        wait_for_drpolicy_network_mapping_status(
            drpolicy_name, DRPOLICY_NM_STATUS_VALID
        )

        nm_status = get_drpolicy_network_mapping_status(drpolicy_name)

        # TODO: confirm exact field names in nm_status
        assert nm_status.get("translationMethod") == expected_method, (
            f"Expected translationMethod={expected_method}, "
            f"got {nm_status.get('translationMethod')}"
        )
        assert nm_status.get("patternMappings") == expected_pattern_count, (
            f"Expected patternMappings={expected_pattern_count}, "
            f"got {nm_status.get('patternMappings')}"
        )
        assert nm_status.get("explicitMappings") == expected_explicit_count, (
            f"Expected explicitMappings={expected_explicit_count}, "
            f"got {nm_status.get('explicitMappings')}"
        )

        if cm_name == "vm-ip-map-explicit":
            # Unmapped IP must not appear in the explicit mapping list
            nm_raw = get_drpolicy_network_mapping_status(drpolicy_name)
            raw_data = str(nm_raw)
            assert UNMAPPED_IP_DR1 not in raw_data, (
                f"Unmapped IP {UNMAPPED_IP_DR1} should not appear in "
                "explicit-only ConfigMap status"
            )

        logger.info(
            f"ConfigMap '{cm_name}' validated: method={expected_method}, "
            f"patternMappings={expected_pattern_count}, "
            f"explicitMappings={expected_explicit_count}"
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    @pytest.mark.parametrize(
        "scenario,cm_data",
        [
            pytest.param(
                "missing",
                None,
                id="missing-configmap",
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
            pytest.param(
                "empty",
                {},
                id="empty-configmap",
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
            pytest.param(
                "invalid-regex",
                {"patternMappings": "[invalid-regex"},  # TODO: confirm format
                id="invalid-regex-configmap",
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
        ],
    )
    def test_invalid_configmap_handling(self, scenario, cm_data):
        """
        TC-14: Verify graceful handling when the ConfigMap is missing, empty,
        or contains an invalid regex pattern. In all cases the VRG/DRPC
        condition NetworkMappingLoaded should be False with a descriptive reason.
        DRPC operations must not crash.
        """
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies, "No DRPolicy found on hub"
        drpolicy_name = existing_policies[0]["metadata"]["name"]

        if cm_data is not None:
            # Create a deliberately bad ConfigMap
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".yaml", delete=False
            ) as f:
                manifest = {
                    "apiVersion": "v1",
                    "kind": constants.CONFIGMAP,
                    "metadata": {
                        "name": f"vm-ip-map-bad-{scenario}",
                        "namespace": VM_IP_TRANSLATION_NS,
                    },
                    "data": cm_data,
                }
                templating.dump_data_to_temp_yaml(manifest, f.name)
                exec_cmd(f"oc create -f {f.name}")
            cm_name = f"vm-ip-map-bad-{scenario}"
        else:
            # Missing ConfigMap: nothing to create
            cm_name = "vm-ip-map-nonexistent"

        try:
            # TODO: trigger or reference the ConfigMap from the DRPolicy
            # then poll for the condition reflecting the invalid state
            for sample in TimeoutSampler(
                timeout=120,
                sleep=5,
                func=lambda: get_drpolicy_network_mapping_status(drpolicy_name).get(
                    "status", ""
                ),
            ):
                if sample == DRPOLICY_NM_STATUS_INVALID:
                    break
            nm_status = get_drpolicy_network_mapping_status(drpolicy_name)
            assert nm_status.get("status") == DRPOLICY_NM_STATUS_INVALID, (
                f"Expected networkMapping.status={DRPOLICY_NM_STATUS_INVALID} "
                f"for scenario '{scenario}', got {nm_status}"
            )
            # Reason must be non-empty to aid diagnosis
            assert nm_status.get(
                "reason"
            ), "Expected a non-empty reason alongside Invalid status"
            logger.info(
                f"Scenario '{scenario}': DRPolicy correctly reports Invalid "
                f"with reason: {nm_status.get('reason')}"
            )
        finally:
            if cm_data is not None:
                try:
                    config.switch_acm_ctx()
                    exec_cmd(
                        f"oc delete configmap {cm_name} -n {VM_IP_TRANSLATION_NS} --ignore-not-found"
                    )
                except Exception as exc:
                    logger.warning(f"Cleanup failed for '{cm_name}': {exc}")

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_drclusterconfig_nad_discovery(self, setup_udn_nad):
        """
        TC-6 / TC-7: Verify that:
          - DRClusterConfig controller lists all NADs labeled with
            ramendr.openshift.io/network-id in status.networkAttachments
          - DRPolicy controller validates that labeled NADs exist on both
            clusters and reports NADsSynced condition correctly
        """
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies, "No DRPolicy found on hub"
        drpolicy_name = existing_policies[0]["metadata"]["name"]
        drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)

        # Verify NADsSynced condition on DRPolicy
        # TODO: confirm condition name and expected reason/status values
        conditions = drpolicy_ocp.get().get("status", {}).get("conditions", [])
        nads_synced = next(
            (c for c in conditions if c.get("type") == DRPOLICY_NADS_SYNCED_CONDITION),
            None,
        )
        assert (
            nads_synced is not None
        ), f"DRPolicy {drpolicy_name} missing condition '{DRPOLICY_NADS_SYNCED_CONDITION}'"
        assert (
            nads_synced.get("status") == "True"
        ), f"Expected NADsSynced=True, got: {nads_synced}"
        logger.info(f"DRPolicy NADsSynced condition: {nads_synced}")

        # Verify DRClusterConfig.status.networkAttachments on each managed cluster
        drcluster_list = dr_helpers.get_all_drclusters()
        assert drcluster_list, "No DRClusters found on hub"
        for drcluster in drcluster_list:
            cluster_name = drcluster["metadata"]["name"]
            # DRClusterConfig lives on the hub and is named after the managed cluster
            config.switch_acm_ctx()
            drclusterconfig_ocp = ocp.OCP(
                kind=constants.DRCLUSTERCONFIG, resource_name=cluster_name
            )
            net_attachments = (
                drclusterconfig_ocp.get()
                .get("status", {})
                .get(DRCLUSTERCONFIG_NAD_STATUS_FIELD, [])
            )
            assert net_attachments, (
                f"DRClusterConfig for {cluster_name} has empty "
                f"status.{DRCLUSTERCONFIG_NAD_STATUS_FIELD} — "
                f"expected at least one labeled NAD to be discovered"
            )
            logger.info(
                f"DRClusterConfig {cluster_name} discovered NADs: {net_attachments}"
            )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_vrg_network_mapping_loaded_condition(
        self, setup_udn_nad, setup_nad_in_namespace, discovered_apps_dr_workload_cnv
    ):
        """
        TC-7b: Verify that VRG correctly reports NetworkMappingLoaded=True
        when a valid network mapping ConfigMap is active and the VMI reports
        the expected translated IP.

        Requires a workload with UDN/IPAMClaim configured.
        # TODO: Extend discovered_apps_dr_workload_cnv to support static-IP
        # VMs with IPAMClaims, or create a dedicated fixture.
        """
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        assert cnv_workloads, "No CNV workload deployed"
        workload = cnv_workloads[-1]
        namespace = workload.workload_namespace
        resource_name = workload.discovered_apps_placement_name + "-drpc"

        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies
        drpolicy_name = existing_policies[0]["metadata"]["name"]

        # Create a pattern-only ConfigMap so there is a valid mapping
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as f:
            manifest = {
                "apiVersion": "v1",
                "kind": constants.CONFIGMAP,
                "metadata": {
                    "name": "vm-ip-map-vrg-test",
                    "namespace": VM_IP_TRANSLATION_NS,
                },
                "data": build_network_mapping_cm_data(
                    pattern_rules=[{"source": DR1_SUBNET, "destination": DR2_SUBNET}]
                ),
            }
            templating.dump_data_to_temp_yaml(manifest, f.name)
            config.switch_acm_ctx()
            exec_cmd(f"oc create -f {f.name}")

        try:
            wait_for_drpolicy_network_mapping_status(
                drpolicy_name, DRPOLICY_NM_STATUS_VALID
            )

            # Check VRG condition on the primary cluster
            primary_cluster = workload.preferred_primary_cluster
            config.switch_to_cluster_by_name(primary_cluster)
            # TODO: use a VRG helper once the condition schema is finalized
            vrg_ocp = ocp.OCP(
                kind="VolumeReplicationGroup",  # TODO: add constant
                namespace=namespace,
                resource_name=resource_name,
            )
            vrg_conditions = vrg_ocp.get().get("status", {}).get("conditions", [])
            nm_condition = next(
                (c for c in vrg_conditions if c.get("type") == VRG_CONDITION_NM_LOADED),
                None,
            )
            assert (
                nm_condition is not None
            ), f"VRG missing condition '{VRG_CONDITION_NM_LOADED}'"
            assert (
                nm_condition.get("status") == "True"
            ), f"Expected {VRG_CONDITION_NM_LOADED}=True, got: {nm_condition}"
            logger.info(f"VRG {VRG_CONDITION_NM_LOADED} condition: {nm_condition}")
        finally:
            config.switch_acm_ctx()
            exec_cmd(
                "oc delete configmap vm-ip-map-vrg-test "
                f"-n {VM_IP_TRANSLATION_NS} --ignore-not-found"
            )


# ---------------------------------------------------------------------------
# Class 2: Full DR cycle with IP translation
# ---------------------------------------------------------------------------


@rdr
@tier2
@turquoise_squad
@skipif_ocs_version("<4.21")
class TestVMIPTranslationDRCycle:
    """
    Full failover + failback scenarios exercising actual IP translation,
    backward compatibility, and opt-in enforcement.

    Prerequisites (in addition to base RDR environment):
      - ODF >= 4.21 with RHSTOR-8082 feature enabled
      - UDN vm-network on both clusters with ipam.lifecycle: Persistent
        (setup_udn_nad fixture creates this)
      - NAD vm-network in the workload namespace with allowPersistentIPs: true
        and label ramendr.openshift.io/network-id
        (setup_nad_in_namespace fixture creates this per test)
      - VMs deployed with static IPs and IPAMClaims on the UDN secondary network
        (discovered_apps_dr_workload_cnv is used as placeholder — replace with
        a static-IP workload fixture once available, see TODO below)

    # TODO: Create a static-IP VM workload fixture (e.g.
    # discovered_apps_dr_workload_cnv_static_ip) that deploys VMs configured
    # with a secondary UDN interface and static IP via IPAMClaim.
    # The workload directory in the DR workload repo will need a matching
    # VM manifest with network annotations pointing to the UDN/NAD.
    """

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_failover_failback_ip_translation(
        self,
        setup_udn_nad,
        setup_nad_in_namespace,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        TC-4 / TC-9 / TC-12: Validate translated IP assignment at the
        IPAMClaim layer during failover and restoration of original IP
        during failback for two VMs:

          test-vm-1   (192.168.1.50)  → pattern rule   → 192.168.2.50 on dr2
          test-vm-db  (192.168.1.100) → explicit rule   → 192.168.2.200 on dr2
          (same ConfigMap covers both VMs in a single failover — TC-12)

        After failback both VMs must recover their original dr1 IPs (TC-4).
        """
        # Deploy two static-IP workloads
        # TODO: replace with a static-IP workload fixture
        cnv_workload_1 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        cnv_workload_2 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=True
        )
        assert cnv_workload_1 and cnv_workload_2, "Workload deployment failed"
        wl1 = cnv_workload_1[-1]
        wl2 = cnv_workload_2[-1]
        namespace = wl1.workload_namespace
        primary_cluster = wl1.preferred_primary_cluster

        # pattern-with-overrides: vm-1 via pattern, vm-db via explicit
        network_mapping_configmap(
            "vm-ip-map-failover",
            pattern_rules=[{"source": DR1_SUBNET, "destination": DR2_SUBNET}],
            explicit_rules=[
                {"source": VM_DB_IP_DR1, "destination": VM_DB_IP_DR2_EXPLICIT}
            ],
        )

        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies
        drpolicy_name = existing_policies[0]["metadata"]["name"]
        wait_for_drpolicy_network_mapping_status(
            drpolicy_name, DRPOLICY_NM_STATUS_VALID
        )

        config.switch_to_cluster_by_name(primary_cluster)
        resource_name_1 = wl1.discovered_apps_placement_name + "-drpc"
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            namespace, discovered_apps=True, resource_name=resource_name_1
        )

        # --- Failover: shut down primary and fail over to secondary ---
        active_primary_index = config.cur_index
        active_primary_nodes = get_node_objs()
        logger.info("Shutting down primary cluster nodes for failover")
        nodes_multicluster[active_primary_index].stop_nodes(active_primary_nodes)
        dr_helpers.wait_for_managed_cluster_unreachable(primary_cluster)

        for wl in (wl1, wl2):
            resource_name = wl.discovered_apps_placement_name + "-drpc"
            dr_helpers.failover(
                failover_cluster=secondary_cluster,
                namespace=namespace,
                discovered_apps=True,
                workload_placement_name=resource_name,
                old_primary=primary_cluster,
                skip_odf_cli_validation=True,
            )

        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            wl1.workload_pvc_count + wl2.workload_pvc_count,
            wl1.workload_pod_count + wl2.workload_pod_count,
            namespace,
            timeout=1800,
            discovered_apps=True,
            vrg_name=resource_name_1,
            skip_replication_resources=True,
        )

        # Verify translated IPs on secondary cluster
        # TODO: replace vm_name placeholders with actual static-IP VM names
        logger.info("Verifying translated IPAMClaim IPs on secondary cluster")
        vm1_ip_secondary = get_ipamclaim_ip(secondary_cluster, namespace, wl1.vm_name)
        vmdb_ip_secondary = get_ipamclaim_ip(secondary_cluster, namespace, wl2.vm_name)
        assert (
            vm1_ip_secondary == VM1_IP_DR2_PATTERN
        ), f"VM1 expected translated IP {VM1_IP_DR2_PATTERN}, got {vm1_ip_secondary}"
        assert vmdb_ip_secondary == VM_DB_IP_DR2_EXPLICIT, (
            f"VM-DB expected explicit override IP {VM_DB_IP_DR2_EXPLICIT}, "
            f"got {vmdb_ip_secondary}"
        )
        logger.info(
            f"Failover IP translation verified: vm1={vm1_ip_secondary}, "
            f"vm-db={vmdb_ip_secondary}"
        )

        # Recover primary cluster
        config.switch_to_cluster_by_name(primary_cluster)
        logger.info("Recovering primary cluster")
        nodes_multicluster[active_primary_index].start_nodes(active_primary_nodes)
        wait_for_nodes_status([n.name for n in active_primary_nodes], timeout=900)
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)

        # Cleanup stale resources on the old primary after failover
        for wl in (wl1, wl2):
            resource_name = wl.discovered_apps_placement_name + "-drpc"
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=resource_name,
                old_primary=primary_cluster,
                workload_namespace=namespace,
                workload_dir=wl.workload_dir,
                vrg_name=resource_name,
                skip_resource_deletion_verification=True,
            )
        config.switch_to_cluster_by_name(primary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace,
            discovered_apps=True,
            vrg_name=resource_name_1,
        )

        # --- Failback (Relocate): secondary → primary ---
        logger.info("Performing failback (Relocate) to primary cluster")
        for wl in (wl1, wl2):
            resource_name = wl.discovered_apps_placement_name + "-drpc"
            dr_helpers.relocate(
                preferred_cluster=primary_cluster,
                namespace=namespace,
                workload_placement_name=resource_name,
                discovered_apps=True,
                old_primary=secondary_cluster,
                workload_instance=wl,
                skip_odf_cli_validation=True,
            )

        config.switch_to_cluster_by_name(secondary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace,
            discovered_apps=True,
            vrg_name=resource_name_1,
        )

        # Verify original IPs restored on primary cluster after failback
        logger.info("Verifying original IPAMClaim IPs restored on primary cluster")
        vm1_ip_primary = get_ipamclaim_ip(primary_cluster, namespace, wl1.vm_name)
        vmdb_ip_primary = get_ipamclaim_ip(primary_cluster, namespace, wl2.vm_name)
        assert (
            vm1_ip_primary == VM1_IP_DR1
        ), f"VM1 expected original IP {VM1_IP_DR1} after failback, got {vm1_ip_primary}"
        assert vmdb_ip_primary == VM_DB_IP_DR1, (
            f"VM-DB expected original IP {VM_DB_IP_DR1} after failback, "
            f"got {vmdb_ip_primary}"
        )
        logger.info(
            f"Failback IP restoration verified: vm1={vm1_ip_primary}, "
            f"vm-db={vmdb_ip_primary}"
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_backward_compatibility_no_mapping(
        self,
        setup_udn_nad,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        TC-5: Verify that existing VMs without any network mapping ConfigMap
        fail over and fail back without error — feature is opt-in only and must
        not introduce regressions.
        """
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        assert cnv_workloads, "No CNV workload deployed"
        wl = cnv_workloads[-1]
        namespace = wl.workload_namespace
        resource_name = wl.discovered_apps_placement_name + "-drpc"
        primary_cluster = wl.preferred_primary_cluster

        # Explicitly confirm no network mapping ConfigMap exists
        config.switch_acm_ctx()
        cm_list = ocp.OCP(kind=constants.CONFIGMAP, namespace=VM_IP_TRANSLATION_NS).get(
            selector="app=ramen-network-mapping"
        )  # TODO: confirm label
        assert not cm_list.get("items"), (
            "Expected no network mapping ConfigMap for backward-compat test, "
            f"found: {[i['metadata']['name'] for i in cm_list.get('items', [])]}"
        )

        config.switch_to_cluster_by_name(primary_cluster)
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            namespace, discovered_apps=True, resource_name=resource_name
        )

        active_primary_index = config.cur_index
        active_primary_nodes = get_node_objs()
        nodes_multicluster[active_primary_index].stop_nodes(active_primary_nodes)
        dr_helpers.wait_for_managed_cluster_unreachable(primary_cluster)

        dr_helpers.failover(
            failover_cluster=secondary_cluster,
            namespace=namespace,
            discovered_apps=True,
            workload_placement_name=resource_name,
            old_primary=primary_cluster,
            skip_odf_cli_validation=True,
        )

        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            namespace,
            timeout=1800,
            discovered_apps=True,
            vrg_name=resource_name,
            skip_replication_resources=True,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=wl.vm_name,
            namespace=namespace,
            phase=constants.STATUS_RUNNING,
        )
        logger.info(
            "Failover completed without network mapping — backward compatibility OK"
        )

        # Recover and relocate back
        config.switch_to_cluster_by_name(primary_cluster)
        nodes_multicluster[active_primary_index].start_nodes(active_primary_nodes)
        wait_for_nodes_status([n.name for n in active_primary_nodes], timeout=900)
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)

        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=resource_name,
            old_primary=primary_cluster,
            workload_namespace=namespace,
            workload_dir=wl.workload_dir,
            vrg_name=resource_name,
            skip_resource_deletion_verification=True,
        )
        wait_for_all_resources_deletion(
            namespace=namespace, discovered_apps=True, vrg_name=resource_name
        )

        dr_helpers.relocate(
            preferred_cluster=primary_cluster,
            namespace=namespace,
            workload_placement_name=resource_name,
            discovered_apps=True,
            old_primary=secondary_cluster,
            workload_instance=wl,
            skip_odf_cli_validation=True,
        )

        config.switch_to_cluster_by_name(secondary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace, discovered_apps=True, vrg_name=resource_name
        )
        logger.info(
            "Failback completed without network mapping — backward compatibility OK"
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_opt_in_required(
        self,
        setup_udn_nad,
        setup_nad_in_namespace,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        TC-10: Verify that IP translation is applied only when
        networkMapping.enabled=true in the ConfigMap. An existing DRPC or one
        created after upgrade must not be affected without the opt-in.
        """
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        assert cnv_workloads
        wl = cnv_workloads[-1]
        namespace = wl.workload_namespace
        resource_name = wl.discovered_apps_placement_name + "-drpc"
        primary_cluster = wl.preferred_primary_cluster

        # Create a ConfigMap with enabled=False — translation must NOT occur
        network_mapping_configmap(
            "vm-ip-map-opt-out",
            pattern_rules=[{"source": DR1_SUBNET, "destination": DR2_SUBNET}],
            enabled=False,
        )

        config.switch_to_cluster_by_name(primary_cluster)
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            namespace, discovered_apps=True, resource_name=resource_name
        )

        active_primary_index = config.cur_index
        active_primary_nodes = get_node_objs()
        nodes_multicluster[active_primary_index].stop_nodes(active_primary_nodes)
        dr_helpers.wait_for_managed_cluster_unreachable(primary_cluster)

        dr_helpers.failover(
            failover_cluster=secondary_cluster,
            namespace=namespace,
            discovered_apps=True,
            workload_placement_name=resource_name,
            old_primary=primary_cluster,
            skip_odf_cli_validation=True,
        )

        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            namespace,
            timeout=1800,
            discovered_apps=True,
            vrg_name=resource_name,
            skip_replication_resources=True,
        )

        # IP must NOT be translated — VM retains its original IP
        # TODO: confirm how to check IPAMClaim when opt-out is active
        vm_ip = get_ipamclaim_ip(secondary_cluster, namespace, wl.vm_name)
        assert (
            vm_ip == VM1_IP_DR1
        ), f"Expected untranslated IP {VM1_IP_DR1} (opt-out), got {vm_ip}"
        logger.info(f"Opt-in disabled: VM IP {vm_ip} correctly not translated")

        # Recover
        config.switch_to_cluster_by_name(primary_cluster)
        nodes_multicluster[active_primary_index].start_nodes(active_primary_nodes)
        wait_for_nodes_status([n.name for n in active_primary_nodes], timeout=900)
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)

        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=resource_name,
            old_primary=primary_cluster,
            workload_namespace=namespace,
            workload_dir=wl.workload_dir,
            vrg_name=resource_name,
            skip_resource_deletion_verification=True,
        )
        config.switch_to_cluster_by_name(primary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace, discovered_apps=True, vrg_name=resource_name
        )

        dr_helpers.relocate(
            preferred_cluster=primary_cluster,
            namespace=namespace,
            workload_placement_name=resource_name,
            discovered_apps=True,
            old_primary=secondary_cluster,
            workload_instance=wl,
            skip_odf_cli_validation=True,
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace, discovered_apps=True, vrg_name=resource_name
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_proportional_translation(
        self,
        setup_udn_nad,
        setup_nad_in_namespace,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        TC-11: Verify proportional translation correctly maps IPs when source
        and destination subnets have different sizes (e.g. /24 → /16).
        The host portion of the address is scaled proportionally into the
        destination subnet range.

        # TODO: Confirm the proportional mapping algorithm with the developer
        # and update expected_ip below accordingly.
        """
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        assert cnv_workloads
        wl = cnv_workloads[-1]
        namespace = wl.workload_namespace
        resource_name = wl.discovered_apps_placement_name + "-drpc"
        primary_cluster = wl.preferred_primary_cluster

        network_mapping_configmap(
            "vm-ip-map-proportional",
            pattern_rules=[{"source": PROP_SRC_SUBNET, "destination": PROP_DST_SUBNET}],
        )

        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        wait_for_drpolicy_network_mapping_status(
            existing_policies[0]["metadata"]["name"], DRPOLICY_NM_STATUS_VALID
        )

        config.switch_to_cluster_by_name(primary_cluster)
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            namespace, discovered_apps=True, resource_name=resource_name
        )

        active_primary_index = config.cur_index
        active_primary_nodes = get_node_objs()
        nodes_multicluster[active_primary_index].stop_nodes(active_primary_nodes)
        dr_helpers.wait_for_managed_cluster_unreachable(primary_cluster)

        dr_helpers.failover(
            failover_cluster=secondary_cluster,
            namespace=namespace,
            discovered_apps=True,
            workload_placement_name=resource_name,
            old_primary=primary_cluster,
            skip_odf_cli_validation=True,
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            namespace,
            timeout=1800,
            discovered_apps=True,
            vrg_name=resource_name,
            skip_replication_resources=True,
        )

        translated_ip = get_ipamclaim_ip(secondary_cluster, namespace, wl.vm_name)
        # TODO: compute expected IP once proportional mapping algorithm is confirmed
        assert translated_ip.startswith(
            "172.16."
        ), f"Expected proportionally translated IP in 172.16.0.0/16, got {translated_ip}"
        logger.info(f"Proportional translation result: {translated_ip}")

        # Recover and relocate
        config.switch_to_cluster_by_name(primary_cluster)
        nodes_multicluster[active_primary_index].start_nodes(active_primary_nodes)
        wait_for_nodes_status([n.name for n in active_primary_nodes], timeout=900)
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=resource_name,
            old_primary=primary_cluster,
            workload_namespace=namespace,
            workload_dir=wl.workload_dir,
            vrg_name=resource_name,
            skip_resource_deletion_verification=True,
        )
        config.switch_to_cluster_by_name(primary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace, discovered_apps=True, vrg_name=resource_name
        )
        dr_helpers.relocate(
            preferred_cluster=primary_cluster,
            namespace=namespace,
            workload_placement_name=resource_name,
            discovered_apps=True,
            old_primary=secondary_cluster,
            workload_instance=wl,
            skip_odf_cli_validation=True,
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace, discovered_apps=True, vrg_name=resource_name
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_mixed_interface_translation(
        self,
        setup_udn_nad,
        setup_nad_in_namespace,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        TC-13: On a single namespace with multiple VMs where one VM has both
        a DHCP interface and a static IP interface, verify that IP translation
        is applied only to the static interface — the DHCP interface is not
        touched and acquires its IP from the secondary site's DHCP server.

        # TODO: Requires a VM fixture that creates a multi-homed VM
        # (one DHCP NIC + one static-IP UDN NIC). Until available,
        # this test validates the principle against the existing workload.
        """
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        assert cnv_workloads
        wl = cnv_workloads[-1]
        namespace = wl.workload_namespace
        resource_name = wl.discovered_apps_placement_name + "-drpc"
        primary_cluster = wl.preferred_primary_cluster

        network_mapping_configmap(
            "vm-ip-map-mixed-iface",
            pattern_rules=[{"source": DR1_SUBNET, "destination": DR2_SUBNET}],
        )
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        wait_for_drpolicy_network_mapping_status(
            existing_policies[0]["metadata"]["name"], DRPOLICY_NM_STATUS_VALID
        )

        config.switch_to_cluster_by_name(primary_cluster)
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            namespace, discovered_apps=True, resource_name=resource_name
        )

        active_primary_index = config.cur_index
        active_primary_nodes = get_node_objs()
        nodes_multicluster[active_primary_index].stop_nodes(active_primary_nodes)
        dr_helpers.wait_for_managed_cluster_unreachable(primary_cluster)

        dr_helpers.failover(
            failover_cluster=secondary_cluster,
            namespace=namespace,
            discovered_apps=True,
            workload_placement_name=resource_name,
            old_primary=primary_cluster,
            skip_odf_cli_validation=True,
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            namespace,
            timeout=1800,
            discovered_apps=True,
            vrg_name=resource_name,
            skip_replication_resources=True,
        )

        # Static IP interface: must be translated
        static_ip = get_ipamclaim_ip(secondary_cluster, namespace, wl.vm_name)
        assert static_ip.startswith(
            "192.168.2."
        ), f"Static IP must be translated to 192.168.2.x subnet, got {static_ip}"
        # DHCP interface: acquired from secondary DHCP — must NOT be 192.168.1.x
        # TODO: retrieve DHCP interface IP from VMI status once multi-homed VM
        # fixture is available and add assertion here.
        logger.info(f"Mixed interface: static IP translated to {static_ip}")

        # Recover and relocate
        config.switch_to_cluster_by_name(primary_cluster)
        nodes_multicluster[active_primary_index].start_nodes(active_primary_nodes)
        wait_for_nodes_status([n.name for n in active_primary_nodes], timeout=900)
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=resource_name,
            old_primary=primary_cluster,
            workload_namespace=namespace,
            workload_dir=wl.workload_dir,
            vrg_name=resource_name,
            skip_resource_deletion_verification=True,
        )
        config.switch_to_cluster_by_name(primary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace, discovered_apps=True, vrg_name=resource_name
        )
        dr_helpers.relocate(
            preferred_cluster=primary_cluster,
            namespace=namespace,
            workload_placement_name=resource_name,
            discovered_apps=True,
            old_primary=secondary_cluster,
            workload_instance=wl,
            skip_odf_cli_validation=True,
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        wait_for_all_resources_deletion(
            namespace=namespace, discovered_apps=True, vrg_name=resource_name
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_ip_outside_destination_subnet(
        self, setup_udn_nad, network_mapping_configmap
    ):
        """
        TC-15: Verify behavior when translation produces an IP outside the
        destination subnet (e.g. a /30 destination subnet where the host
        octet from the source does not fit). The controller must detect this
        and report an error condition rather than silently assigning an invalid IP.

        # TODO: Confirm the expected error condition name and reason string
        # with the developer once the feature controller is implemented.
        """
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies
        drpolicy_name = existing_policies[0]["metadata"]["name"]

        # A /30 destination gives only 4 addresses (.0-.3) — a source host
        # octet of 50 (from VM1_IP_DR1) cannot fit in this range.
        network_mapping_configmap(
            "vm-ip-map-overflow",
            pattern_rules=[{"source": DR1_SUBNET, "destination": "192.168.2.0/30"}],
        )

        # DRPolicy must report Invalid (or a specific overflow reason)
        for sample in TimeoutSampler(
            timeout=120,
            sleep=5,
            func=lambda: get_drpolicy_network_mapping_status(drpolicy_name).get(
                "status", ""
            ),
        ):
            if sample in (DRPOLICY_NM_STATUS_INVALID, DRPOLICY_NM_STATUS_VALID):
                break

        nm_status = get_drpolicy_network_mapping_status(drpolicy_name)
        # TODO: confirm whether controller rejects this upfront (Invalid) or
        # reports the overflow at translation time. Adjust assertion accordingly.
        assert nm_status.get("status") == DRPOLICY_NM_STATUS_INVALID, (
            "Expected DRPolicy to report Invalid when destination subnet is "
            f"too small for the host octet, got: {nm_status}"
        )
        logger.info(f"IP-outside-dest-subnet correctly reported Invalid: {nm_status}")


# ---------------------------------------------------------------------------
# Class 3: Miscellaneous — NAD errors, cleanup, and platform-specific
# ---------------------------------------------------------------------------


@rdr
@tier2
@turquoise_squad
@skipif_ocs_version("<4.21")
class TestVMIPTranslationMiscellaneous:
    """
    Edge cases: missing/mislabeled NADs, DR unprotect cleanup, hub recovery,
    post-upgrade behavior, and BM HCP cluster validation.

    Prerequisites:
      - ODF >= 4.21 with RHSTOR-8082 feature enabled
      - UDN vm-network on both managed clusters (setup_udn_nad fixture)
      - ACM hub cluster registered with at least two DRClusters
    """

    @pytest.mark.polarion_id("OCS-XXXX")
    @pytest.mark.parametrize(
        "scenario",
        [
            pytest.param(
                "missing-nad",
                id="missing-nad",
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
            pytest.param(
                "dhcp-nad-labeled-static",
                id="dhcp-nad-labeled-static",
                marks=pytest.mark.polarion_id("OCS-XXXX"),
            ),
        ],
    )
    def test_missing_incorrectly_labeled_nad(self, setup_udn_nad, scenario):
        """
        TC-16: Verify that a missing NAD on the DR cluster or a DHCP NAD
        incorrectly labeled as static are both handled gracefully — neither
        scenario should cause a controller crash. The DRPolicy NADsSynced
        condition should reflect the misconfiguration with a descriptive reason.
        """
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies
        drpolicy_name = existing_policies[0]["metadata"]["name"]
        drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)

        # TODO: For "missing-nad": temporarily delete the NAD on one managed
        # cluster and verify the DRPolicy reports NADsSynced=False.
        # For "dhcp-nad-labeled-static": add the NAD_DR_LABEL to a DHCP NAD
        # and verify the controller reports a warning condition.
        # These steps require test environment manipulation that depends on
        # the exact NAD/UDN setup — implement once the feature is available.

        conditions = drpolicy_ocp.get().get("status", {}).get("conditions", [])
        nads_synced = next(
            (c for c in conditions if c.get("type") == DRPOLICY_NADS_SYNCED_CONDITION),
            None,
        )
        assert nads_synced is not None, (
            f"NADsSynced condition missing from DRPolicy {drpolicy_name} "
            f"for scenario '{scenario}'"
        )
        logger.info(
            f"Scenario '{scenario}': NADsSynced condition present with "
            f"status={nads_synced.get('status')}, reason={nads_synced.get('reason')}"
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_dr_protection_disable_cleanup(
        self,
        setup_udn_nad,
        setup_nad_in_namespace,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
    ):
        """
        TC-17: Verify that disabling DR protection for a VM that had IP
        translation configured correctly cleans up all translation resources
        (IPAMClaims, network mapping references) so that no orphans remain.
        """
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        assert cnv_workloads
        wl = cnv_workloads[-1]
        namespace = wl.workload_namespace
        resource_name = wl.discovered_apps_placement_name + "-drpc"
        primary_cluster = wl.preferred_primary_cluster

        network_mapping_configmap(
            "vm-ip-map-disable-test",
            pattern_rules=[{"source": DR1_SUBNET, "destination": DR2_SUBNET}],
        )
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        wait_for_drpolicy_network_mapping_status(
            existing_policies[0]["metadata"]["name"], DRPOLICY_NM_STATUS_VALID
        )

        # Disable DR protection by deleting the DRPC
        config.switch_acm_ctx()
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE, resource_name=resource_name
        )
        drpc_obj.delete()
        logger.info(f"Deleted DRPC {resource_name} to disable DR protection")

        # Verify IPAMClaim translation resources are cleaned up on primary
        config.switch_to_cluster_by_name(primary_cluster)
        # Poll until no translated IPAMClaims remain
        for sample in TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind="IPAMClaim",  # TODO: add constant
                namespace=namespace,
            ).get(
                selector="ramen-translation=true"
            ),  # TODO: confirm label
        ):
            if not sample.get("items"):
                break
        logger.info("Translation IPAMClaims cleaned up after DR protection disabled")

        # Verify VRG is gone (no orphaned replication resources)
        vrg_list = ocp.OCP(
            kind="VolumeReplicationGroup",  # TODO: add constant
            namespace=namespace,
        ).get()
        assert not vrg_list.get("items"), (
            "Expected VRG to be cleaned up after disabling DR, "
            f"found: {vrg_list.get('items')}"
        )
        logger.info("VRG cleaned up after DR protection disabled")

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_hub_recovery_preserves_ip_translation(
        self,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        TC-18 (Hub recovery): Verify that after the ACM hub cluster recovers
        from a failure, IP translation configuration is restored and subsequent
        failover/failback still applies the correct IP mappings.

        # TODO: Hub failure simulation requires the hub_failure_recovery fixture
        # or equivalent infrastructure. Implement once available.
        """
        pytest.skip(
            "Hub recovery simulation not yet automated — implement once "
            "hub failure/recovery fixture is available"
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_post_upgrade_ip_translation(
        self,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        TC-19 (Post-upgrade): Verify that IP translation works correctly after
        an ODF/Ramen upgrade. Existing DRPCs must continue to honor translation
        rules without requiring reconfiguration.

        # TODO: Requires upgrade orchestration — run as part of the upgrade
        # test pipeline once the feature ships.
        """
        pytest.skip(
            "Post-upgrade validation to be run as part of the upgrade test pipeline"
        )

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_bm_hcp_cluster_ip_translation(
        self,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        TC-20 (BM HCP): Verify that IP translation works correctly on a
        bare-metal HyperShift (HCP) cluster. The UDN/IPAMClaim path must
        function identically regardless of the control plane topology.

        # TODO: Requires a BM HCP environment. Mark with the appropriate
        # environment skip condition once available.
        """
        pytest.skip(
            "BM HCP cluster required — skip until a HCP test environment "
            "with UDN support is provisioned"
        )
