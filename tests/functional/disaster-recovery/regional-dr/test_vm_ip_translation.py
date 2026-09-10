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
  TestVMIPTranslationMiscellaneous — Edge cases: missing/mislabeled NADs,
                                     DR unprotect cleanup, hub recovery,
                                     post-upgrade behavior, BM HCP

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
from ocs_ci.helpers.dr_helpers_vm_ip_translation import (
    DRCLUSTERCONFIG_NAD_STATUS_FIELD,
    DRPOLICY_NADS_SYNCED_CONDITION,
    DRPOLICY_NM_METHOD_EXPLICIT,
    DRPOLICY_NM_METHOD_PATTERN,
    DRPOLICY_NM_METHOD_PATTERN_OVERRIDES,
    DRPOLICY_NM_STATUS_INVALID,
    DRPOLICY_NM_STATUS_VALID,
    VM_IP_TRANSLATION_NS,
    VM_NETWORK_NAD_NAME,
    VRG_CONDITION_NM_LOADED,
    get_drpolicy_network_mapping_status,
    wait_for_drpolicy_network_mapping_status,
    wait_for_drpolicy_network_peers,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

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
@skipif_ocs_version("<4.23")
class TestVMIPTranslationConfigMap:
    """
    Validate that the DRPolicy controller correctly classifies each translation
    ConfigMap type and that the DRClusterConfig NAD discovery works as expected.
    No VM failover is performed in this class.

    Prerequisites:
      - ODF >= 4.23 with RHSTOR-8082 feature enabled
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
        logger.test_step("Get the DRPolicy name from the hub cluster")
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies, "No DRPolicy found on hub"
        drpolicy_name = existing_policies[0]["metadata"]["name"]

        logger.test_step(f"Create the network mapping ConfigMap '{cm_name}'")
        network_mapping_configmap(
            cm_name, pattern_rules=pattern_rules, explicit_rules=explicit_rules
        )

        logger.test_step(
            f"Wait for DRPolicy '{drpolicy_name}' to report networkMapping "
            f"status {DRPOLICY_NM_STATUS_VALID}"
        )
        # Poll until DRPolicy reports Valid status for the new ConfigMap
        # TODO: DRPolicy must reference the ConfigMap by name — confirm the
        # linkage mechanism (annotation, spec field, or auto-discovery by name).
        wait_for_drpolicy_network_mapping_status(
            drpolicy_name, DRPOLICY_NM_STATUS_VALID
        )

        logger.test_step(
            f"Verify the translation method is classified as {expected_method} "
            f"with {expected_pattern_count} pattern and "
            f"{expected_explicit_count} explicit mappings"
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
            logger.test_step(
                f"Verify the unmapped IP {UNMAPPED_IP_DR1} is not implicitly "
                "translated by the explicit-only ConfigMap"
            )
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
        logger.test_step("Get the DRPolicy name from the hub cluster")
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies, "No DRPolicy found on hub"
        drpolicy_name = existing_policies[0]["metadata"]["name"]

        logger.test_step(f"Set up the '{scenario}' ConfigMap scenario")
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
            logger.test_step(
                f"Wait for DRPolicy '{drpolicy_name}' to report networkMapping "
                f"status {DRPOLICY_NM_STATUS_INVALID}"
            )
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
            logger.test_step(
                "Verify the Invalid status is reported with a descriptive reason"
            )
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
        logger.test_step("Get the DRPolicy name from the hub cluster")
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies, "No DRPolicy found on hub"
        drpolicy_name = existing_policies[0]["metadata"]["name"]
        drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)

        logger.test_step(
            f"Verify the {DRPOLICY_NADS_SYNCED_CONDITION} condition is True on "
            f"DRPolicy '{drpolicy_name}'"
        )
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

        logger.test_step(
            "Verify each DRClusterConfig has discovered the labeled NADs in "
            f"status.{DRCLUSTERCONFIG_NAD_STATUS_FIELD}"
        )
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
        self,
        setup_udn_nad,
        discovered_apps_dr_workload_cnv,
        network_mapping_configmap,
    ):
        """
        TC-7b: Verify that VRG correctly reports NetworkMappingLoaded=True
        when a valid network mapping ConfigMap is active and the VMI reports
        the expected translated IP.

        Requires a workload with UDN/IPAMClaim configured.
        # TODO: Extend discovered_apps_dr_workload_cnv to support static-IP
        # VMs with IPAMClaims, or create a dedicated fixture.
        """
        logger.test_step("Deploy and DR protect a CNV workload")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        assert cnv_workloads, "No CNV workload deployed"
        workload = cnv_workloads[-1]
        namespace = workload.workload_namespace
        resource_name = workload.discovered_apps_placement_name + "-drpc"

        logger.test_step("Get the DRPolicy name from the hub cluster")
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies, "No DRPolicy found on hub"
        drpolicy_name = existing_policies[0]["metadata"]["name"]

        cluster_names = setup_udn_nad["cluster_names"]
        cluster_subnets = setup_udn_nad["cluster_subnets"]

        logger.test_step(
            "Create the network mapping ConfigMap and link it to the DRPolicy"
        )
        # Create ConfigMap on hub and link to DRPolicy; fixture handles teardown
        network_mapping_configmap(
            "vm-ip-map-vrg-test",
            cluster1_name=cluster_names[0],
            cluster2_name=cluster_names[1],
            nad_namespace=setup_udn_nad["workload_namespace"],
            nad_name=VM_NETWORK_NAD_NAME,
            cluster1_cidr=cluster_subnets[cluster_names[0]],
            cluster2_cidr=cluster_subnets[cluster_names[1]],
            drpolicy_name=drpolicy_name,
        )

        logger.test_step(
            f"Wait for DRPolicy '{drpolicy_name}' to populate its network peers"
        )
        wait_for_drpolicy_network_peers(drpolicy_name)

        logger.test_step(
            f"Verify the VRG on the primary cluster reports "
            f"{VRG_CONDITION_NM_LOADED}=True"
        )
        # Check VRG condition on the primary cluster
        primary_cluster = workload.preferred_primary_cluster
        config.switch_to_cluster_by_name(primary_cluster)
        vrg_ocp = ocp.OCP(
            kind="VolumeReplicationGroup",
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


# ---------------------------------------------------------------------------
# Class 2: Miscellaneous — NAD errors, cleanup, and platform-specific
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
        logger.test_step("Get the DRPolicy name from the hub cluster")
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        assert existing_policies
        drpolicy_name = existing_policies[0]["metadata"]["name"]
        drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)

        logger.test_step(
            f"Verify the {DRPOLICY_NADS_SYNCED_CONDITION} condition reflects the "
            f"'{scenario}' misconfiguration without crashing the controller"
        )
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
        logger.test_step("Deploy and DR protect a CNV workload")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=True, shared_drpc_protection=False
        )
        assert cnv_workloads
        wl = cnv_workloads[-1]
        namespace = wl.workload_namespace
        resource_name = wl.discovered_apps_placement_name + "-drpc"
        primary_cluster = wl.preferred_primary_cluster

        logger.test_step(
            "Create the network mapping ConfigMap and wait for the DRPolicy to "
            "report it Valid"
        )
        network_mapping_configmap(
            "vm-ip-map-disable-test",
            pattern_rules=[{"source": DR1_SUBNET, "destination": DR2_SUBNET}],
        )
        config.switch_acm_ctx()
        existing_policies = dr_helpers.get_all_drpolicy()
        wait_for_drpolicy_network_mapping_status(
            existing_policies[0]["metadata"]["name"], DRPOLICY_NM_STATUS_VALID
        )

        logger.test_step(f"Disable DR protection by deleting DRPC '{resource_name}'")
        # Disable DR protection by deleting the DRPC
        config.switch_acm_ctx()
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE, resource_name=resource_name
        )
        drpc_obj.delete()
        logger.info(f"Deleted DRPC {resource_name} to disable DR protection")

        logger.test_step(
            "Verify the translated IPAMClaims are cleaned up on the primary cluster"
        )
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

        logger.test_step("Verify no orphaned VRG remains in the workload namespace")
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
