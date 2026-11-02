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
  TestVMIPTranslationConfigMap        — ConfigMap validation, NAD discovery,
                                        controller conditions (no failover needed)
  TestVMIPTranslationMiscellaneous    — Edge cases: missing/mislabeled NADs,
                                        DR unprotect cleanup, hub recovery,
                                        post-upgrade behavior, BM HCP
  TestVMIPTranslationRulePrecedence   — Failover-based: verifies explicit mapping
                                        takes precedence over regex when both are
                                        present in the same ConfigMap (CLI path)
  TestVMIPTranslationMixedNamespace   — Failover-based: mixed static-IP and
                                        non-static VMs in one namespace across
                                        multiple DRPCs (standalone + shared);
                                        includes ACM fleet UI verification

UI-path static-IP coverage (standalone DRPC, pattern-with-overrides rule) is in
test_acm_kubevirt_dr_integration.py [standalone_static-ip].

NOTE: Several assertions reference DRPolicy/VRG/DRClusterConfig status field
names that are not yet finalized. These are marked with # TODO comments.
Update the constants at the top of this file once the developer confirms the
API schema (RHSTOR-8082 open questions).
"""

import logging
import tempfile
from time import sleep

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
    tier1,
    tier2,
)
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import (
    check_or_assign_drpolicy_for_discovered_vms_via_ui,
    navigate_using_fleet_virtualization,
)
from ocs_ci.helpers.dr_helpers_vm_ip_translation import (
    DRCLUSTERCONFIG_NAD_STATUS_FIELD,
    DRPC_CONDITION_NM_LOADED,
    DRPOLICY_NADS_SYNCED_CONDITION,
    DRPOLICY_NM_METHOD_EXPLICIT,
    DRPOLICY_NM_METHOD_PATTERN,
    PRIMARY_SUBNET,
    SECONDARY_SUBNET,
    UDN_DR_LABEL,
    UDN_DR_LABEL_VALUE,
    VM_NETWORK_NAD_NAME,
    VM_NETWORK_UDN_NAME,
    build_udn_manifest,
    create_raw_network_mapping_configmap,
    delete_udn_ssh_helper_pod,
    get_drpolicy_name,
    get_drpolicy_network_attachments_condition,
    get_drpc_network_mapping_loaded_condition,
    get_ipamclaim_ip,
    get_vm_ip_from_vmi,
    run_dd_io_via_udn_pod,
    validate_data_integrity_vm_via_udn_pod,
    verify_drpolicy_network_attachments_validated,
    verify_vm_ip_and_ipamclaim,
    verify_vrg_static_ip_discovery,
    verify_vrg_static_ip_translation,
    wait_for_drclusterconfig_nad_discovery,
    wait_for_drpc_network_mapping_loaded,
    wait_for_drpolicy_network_peers,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.exceptions import TimeoutExpiredError
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
VM_DB_IP_DR2_EXPLICIT = "192.168.2.150"  # overridden by explicit rule
VM_DB_IP_DR2_PATTERN = "192.168.2.100"  # what pattern alone would give (NOT used)
UNMAPPED_IP_DR1 = "192.168.1.99"  # no explicit rule → untranslated
DR1_SUBNET_2 = "192.168.10.0/24"
DR2_SUBNET_2 = "192.168.20.0/24"


# ---------------------------------------------------------------------------
# Class 1: ConfigMap validation and controller condition tests
# (No failover required — hub-only or managed-cluster inspection)
# ---------------------------------------------------------------------------


@rdr
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

    # Explicit destination for VM-1 (deviates from host-preserving pattern)
    VM1_STATIC_IP_PRIMARY = "192.168.1.11"
    VM1_EXPLICIT_DEST = "192.168.2.200"

    # VM-2 has no explicit rule — regex preserves host octet
    VM2_STATIC_IP_PRIMARY = "192.168.1.12"
    VM2_REGEX_DEST = "192.168.2.12"

    @tier1
    @pytest.mark.parametrize(
        "cm_name,explicit_mappings,regex_mappings,expected_method",
        [
            pytest.param(
                # pattern-only: no explicit entries; auto-generated host-preserving
                # bidirectional regex is produced by regex_mappings=None
                "vm-ip-map-pattern",
                None,  # explicit_mappings
                None,  # regex_mappings=None → auto-generate from cluster CIDRs
                DRPOLICY_NM_METHOD_PATTERN,
                id="pattern-only",
                marks=pytest.mark.polarion_id("OCS-8386"),
            ),
            pytest.param(
                # explicit-only: two IP-to-IP rules; regex suppressed with {}
                "vm-ip-map-explicit",
                # explicit_mappings filled at runtime using cluster names from
                # setup_udn_nad — see test body
                "runtime",
                {},  # regex_mappings={} → no regex, no auto-generate
                DRPOLICY_NM_METHOD_EXPLICIT,
                id="explicit-only",
                marks=pytest.mark.polarion_id("OCS-8385"),
            ),
        ],
    )
    def test_vm_static_ip_translation_e2e(
        self,
        request,
        setup_udn_nad,
        cnv_workload_with_static_ip,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
        cm_name,
        explicit_mappings,
        regex_mappings,
        expected_method,
    ):
        """
        TC-1 / TC-2 / TC-4 / TC-7: Verify pattern-only and explicit-only network-mapping
        configs end-to-end across multiple static-IP VM workloads in multiple namespaces.

        Namespaces:
          - vm-ip-translation-test (ns1): primary 192.168.1.0/24, secondary 192.168.2.0/24
          - test-vm-static-ip (ns2):      primary 192.168.10.0/24, secondary 192.168.20.0/24

        Both namespaces share a single network mapping ConfigMap (one entry per NAD)
        and a single DRPolicy. Each workload gets its own DRPC.

        Prerequisites:
        - ODF >= 4.23 with RHSTOR-8082 feature enabled
        - ACM hub cluster with at least two registered DRClusters
        - UDN vm-network on both managed clusters for both namespaces (setup_udn_nad fixture)
        - NADs labeled with ramendr.openshift.io/dr-network=true (setup_udn_nad fixture)
        - DRClusterConfig with NAD discovery enabled on both managed clusters
        """
        _ = node_restart_teardown

        primary_cluster = setup_udn_nad["primary_cluster"]
        secondary_cluster = setup_udn_nad["secondary_cluster"]
        workload_namespace = setup_udn_nad[
            "workload_namespace"
        ]  # vm-ip-translation-test
        workload_namespace_2 = setup_udn_nad[
            "workload_namespace_2"
        ]  # test-vm-static-ip
        subnets_ns1 = setup_udn_nad["subnets_ns1"]
        subnets_ns2 = setup_udn_nad["subnets_ns2"]

        request.addfinalizer(
            lambda: delete_udn_ssh_helper_pod(
                cluster_names=[primary_cluster, secondary_cluster],
                namespaces=[workload_namespace, workload_namespace_2],
            )
        )

        # Steps a & b: Both namespaces and UDNs already created by setup_udn_nad.
        logger.info(
            f"Step a & b: Namespaces '{workload_namespace}' and '{workload_namespace_2}' "
            f"with UDN '{VM_NETWORK_UDN_NAME}' created on both clusters"
        )

        # Step c: Deploy static-IP VMs in both namespaces.
        # pvc_vm=2 iterates dr_cnv_discovered_apps_static_ip[0] (ns1) and [1] (ns2).
        logger.test_step(
            f"Step c: Deploy static-IP VMs in both namespaces on primary '{primary_cluster}'"
        )
        config.switch_to_cluster_by_name(primary_cluster)
        wl_all = cnv_workload_with_static_ip(
            pvc_vm=2, dr_protect=False, shared_drpc_protection=False
        )
        assert len(wl_all) >= 2, "Expected two workloads (one per namespace)"
        workload = wl_all[0]  # vm-ip-translation-test (ns1)
        workload2 = wl_all[1]  # test-vm-static-ip (ns2)

        drpc_name = workload.discovered_apps_placement_name
        drpc_name_2 = workload2.discovered_apps_placement_name

        # Verify VM-1 (ns1) IP matches expected static IP
        primary_vm_ip = verify_vm_ip_and_ipamclaim(
            cluster_name=primary_cluster,
            namespace=workload_namespace,
            vm_name=workload.vm_name,
            interface_name=VM_NETWORK_UDN_NAME,
        )
        logger.info(
            f"VM '{workload.vm_name}' running on '{primary_cluster}' "
            f"in '{workload_namespace}' with IP '{primary_vm_ip}'"
        )
        assert (
            primary_vm_ip == self.VM1_STATIC_IP_PRIMARY
        ), f"Expected VM IP '{self.VM1_STATIC_IP_PRIMARY}' on primary, got '{primary_vm_ip}'"

        # Verify VM-2 (ns2) — static IP is defined in vm-workload-4 manifest
        primary_vm2_ip = verify_vm_ip_and_ipamclaim(
            cluster_name=primary_cluster,
            namespace=workload_namespace_2,
            vm_name=workload2.vm_name,
            interface_name=VM_NETWORK_UDN_NAME,
        )
        logger.info(
            f"VM '{workload2.vm_name}' running on '{primary_cluster}' "
            f"in '{workload_namespace_2}' with IP '{primary_vm2_ip}'"
        )
        vm2_host_octet = primary_vm2_ip.split(".")[-1]
        expected_vm2_translated_ip = f"192.168.20.{vm2_host_octet}"

        logger.test_step("Switch to ACM hub and get the DRPolicy name")
        drpolicy_name = get_drpolicy_name()

        expected_translated_ip = (
            self.VM1_EXPLICIT_DEST if expected_method == "explicit" else "192.168.2.11"
        )

        # Resolve "runtime" explicit_mappings for ns1; ns2 always uses auto pattern
        if explicit_mappings == "runtime":
            if expected_method == "explicit":
                explicit_mappings_ns1 = [
                    {
                        primary_cluster: VM_DB_IP_DR1,
                        secondary_cluster: VM_DB_IP_DR2_EXPLICIT,
                    },
                    {
                        primary_cluster: self.VM1_STATIC_IP_PRIMARY,
                        secondary_cluster: expected_translated_ip,
                    },
                ]
                explicit_mappings_ns2 = [
                    {
                        primary_cluster: primary_vm2_ip,
                        secondary_cluster: expected_vm2_translated_ip,
                    },
                ]
            else:  # pattern-with-overrides
                explicit_mappings_ns1 = [
                    {
                        primary_cluster: VM_DB_IP_DR1,
                        secondary_cluster: VM_DB_IP_DR2_EXPLICIT,
                    },
                ]
                explicit_mappings_ns2 = None
        else:
            explicit_mappings_ns1 = explicit_mappings
            explicit_mappings_ns2 = None

        # Step d & e: Create the network mapping ConfigMap with both NADs (one per namespace)
        # and link it to the DRPolicy via networkMappingRef.
        logger.test_step(
            f"Step d & e: Create ConfigMap '{cm_name}' with both NADs "
            f"and patch DRPolicy '{drpolicy_name}' networkMappingRef"
        )
        network_mapping_configmap(
            cm_name,
            cluster1_name=primary_cluster,
            cluster2_name=secondary_cluster,
            network_mappings=[
                {
                    # NAD in vm-ip-translation-test (ns1)
                    "nad_namespace": workload_namespace,
                    "nad_name": VM_NETWORK_NAD_NAME,
                    "cluster1_cidr": subnets_ns1[primary_cluster],
                    "cluster2_cidr": subnets_ns1[secondary_cluster],
                    "explicit_mappings": explicit_mappings_ns1,
                    "regex_mappings": regex_mappings,
                },
                {
                    # NAD in test-vm-static-ip (ns2) — uses auto host-preserving pattern
                    # when no explicit_mappings_ns2 provided, or explicit in explicit mode
                    "nad_namespace": workload_namespace_2,
                    "nad_name": VM_NETWORK_NAD_NAME,
                    "cluster1_cidr": subnets_ns2[primary_cluster],
                    "cluster2_cidr": subnets_ns2[secondary_cluster],
                    "explicit_mappings": explicit_mappings_ns2,
                    "regex_mappings": {} if expected_method == "explicit" else None,
                },
            ],
            drpolicy_name=drpolicy_name,
        )

        # Verify DRPolicy NetworkAttachmentsValidated covers both NADs
        logger.test_step(
            "Verify DRPolicy NetworkAttachmentsValidated condition is Validated and has peers"
        )
        verify_drpolicy_network_attachments_validated(drpolicy_name)

        # Step f: DR-protect both workloads via CLI (DRPC with vm-recipe).
        logger.test_step(
            "Step f: DR protect both VM workloads via CLI (DRPC with vm-recipe)"
        )
        config.switch_acm_ctx()
        workload.create_placement()
        workload.create_drpc(use_vm_recipe=True)
        workload2.create_placement()
        workload2.create_drpc(use_vm_recipe=True)

        # Step g: Wait for NetworkMappingLoaded=True on both DRPCs
        logger.test_step(
            f"Step g: Wait for {DRPC_CONDITION_NM_LOADED}=True on "
            f"DRPC '{drpc_name}' (ns1) and DRPC '{drpc_name_2}' (ns2)"
        )
        wait_for_drpc_network_mapping_loaded(drpc_name, expected_status="True")
        wait_for_drpc_network_mapping_loaded(drpc_name_2, expected_status="True")

        # Verify VRG staticIPDiscovery and staticIPTranslationSpec for ns1
        logger.test_step(
            f"Verify VRG IP discovery/translation for ns1 workload on "
            f"primary '{primary_cluster}' and secondary '{secondary_cluster}'"
        )
        verify_vrg_static_ip_discovery(
            cluster_name=primary_cluster,
            workload=workload,
            expected_ip=self.VM1_STATIC_IP_PRIMARY,
            network_name=VM_NETWORK_NAD_NAME,
        )
        verify_vrg_static_ip_translation(
            cluster_name=secondary_cluster,
            workload=workload,
            expected_translated_ip=expected_translated_ip,
            network_name=VM_NETWORK_NAD_NAME,
        )

        # Verify VRG staticIPDiscovery and staticIPTranslationSpec for ns2
        logger.test_step(
            f"Verify VRG IP discovery/translation for ns2 workload on "
            f"primary '{primary_cluster}' and secondary '{secondary_cluster}'"
        )
        verify_vrg_static_ip_discovery(
            cluster_name=primary_cluster,
            workload=workload2,
            expected_ip=primary_vm2_ip,
            network_name=VM_NETWORK_NAD_NAME,
        )
        verify_vrg_static_ip_translation(
            cluster_name=secondary_cluster,
            workload=workload2,
            expected_translated_ip=expected_vm2_translated_ip,
            network_name=VM_NETWORK_NAD_NAME,
        )

        logger.info(
            f"ConfigMap '{cm_name}' with both NADs validated (method={expected_method}); "
            f"VRGs for both namespaces verified successfully."
        )

        # ------------------------------------------------------------------
        # Data IO & Data Integrity before Failover (ns1 workload)
        # ------------------------------------------------------------------
        logger.test_step("Write initial test data and compute md5sum on primary (ns1)")
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]
        md5sum_original = run_dd_io_via_udn_pod(
            cluster_name=primary_cluster,
            namespace=workload_namespace,
            vm_ip=self.VM1_STATIC_IP_PRIMARY,
            file_path=vm_filepaths[0],
            username=workload.vm_username,
            verify=True,
        )
        logger.info(
            f"Original md5sum of {vm_filepaths[0]} on VM '{workload.vm_name}': {md5sum_original}"
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload_namespace,
            discovered_apps=True,
            resource_name=drpc_name,
        )
        wait_time = scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for replication sync...")
        sleep(wait_time * 60)

        config.switch_acm_ctx()
        drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESPACE, resource_name=drpc_name)
        drpc_obj_2 = DRPC(
            namespace=constants.DR_OPS_NAMESPACE, resource_name=drpc_name_2
        )
        drpc_obj.wait_for_peer_ready_status()
        drpc_obj_2.wait_for_peer_ready_status()

        def _check_drpc_protected(drpc_resource):
            conds = drpc_resource.get().get("status", {}).get("conditions", [])
            for c in conds:
                if c.get("type") == "Protected":
                    return c.get("status") == "True"
            return False

        for drpc_resource, name in [(drpc_obj, drpc_name), (drpc_obj_2, drpc_name_2)]:
            logger.info(f"Waiting for DRPC '{name}' condition Protected=True...")
            sample = TimeoutSampler(
                timeout=300,
                sleep=10,
                func=_check_drpc_protected,
                drpc_resource=drpc_resource,
            )
            assert sample.wait_for_func_status(
                result=True
            ), f"DRPC '{name}' Protected condition is not True"

        # ------------------------------------------------------------------
        # Failover — power off primary nodes, then failover both workloads
        # ------------------------------------------------------------------
        logger.test_step(f"Shut down all nodes on primary cluster '{primary_cluster}'")
        config.switch_to_cluster_by_name(primary_cluster)
        active_primary_index = config.cur_index
        active_primary_node_objs = get_node_objs()
        nodes_multicluster[active_primary_index].stop_nodes(active_primary_node_objs)
        logger.info("Primary cluster nodes stopped, waiting 300s...")
        sleep(300)

        logger.test_step(
            f"Failover ns1 workload (DRPC '{drpc_name}') to secondary '{secondary_cluster}'"
        )
        dr_helpers.failover(
            failover_cluster=secondary_cluster,
            namespace=workload_namespace,
            discovered_apps=True,
            workload_placement_name=drpc_name,
            old_primary=primary_cluster,
            skip_odf_cli_validation=True,
        )
        logger.info(
            f"Waiting {scheduling_interval}m (1 schedule interval) before initiating ns2 failover"
        )
        sleep(scheduling_interval * 60)

        logger.test_step(
            f"Failover ns2 workload (DRPC '{drpc_name_2}') to secondary '{secondary_cluster}'"
        )
        dr_helpers.failover(
            failover_cluster=secondary_cluster,
            namespace=workload_namespace_2,
            discovered_apps=True,
            workload_placement_name=drpc_name_2,
            old_primary=primary_cluster,
            skip_odf_cli_validation=True,
        )

        logger.test_step("Verify both VMs are running on secondary after failover")
        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload_namespace,
            discovered_apps=True,
            vrg_name=drpc_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=workload.vm_name,
            namespace=workload_namespace,
            phase=constants.STATUS_RUNNING,
        )
        dr_helpers.wait_for_all_resources_creation(
            workload2.workload_pvc_count,
            workload2.workload_pod_count,
            workload_namespace_2,
            discovered_apps=True,
            vrg_name=drpc_name_2,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=workload2.vm_name,
            namespace=workload_namespace_2,
            phase=constants.STATUS_RUNNING,
        )

        # ------------------------------------------------------------------
        # Verify IP translation and Data Integrity on Failover
        # ------------------------------------------------------------------
        config.switch_to_cluster_by_name(secondary_cluster)

        logger.test_step("Verify IP translation for ns1 VM on secondary after failover")
        vmi_ip_secondary = verify_vm_ip_and_ipamclaim(
            secondary_cluster,
            workload_namespace,
            workload.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        logger.info(f"Translated VM IP on secondary (ns1): {vmi_ip_secondary}")
        assert (
            vmi_ip_secondary == expected_translated_ip
        ), f"Expected translated IP {expected_translated_ip}, got {vmi_ip_secondary}"

        logger.test_step("Verify IP translation for ns2 VM on secondary after failover")
        vmi_ip2_secondary = verify_vm_ip_and_ipamclaim(
            secondary_cluster,
            workload_namespace_2,
            workload2.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        logger.info(f"Translated VM IP on secondary (ns2): {vmi_ip2_secondary}")
        assert (
            vmi_ip2_secondary == expected_vm2_translated_ip
        ), f"Expected translated IP {expected_vm2_translated_ip}, got {vmi_ip2_secondary}"

        logger.test_step(
            "Verify initial data integrity on secondary after failover (ns1)"
        )
        validate_data_integrity_vm_via_udn_pod(
            cluster_name=secondary_cluster,
            namespace=workload_namespace,
            vm_ip=vmi_ip_secondary,
            file_path=vm_filepaths[0],
            md5sum_original=md5sum_original,
            app_state="Failover",
            username=workload.vm_username,
        )

        logger.test_step("Write post-failover test data (file2) on secondary (ns1)")
        md5sum_failover = run_dd_io_via_udn_pod(
            cluster_name=secondary_cluster,
            namespace=workload_namespace,
            vm_ip=vmi_ip_secondary,
            file_path=vm_filepaths[1],
            username=workload.vm_username,
            verify=True,
        )
        logger.info(f"Post-failover md5sum of {vm_filepaths[1]}: {md5sum_failover}")

        # ------------------------------------------------------------------
        # Recovery of Primary cluster
        # ------------------------------------------------------------------
        logger.test_step(f"Recover primary cluster '{primary_cluster}'")
        config.switch_to_cluster_by_name(primary_cluster)
        nodes_multicluster[active_primary_index].start_nodes(active_primary_node_objs)
        wait_for_nodes_status([n.name for n in active_primary_node_objs])
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=30, delay=30)

        config.switch_acm_ctx()
        drpc_obj.wait_for_progression_status(
            status=constants.STATUS_COMPLETED, timeout=900
        )
        drpc_obj_2.wait_for_progression_status(
            status=constants.STATUS_COMPLETED, timeout=900
        )

        # ------------------------------------------------------------------
        # Relocate both workloads back to Primary
        # ------------------------------------------------------------------
        config.switch_to_cluster_by_name(secondary_cluster)
        logger.info(f"Waiting {wait_time} minutes before relocating...")
        sleep(wait_time * 60)

        logger.test_step(f"Relocate ns1 workload back to primary '{primary_cluster}'")
        dr_helpers.relocate(
            preferred_cluster=primary_cluster,
            namespace=workload_namespace,
            workload_placement_name=drpc_name,
            discovered_apps=True,
            old_primary=secondary_cluster,
            workload_instance=workload,
            vm_auto_cleanup=True,
            skip_odf_cli_validation=True,
        )
        logger.info(
            f"Waiting {scheduling_interval}m (1 schedule interval) before initiating ns2 relocate"
        )
        sleep(scheduling_interval * 60)

        logger.test_step(f"Relocate ns2 workload back to primary '{primary_cluster}'")
        dr_helpers.relocate(
            preferred_cluster=primary_cluster,
            namespace=workload_namespace_2,
            workload_placement_name=drpc_name_2,
            discovered_apps=True,
            old_primary=secondary_cluster,
            workload_instance=workload2,
            vm_auto_cleanup=True,
            skip_odf_cli_validation=True,
        )

        logger.test_step("Verify resources creation on primary after relocate")
        config.switch_to_cluster_by_name(primary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload_namespace,
            discovered_apps=True,
            vrg_name=drpc_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=workload.vm_name,
            namespace=workload_namespace,
            phase=constants.STATUS_RUNNING,
        )
        dr_helpers.wait_for_all_resources_creation(
            workload2.workload_pvc_count,
            workload2.workload_pod_count,
            workload_namespace_2,
            discovered_apps=True,
            vrg_name=drpc_name_2,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=workload2.vm_name,
            namespace=workload_namespace_2,
            phase=constants.STATUS_RUNNING,
        )

        # ------------------------------------------------------------------
        # Verify IP restoration and Data Integrity on Relocate
        # ------------------------------------------------------------------
        logger.test_step(
            "Verify ns1 VM IP is restored to original on primary after relocate"
        )
        vmi_ip_restored = verify_vm_ip_and_ipamclaim(
            primary_cluster,
            workload_namespace,
            workload.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        logger.info(f"Restored VM IP on primary (ns1): {vmi_ip_restored}")
        assert (
            vmi_ip_restored == self.VM1_STATIC_IP_PRIMARY
        ), f"Expected restored IP {self.VM1_STATIC_IP_PRIMARY}, got '{vmi_ip_restored}'"

        logger.test_step(
            "Verify ns2 VM IP is restored to original on primary after relocate"
        )
        vmi_ip2_restored = verify_vm_ip_and_ipamclaim(
            primary_cluster,
            workload_namespace_2,
            workload2.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        logger.info(f"Restored VM IP on primary (ns2): {vmi_ip2_restored}")
        assert (
            vmi_ip2_restored == primary_vm2_ip
        ), f"Expected restored IP {primary_vm2_ip}, got '{vmi_ip2_restored}'"

        logger.test_step("Verify initial data integrity after relocate (ns1)")
        validate_data_integrity_vm_via_udn_pod(
            cluster_name=primary_cluster,
            namespace=workload_namespace,
            vm_ip=vmi_ip_restored,
            file_path=vm_filepaths[0],
            md5sum_original=md5sum_original,
            app_state="Relocate",
            username=workload.vm_username,
        )
        logger.test_step("Verify post-failover data integrity after relocate (ns1)")
        validate_data_integrity_vm_via_udn_pod(
            cluster_name=primary_cluster,
            namespace=workload_namespace,
            vm_ip=vmi_ip_restored,
            file_path=vm_filepaths[1],
            md5sum_original=md5sum_failover,
            app_state="Relocate",
            username=workload.vm_username,
        )

        logger.test_step("Write post-relocate test data (file3) on primary (ns1)")
        run_dd_io_via_udn_pod(
            cluster_name=primary_cluster,
            namespace=workload_namespace,
            vm_ip=vmi_ip_restored,
            file_path=vm_filepaths[2],
            username=workload.vm_username,
            verify=True,
        )
        logger.info(
            "E2E VM IP translation and DR failover/relocate test across multiple namespaces "
            "completed successfully!"
        )

        # Teardown: Disable DR protection
        logger.test_step(
            "Teardown: Disable DR protection using standard disable_dr_rdr"
        )
        delete_udn_ssh_helper_pod(
            cluster_names=[primary_cluster, secondary_cluster],
            namespaces=[workload_namespace, workload_namespace_2],
        )
        dr_helpers.disable_dr_rdr(discovered_apps=True)

    @pytest.mark.polarion_id("OCS-8387")
    def test_explicit_rule_takes_precedence_over_regex(
        self,
        setup_udn_nad,
        cnv_workload_with_static_ip,
        network_mapping_configmap,
    ):
        """
        Verify that an explicit IP mapping takes precedence over a regex/host-preserving
        rule when both are present in the same ConfigMap.

        Flow (order matters for NetworkMappingLoaded=True on first DRPC sync):
          1. Fetch the existing DRPolicy from the hub.
          2. Deploy VM without DR protection so no DRPC exists yet.
          3. Create the ConfigMap with both explicitMappings and regexMappings, then
             patch networkMappingRef on the existing DRPolicy.
          4. Wait for NetworkAttachmentsValidated=True on the policy.
          5. DR-protect the VM via CLI (Placement + DRPC referencing the vm-recipe).
             Because the DRPolicy already carries the networkMappingRef, the controller
             sets NetworkMappingLoaded=True on the DRPC immediately after creation.
          6. Wait for NetworkMappingLoaded=True on the DRPC.
          7. Failover to secondary; verify VM receives the explicit destination IP
             (192.168.2.200) rather than the host-preserving regex result (192.168.2.11).
          8. Relocate back to primary; verify the original IP (192.168.1.11) is restored.
        """
        expected_msg = "Network mapping loaded successfully"
        cluster_subnets = setup_udn_nad["cluster_subnets"]
        primary_cluster_name = setup_udn_nad["primary_cluster"]
        secondary_cluster_name = setup_udn_nad["secondary_cluster"]

        # ------------------------------------------------------------------
        # Step 1: Fetch the existing DRPolicy — no new policy needed
        # ------------------------------------------------------------------
        existing_drpolicy_name = get_drpolicy_name()
        logger.info(f"Using existing DRPolicy '{existing_drpolicy_name}'")

        # ------------------------------------------------------------------
        # Step 2: Deploy VM only — no DR protection yet
        # ------------------------------------------------------------------
        logger.test_step(
            "Deploy VM-1 without DR protection — DRPC will be created after "
            "the ConfigMap is in place"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        wl1 = cnv_workload_with_static_ip(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=False
        )
        assert wl1, "VM-1 workload deployment failed"
        workload1 = wl1[0]

        logger.test_step("Record VM-1 IP and verify IPAMClaim matches VMI on primary")
        vm1_ip = verify_vm_ip_and_ipamclaim(
            primary_cluster_name,
            workload1.workload_namespace,
            workload1.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        assert (
            vm1_ip == self.VM1_STATIC_IP_PRIMARY
        ), f"VM-1: expected IP {self.VM1_STATIC_IP_PRIMARY} on primary, got {vm1_ip}"
        logger.info(f"VM-1 primary IP confirmed: {vm1_ip}")

        # ------------------------------------------------------------------
        # Step 3: Create ConfigMap + patch networkMappingRef on existing policy
        # ------------------------------------------------------------------
        logger.test_step(
            "Create ConfigMap with explicitMappings for VM-1 and "
            "host-preserving regexMappings, then link to existing DRPolicy"
        )
        network_mapping_configmap(
            "vm-ip-map-precedence-test",
            cluster1_name=primary_cluster_name,
            cluster2_name=secondary_cluster_name,
            nad_namespace=setup_udn_nad["workload_namespace"],
            nad_name=VM_NETWORK_NAD_NAME,
            cluster1_cidr=cluster_subnets[primary_cluster_name],
            cluster2_cidr=cluster_subnets[secondary_cluster_name],
            explicit_mappings=[
                {
                    primary_cluster_name: self.VM1_STATIC_IP_PRIMARY,
                    secondary_cluster_name: self.VM1_EXPLICIT_DEST,
                }
            ],
            # regex_mappings=None → auto-generates host-preserving bidirectional
            # regex from cluster CIDRs; explicit entry above takes precedence for VM-1
            regex_mappings=None,
            drpolicy_name=existing_drpolicy_name,
        )

        # ------------------------------------------------------------------
        # Step 4: Wait for NetworkAttachmentsValidated=True on the existing policy
        # ------------------------------------------------------------------
        logger.test_step(
            f"Wait for DRPolicy '{existing_drpolicy_name}' NetworkAttachmentsValidated=True"
        )
        verify_drpolicy_network_attachments_validated(
            existing_drpolicy_name, timeout=300
        )
        logger.info(
            f"DRPolicy '{existing_drpolicy_name}' NetworkAttachmentsValidated=True — "
            "proceeding to DR-protect VM"
        )

        # ------------------------------------------------------------------
        # Step 5: DR-protect VM-1 using the existing DRPolicy via CLI
        # The DRPC must reference the vm-recipe (as the ACM UI does) or Ramen
        # does not discover the VM static IPs. Placement/DRPC names are the
        # ones CnvWorkloadDiscoveredApps.delete_workload() cleans up.
        # ------------------------------------------------------------------
        logger.test_step(
            "DR-protect VM-1 via CLI (DRPC with vm-recipe) "
            f"using DRPolicy '{existing_drpolicy_name}'"
        )

        workload1.create_placement()
        workload1.create_drpc(use_vm_recipe=True)

        # ------------------------------------------------------------------
        # Step 6: Wait for NetworkMappingLoaded=True on the new DRPC
        # ------------------------------------------------------------------
        drpc_name = workload1.discovered_apps_placement_name
        logger.test_step(
            f"Wait for DRPC '{drpc_name}' to report NetworkMappingLoaded=True"
        )
        wait_for_drpc_network_mapping_loaded(
            drpc_name, expected_status="True", expected_message_substring=expected_msg
        )
        logger.info(f"DRPC '{drpc_name}' reports NetworkMappingLoaded=True")

        # ------------------------------------------------------------------
        # Step 6b: Verify VRG staticIPDiscovery on primary and
        #          staticIPTranslationSpec on secondary.
        # workload1 is a CnvWorkloadDiscoveredApps — the helper resolves
        # .workload_namespace and .discovered_apps_placement_name via getattr
        # fallbacks (and get_drpc_name as last resort).
        # ------------------------------------------------------------------
        logger.test_step(
            f"Verify VRG staticIPDiscovery on primary cluster '{primary_cluster_name}' "
            f"— expect source IP {self.VM1_STATIC_IP_PRIMARY}"
        )
        verify_vrg_static_ip_discovery(
            cluster_name=primary_cluster_name,
            workload=workload1,
            expected_ip=self.VM1_STATIC_IP_PRIMARY,
            network_name=VM_NETWORK_NAD_NAME,
        )
        logger.test_step(
            f"Verify VRG staticIPTranslationSpec on secondary cluster '{secondary_cluster_name}' "
            f"— expect explicit translated IP {self.VM1_EXPLICIT_DEST}"
        )
        verify_vrg_static_ip_translation(
            cluster_name=secondary_cluster_name,
            workload=workload1,
            expected_translated_ip=self.VM1_EXPLICIT_DEST,
            network_name=VM_NETWORK_NAD_NAME,
        )
        logger.info(
            f"VRG IP verification passed: discovery={self.VM1_STATIC_IP_PRIMARY} on primary, "
            f"translation={self.VM1_EXPLICIT_DEST} on secondary"
        )

        # ------------------------------------------------------------------
        # Step 7: Wait for replication sync before failover
        # ------------------------------------------------------------------
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload1.workload_namespace,
            discovered_apps=True,
            resource_name=drpc_name,
        )
        wait_time = 2 * scheduling_interval
        logger.test_step(
            f"Wait {wait_time} minutes for replication sync before failover"
        )
        sleep(wait_time * 60)

        # ------------------------------------------------------------------
        # Step 8: Failover to secondary and verify explicit IP
        # ------------------------------------------------------------------
        logger.test_step(
            f"Failover workload VM-1 to secondary cluster '{secondary_cluster_name}'"
        )
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=workload1.workload_namespace,
            discovered_apps=True,
            workload_placement_name=drpc_name,
            old_primary=primary_cluster_name,
            timeout=420,
            skip_odf_cli_validation=True,
        )

        # DV-based VM workloads: Ramen cleans up the old-primary resources
        # automatically after failover — no manual oc delete -k needed.
        # Just wait for progression=Completed before checking the secondary.
        logger.test_step(
            "Wait for automatic post-failover cleanup — progression=Completed"
        )

        drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESPACE, resource_name=drpc_name)
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
        logger.info("Post-failover cleanup completed — progression=Completed")

        logger.test_step("Wait for workload to be running on secondary after failover")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload1.workload_pvc_count,
            workload1.workload_pod_count,
            workload1.workload_namespace,
            discovered_apps=True,
            vrg_name=drpc_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=workload1.vm_name,
            namespace=workload1.workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        logger.test_step(
            "Verify VM-1 received explicit destination IP "
            f"{self.VM1_EXPLICIT_DEST} (not host-preserving regex result 192.168.2.11)"
        )
        vm1_ip_secondary = verify_vm_ip_and_ipamclaim(
            secondary_cluster_name,
            workload1.workload_namespace,
            workload1.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        assert vm1_ip_secondary == self.VM1_EXPLICIT_DEST, (
            f"VM-1: explicit rule not applied — expected {self.VM1_EXPLICIT_DEST}, "
            f"got {vm1_ip_secondary}. Regex would have given 192.168.2.11."
        )
        logger.info(
            f"VM-1 explicit rule verified: {self.VM1_STATIC_IP_PRIMARY} "
            f"→ {vm1_ip_secondary}"
        )

        # ------------------------------------------------------------------
        # Step 9: Relocate back to primary and verify original IP
        # ------------------------------------------------------------------
        logger.test_step(
            f"Relocate workload back to primary cluster '{primary_cluster_name}'"
        )
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=workload1.workload_namespace,
            workload_placement_name=drpc_name,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=workload1,
            vm_auto_cleanup=True,
            skip_odf_cli_validation=True,
        )
        # DV-based VM workloads: Ramen cleans up the old-primary (secondary)
        # resources automatically — just wait for progression=Completed.
        logger.test_step(
            "Wait for automatic post-relocate cleanup — progression=Completed"
        )
        config.switch_acm_ctx()
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
        logger.info("Post-relocate cleanup completed — progression=Completed")

        logger.test_step("Wait for workload to be running on primary after relocate")
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload1.workload_pvc_count,
            workload1.workload_pod_count,
            workload1.workload_namespace,
            discovered_apps=True,
            vrg_name=drpc_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=workload1.vm_name,
            namespace=workload1.workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        logger.test_step(
            "Verify VM-1 IP is restored to original "
            f"{self.VM1_STATIC_IP_PRIMARY} after relocate"
        )
        vm1_ip_after_relocate = get_vm_ip_from_vmi(
            primary_cluster_name,
            workload1.workload_namespace,
            workload1.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        assert vm1_ip_after_relocate == self.VM1_STATIC_IP_PRIMARY, (
            f"VM-1: IP not restored after relocate — expected "
            f"{self.VM1_STATIC_IP_PRIMARY}, got {vm1_ip_after_relocate}"
        )
        logger.info(
            f"VM-1 IP restored after relocate: "
            f"{vm1_ip_secondary} → {vm1_ip_after_relocate}"
        )

    @tier2
    @pytest.mark.polarion_id("OCS-8391")
    def test_invalid_configmap_handling(
        self,
        setup_udn_nad,
        cnv_workload_with_static_ip,
    ):
        """
        TC-14: Verify graceful handling when the ConfigMap referenced by the
        DRPolicy is missing, empty, or has an invalid structure.

        Confirmed behaviour on ODF 5.0.0-53 (from live cluster testing):
        - Ramen sets DRPC.status.conditions[NetworkMappingLoaded]=False with
          reason=NetworkMappingLoadFailed and a descriptive message for each
          bad scenario.
        - The workload continues to be protected (Protected=True) — the bad
          ConfigMap does not break replication.
        - networkMappingRef must be set/removed via json-patch (merge-patch
          silently no-ops when the field was previously absent).

        Scenarios
        ---------
        missing       -- networkMappingRef points to a name with no ConfigMap;
                         message: ConfigMap "<name>" not found in namespace
        empty         -- CM present; mappings.yaml has no networkMappings key;
                         message: networkMappings list must not be empty
        invalid-regex -- CM present; regexMappings uses invalid direction key
                         (arbitrary string instead of <c1>-to-<c2> format);
                         message: unrecognized direction key "<key>"
        """
        primary_cluster = setup_udn_nad["primary_cluster"]
        secondary_cluster = setup_udn_nad["secondary_cluster"]
        primary_subnet = setup_udn_nad["cluster_subnets"][primary_cluster]
        secondary_subnet = setup_udn_nad["cluster_subnets"][secondary_cluster]

        config.switch_acm_ctx()
        drpolicy_name = get_drpolicy_name()
        logger.info(f"Using existing DRPolicy '{drpolicy_name}'")

        logger.test_step("Deploy VM-1 without DR protection")
        config.switch_to_cluster_by_name(primary_cluster)
        wl1 = cnv_workload_with_static_ip(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=False
        )
        assert wl1, "VM-1 workload deployment failed"
        workload1 = wl1[0]

        # DR-protect via CLI: the DRPC must reference the vm-recipe (as the ACM
        # UI does). Placement/DRPC names are the ones
        # CnvWorkloadDiscoveredApps.delete_workload() cleans up.
        logger.test_step(
            "DR-protect VM-1 via CLI (DRPC with vm-recipe) "
            f"using DRPolicy '{drpolicy_name}'"
        )

        workload1.create_placement()
        workload1.create_drpc(use_vm_recipe=True)

        drpc_name = workload1.discovered_apps_placement_name
        drpc_namespace = constants.DR_OPS_NAMESPACE
        drpc_obj = DRPC(namespace=drpc_namespace, resource_name=drpc_name)

        # Scenarios to test sequentially against the single workload
        primary_prefix = primary_subnet.split("/")[0].rsplit(".", 1)[0]
        escaped_prefix = primary_prefix.replace(".", "\\.")
        missing_cm_name = "vm-ip-map-missing"
        empty_cm_name = "vm-ip-map-empty"
        invalid_regex_cm_name = "vm-ip-map-invalid-regex"
        scenarios = [
            {
                "name": "missing",
                "cm_name": missing_cm_name,
                "create_cm": False,
                "cm_data": None,
                "expected_msg": f'network-mapping ConfigMap "{missing_cm_name}" not found in namespace "{constants.DR_OPS_NAMESPACE}"',  # noqa: E501
            },
            {
                "name": "empty",
                "cm_name": empty_cm_name,
                "create_cm": True,
                "cm_data": {"mappings.yaml": "version: v1\n"},
                "expected_msg": f'failed to parse network-mapping ConfigMap "{empty_cm_name}": invalid network mapping in ConfigMap "{empty_cm_name}": networkMappings list must not be empty',  # noqa: E501
            },
            {
                "name": "invalid-structure",
                "cm_name": invalid_regex_cm_name,
                "create_cm": True,
                "cm_data": {
                    "mappings.yaml": yaml.dump(
                        {
                            "version": "v1",
                            "networkMappings": [
                                {
                                    "cluster1": {
                                        "name": primary_cluster,
                                        "network": {
                                            "name": setup_udn_nad["nad_name"],
                                            "namespace": setup_udn_nad[
                                                "workload_namespace"
                                            ],
                                            "cidr": primary_subnet,
                                        },
                                    },
                                    "cluster2": {
                                        "name": secondary_cluster,
                                        "network": {
                                            "name": setup_udn_nad["nad_name"],
                                            "namespace": setup_udn_nad[
                                                "workload_namespace"
                                            ],
                                            "cidr": secondary_subnet,
                                        },
                                    },
                                    "regexMappings": {
                                        "invalid-direction-key": [
                                            {
                                                "pattern": f"^{escaped_prefix}\\.(\\d+)$",
                                                "replacement": "192.168.2.$1",
                                            }
                                        ]
                                    },
                                }
                            ],
                        }
                    )
                },
                "expected_msg": f'failed to parse network-mapping ConfigMap "{invalid_regex_cm_name}": invalid network mapping in ConfigMap "{invalid_regex_cm_name}": networkMappings[0]: networkRef.nadNamespace is required',  # noqa: E501
            },
        ]

        for sc in scenarios:
            scenario_name = sc["name"]
            cm_name = sc["cm_name"]
            expected_msg = sc["expected_msg"]
            logger.test_step(f"[{scenario_name}] Test bad ConfigMap '{cm_name}'")

            if sc["create_cm"]:
                create_raw_network_mapping_configmap(
                    name=cm_name,
                    data_dict=sc["cm_data"],
                    namespace=constants.DR_OPS_NAMESPACE,
                )

            try:
                config.switch_acm_ctx()
                drpolicy_data = ocp.OCP(kind="drpolicy").get(
                    resource_name=drpolicy_name
                )
                op = (
                    "replace"
                    if "networkMappingRef" in drpolicy_data.get("spec", {})
                    else "add"
                )
                exec_cmd(
                    f"oc patch drpolicy {drpolicy_name} --type=json"
                    f' -p \'[{{"op":"{op}",'
                    f'"path":"/spec/networkMappingRef",'
                    f'"value":{{"name":"{cm_name}"}}}}]\''
                )

                logger.test_step(
                    f"[{scenario_name}] Wait for DRPC '{drpc_name}' to report "
                    f"{DRPC_CONDITION_NM_LOADED}=False with message matching '{expected_msg}' (timeout 120s)"
                )
                wait_for_drpc_network_mapping_loaded(
                    drpc_name,
                    expected_status="False",
                    expected_message_substring=expected_msg,
                )

                logger.test_step(
                    f"[{scenario_name}] Verify {DRPC_CONDITION_NM_LOADED}=False "
                    f"with reason=NetworkMappingLoadFailed and message containing '{expected_msg}'"
                )
                nm_cond = get_drpc_network_mapping_loaded_condition(drpc_name)
                assert (
                    nm_cond is not None
                ), f"[{scenario_name}] {DRPC_CONDITION_NM_LOADED} condition is missing"
                assert (
                    nm_cond.get("status") == "False"
                ), f"[{scenario_name}] Expected status=False, got: {nm_cond.get('status')}"
                assert (
                    nm_cond.get("reason") == "NetworkMappingLoadFailed"
                ), f"[{scenario_name}] Expected reason=NetworkMappingLoadFailed, got: {nm_cond.get('reason')}"
                assert expected_msg in nm_cond.get(
                    "message", ""
                ), f"[{scenario_name}] Expected '{expected_msg}' in message, got: {nm_cond.get('message')}"

                logger.test_step(
                    f"[{scenario_name}] Verify workload replication is unaffected (Protected=True)"
                )
                drpc_obj.wait_for_clusterdataprotected_status()
                assert drpc_obj.get_clusterdataprotected_status() is True

                logger.info(
                    f"[{scenario_name}] DRPC correctly reports {DRPC_CONDITION_NM_LOADED}=False — "
                    f"reason={nm_cond.get('reason')}, message={nm_cond.get('message')}"
                )

            finally:
                config.switch_acm_ctx()
                try:
                    exec_cmd(
                        f"oc patch drpolicy {drpolicy_name} --type=json"
                        f' -p \'[{{"op":"remove",'
                        f'"path":"/spec/networkMappingRef"}}]\''
                    )
                    logger.info(
                        f"[{scenario_name}] Removed networkMappingRef from DRPolicy {drpolicy_name}"
                    )
                except Exception as exc:
                    logger.warning(
                        f"[{scenario_name}] Failed to remove networkMappingRef: {exc}"
                    )
                if sc["create_cm"]:
                    try:
                        exec_cmd(
                            f"oc delete configmap {cm_name} -n {constants.DR_OPS_NAMESPACE} --ignore-not-found"
                        )
                        logger.info(f"[{scenario_name}] Deleted CM '{cm_name}'")
                    except Exception as exc:
                        logger.warning(
                            f"[{scenario_name}] Cleanup failed for CM '{cm_name}': {exc}"
                        )


# ---------------------------------------------------------------------------
# Class 2: Miscellaneous — NAD errors, cleanup, and platform-specific
# ---------------------------------------------------------------------------


@rdr
@tier2
@turquoise_squad
@skipif_ocs_version("<4.23")
class TestVMIPTranslationMiscellaneous:
    """even till now
    Edge cases: missing/mislabeled NADs, DR unprotect cleanup, hub recovery,
    post-upgrade behavior, and BM HCP cluster validation.

    Prerequisites:
      - ODF >= 4.21 with RHSTOR-8082 feature enabled
      - UDN vm-network on both managed clusters (setup_udn_nad fixture)
      - ACM hub cluster registered with at least two DRClusters
    """

    @pytest.mark.polarion_id("OCS-8388")
    def test_drclusterconfig_nad_discovery(self, setup_udn_nad):
        """
        TC-6: Verify that the DRClusterConfig controller on each managed
        cluster lists the NAD labeled with ramendr.openshift.io/dr-network:
        "true" in status.networkAttachments.

        DRClusterConfig is a cluster-scoped resource on the managed clusters
        (named after the cluster), so no DRPolicy or hub-side check is needed.
        """
        drcluster_list = dr_helpers.get_all_drclusters()
        assert drcluster_list, "No DRClusters found on hub"

        logger.test_step(
            "Verify each DRClusterConfig has discovered the labeled NAD in "
            f"status.{DRCLUSTERCONFIG_NAD_STATUS_FIELD}"
        )
        for cluster_name in drcluster_list:
            net_attachments = wait_for_drclusterconfig_nad_discovery(cluster_name)
            assert net_attachments, (
                f"DRClusterConfig for {cluster_name} has empty "
                f"status.{DRCLUSTERCONFIG_NAD_STATUS_FIELD} — "
                f"expected the labeled NAD to be discovered"
            )

    @pytest.mark.polarion_id("OCS-8393")
    def test_missing_incorrectly_labeled_nad(
        self, setup_udn_nad, network_mapping_configmap
    ):
        """
        TC-6 / TC-16: Verify graceful handling of NAD misconfigurations across
        three scenarios run sequentially in a single test:

        1. missing-nad:
           Delete the UDN (and thus its owned NAD) from the secondary cluster.
           The DRPolicy NADsSynced condition must transition to False with a
           descriptive reason. The controller must not crash or dead-lock.
           Restore the UDN after the assertion before proceeding.

        2. wrong-label:
           Update the DR discovery label on the UDN to "" (empty value instead
           of "true") on both clusters. The DRPolicy controller must surface
           NoEligibleNetworkAttachments — it must not treat the wrongly-labelled
           NAD as a valid static-IP network. Restore the correct label afterward.

        3. no-labeled-nads:
           Remove the DR discovery label from the UDN on both clusters entirely.
           Each DRClusterConfig.status.networkAttachments must become empty and
           the DRPolicy condition NetworkAttachmentsValidated must be True with
           reason NoEligibleNetworkAttachments. Restore the label afterward.
        """
        workload_namespace = setup_udn_nad["workload_namespace"]
        primary_cluster = setup_udn_nad["primary_cluster"]
        secondary_cluster = setup_udn_nad["secondary_cluster"]
        cluster_subnets = setup_udn_nad["cluster_subnets"]

        logger.test_step("Get the DRPolicy name from the hub cluster")
        drpolicy_name = get_drpolicy_name()
        drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY, resource_name=drpolicy_name)

        # The DRPolicy controller only reports NetworkAttachmentsValidated (and
        # networkPeers) once spec.networkMappingRef is set; without it the
        # condition is never emitted.
        logger.test_step(
            f"Create network mapping ConfigMap and link it to DRPolicy '{drpolicy_name}'"
        )
        network_mapping_configmap(
            "vm-ip-map-nad-negative-test",
            cluster1_name=primary_cluster,
            cluster2_name=secondary_cluster,
            nad_namespace=workload_namespace,
            nad_name=VM_NETWORK_NAD_NAME,
            cluster1_cidr=cluster_subnets[primary_cluster],
            cluster2_cidr=cluster_subnets[secondary_cluster],
            drpolicy_name=drpolicy_name,
        )

        # ── Scenario 1: missing-nad ──────────────────────────────────────────
        logger.test_step(
            "Scenario 1 (missing-nad): Delete the UserDefinedNetwork "
            f"'{VM_NETWORK_UDN_NAME}' from secondary cluster '{secondary_cluster}'"
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        exec_cmd(
            f"oc delete userdefinednetwork {VM_NETWORK_UDN_NAME} "
            f"-n {workload_namespace} --ignore-not-found"
        )
        logger.info(
            f"Deleted UDN {VM_NETWORK_UDN_NAME} from {workload_namespace} "
            f"on {secondary_cluster}"
        )

        try:
            logger.test_step(
                f"Wait for DRPolicy '{drpolicy_name}' to report "
                f"{DRPOLICY_NADS_SYNCED_CONDITION}=False"
            )
            for sample in TimeoutSampler(
                timeout=180,
                sleep=5,
                func=lambda: next(
                    (
                        c.get("status")
                        for c in drpolicy_ocp.get()
                        .get("status", {})
                        .get("conditions", [])
                        if c.get("type") == DRPOLICY_NADS_SYNCED_CONDITION
                    ),
                    None,
                ),
            ):
                if sample == "False":
                    break

            logger.test_step(
                f"Verify {DRPOLICY_NADS_SYNCED_CONDITION}=False with a "
                "descriptive reason"
            )
            conditions = drpolicy_ocp.get().get("status", {}).get("conditions", [])
            nads_synced = next(
                (
                    c
                    for c in conditions
                    if c.get("type") == DRPOLICY_NADS_SYNCED_CONDITION
                ),
                None,
            )
            assert nads_synced is not None, (
                f"DRPolicy {drpolicy_name} missing condition "
                f"'{DRPOLICY_NADS_SYNCED_CONDITION}'"
            )
            assert nads_synced.get("status") == "False", (
                f"Expected {DRPOLICY_NADS_SYNCED_CONDITION}=False after NAD "
                f"removal, got: {nads_synced}"
            )
            assert nads_synced.get("message") or nads_synced.get("reason"), (
                f"{DRPOLICY_NADS_SYNCED_CONDITION} condition has empty message "
                f"and reason: {nads_synced}"
            )
            logger.info(
                f"missing-nad: DRPolicy correctly reports "
                f"{DRPOLICY_NADS_SYNCED_CONDITION}=False — "
                f"reason={nads_synced.get('reason')}"
            )
        finally:
            logger.test_step(
                f"Restore UDN '{VM_NETWORK_UDN_NAME}' on secondary cluster "
                f"'{secondary_cluster}'"
            )
            config.switch_to_cluster_by_name(secondary_cluster)
            udn_manifest = build_udn_manifest(
                name=VM_NETWORK_UDN_NAME,
                namespace=workload_namespace,
                subnet=cluster_subnets[secondary_cluster],
            )
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".yaml", delete=False
            ) as f:
                templating.dump_data_to_temp_yaml(udn_manifest, f.name)
                exec_cmd(f"oc apply -f {f.name}")
            logger.info(f"Restored UDN {VM_NETWORK_UDN_NAME} on {secondary_cluster}")

        # ── Scenario 2: wrong-label ──────────────────────────────────────────
        # Update the DR discovery label on the existing UDN to "" (empty value
        # instead of "true") on both clusters. The discovery selector requires
        # value "true", so the UDN/NAD must be dropped.
        target_clusters = [primary_cluster, secondary_cluster]
        logger.test_step(
            f"Scenario 2 (wrong-label): Update UDN '{VM_NETWORK_UDN_NAME}' DR label "
            f"to '{UDN_DR_LABEL}=\"\"' (wrong value) on both clusters"
        )
        try:
            for cluster_name in target_clusters:
                config.switch_to_cluster_by_name(cluster_name)
                exec_cmd(
                    f"oc label userdefinednetwork {VM_NETWORK_UDN_NAME} "
                    f"-n {workload_namespace} "
                    f"{UDN_DR_LABEL}='' --overwrite"
                )
                logger.info(
                    f"Set '{UDN_DR_LABEL}=\"\"' on UDN "
                    f"'{VM_NETWORK_UDN_NAME}' on {cluster_name}"
                )

            logger.test_step(
                f"Verify DRPolicy '{drpolicy_name}' "
                f"{DRPOLICY_NADS_SYNCED_CONDITION}=True with "
                f"reason=NoEligibleNetworkAttachments "
                f"(wrong-label UDN/NAD must not be discovered)"
            )
            for sample_status, sample_reason, _ in TimeoutSampler(
                timeout=120,
                sleep=5,
                func=get_drpolicy_network_attachments_condition,
                drpolicy_name=drpolicy_name,
            ):
                if (
                    sample_status == "True"
                    and sample_reason == "NoEligibleNetworkAttachments"
                ):
                    break

            status, reason, message = get_drpolicy_network_attachments_condition(
                drpolicy_name
            )
            assert status == "True", f"Expected status=True, got: {status}"
            assert (
                reason == "NoEligibleNetworkAttachments"
            ), f"Expected reason=NoEligibleNetworkAttachments, got: {reason}"
            assert "No NADs eligible for static-IP translation" in (
                message or ""
            ), f"Expected message to mention no eligible NADs, got: {message}"
            logger.info(
                f"wrong-label: {DRPOLICY_NADS_SYNCED_CONDITION} "
                f"status={status}, reason={reason}, message={message}"
            )
        finally:
            logger.test_step(
                f"Restore '{UDN_DR_LABEL}={UDN_DR_LABEL_VALUE}' on UDN "
                f"'{VM_NETWORK_UDN_NAME}' on both clusters"
            )
            for cluster_name in target_clusters:
                config.switch_to_cluster_by_name(cluster_name)
                exec_cmd(
                    f"oc label userdefinednetwork {VM_NETWORK_UDN_NAME} "
                    f"-n {workload_namespace} "
                    f"{UDN_DR_LABEL}={UDN_DR_LABEL_VALUE} --overwrite"
                )
                logger.info(
                    f"Restored '{UDN_DR_LABEL}={UDN_DR_LABEL_VALUE}' on "
                    f"UDN '{VM_NETWORK_UDN_NAME}' on {cluster_name}"
                )

        # ── Scenario 3: no-labeled-nads ──────────────────────────────────────
        drcluster_list = dr_helpers.get_all_drclusters()
        assert drcluster_list, "No DRClusters found on hub"

        logger.test_step(
            "Scenario 3 (no-labeled-nads): Remove the DR discovery label from "
            "the UDN on both clusters"
        )
        try:
            for cluster_name in drcluster_list:
                config.switch_to_cluster_by_name(cluster_name)
                exec_cmd(
                    f"oc label userdefinednetwork {VM_NETWORK_UDN_NAME} "
                    f"-n {workload_namespace} {UDN_DR_LABEL}-"
                )
                logger.info(
                    f"Removed label '{UDN_DR_LABEL}' from UDN on {cluster_name}"
                )

            logger.test_step(
                "Verify DRClusterConfig.status.networkAttachments is empty "
                "on both clusters"
            )
            for cluster_name in drcluster_list:

                def _get_net_attachments(cluster_name=cluster_name):
                    config.switch_to_cluster_by_name(cluster_name)
                    return (
                        ocp.OCP(
                            kind=constants.DRCLUSTERCONFIG,
                            resource_name=cluster_name,
                        )
                        .get()
                        .get("status", {})
                        .get(DRCLUSTERCONFIG_NAD_STATUS_FIELD, [])
                    )

                for sample in TimeoutSampler(
                    timeout=120, sleep=5, func=_get_net_attachments
                ):
                    if not sample:
                        break
                logger.info(
                    f"DRClusterConfig {cluster_name} has zero discovered NADs, "
                    "as expected"
                )

            logger.test_step(
                f"Verify DRPolicy '{drpolicy_name}' reports "
                f"{DRPOLICY_NADS_SYNCED_CONDITION}=True with reason "
                "NoEligibleNetworkAttachments"
            )
            for sample_status, sample_reason, _ in TimeoutSampler(
                timeout=120,
                sleep=5,
                func=get_drpolicy_network_attachments_condition,
                drpolicy_name=drpolicy_name,
            ):
                if (
                    sample_status == "True"
                    and sample_reason == "NoEligibleNetworkAttachments"
                ):
                    break

            status, reason, message = get_drpolicy_network_attachments_condition(
                drpolicy_name
            )
            assert status == "True", f"Expected status=True, got: {status}"
            assert (
                reason == "NoEligibleNetworkAttachments"
            ), f"Expected reason=NoEligibleNetworkAttachments, got: {reason}"
            assert "No NADs eligible for static-IP translation" in (
                message or ""
            ), f"Expected message to mention no eligible NADs, got: {message}"
            logger.info(
                f"no-labeled-nads: {DRPOLICY_NADS_SYNCED_CONDITION} "
                f"status={status}, reason={reason}, message={message}"
            )
        finally:
            logger.test_step("Restore the DR discovery label on the UDNs")
            for cluster_name in drcluster_list:
                config.switch_to_cluster_by_name(cluster_name)
                exec_cmd(
                    f"oc label userdefinednetwork {VM_NETWORK_UDN_NAME} "
                    f"-n {workload_namespace} "
                    f"{UDN_DR_LABEL}={UDN_DR_LABEL_VALUE} --overwrite"
                )
                logger.info(f"Restored label '{UDN_DR_LABEL}' on {cluster_name}")

    @pytest.mark.polarion_id("OCS-8392")
    def test_out_of_range_ip_ipamclaim_error(
        self,
        setup_udn_nad,
        cnv_workload_with_static_ip,
        network_mapping_configmap,
    ):
        """
        TC-14 (req #14): Verify that when a translation rule maps the VM to an
        IP that is outside every known subnet on the secondary cluster:
          - the IPAMClaim on the secondary reports IPsAllocated=False with a
            message containing "not contained in any known subnet"
          - the VM does not reach Running on the secondary
          - the failover still reaches progression=Completed (no dead-lock)

        An explicit mapping rule is used with a destination IP that is outside
        both PRIMARY_SUBNET and SECONDARY_SUBNET (10.99.99.99), and no regex
        rule, so only that explicit translation applies.

        The primary cluster stays up: this is a failover with both clusters
        healthy, not a disaster scenario.

        Flow (order matters for NetworkMappingLoaded=True on first DRPC sync):
          1. Deploy the VM without DR protection.
          2. Create the ConfigMap and link it to the existing DRPolicy.
          3. Wait for NetworkAttachmentsValidated=True on the policy.
          4. DR-protect the VM via CLI (Placement + DRPC with the vm-recipe).
          5. Verify the VRG holds the out-of-range translation on the secondary.
          6. Fail over to the secondary and verify the IPAMClaim error.
          7. Verify the VM is not Running and the DRPC progression is Completed.
          8. Relocate back to the primary.
        """
        cluster_subnets = setup_udn_nad["cluster_subnets"]
        primary_cluster = setup_udn_nad["primary_cluster"]
        secondary_cluster = setup_udn_nad["secondary_cluster"]

        # Out-of-range destination: an IP that does not belong to either cluster's
        # UDN subnet, guaranteeing the IPAMClaim error.
        out_of_range_ip = "10.99.99.99"

        logger.test_step("Get the DRPolicy name from the hub cluster")
        drpolicy_name = get_drpolicy_name()

        logger.test_step("Deploy the static-IP CNV workload without DR protection")
        config.switch_to_cluster_by_name(primary_cluster)
        wl_list = cnv_workload_with_static_ip(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=False
        )
        assert wl_list, "Static-IP CNV workload deployment failed"
        workload = wl_list[0]
        namespace = workload.workload_namespace
        drpc_name = workload.discovered_apps_placement_name

        logger.test_step("Record the VM's primary IP from its IPAMClaim")
        vm_ip_primary = get_ipamclaim_ip(
            primary_cluster, namespace, workload.vm_name, VM_NETWORK_NAD_NAME
        )
        assert (
            vm_ip_primary
        ), f"Could not read IPAMClaim IP for {workload.vm_name} on primary"
        logger.info(f"VM {workload.vm_name} primary IP: {vm_ip_primary}")

        logger.test_step(
            f"Create ConfigMap with out-of-range explicit rule: "
            f"{vm_ip_primary} -> {out_of_range_ip}"
        )
        network_mapping_configmap(
            "vm-ip-map-out-of-range",
            cluster1_name=primary_cluster,
            cluster2_name=secondary_cluster,
            nad_namespace=setup_udn_nad["workload_namespace"],
            nad_name=VM_NETWORK_NAD_NAME,
            cluster1_cidr=cluster_subnets[primary_cluster],
            cluster2_cidr=cluster_subnets[secondary_cluster],
            explicit_mappings=[
                {primary_cluster: vm_ip_primary, secondary_cluster: out_of_range_ip}
            ],
            # Empty dict -> no regex, no auto-generate: only the explicit rule applies
            regex_mappings={},
            drpolicy_name=drpolicy_name,
        )

        logger.test_step(
            f"Wait for DRPolicy '{drpolicy_name}' NetworkAttachmentsValidated=True"
        )
        verify_drpolicy_network_attachments_validated(drpolicy_name, timeout=300)

        logger.test_step(
            f"DR-protect the VM via CLI (DRPC with vm-recipe) using DRPolicy "
            f"'{drpolicy_name}'"
        )
        workload.create_placement()
        workload.create_drpc(use_vm_recipe=True)

        logger.test_step(f"Wait for DRPC '{drpc_name}' NetworkMappingLoaded=True")
        wait_for_drpc_network_mapping_loaded(
            drpc_name,
            expected_status="True",
            expected_message_substring="Network mapping loaded successfully",
        )

        logger.test_step(
            f"Verify VRG staticIPTranslationSpec on secondary '{secondary_cluster}' "
            f"holds the out-of-range IP {out_of_range_ip}"
        )
        verify_vrg_static_ip_translation(
            cluster_name=secondary_cluster,
            workload=workload,
            expected_translated_ip=out_of_range_ip,
            network_name=VM_NETWORK_NAD_NAME,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            namespace, discovered_apps=True, resource_name=drpc_name
        )
        logger.test_step(
            f"Wait {scheduling_interval} minutes for replication sync before failover"
        )
        sleep(scheduling_interval * 60)

        logger.test_step(f"Failover to secondary cluster '{secondary_cluster}'")
        dr_helpers.failover(
            failover_cluster=secondary_cluster,
            namespace=namespace,
            discovered_apps=True,
            workload_placement_name=drpc_name,
            old_primary=primary_cluster,
            timeout=420,
            skip_odf_cli_validation=True,
        )

        logger.test_step(
            "Verify the IPAMClaim on secondary reports IPsAllocated=False with "
            f"'not contained in any known subnet' for {out_of_range_ip}"
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        ipamclaim_name = f"{workload.vm_name}.{VM_NETWORK_NAD_NAME}"

        def _get_ips_allocated_condition():
            claim = ocp.OCP(
                kind="IPAMClaim",
                namespace=namespace,
                resource_name=ipamclaim_name,
            ).get(dont_raise=True)
            if not claim:
                return None
            return next(
                (
                    c
                    for c in claim.get("status", {}).get("conditions", [])
                    if c.get("type") == "IPsAllocated"
                ),
                None,
            )

        ips_allocated = None
        for sample in TimeoutSampler(
            timeout=300, sleep=10, func=_get_ips_allocated_condition
        ):
            if sample and sample.get("status") == "False":
                ips_allocated = sample
                break
        logger.info(
            f"IPAMClaim '{ipamclaim_name}' IPsAllocated condition: {ips_allocated}"
        )
        assert (
            ips_allocated is not None
        ), f"IPAMClaim '{ipamclaim_name}' did not report IPsAllocated=False"
        assert (
            ips_allocated.get("reason") == "InternalError"
        ), f"Expected IPsAllocated reason InternalError, got: {ips_allocated}"
        assert out_of_range_ip in ips_allocated.get("message", ""), (
            f"Expected the IPsAllocated message to mention {out_of_range_ip}, "
            f"got: {ips_allocated.get('message')}"
        )
        assert "not contained in any known subnet" in ips_allocated.get(
            "message", ""
        ), (
            "Expected the IPsAllocated message to say 'not contained in any "
            f"known subnet', got: {ips_allocated.get('message')}"
        )

        logger.test_step(
            "Verify the VM is NOT Running on secondary (IPAM cannot allocate the IP)"
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        vmi_obj = ocp.OCP(
            kind=constants.VIRTUAL_MACHINE_INSTANCES,
            namespace=namespace,
            resource_name=workload.vm_name,
        )

        def _get_vmi_phase():
            vmi = vmi_obj.get(dont_raise=True)
            return (vmi or {}).get("status", {}).get("phase")

        # Poll for a fixed window; the VM must never reach Running. TimeoutSampler
        # raises TimeoutExpiredError when the window ends, which is the success path.
        try:
            for vmi_phase in TimeoutSampler(timeout=120, sleep=10, func=_get_vmi_phase):
                assert vmi_phase != constants.STATUS_RUNNING, (
                    f"VM {workload.vm_name} reached Running on secondary despite "
                    f"out-of-range IP {out_of_range_ip}"
                )
        except TimeoutExpiredError:
            logger.info(
                f"VM {workload.vm_name} did not reach Running on secondary, as expected"
            )

        logger.test_step(
            "Verify the DRPC progression reaches Completed (no dead-lock despite "
            "the IPAMClaim error)"
        )
        config.switch_acm_ctx()
        drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESPACE, resource_name=drpc_name)
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
        logger.info(
            f"DRPC '{drpc_name}' progression=Completed despite the IPAMClaim error"
        )

        # ------------------------------------------------------------------
        # Relocate back. It does not depend on the secondary VM ever starting.
        # ------------------------------------------------------------------
        logger.test_step(f"Relocate workload back to primary '{primary_cluster}'")
        dr_helpers.relocate(
            preferred_cluster=primary_cluster,
            namespace=namespace,
            workload_placement_name=drpc_name,
            discovered_apps=True,
            old_primary=secondary_cluster,
            workload_instance=workload,
            vm_auto_cleanup=True,
            skip_odf_cli_validation=True,
        )
        config.switch_acm_ctx()
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
        logger.info("Out-of-range IP test teardown complete")


# ---------------------------------------------------------------------------
# Class 3: Mixed namespace — static-IP and non-static VMs, multiple DRPCs
# ---------------------------------------------------------------------------


@rdr
@tier2
@turquoise_squad
@skipif_ocs_version("<4.23")
class TestVMIPTranslationMixedNamespace:
    """
    TC-X: Mixed static-IP and non-static VMs coexist across workloads and DRPCs:
      Static-IP VM (standalone) + Static-IP VMs (shared) + Non-static VM (DHCP)

    Verifies:
      - The non-static VM is unaffected by IP translation rules
        (no IPAMClaim → no translation → IP unchanged post-failover)
      - Static-IP VMs all receive correctly translated IPs after failover
      - DRPCs can failover and relocate cleanly

    Prerequisites:
      - ODF >= 4.23 with RHSTOR-8082 feature enabled
      - UDN vm-network on both managed clusters (setup_udn_nad fixture)
    """

    @pytest.mark.polarion_id("OCS-8390")
    def test_mixed_namespace_multiple_drpcs(
        self,
        setup_acm_ui,
        setup_udn_nad,
        discovered_apps_dr_workload_cnv,
        cnv_workload_with_static_ip,
        network_mapping_configmap,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Deploy static-IP and non-static (DHCP) VM workloads:
          - Static-IP VM (standalone): deployed via cnv_workload_with_static_ip(pvc_vm=1, shared_drpc_protection=False)
          - Static-IP VMs (shared):    deployed via cnv_workload_with_static_ip(pvc_vm=2, shared_drpc_protection=True)
          - Non-static VM (DHCP):      deployed via discovered_apps_dr_workload_cnv(pvc_vm=1)

        Also covers requirement #12 (dual/mixed interface selectivity):
          The ConfigMap intentionally includes an explicit mapping rule for the
          DHCP VM's primary IP. After failover this rule must be silently ignored —
          translation is IPAMClaim-gated, not IP-address-gated. Proof: no IPAMClaim
          for non-static VM on secondary, and its IP does NOT equal the mapped
          destination in the explicit rule.

        After failover:
          - non-static VM: no IPAMClaim on secondary, IP != explicit-rule dest
          - static-IP VMs: each receives correctly translated IP
          - ACM fleet virtualization UI shows all VMs running on secondary
        After relocate:
          - All VMs restored to their original IPs
          - ACM fleet virtualization UI shows all VMs running on primary
        """
        _ = (setup_acm_ui, node_restart_teardown)
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt"]
        md5sums_original = {}
        md5sums_failover = {}

        cluster_subnets = setup_udn_nad["cluster_subnets"]
        primary_cluster_name = setup_udn_nad["primary_cluster"]
        secondary_cluster_name = setup_udn_nad["secondary_cluster"]

        logger.test_step("Get the DRPolicy name from the hub cluster")
        drpolicy_name = get_drpolicy_name()

        # ------------------------------------------------------------------
        # Deploy phase
        #
        # DRPC layout (all VMs share namespace vm-ip-translation-test):
        #   drpc_1_name  — VM-1 (Standalone) + VM-2 (Shared with VM-1)
        #   drpc_3_name  — VM-3 (Standalone) + non-static VM (Shared with VM-3)
        # ------------------------------------------------------------------

        logger.test_step(
            f"Switch to primary cluster '{primary_cluster_name}' and deploy static-IP VM-1"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        wl1 = cnv_workload_with_static_ip(
            pvc_vm=1,
            dr_protect=False,
            shared_drpc_protection=False,
        )
        assert wl1, "VM-1 static-IP workload deployment failed"
        workload_1 = wl1[0]

        # All workloads share the same namespace
        workload_namespace = workload_1.workload_namespace

        # DRPC-1: covers VM-1 (Standalone anchor) + VM-2 (Shared)
        drpc_1_name = workload_1.discovered_apps_placement_name + "-drpc"

        logger.test_step("Deploy static-IP VM-2 and VM-3 (from shared config list)")
        # pvc_vm=2 deploys shared[0]=VM-2 and shared[1]=VM-3 in one call
        wl_shared = cnv_workload_with_static_ip(
            pvc_vm=2,
            dr_protect=False,
            shared_drpc_protection=True,
        )
        # factory() accumulates all instances across calls; after the VM-1 call
        # wl_shared = [VM-1, VM-2, VM-3] — so VM-2 is at index 1, VM-3 at index 2.
        assert len(wl_shared) >= 3, "VM-2/VM-3 static-IP workload deployment failed"
        workload_2 = wl_shared[1]
        workload_3 = wl_shared[2]

        # DRPC-2: covers VM-3 (Standalone anchor) + non-static VM (Shared)
        # UI creates DRPC as "<protection_name>-drpc"; protection name for DRPC-2
        # is placement_name + "-2", so the resulting DRPC is placement_name + "-2-drpc".
        drpc_3_name = workload_3.discovered_apps_placement_name + "-2-drpc"

        logger.test_step("Deploy non-static VM (DHCP) — will share DRPC-2 with VM-3")
        wl_non_static = discovered_apps_dr_workload_cnv(
            pvc_vm=1,
            dr_protect=False,
            shared_drpc_protection=False,
            workload_namespace=workload_namespace,
        )
        assert wl_non_static, "Non-static VM deployment failed"
        workload_non_static = wl_non_static[0]

        static_workloads = [workload_1, workload_2, workload_3]
        all_workloads = static_workloads + [workload_non_static]

        # drpc_workload_groups drives failover/relocate loops:
        #   (drpc_name, namespace_anchor_workload, [all workloads under that drpc])
        drpc_workload_groups = [
            (drpc_1_name, workload_1, [workload_1, workload_2]),
            (drpc_3_name, workload_3, [workload_3, workload_non_static]),
        ]

        # ------------------------------------------------------------------
        # Network mapping ConfigMap
        # ------------------------------------------------------------------

        logger.test_step(
            "Record DHCP VM IP on primary before creating the ConfigMap — "
            "this IP will be used as the 'bait' explicit rule"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        dhcp_vm_ip_primary = get_vm_ip_from_vmi(
            primary_cluster_name,
            workload_non_static.workload_namespace,
            workload_non_static.vm_name,
        )
        assert (
            dhcp_vm_ip_primary
        ), f"Could not read IP from DHCP VM {workload_non_static.vm_name} on primary"
        # Construct the explicit "bait" destination — use an address in the
        # secondary subnet that would never be assigned by DHCP, making it
        # unambiguous if the rule were incorrectly applied.
        dhcp_bait_secondary_prefix = cluster_subnets[secondary_cluster_name].rsplit(
            ".", 1
        )[0]
        dhcp_vm_bait_dest = f"{dhcp_bait_secondary_prefix}.250"
        logger.info(
            f"DHCP VM {workload_non_static.vm_name} primary IP: {dhcp_vm_ip_primary} — "
            f"explicit bait rule will map it to {dhcp_vm_bait_dest}"
        )

        logger.test_step(
            "Create the network mapping ConfigMap with:\n"
            "  - host-preserving regex (covers all static-IP VMs)\n"
            "  - explicit 'bait' rule for DHCP VM's IP (must be ignored on failover)"
        )
        network_mapping_configmap(
            "vm-ip-map-mixed-ns",
            cluster1_name=primary_cluster_name,
            cluster2_name=secondary_cluster_name,
            nad_namespace=setup_udn_nad["workload_namespace"],
            nad_name=VM_NETWORK_NAD_NAME,
            cluster1_cidr=cluster_subnets[primary_cluster_name],
            cluster2_cidr=cluster_subnets[secondary_cluster_name],
            # Bait rule: explicit mapping for the DHCP VM's IP.
            # Translation is IPAMClaim-gated so this must be silently ignored.
            explicit_mappings=[
                {
                    primary_cluster_name: dhcp_vm_ip_primary,
                    secondary_cluster_name: dhcp_vm_bait_dest,
                },
            ],
            # regex_mappings=None → auto host-preserving bidirectional regex for
            # all static-IP VMs (covers the UDN subnet)
            regex_mappings=None,
            drpolicy_name=drpolicy_name,
        )

        logger.test_step(
            f"Wait for DRPolicy '{drpolicy_name}' to populate networkPeers"
        )
        wait_for_drpolicy_network_peers(drpolicy_name)

        # ------------------------------------------------------------------
        # DR protection via ACM UI fleet virtualization page
        # ------------------------------------------------------------------
        logger.test_step(
            "Assign DR Protection via ACM UI Fleet Virtualization VMs page"
        )
        config.switch_acm_ctx()
        acm_obj = AcmAddClusters()
        assert navigate_using_fleet_virtualization(
            acm_obj
        ), "Could not navigate to ACM fleet virtualization page"

        # Enrollment order and protection types (all in workload_namespace):
        #   VM-1  → Standalone  (creates DRPC-1)
        #   VM-2  → Shared      (joins DRPC-1)
        #   VM-3  → Standalone  (creates DRPC-2)
        #   non-static → Shared (joins DRPC-2)
        #
        # navigate_using_fleet_virtualization is called once above;
        # ensure_on_fleet_vms_page inside each call handles navigating back.
        drpc_1_prot_name = workload_1.discovered_apps_placement_name
        # Protection name for DRPC-2: append "-2" so the UI names the placement
        # "<placement>-2" and the resulting DRPC is "<placement>-2-drpc".
        drpc_3_prot_name = workload_3.discovered_apps_placement_name + "-2"
        for vm, standalone_flag, prot_name in [
            (workload_1, True, drpc_1_prot_name),
            (workload_2, False, drpc_1_prot_name),
            (workload_3, True, drpc_3_prot_name),
            (workload_non_static, False, drpc_3_prot_name),
        ]:
            assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                acm_obj,
                vms=[vm],
                managed_cluster_name=primary_cluster_name,
                standalone=standalone_flag,
                protection_name=prot_name,
                namespace=workload_namespace,
            ), f"Failed to assign DR protection to {vm.vm_name} via UI"

        # Patch discovered_apps_placement_name on workload_3 and workload_non_static
        # so their teardown deletes the correct DRPC-2 (vm-ip-translation-test-2-drpc).
        # The UI enrolled them under protection_name=drpc_3_prot_name; the default
        # placement_name on these objects is still "vm-ip-translation-test" which maps
        # to DRPC-1 — wrong for teardown.
        workload_3.discovered_apps_placement_name = drpc_3_prot_name
        workload_non_static.discovered_apps_placement_name = drpc_3_prot_name

        # ------------------------------------------------------------------
        # Wait for DR resource creation and running status on primary
        # ------------------------------------------------------------------
        logger.test_step(
            "Wait for all VRG resources creation and running status on primary"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        # With multiple DRPCs in the same namespace and CG enabled, each DRPC
        # creates exactly 1 VolumeReplication. wait_for_all_resources_creation
        # counts ALL VRs in the namespace, so we must pass the namespace-wide
        # total PVC/pod counts and skip the per-DRPC VR count check (it would
        # always expect 1 per call but see len(drpc_workload_groups) total VRs).
        total_pvc_count = sum(wl.workload_pvc_count for wl in all_workloads)
        total_pod_count = sum(wl.workload_pod_count for wl in all_workloads)
        for drpc_name, main_wl, wls in drpc_workload_groups:
            dr_helpers.wait_for_all_resources_creation(
                total_pvc_count,
                total_pod_count,
                main_wl.workload_namespace,
                discovered_apps=True,
                vrg_name=drpc_name,
                skip_replication_resources=True,
            )
        for wl in all_workloads:
            dr_helpers.wait_for_cnv_workload(
                vm_name=wl.vm_name,
                namespace=wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        # ------------------------------------------------------------------
        # Pre-failover IP snapshot
        # ------------------------------------------------------------------

        logger.test_step(
            "Record IPs and verify IPAMClaim matches VMI for all static-IP VMs "
            "on primary before failover"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        ip_primary = {}
        for wl in static_workloads:
            vm_ip = verify_vm_ip_and_ipamclaim(
                primary_cluster_name,
                wl.workload_namespace,
                wl.vm_name,
                VM_NETWORK_NAD_NAME,
            )
            assert vm_ip.startswith(
                PRIMARY_SUBNET.rsplit(".", 1)[0]
            ), f"{wl.vm_name}: IP {vm_ip} is not in primary subnet {PRIMARY_SUBNET}"
            ip_primary[wl.vm_name] = vm_ip
            logger.info(f"{wl.vm_name} primary IP: {vm_ip}")

        logger.test_step(
            f"Record non-static VM ({workload_non_static.vm_name}) IP on primary (should be unchanged after failover)"
        )
        non_static_ip_primary = get_vm_ip_from_vmi(
            primary_cluster_name,
            workload_non_static.workload_namespace,
            workload_non_static.vm_name,
        )
        logger.info(
            f"non-static VM {workload_non_static.vm_name} primary IP: {non_static_ip_primary}"
        )

        # ------------------------------------------------------------------
        # I/O baseline
        # ------------------------------------------------------------------

        logger.test_step("Write initial data to all VMs and record checksums")
        # Static-IP VMs sit on a UDN — they must be reached via a pod on the
        # same UDN, not via virtctl (which uses the default network).
        for wl in static_workloads:
            md5sums_original[wl.vm_name] = run_dd_io_via_udn_pod(
                cluster_name=primary_cluster_name,
                namespace=wl.workload_namespace,
                vm_ip=ip_primary[wl.vm_name],
                file_path=vm_filepaths[0],
                username=wl.vm_username,
                verify=True,
            )
            logger.info(
                f"Original checksum on {wl.vm_name} ({ip_primary[wl.vm_name]}): "
                f"{md5sums_original[wl.vm_name]}"
            )
        # Non-static DHCP VM: IO skipped — its purpose is only to verify no
        # IP translation is applied, not to validate data integrity.

        logger.test_step("Wait for replication sync before shutting down primary")
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload_1.workload_namespace,
            discovered_apps=True,
            resource_name=drpc_1_name,
        )
        sleep(2 * scheduling_interval * 60)

        # ------------------------------------------------------------------
        # Failover
        # ------------------------------------------------------------------

        logger.test_step(
            f"Shut down all nodes on primary cluster '{primary_cluster_name}'"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        active_primary_index = config.cur_index
        active_primary_node_objs = get_node_objs()
        nodes_multicluster[active_primary_index].stop_nodes(active_primary_node_objs)
        sleep(300)

        logger.test_step(
            f"Failover DRPCs to secondary cluster '{secondary_cluster_name}'"
        )
        for drpc_name, main_wl, _ in drpc_workload_groups:
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=main_wl.workload_namespace,
                discovered_apps=True,
                workload_placement_name=drpc_name,
                old_primary=primary_cluster_name,
                skip_odf_cli_validation=True,
            )
            logger.info(
                f"Waiting {scheduling_interval}m (1 schedule interval) before initiating failover for next drpc"
            )
            sleep(scheduling_interval * 60)

        logger.test_step("Wait for all workloads to be running on secondary")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for drpc_name, main_wl, wls in drpc_workload_groups:
            dr_helpers.wait_for_all_resources_creation(
                sum(wl.workload_pvc_count for wl in wls),
                sum(wl.workload_pod_count for wl in wls),
                main_wl.workload_namespace,
                discovered_apps=True,
                vrg_name=drpc_name,
            )
        for wl in all_workloads:
            dr_helpers.wait_for_cnv_workload(
                vm_name=wl.vm_name,
                namespace=wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        # ------------------------------------------------------------------
        # Post-failover IP assertions
        # ------------------------------------------------------------------

        logger.test_step(
            "Verify all static-IP VMs received translated IPs on secondary "
            f"(must be in {SECONDARY_SUBNET}, host octet preserved)"
        )
        secondary_prefix = SECONDARY_SUBNET.rsplit(".", 1)[0]  # "192.168.2"
        ip_secondary = {}
        for wl in static_workloads:
            vm_ip = verify_vm_ip_and_ipamclaim(
                secondary_cluster_name,
                wl.workload_namespace,
                wl.vm_name,
                VM_NETWORK_NAD_NAME,
            )
            assert vm_ip != ip_primary[wl.vm_name], (
                f"{wl.vm_name}: IP was not translated after failover "
                f"(still {vm_ip})"
            )
            assert vm_ip.startswith(secondary_prefix), (
                f"{wl.vm_name}: translated IP {vm_ip} not in "
                f"secondary subnet {SECONDARY_SUBNET}"
            )
            primary_host = ip_primary[wl.vm_name].split(".")[-1]
            secondary_host = vm_ip.split(".")[-1]
            assert primary_host == secondary_host, (
                f"{wl.vm_name}: host octet not preserved by regex rule — "
                f"primary {ip_primary[wl.vm_name]}, secondary {vm_ip}"
            )
            ip_secondary[wl.vm_name] = vm_ip
            logger.info(f"{wl.vm_name} translated: {ip_primary[wl.vm_name]} → {vm_ip}")

        logger.test_step(
            f"Verify non-static VM ({workload_non_static.vm_name}) IP is unchanged after failover "
            "(no IPAMClaim → no translation)"
        )
        non_static_ip_secondary = get_vm_ip_from_vmi(
            secondary_cluster_name,
            workload_non_static.workload_namespace,
            workload_non_static.vm_name,
        )
        # Non-static VMs get a new DHCP/OVN-assigned IP on the secondary; we only
        # assert there is no IPAMClaim-based translation entry for this VM.
        ipamclaim_non_static = get_ipamclaim_ip(
            secondary_cluster_name,
            workload_non_static.workload_namespace,
            workload_non_static.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        assert not ipamclaim_non_static, (
            f"Non-static VM {workload_non_static.vm_name} should have no translated "
            f"IPAMClaim on secondary, but found IP: {ipamclaim_non_static}"
        )
        # Req #12: the bait explicit rule must NOT have been applied.
        # If translation were IP-address-gated instead of IPAMClaim-gated,
        # the VM would have received dhcp_vm_bait_dest.
        assert non_static_ip_secondary != dhcp_vm_bait_dest, (
            f"DHCP VM {workload_non_static.vm_name} incorrectly received the explicit "
            f"bait destination {dhcp_vm_bait_dest} after failover — "
            f"translation must be IPAMClaim-gated, not IP-address-gated"
        )
        logger.info(
            f"Non-static VM {workload_non_static.vm_name}: no IPAMClaim on secondary, "
            f"IP {non_static_ip_secondary} ≠ bait dest {dhcp_vm_bait_dest} — "
            f"explicit rule correctly ignored (IPAMClaim-gated translation confirmed)"
        )

        logger.test_step(
            "Verify ACM fleet virtualization UI shows all VMs running on secondary "
            "after failover"
        )
        config.switch_acm_ctx()
        acm_obj = AcmAddClusters()
        assert navigate_using_fleet_virtualization(
            acm_obj
        ), "Could not navigate to ACM fleet virtualization page"
        for wl in all_workloads:
            navigate_using_fleet_virtualization(acm_obj)
            assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                acm_obj,
                vms=[wl],
                protection_name=wl.discovered_apps_placement_name,
                namespace=wl.workload_namespace,
                managed_cluster_name=secondary_cluster_name,
                assign_policy=False,
            ), f"VM {wl.vm_name} not shown as running on secondary in ACM UI"
            logger.info(f"ACM UI confirms {wl.vm_name} running on secondary")

        logger.test_step("Validate data integrity on secondary after failover")
        for wl in static_workloads:
            validate_data_integrity_vm_via_udn_pod(
                cluster_name=secondary_cluster_name,
                namespace=wl.workload_namespace,
                vm_ip=ip_secondary[wl.vm_name],
                file_path=vm_filepaths[0],
                username=wl.vm_username,
                md5sum_original=md5sums_original[wl.vm_name],
            )

        logger.test_step("Write new data to all VMs post-failover")
        for wl in static_workloads:
            md5sums_failover[wl.vm_name] = run_dd_io_via_udn_pod(
                cluster_name=secondary_cluster_name,
                namespace=wl.workload_namespace,
                vm_ip=ip_secondary[wl.vm_name],
                file_path=vm_filepaths[1],
                username=wl.vm_username,
                verify=True,
            )

        # ------------------------------------------------------------------
        # Recovery + Relocate
        # ------------------------------------------------------------------

        logger.test_step(f"Recover primary cluster '{primary_cluster_name}'")
        config.switch_to_cluster_by_name(primary_cluster_name)
        nodes_multicluster[active_primary_index].start_nodes(active_primary_node_objs)
        wait_for_nodes_status([n.name for n in active_primary_node_objs])
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=30, delay=30)

        logger.test_step(
            f"Relocate DRPCs back to primary cluster '{primary_cluster_name}'"
        )
        for drpc_name, main_wl, _ in drpc_workload_groups:
            dr_helpers.relocate(
                preferred_cluster=primary_cluster_name,
                namespace=main_wl.workload_namespace,
                workload_placement_name=drpc_name,
                discovered_apps=True,
                old_primary=secondary_cluster_name,
                workload_instance=main_wl,
                vm_auto_cleanup=True,
                skip_odf_cli_validation=True,
            )
            logger.info(
                f"Waiting {scheduling_interval}m (1 schedule interval) before initiating relocate of next drpc"
            )
            sleep(scheduling_interval * 60)

        logger.test_step(
            "Wait for all workloads to be running on primary after relocate"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        for drpc_name, main_wl, wls in drpc_workload_groups:
            dr_helpers.wait_for_all_resources_creation(
                total_pvc_count,
                total_pod_count,
                main_wl.workload_namespace,
                discovered_apps=True,
                vrg_name=drpc_name,
                skip_replication_resources=True,
            )
        for wl in all_workloads:
            dr_helpers.wait_for_cnv_workload(
                vm_name=wl.vm_name,
                namespace=wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        # ------------------------------------------------------------------
        # Post-relocate IP assertions
        # ------------------------------------------------------------------

        logger.test_step(
            "Verify all static-IP VMs are restored to their original IPs "
            "after relocate"
        )
        for wl in static_workloads:
            vm_ip_restored = get_vm_ip_from_vmi(
                primary_cluster_name,
                wl.workload_namespace,
                wl.vm_name,
                VM_NETWORK_NAD_NAME,
            )
            assert vm_ip_restored == ip_primary[wl.vm_name], (
                f"{wl.vm_name}: IP not restored after relocate — expected "
                f"{ip_primary[wl.vm_name]}, got {vm_ip_restored}"
            )
            logger.info(
                f"{wl.vm_name} IP restored: "
                f"{ip_secondary[wl.vm_name]} → {vm_ip_restored}"
            )

        logger.test_step(
            f"Verify non-static VM ({workload_non_static.vm_name}) has no stale translated IPAMClaim "
            "on primary after relocate"
        )
        ipamclaim_non_static_restored = get_ipamclaim_ip(
            primary_cluster_name,
            workload_non_static.workload_namespace,
            workload_non_static.vm_name,
            VM_NETWORK_NAD_NAME,
        )
        assert not ipamclaim_non_static_restored, (
            f"Non-static VM {workload_non_static.vm_name} should have no IPAMClaim "
            f"on primary after relocate, but found: {ipamclaim_non_static_restored}"
        )
        logger.info(
            f"Non-static VM {workload_non_static.vm_name} has no stale IPAMClaim "
            "on primary after relocate — correct"
        )

        logger.test_step(
            "Verify ACM fleet virtualization UI shows all VMs running on primary "
            "after relocate"
        )
        config.switch_acm_ctx()
        acm_obj = AcmAddClusters()
        assert navigate_using_fleet_virtualization(
            acm_obj
        ), "Could not navigate to ACM fleet virtualization page"
        for wl in all_workloads:
            navigate_using_fleet_virtualization(acm_obj)
            assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                acm_obj,
                vms=[wl],
                protection_name=wl.discovered_apps_placement_name,
                namespace=wl.workload_namespace,
                managed_cluster_name=primary_cluster_name,
                assign_policy=False,
            ), f"VM {wl.vm_name} not shown as running on primary in ACM UI"
            logger.info(f"ACM UI confirms {wl.vm_name} running on primary")

        logger.test_step(
            "Validate data integrity on primary after relocate (file1 and file2)"
        )
        for wl in static_workloads:
            validate_data_integrity_vm_via_udn_pod(
                cluster_name=primary_cluster_name,
                namespace=wl.workload_namespace,
                vm_ip=ip_primary[wl.vm_name],
                file_path=vm_filepaths[0],
                username=wl.vm_username,
                md5sum_original=md5sums_original[wl.vm_name],
            )
            validate_data_integrity_vm_via_udn_pod(
                cluster_name=primary_cluster_name,
                namespace=wl.workload_namespace,
                vm_ip=ip_primary[wl.vm_name],
                file_path=vm_filepaths[1],
                username=wl.vm_username,
                md5sum_original=md5sums_failover[wl.vm_name],
            )
