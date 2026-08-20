import logging
import tempfile

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

import pytest

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
    tier2,
)
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.helpers.dr_helpers import (
    wait_for_all_resources_deletion,
    wait_for_managed_cluster_unreachable,
    wait_for_resource_existence,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.acm.acm import AcmAddClusters, login_to_acm
from ocs_ci.ocs.ui.base_ui import close_browser
from ocs_ci.helpers.dr_helpers_ui import (
    check_or_assign_drpolicy_for_discovered_vms_via_ui,
    navigate_using_fleet_virtualization,
    remove_drprotection_for_discovered_vm_via_ui,
)
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.templating import load_yaml, dump_data_to_temp_yaml
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check, exec_cmd

logger = logging.getLogger(__name__)


def verify_drpc_protected_vms(resource_name, expected_vms, unexpected_vms=None):
    """
    Verify DRPC PROTECTED_VMS list via CLI.

    Args:
        resource_name (str): DRPC resource name.
        expected_vms (list): VM names that must be in PROTECTED_VMS.
        unexpected_vms (list): VM names that must NOT be in
            PROTECTED_VMS (default None).

    Raises:
        AssertionError: if any expected VM is missing or unexpected
            VM is present.
    """
    config.switch_acm_ctx()
    drpc_obj = DRPC(
        namespace=constants.DR_OPS_NAMESPACE,
        resource_name=resource_name,
    )
    drpc_data = drpc_obj.get()
    protected_vms = (
        drpc_data.get("spec", {})
        .get("kubeObjectProtection", {})
        .get("recipeParameters", {})
        .get("PROTECTED_VMS", [])
    )
    for vm_name in expected_vms:
        assert (
            vm_name in protected_vms
        ), f"VM '{vm_name}' not found in PROTECTED_VMS: {protected_vms}"
    for vm_name in unexpected_vms or []:
        assert (
            vm_name not in protected_vms
        ), f"VM '{vm_name}' still in PROTECTED_VMS: {protected_vms}"
    logger.info(f"DRPC PROTECTED_VMS verified: {protected_vms}")


@rdr
@tier2
@turquoise_squad
@skipif_ocs_version("<4.19")
class TestACMKubevirtDRIntergration:
    """
    Test ACM Kubevirt DR Integration by DR Protecting Discovered VMs via VMs page of ACM UI as Standalone
    and Shared Protection type and perform DR operation on them- RHSTOR-6413

    """

    @pytest.mark.parametrize(
        argnames=["protection_type"],
        argvalues=[
            pytest.param(
                False, id="standalone", marks=pytest.mark.polarion_id("OCS-8047")
            ),
            pytest.param(True, id="shared", marks=pytest.mark.polarion_id("OCS-8048")),
        ],
    )
    def test_acm_kubevirt_using_different_protection_types(
        self,
        setup_acm_ui,
        protection_type,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        DR operation on discovered VMs using Standalone and Shared
        Protection type. In shared protection, both VMs are tied to a
        single DRPC in the same namespace where the same DRPolicy is
        applied via UI to both the apps.

        Test steps:

        1. Deploy a CNV discovered workload in a test NS via CLI
        2. (Shared only) Deploy a 2nd CNV workload in the same
           namespace via CLI
        3. DR protect workloads via ACM Fleet Virtualization UI
           (Standalone for 1st VM, Shared for 2nd VM)
        4. Write data to VMs, record md5sums
        5. Shut down all nodes of the primary managed cluster
        6. Failover workloads to the secondary cluster via CLI
        7. Verify data integrity and VM status on the secondary
           cluster
        8. Recover the down managed cluster and perform cleanup
        9. Verify VM status via ACM UI after failover
        10. Relocate workloads back to the primary cluster
        11. Verify VM status via ACM UI after relocate
        12. Remove DR protection via ACM UI and verify DRPC
            deletion

        """
        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        logger.test_step("Deploy 1st CNV workload")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=False
        )

        if protection_type:
            # Deploy second workload for Shared protection (uses same namespace as first)
            logger.test_step("Deploy 2nd CNV workload in the existing namespace")
            cnv_workloads = discovered_apps_dr_workload_cnv(
                pvc_vm=1, dr_protect=False, shared_drpc_protection=True
            )

        assert cnv_workloads, "No discovered VM found"
        config.switch_acm_ctx()
        protection_name = cnv_workloads[0].workload_namespace
        logger.info(f"Protection name is {protection_name}")
        resource_name = cnv_workloads[0].discovered_apps_placement_name + "-drpc"

        logger.info(f"CNV workloads instance is {cnv_workloads}")

        config.switch_acm_ctx()
        login_to_acm()
        acm_obj = AcmAddClusters()
        primary_cluster_name = cnv_workloads[0].preferred_primary_cluster
        logger.info(
            f"Primary managed cluster name is {cnv_workloads[0].preferred_primary_cluster}"
        )
        logger.test_step("DR protect workloads via ACM UI")
        for i, vm in enumerate(cnv_workloads):
            standalone_flag = (not protection_type) or (i == 0)
            if protection_type and i == 1:
                # Wait for the standalone DRPC (created in iteration 0) to exist
                # before opening the enrollment wizard in Shared mode. The ACM UI
                # queries existing DRPCs when rendering the wizard; if the DRPC is
                # not yet present the #shared-vm-protection radio button won't appear.
                logger.info(
                    f"Waiting for DRPC {protection_name}-drpc to exist"
                    " before Shared enrollment"
                )
                config.switch_acm_ctx()
                wait_for_resource_existence(
                    kind=constants.DRPC,
                    namespace=constants.DR_OPS_NAMESPACE,
                    resource_name=f"{protection_name}-drpc",
                    timeout=120,
                    should_exist=True,
                )
            logger.assertion("navigate_using_fleet_virtualization: expected=True")
            assert navigate_using_fleet_virtualization(acm_obj)
            logger.assertion(
                f"check_or_assign_drpolicy_for_discovered_vms_via_ui:"
                f" vm={vm.vm_name}, standalone={standalone_flag}, expected=True"
            )
            assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                acm_obj,
                vms=[vm],
                managed_cluster_name=primary_cluster_name,
                standalone=standalone_flag,
                protection_name=protection_name,
                namespace=cnv_workloads[0].workload_namespace,
            )

        logger.info(
            f'Placement name is "{cnv_workloads[0].discovered_apps_placement_name}"'
        )

        if protection_type:
            logger.test_step("Verify DRPC PROTECTED_VMS contains both VMs")
            verify_drpc_protected_vms(
                resource_name,
                expected_vms=[wl.vm_name for wl in cnv_workloads],
            )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )

        logger.info(f"Primary cluster name before failover is {primary_cluster_name}")

        config.switch_to_cluster_by_name(primary_cluster_name)

        workload_pvc_count = (
            cnv_workloads[0].workload_pvc_count * 2
            if protection_type
            else cnv_workloads[0].workload_pvc_count
        )
        workload_pod_count = (
            cnv_workloads[0].workload_pod_count * 2
            if protection_type
            else cnv_workloads[0].workload_pod_count
        )
        dr_helpers.wait_for_all_resources_creation(
            workload_pvc_count,
            workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )

        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()

        # Creating a file (file1) on VM and calculating its MD5sum
        for cnv_wl in cnv_workloads:
            md5sum_original.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[0],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        for cnv_wl, md5sum in zip(cnv_workloads, md5sum_original):
            logger.info(
                f"Original checksum of file {vm_filepaths[0]} on VM {cnv_wl.workload_name}: {md5sum}"
            )

        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Shutdown primary managed cluster nodes
        active_primary_index = config.cur_index
        active_primary_cluster_node_objs = get_node_objs()
        logger.test_step("Shut down all nodes of the primary managed cluster")
        nodes_multicluster[active_primary_index].stop_nodes(
            active_primary_cluster_node_objs
        )
        logger.info(
            f"All nodes of the primary managed cluster {primary_cluster_name} are powered off, "
            "waiting for cluster to be unreachable"
        )
        wait_for_managed_cluster_unreachable(primary_cluster_name)

        logger.test_step("Failover workloads to secondary cluster")
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=resource_name,
            old_primary=primary_cluster_name,
            skip_odf_cli_validation=True,
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload_pvc_count,
            workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_workloads[0].workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        # Validating data integrity (file1) after failing-over VMs to secondary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )

        # Creating a file (file2) post failover
        for cnv_wl in cnv_workloads:
            md5sum_failover.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[1],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        for cnv_wl, md5sum in zip(cnv_workloads, md5sum_failover):
            logger.info(
                f"Checksum of files written after Failover: {vm_filepaths[1]} on VM {cnv_wl.workload_name}: {md5sum}"
            )

        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.test_step("Recover down managed cluster")
        nodes_multicluster[active_primary_index].start_nodes(
            active_primary_cluster_node_objs
        )
        wait_for_nodes_status(
            [node.name for node in active_primary_cluster_node_objs], timeout=900
        )
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        logger.assertion("ceph_health_check: expected=True")
        assert ceph_health_check(tries=10, delay=30)

        logger.test_step("Cleanup after successful failover")
        for cnv_wl in cnv_workloads:
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=resource_name,
                old_primary=primary_cluster_name,
                workload_namespace=cnv_workloads[0].workload_namespace,
                workload_dir=cnv_wl.workload_dir,
                vrg_name=resource_name,
                skip_resource_deletion_verification=True,
            )

        wait_for_all_resources_deletion(
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        config.switch_acm_ctx()
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE, resource_name=resource_name
        )
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        logger.test_step("Verify VM status via ACM UI after failover")
        logger.info("Refreshing browser session before UI verification")
        try:
            close_browser()
        except Exception:
            logger.warning("Browser already closed or crashed, proceeding")
        config.switch_acm_ctx()
        login_to_acm()
        acm_obj = AcmAddClusters()
        for cnv_wl in cnv_workloads:
            logger.assertion("navigate_using_fleet_virtualization: expected=True")
            assert navigate_using_fleet_virtualization(acm_obj)
            logger.assertion(
                f"check_or_assign_drpolicy_for_discovered_vms_via_ui:"
                f" vm={cnv_wl.vm_name}, cluster={secondary_cluster_name}, expected=True"
            )
            assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                acm_obj,
                vms=[cnv_wl],
                protection_name=protection_name,
                namespace=cnv_workloads[0].workload_namespace,
                managed_cluster_name=secondary_cluster_name,
                assign_policy=False,
            )
        config.switch_to_cluster_by_name(secondary_cluster_name)

        # Doing Relocate in below code
        config.switch_to_cluster_by_name(primary_cluster_name)

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        logger.test_step("Relocate workloads back to primary cluster")
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            workload_placement_name=resource_name,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=cnv_workloads[0],
            workload_instances_shared=cnv_workloads,
        )
        # Cleanup is handled as part of the Relocate function and checks are done below
        # Switch to old_primary (c2) where do_discovered_apps_cleanup ran with --wait=false
        config.switch_to_cluster_by_name(secondary_cluster_name)
        wait_for_all_resources_deletion(
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        config.switch_acm_ctx()
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE, resource_name=resource_name
        )
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        # Verify resources creation on primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload_pvc_count,
            workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        logger.test_step("Verify VM status via ACM UI after relocate")
        logger.info("Refreshing browser session before UI verification")
        try:
            close_browser()
        except Exception:
            logger.warning("Browser already closed or crashed, proceeding")
        config.switch_acm_ctx()
        login_to_acm()
        acm_obj = AcmAddClusters()
        for cnv_wl in cnv_workloads:
            logger.assertion("navigate_using_fleet_virtualization: expected=True")
            assert navigate_using_fleet_virtualization(acm_obj)
            logger.assertion(
                f"check_or_assign_drpolicy_for_discovered_vms_via_ui:"
                f" vm={cnv_wl.vm_name}, cluster={primary_cluster_name}, expected=True"
            )
            assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                acm_obj,
                vms=[cnv_wl],
                protection_name=protection_name,
                namespace=cnv_workloads[0].workload_namespace,
                managed_cluster_name=primary_cluster_name,
                assign_policy=False,
            )
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Validating data integrity (file1) after relocating VMs back to primary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )

        # Validating data integrity (file2) after relocating VMs back to primary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[1], md5sum_failover, "Relocate"
        )

        # Creating a file (file3) post relocate
        for cnv_wl in cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )

        # ------------------------------------------------------------------ #
        # Remove DR protection scenario                                        #
        # Standalone run: remove protection from the single VM and verify     #
        #   its DRPC is deleted.                                               #
        # Shared run:     remove only the 2nd (Shared) VM from the DRPC and  #
        #   verify the 1st VM is still protected with its DRPC intact.        #
        # ------------------------------------------------------------------ #
        config.switch_acm_ctx()
        logger.test_step("Remove DR protection via ACM UI")
        logger.assertion("navigate_using_fleet_virtualization: expected=True")
        assert navigate_using_fleet_virtualization(acm_obj)

        if protection_type:
            # Shared run – cnv_workloads[1] is the Shared VM; remove it
            logger.info(
                "Removing DR protection from the Shared VM "
                f"'{cnv_workloads[1].vm_name}'"
            )
            logger.assertion(
                f"remove_drprotection_for_discovered_vm_via_ui:"
                f" vm={cnv_workloads[1].vm_name}, expected=True"
            )
            assert remove_drprotection_for_discovered_vm_via_ui(
                acm_obj,
                vm=cnv_workloads[1],
                managed_cluster_name=primary_cluster_name,
                namespace=cnv_workloads[0].workload_namespace,
            )
            # Validate: DRPC still exists (1st VM is still enrolled)
            logger.info("Validating DRPC still exists for the remaining Standalone VM")
            config.switch_acm_ctx()
            wait_for_resource_existence(
                kind=constants.DRPC,
                namespace=constants.DR_OPS_NAMESPACE,
                resource_name=resource_name,
                timeout=120,
                should_exist=True,
            )
            # Validate: 1st VM still protected, 2nd VM removed from DRPC
            logger.info("Validating DRPC PROTECTED_VMS contains only the 1st VM")
            verify_drpc_protected_vms(
                resource_name,
                expected_vms=[cnv_workloads[0].vm_name],
                unexpected_vms=[cnv_workloads[1].vm_name],
            )
            logger.info("Shared VM removed from DRPC; Standalone VM remains protected")
        else:
            # Standalone run – remove the single VM's protection entirely
            logger.info(
                "Removing DR protection from the Standalone VM "
                f"'{cnv_workloads[0].vm_name}'"
            )
            logger.assertion(
                f"remove_drprotection_for_discovered_vm_via_ui:"
                f" vm={cnv_workloads[0].vm_name}, expected=True"
            )
            assert remove_drprotection_for_discovered_vm_via_ui(
                acm_obj,
                vm=cnv_workloads[0],
                managed_cluster_name=primary_cluster_name,
                namespace=cnv_workloads[0].workload_namespace,
            )
            # Validate: DRPC is deleted
            logger.info(
                "Validating DRPC is deleted after Standalone protection removal"
            )
            config.switch_acm_ctx()
            wait_for_resource_existence(
                kind=constants.DRPC,
                namespace=constants.DR_OPS_NAMESPACE,
                resource_name=resource_name,
                timeout=120,
                should_exist=False,
            )
            logger.info("Standalone VM DR protection removed and DRPC deleted")

        try:
            close_browser()
        except Exception:
            logger.warning("Browser already closed or crashed, proceeding")

    @pytest.mark.polarion_id("OCS-8045")
    def test_acm_kubevirt_mixed_protection_types(
        self,
        setup_acm_ui,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        DR operation on multiple discovered VMs in the same namespace
        using mixed protection types (some Standalone, some Shared).
        This test validates that VMs with different protection types
        can coexist in the same namespace and perform DR operations
        successfully.

        Bug: Granular VM DR is broken
        ref: https://redhat.atlassian.net/browse/DFBUGS-8039

        Test steps:

        1. Deploy 4 CNV discovered workloads in a single namespace
           via CLI
        2. Create a second DRPolicy with a unique runtime name
           and a different scheduling interval
        3. DR protect VM 1 as Standalone with default DRPolicy via
           ACM UI, validate VGR-VGRC binding and PVC refs
        4. DR protect VM 3 as Shared (tied to VM 1's DRPC),
           validate PVC refs updated
        5. DR protect VM 2 as Standalone with the second DRPolicy
           via ACM UI, validate 2 VGRs and PVC refs
        6. DR protect VM 4 as Shared (tied to VM 2's DRPC),
           validate PVC refs updated
        7. Write data to all VMs, record md5sums
        8. Shut down all nodes of the primary managed cluster
        9. Failover all workloads to the secondary cluster
        10. Verify VGR-VGRC binding on secondary cluster
        11. Verify data integrity on all VMs after failover
        12. Recover the down managed cluster and perform cleanup
        13. Verify all VM statuses via ACM UI after failover
        14. Relocate all workloads back to the primary cluster
        15. Verify VGR-VGRC binding on primary cluster
        16. Verify all VM statuses via ACM UI after relocate
        17. Validate data integrity after relocate
        18. Remove VM 3 (Shared) from DRPC1, verify DRPC1 persists
        19. Remove VM 1 (Standalone, last VM) from DRPC1, verify
            DRPC1 is deleted
        20. Remove VM 4 (Shared) from DRPC2, verify DRPC2 persists
        21. Remove VM 2 (Standalone, last VM) from DRPC2, verify
            DRPC2 is deleted
        22. Delete the second DRPolicy

        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]
        all_cnv_workloads = []
        drpc_resources = []

        logger.test_step("Deploy 1st CNV workload (Standalone protection)")
        cnv_workload_1 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=False
        )
        all_cnv_workloads.append(cnv_workload_1[-1])

        # VM 2 (index 1): will become 2nd Standalone; shared_drpc_protection=True means
        # "deploy into the same namespace as the existing workload", not the DR protection type
        logger.test_step("Deploy 2nd CNV workload (will use Standalone DR protection)")
        cnv_workload_2 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        all_cnv_workloads.append(cnv_workload_2[-1])

        # VM 3 (index 2): will be enrolled as Shared with VM 1
        logger.test_step("Deploy 3rd CNV workload (will be Shared with VM 1)")
        cnv_workload_3 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        all_cnv_workloads.append(cnv_workload_3[-1])

        # VM 4 (index 3): will be enrolled as Shared with VM 2
        logger.test_step("Deploy 4th CNV workload (will be Shared with VM 2)")
        cnv_workload_4 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        all_cnv_workloads.append(cnv_workload_4[-1])

        assert all_cnv_workloads, "No discovered VMs found"
        assert (
            len(all_cnv_workloads) == 4
        ), f"Expected 4 VMs, found {len(all_cnv_workloads)}"

        config.switch_acm_ctx()
        workload_namespace = all_cnv_workloads[0].workload_namespace
        logger.info(f"All VMs deployed in namespace: {workload_namespace}")
        primary_cluster_name = all_cnv_workloads[0].preferred_primary_cluster
        logger.info(f"Primary managed cluster name is {primary_cluster_name}")

        # Create a second DRPolicy with a different scheduling interval
        # so each DRPC uses a distinct policy.
        logger.test_step("Create second DRPolicy odr-policy-6m")
        existing_policies = dr_helpers.get_all_drpolicy()
        default_dr_policy_name = existing_policies[0]["metadata"]["name"]
        dr_clusters = existing_policies[0]["spec"]["drClusters"]
        run_id = config.RUN["run_id"]
        dr_policy_6m_name = f"odr-policy-6m-{str(run_id)[-4:]}"
        dr_policy_data = load_yaml(constants.DR_POLICY_ACM_HUB)
        dr_policy_data["metadata"]["name"] = dr_policy_6m_name
        dr_policy_data["spec"]["drClusters"] = dr_clusters
        dr_policy_data["spec"]["schedulingInterval"] = "6m"
        dr_policy_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="dr_policy_6m_", delete=False
        )
        dump_data_to_temp_yaml(dr_policy_data, dr_policy_yaml.name)
        exec_cmd(f"oc create -f {dr_policy_yaml.name}")
        drpolicy_ocp = ocp.OCP(
            kind=constants.DRPOLICY,
            resource_name=dr_policy_6m_name,
        )
        for sample in TimeoutSampler(
            timeout=120,
            sleep=5,
            func=lambda: drpolicy_ocp.get()
            .get("status", {})
            .get("conditions", [{}])[0]
            .get("reason", ""),
        ):
            if sample in constants.DRPOLICY_SUCCESS_REASONS:
                break
        logger.info(f"DRPolicy {dr_policy_6m_name} created and validated")

        login_to_acm()
        acm_obj = AcmAddClusters()

        assert navigate_using_fleet_virtualization(acm_obj)

        # DR protect VMs with mixed protection types
        # VM 1: Standalone (creates new DRPC)
        logger.test_step("DR protect VM 1 with Standalone protection")
        protection_name_1 = f"{workload_namespace}-s1"
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[0]],
            managed_cluster_name=primary_cluster_name,
            standalone=True,
            protection_name=protection_name_1,
            namespace=workload_namespace,
            dr_policy_name=default_dr_policy_name,
        )
        # When DR protected via UI, the DRPC is named after the protection name entered
        resource_name_1 = f"{protection_name_1}-drpc"
        drpc_resources.append(resource_name_1)

        def _pvc_name(vm_name):
            return vm_name.replace("vm-workload-", "vm-") + "-pvc"

        # Validate VGR/VGRC after VM 1 Standalone enrollment
        logger.test_step("Validate VGR-VGRC binding and PVC refs after VM 1")
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_vgr_vgrc_binding(workload_namespace, [resource_name_1])
        dr_helpers.validate_vgr_pvc_refs(
            workload_namespace,
            resource_name_1,
            [_pvc_name(all_cnv_workloads[0].vm_name)],
        )
        config.switch_acm_ctx()

        # Wait for VM 1's DRPC to exist in Kubernetes before enrolling VM 3 as
        # Shared. The ACM UI queries existing DRPCs when opening the enrollment
        # wizard; if the DRPC has not yet been created the Shared option
        # (#shared-vm-protection) will not appear.
        logger.info(
            f"Waiting for DRPC {resource_name_1} to exist before enrolling VM 3 as Shared"
        )
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name_1,
            timeout=120,
            should_exist=True,
        )

        # VM 3: Shared with VM 1 (uses the single existing DRPC from VM 1)
        # NOTE: the UI helper asserts exactly 1 radio button when selecting a Shared DRPC.
        # VM 3 is enrolled immediately after VM 1 so only 1 DRPC exists at this point.
        logger.test_step("DR protect VM 3 with Shared protection (tied to VM 1)")
        assert navigate_using_fleet_virtualization(acm_obj)
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[2]],
            managed_cluster_name=primary_cluster_name,
            standalone=False,
            protection_name=protection_name_1,
            namespace=workload_namespace,
        )

        # Validate VGR/VGRC after VM 3 Shared enrollment
        logger.test_step("Validate VGR-VGRC binding and PVC refs after VM 3")
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_vgr_vgrc_binding(workload_namespace, [resource_name_1])
        dr_helpers.validate_vgr_pvc_refs(
            workload_namespace,
            resource_name_1,
            [
                _pvc_name(all_cnv_workloads[0].vm_name),
                _pvc_name(all_cnv_workloads[2].vm_name),
            ],
        )
        config.switch_acm_ctx()

        # VM 2: Standalone with odr-policy-6m (creates a second independent DRPC)
        logger.test_step(
            "DR protect VM 2 with Standalone protection " f"using {dr_policy_6m_name}"
        )
        protection_name_2 = f"{workload_namespace}-s2"
        resource_name_2 = f"{protection_name_2}-drpc"
        for attempt in range(3):
            try:
                assert navigate_using_fleet_virtualization(acm_obj)
                assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                    acm_obj,
                    vms=[all_cnv_workloads[1]],
                    managed_cluster_name=primary_cluster_name,
                    standalone=True,
                    protection_name=protection_name_2,
                    namespace=workload_namespace,
                    dr_policy_name=dr_policy_6m_name,
                )
                break
            except (AssertionError, Exception) as e:
                logger.warning(
                    f"VM 2 Standalone enrollment attempt {attempt + 1}"
                    f" failed: {e}. Retrying after 30s"
                )
                if attempt == 2:
                    raise
                sleep(30)
        drpc_resources.append(resource_name_2)

        # Validate VGR/VGRC after VM 2 Standalone enrollment
        logger.test_step("Validate VGR-VGRC binding and PVC refs after VM 2")
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_vgr_vgrc_binding(
            workload_namespace,
            [resource_name_1, resource_name_2],
        )
        dr_helpers.validate_vgr_pvc_refs(
            workload_namespace,
            resource_name_2,
            [_pvc_name(all_cnv_workloads[1].vm_name)],
        )
        config.switch_acm_ctx()

        # Override discovered_apps_placement_name on the Standalone workload objects
        # so delete_workload can find the custom UI-created DRPCs at teardown.
        # delete_workload tries {name} and {name}-drpc — by setting the base name
        # to protection_name_X (without -drpc), it resolves to resource_name_X.
        # VM 3 and VM 4 use shared_drpc_protection=True so delete_workload skips
        # DRPC deletion for them entirely.
        all_cnv_workloads[0].discovered_apps_placement_name = protection_name_1
        all_cnv_workloads[1].discovered_apps_placement_name = protection_name_2

        # Wait for VM 2's DRPC to be present before enrolling VM 4 as Shared.
        logger.info(
            f"Waiting for DRPC {resource_name_2} to exist before enrolling VM 4 as Shared"
        )
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name_2,
            timeout=120,
            should_exist=True,
        )

        # VM 4: Shared with VM 2 (uses existing DRPC from VM 2)
        logger.test_step("DR protect VM 4 with Shared protection (tied to VM 2)")
        assert navigate_using_fleet_virtualization(acm_obj)
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[3]],
            managed_cluster_name=primary_cluster_name,
            standalone=False,
            protection_name=protection_name_2,
            namespace=workload_namespace,
        )

        # Validate VGR/VGRC after VM 4 Shared enrollment
        logger.test_step("Validate VGR-VGRC binding and PVC refs after VM 4")
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_vgr_vgrc_binding(
            workload_namespace,
            [resource_name_1, resource_name_2],
        )
        dr_helpers.validate_vgr_pvc_refs(
            workload_namespace,
            resource_name_2,
            [
                _pvc_name(all_cnv_workloads[1].vm_name),
                _pvc_name(all_cnv_workloads[3].vm_name),
            ],
        )
        config.switch_acm_ctx()

        logger.info(f"DRPC resources created: {drpc_resources}")

        # Use the larger scheduling interval (6m from DRPC2) to ensure
        # both DRPCs have completed at least one sync cycle.
        scheduling_interval = max(
            dr_helpers.get_scheduling_interval(
                workload_namespace,
                discovered_apps=True,
                resource_name=resource_name_1,
            ),
            dr_helpers.get_scheduling_interval(
                workload_namespace,
                discovered_apps=True,
                resource_name=resource_name_2,
            ),
        )

        config.switch_to_cluster_by_name(primary_cluster_name)

        total_pvc_count = sum(wl.workload_pvc_count for wl in all_cnv_workloads)
        total_pod_count = sum(wl.workload_pod_count for wl in all_cnv_workloads)

        # Wait for all PVCs and pods namespace-wide. The per-DRPC loop
        # approach was incorrect: wait_for_all_resources_creation counts
        # resources namespace-wide, so DRPC1's running pods would satisfy
        # DRPC2's pod count check before DRPC2's own pods were ready.
        dr_helpers.wait_for_all_resources_creation(
            total_pvc_count,
            total_pod_count,
            workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name_1,
            skip_replication_resources=True,
        )

        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload_namespace,
            discovered_apps=True,
            resource_name=resource_name_1,
        )

        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()

        # Creating a file (file1) on all VMs and calculating MD5sum
        logger.test_step("Write data to all VMs and calculating MD5sum")
        for cnv_wl in all_cnv_workloads:
            md5sum_original.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[0],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        for cnv_wl, md5sum in zip(all_cnv_workloads, md5sum_original):
            logger.info(
                f"Original checksum of file {vm_filepaths[0]} on VM {cnv_wl.workload_name}: {md5sum}"
            )

        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Shutdown primary managed cluster nodes
        active_primary_index = config.cur_index
        active_primary_cluster_node_objs = get_node_objs()
        logger.test_step("Shut down all nodes of the primary managed cluster")
        nodes_multicluster[active_primary_index].stop_nodes(
            active_primary_cluster_node_objs
        )
        logger.info(
            f"All nodes of the primary managed cluster {primary_cluster_name} are powered off, "
            "waiting for cluster to be unreachable"
        )
        wait_for_managed_cluster_unreachable(primary_cluster_name)

        # Failover all workloads (both DRPCs)
        logger.test_step("Failover all workloads to secondary cluster")
        for resource_name in drpc_resources:
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=workload_namespace,
                discovered_apps=True,
                workload_placement_name=resource_name,
                old_primary=primary_cluster_name,
                skip_odf_cli_validation=True,
            )

        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            total_pvc_count,
            total_pod_count,
            workload_namespace,
            timeout=1800,
            discovered_apps=True,
            vrg_name=resource_name_1,
            skip_replication_resources=True,
        )

        # Verify each VGR has a bound VGRC after failover
        logger.test_step("Verify VGR-VGRC binding on secondary cluster")
        dr_helpers.validate_vgr_vgrc_binding(workload_namespace, drpc_resources)

        # Wait for all VMs to be running on secondary cluster
        logger.test_step("Verify all VMs running on secondary cluster")
        with ThreadPoolExecutor(max_workers=len(all_cnv_workloads)) as executor:
            futures = {
                executor.submit(
                    dr_helpers.wait_for_cnv_workload,
                    vm_name=cnv_wl.vm_name,
                    namespace=workload_namespace,
                    phase=constants.STATUS_RUNNING,
                ): cnv_wl
                for cnv_wl in all_cnv_workloads
            }
            for future in as_completed(futures):
                cnv_wl = futures[future]
                future.result()
                logger.info(f"VM {cnv_wl.vm_name} is Running")

        # Validating data integrity (file1) after failing-over VMs to secondary managed cluster
        logger.test_step("Validate data integrity after failover")
        validate_data_integrity_vm(
            all_cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )

        # Creating a file (file2) post failover on all VMs
        logger.test_step("Write additional data post-failover")
        for cnv_wl in all_cnv_workloads:
            md5sum_failover.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[1],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        for cnv_wl, md5sum in zip(all_cnv_workloads, md5sum_failover):
            logger.info(
                f"Checksum of files written after Failover: {vm_filepaths[1]} on VM {cnv_wl.workload_name}: {md5sum}"
            )

        # Recover the down managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.test_step("Recover down managed cluster")
        nodes_multicluster[active_primary_index].start_nodes(
            active_primary_cluster_node_objs
        )
        wait_for_nodes_status(
            [node.name for node in active_primary_cluster_node_objs], timeout=900
        )
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        logger.assertion("ceph_health_check: expected=True")
        assert ceph_health_check(tries=10, delay=30)

        # Cleanup operations after successful failover
        logger.test_step("Cleanup after successful failover")
        # VM 1 (index 0) and VM 3 (index 2) share DRPC1; VM 2 (index 1) and VM 4 (index 3) share DRPC2
        drpc_per_vm = [
            resource_name_1,  # VM 1
            resource_name_2,  # VM 2
            resource_name_1,  # VM 3
            resource_name_2,  # VM 4
        ]
        for cnv_wl, drpc_name in zip(all_cnv_workloads, drpc_per_vm):
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=drpc_name,
                old_primary=primary_cluster_name,
                workload_namespace=workload_namespace,
                workload_dir=cnv_wl.workload_dir,
                vrg_name=drpc_name,
                skip_resource_deletion_verification=True,
            )

        for resource_name in drpc_resources:
            wait_for_all_resources_deletion(
                namespace=workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
            )

        config.switch_acm_ctx()
        for resource_name in drpc_resources:
            drpc_obj = DRPC(
                namespace=constants.DR_OPS_NAMESPACE, resource_name=resource_name
            )
            drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        logger.test_step("Verify all VM statuses via ACM UI after failover")
        logger.assertion("navigate_using_fleet_virtualization: expected=True")
        assert navigate_using_fleet_virtualization(acm_obj)
        logger.assertion(
            f"check_or_assign_drpolicy_for_discovered_vms_via_ui:"
            f" all_vms, cluster={secondary_cluster_name}, expected=True"
        )
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=all_cnv_workloads,
            protection_name=protection_name_1,
            namespace=workload_namespace,
            managed_cluster_name=secondary_cluster_name,
            assign_policy=False,
        )

        # Perform Relocate operation
        config.switch_to_cluster_by_name(primary_cluster_name)

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        logger.test_step("Relocate all workloads back to primary cluster")
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=workload_namespace,
            workload_placement_name=resource_name_1,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=all_cnv_workloads[0],
            workload_instances_shared=[
                all_cnv_workloads[0],
                all_cnv_workloads[2],
            ],
        )
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=workload_namespace,
            workload_placement_name=resource_name_2,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=all_cnv_workloads[1],
            workload_instances_shared=[
                all_cnv_workloads[1],
                all_cnv_workloads[3],
            ],
        )

        for resource_name in drpc_resources:
            wait_for_all_resources_deletion(
                namespace=workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
            )

        config.switch_acm_ctx()
        for resource_name in drpc_resources:
            drpc_obj = DRPC(
                namespace=constants.DR_OPS_NAMESPACE,
                resource_name=resource_name,
            )
            drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            total_pvc_count,
            total_pod_count,
            workload_namespace,
            timeout=1800,
            discovered_apps=True,
            vrg_name=resource_name_1,
            skip_replication_resources=True,
        )

        # Verify each VGR has a bound VGRC after relocate
        logger.test_step("Verify VGR-VGRC binding on primary cluster")
        dr_helpers.validate_vgr_vgrc_binding(workload_namespace, drpc_resources)

        # Wait for all VMs to be running on primary cluster
        logger.info("Waiting for all VMs to reach Running state " "on primary cluster")
        with ThreadPoolExecutor(max_workers=len(all_cnv_workloads)) as executor:
            futures = {
                executor.submit(
                    dr_helpers.wait_for_cnv_workload,
                    vm_name=cnv_wl.vm_name,
                    namespace=workload_namespace,
                    phase=constants.STATUS_RUNNING,
                ): cnv_wl
                for cnv_wl in all_cnv_workloads
            }
            for future in as_completed(futures):
                cnv_wl = futures[future]
                future.result()
                logger.info(f"VM {cnv_wl.vm_name} is Running")

        config.switch_acm_ctx()
        logger.test_step("Verify all VM statuses via ACM UI after relocate")
        logger.assertion("navigate_using_fleet_virtualization: expected=True")
        assert navigate_using_fleet_virtualization(acm_obj)
        logger.assertion(
            f"check_or_assign_drpolicy_for_discovered_vms_via_ui:"
            f" all_vms, cluster={primary_cluster_name}, expected=True"
        )
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=all_cnv_workloads,
            protection_name=protection_name_1,
            namespace=workload_namespace,
            managed_cluster_name=primary_cluster_name,
            assign_policy=False,
        )
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Validating data integrity (file1) after relocating VMs back to primary managed cluster
        logger.test_step("Validate data integrity after relocate")
        validate_data_integrity_vm(
            all_cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )

        # Validating data integrity (file2) after relocating VMs back to primary managed cluster
        logger.test_step("Validate data integrity (file2) after relocate")
        validate_data_integrity_vm(
            all_cnv_workloads, vm_filepaths[1], md5sum_failover, "Relocate"
        )

        # Creating a file (file3) post relocate on all VMs
        logger.info("Writing final data post-relocate")
        for cnv_wl in all_cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )

        # ------------------------------------------------------------------ #
        # Remove DR protection scenario                                        #
        #                                                                      #
        # Validates that a DRPC is NOT deleted until ALL VMs (standalone +      #
        # shared) are removed from it.                                         #
        # ------------------------------------------------------------------ #
        config.switch_acm_ctx()
        acm_obj = AcmAddClusters()

        # -- DRPC1: remove VM3 (Shared), then VM1 (Standalone) ------------- #
        logger.test_step("Remove DR protection from VM 3 (Shared with DRPC1)")
        assert navigate_using_fleet_virtualization(acm_obj)
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[2],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        logger.info("Validating DRPC1 still exists after removing VM 3")
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name_1,
            timeout=120,
            should_exist=True,
        )
        verify_drpc_protected_vms(
            resource_name_1,
            expected_vms=[all_cnv_workloads[0].vm_name],
            unexpected_vms=[all_cnv_workloads[2].vm_name],
        )

        logger.test_step(
            "Remove DR protection from VM 1 (Standalone, last VM in DRPC1)"
        )
        assert navigate_using_fleet_virtualization(acm_obj)
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[0],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        logger.info("Validating DRPC1 is deleted after removing last VM")
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name_1,
            timeout=300,
            should_exist=False,
        )

        # -- DRPC2: remove VM4 (Shared), then VM2 (Standalone) ------------- #
        logger.test_step("Remove DR protection from VM 4 (Shared with DRPC2)")
        assert navigate_using_fleet_virtualization(acm_obj)
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[3],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        logger.info("Validating DRPC2 still exists after removing VM 4")
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name_2,
            timeout=120,
            should_exist=True,
        )
        verify_drpc_protected_vms(
            resource_name_2,
            expected_vms=[all_cnv_workloads[1].vm_name],
            unexpected_vms=[all_cnv_workloads[3].vm_name],
        )

        logger.test_step(
            "Remove DR protection from VM 2 (Standalone, last VM in DRPC2)"
        )
        assert navigate_using_fleet_virtualization(acm_obj)
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[1],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        logger.info("Validating DRPC2 is deleted after removing last VM")
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name_2,
            timeout=300,
            should_exist=False,
        )

        # Delete the second DRPolicy
        logger.test_step("Delete the second DRPolicy")
        dr_helpers.delete_drpolicy(dr_policy_6m_name)

        logger.info(
            f"Test for mixed protection types (Standalone and Shared) "
            f"in namespace {workload_namespace} passed. "
            f"DRPC groups: {protection_name_1}, {protection_name_2}"
        )
