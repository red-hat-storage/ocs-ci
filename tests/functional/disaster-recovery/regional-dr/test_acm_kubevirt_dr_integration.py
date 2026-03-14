import logging

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
    wait_for_resource_existence,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    check_or_assign_drpolicy_for_discovered_vms_via_ui,
    navigate_using_fleet_virtualization,
    remove_drprotection_for_discovered_vm_via_ui,
)
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


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
                False, id="standalone", marks=pytest.mark.polarion_id("OCS-xxxx")
            ),
            pytest.param(True, id="shared", marks=pytest.mark.polarion_id("OCS-yyyy")),
        ],
    )
    # TODO: Add Polarion ID when available
    def test_acm_kubevirt_using_different_protection_types(
        self,
        setup_acm_ui,
        protection_type,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        DR operation on discovered VMs using Standalone and Shared Protection type. In shared protection, both VMs are
        tied to a single DRPC in the same namespace where same DRPolicy is applied via UI to both the apps.

        Test steps:

        1. Deploy a CNV discovered workload in a test NS via CLI
        2. Deploy another CNV discovered workload in the same namespace via CLI
        3. Using ACM UI, DR protect the 1st workload from the VMs page using Standalone as Protection type
        4. Then for shared protection, repeat the above steps and DR protect 2nd workload from the VMs page using
            Shared option, which will use the existing DRPC of the 1st workload and gets tied to it.
        5. Write data, take md5sum, failover this workload via CLI (both VMs) by shutting down the primary managed
        cluster.
        6. After successful failover, check md5sum, recover the down managed cluster and perform cleanup.
        7. Let sync resume and then perform Relocate operation back to the original cluster.


        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        logger.info("Deploy 1st CNV workload")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=False
        )

        if protection_type:
            # Deploy second workload for Shared protection (uses same namespace as first)
            logger.info("Deploy 2nd CNV workload in the existing namespace")
            cnv_workloads = discovered_apps_dr_workload_cnv(
                pvc_vm=1, dr_protect=False, shared_drpc_protection=True
            )

        assert cnv_workloads, "No discovered VM found"
        config.switch_acm_ctx()
        protection_name = cnv_workloads[0].workload_namespace
        logger.info(f"Protection name is {protection_name}")
        resource_name = cnv_workloads[0].discovered_apps_placement_name + "-drpc"

        logger.info(f"CNV workloads instance is {cnv_workloads}")

        acm_obj = AcmAddClusters()
        primary_cluster_name = cnv_workloads[0].preferred_primary_cluster
        logger.info(
            f"Primary managed cluster name is {cnv_workloads[0].preferred_primary_cluster}"
        )
        assert navigate_using_fleet_virtualization(acm_obj)
        for i, vm in enumerate(cnv_workloads):
            standalone_flag = (not protection_type) or (i == 0)
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
        logger.info("Shutting down all the nodes of the primary managed cluster")
        nodes_multicluster[active_primary_index].stop_nodes(
            active_primary_cluster_node_objs
        )
        logger.info(
            f"All nodes of the primary managed cluster {primary_cluster_name} are powered off, "
            "waiting for cluster to be unreachable.."
        )
        sleep(300)

        logger.info("FailingOver the workloads.....")
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=resource_name,
            old_primary=primary_cluster_name,
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
        logger.info("Recover the down managed cluster")
        nodes_multicluster[active_primary_index].start_nodes(
            active_primary_cluster_node_objs
        )
        wait_for_nodes_status([node.name for node in active_primary_cluster_node_objs])
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)

        logger.info("Doing Cleanup Operations after successful failover")
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

        logger.info("On UI, check if VM is running after failover or not")
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=cnv_workloads,
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

        logger.info("Relocating the workloads.....")
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

        config.switch_acm_ctx()
        logger.info("On UI, check if VM is running after relocate or not")
        acm_obj.refresh_page()
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=cnv_workloads,
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
        assert navigate_using_fleet_virtualization(acm_obj)

        if protection_type:
            # Shared run – cnv_workloads[1] is the Shared VM; remove it
            logger.info(
                "Removing DR protection from the Shared VM "
                f"'{cnv_workloads[1].vm_name}'"
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
            # Validate: 1st VM still shows Running and is still protected
            logger.info("Validating 1st VM is still running and protected on UI")
            assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                acm_obj,
                vms=[cnv_workloads[0]],
                protection_name=protection_name,
                namespace=cnv_workloads[0].workload_namespace,
                managed_cluster_name=primary_cluster_name,
                assign_policy=False,
            )
            logger.info("Shared VM removed from DRPC; Standalone VM remains protected")
        else:
            # Standalone run – remove the single VM's protection entirely
            logger.info(
                "Removing DR protection from the Standalone VM "
                f"'{cnv_workloads[0].vm_name}'"
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

    # TODO: Add Polarion ID when available
    @pytest.mark.polarion_id("OCS-zzzz")
    def test_acm_kubevirt_mixed_protection_types(
        self,
        setup_acm_ui,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        DR operation on multiple discovered VMs in the same namespace using mixed protection types
        (some Standalone, some Shared). This test validates that VMs with different protection types
        can coexist in the same namespace and perform DR operations successfully.

        Test steps:

        1. Deploy multiple CNV discovered workloads (4 VMs) in a single test namespace via CLI
        2. Using ACM UI, DR protect the 1st and 2nd VMs from the VMs page using Standalone protection type
        3. DR protect the 3rd VM using Shared protection type (tied to 1st VM's DRPC)
        4. DR protect the 4th VM using Shared protection type (tied to 2nd VM's DRPC)
        5. Write data to all VMs, take md5sum
        6. Failover all workloads via CLI by shutting down the primary managed cluster
        7. After successful failover, verify data integrity on all VMs
        8. Write additional data post-failover
        9. Recover the down managed cluster and perform cleanup
        10. Perform Relocate operation back to the original cluster
        11. Verify data integrity after relocate

        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]
        all_cnv_workloads = []
        drpc_resources = []

        logger.info("Deploy 1st CNV workload (Standalone protection)")
        cnv_workload_1 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=False
        )
        all_cnv_workloads.extend(cnv_workload_1)

        # VM 2 (index 1): will become 2nd Standalone; shared_drpc_protection=True means
        # "deploy into the same namespace as the existing workload", not the DR protection type
        logger.info(
            "Deploy 2nd CNV workload in the same namespace (will use Standalone DR protection)"
        )
        cnv_workload_2 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        all_cnv_workloads.extend(cnv_workload_2)

        # VM 3 (index 2): will be enrolled as Shared with VM 1
        logger.info(
            "Deploy 3rd CNV workload in the same namespace (will be Shared with VM 1)"
        )
        cnv_workload_3 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        all_cnv_workloads.extend(cnv_workload_3)

        # VM 4 (index 3): will be enrolled as Shared with VM 2
        logger.info(
            "Deploy 4th CNV workload in the same namespace (will be Shared with VM 2)"
        )
        cnv_workload_4 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        all_cnv_workloads.extend(cnv_workload_4)

        assert all_cnv_workloads, "No discovered VMs found"
        assert (
            len(all_cnv_workloads) == 4
        ), f"Expected 4 VMs, found {len(all_cnv_workloads)}"

        config.switch_acm_ctx()
        workload_namespace = all_cnv_workloads[0].workload_namespace
        logger.info(f"All VMs deployed in namespace: {workload_namespace}")

        acm_obj = AcmAddClusters()
        primary_cluster_name = all_cnv_workloads[0].preferred_primary_cluster
        logger.info(f"Primary managed cluster name is {primary_cluster_name}")

        assert navigate_using_fleet_virtualization(acm_obj)

        # DR protect VMs with mixed protection types
        # VM 1: Standalone (creates new DRPC)
        logger.info("DR protecting VM 1 with Standalone protection")
        protection_name_1 = f"{workload_namespace}-standalone-1"
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[0]],
            managed_cluster_name=primary_cluster_name,
            standalone=True,
            protection_name=protection_name_1,
            namespace=workload_namespace,
        )
        # When DR protected via UI, the DRPC is named after the protection name entered
        resource_name_1 = f"{protection_name_1}-drpc"
        drpc_resources.append(resource_name_1)

        # VM 3: Shared with VM 1 (uses the single existing DRPC from VM 1)
        # NOTE: the UI helper asserts exactly 1 radio button when selecting a Shared DRPC.
        # VM 3 is enrolled immediately after VM 1 so only 1 DRPC exists at this point.
        logger.info("DR protecting VM 3 with Shared protection (tied to VM 1)")
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[2]],
            managed_cluster_name=primary_cluster_name,
            standalone=False,
            protection_name=protection_name_1,
            namespace=workload_namespace,
        )

        # VM 2: Standalone (creates a second independent DRPC)
        logger.info("DR protecting VM 2 with Standalone protection")
        protection_name_2 = f"{workload_namespace}-standalone-2"
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[1]],
            managed_cluster_name=primary_cluster_name,
            standalone=True,
            protection_name=protection_name_2,
            namespace=workload_namespace,
        )
        resource_name_2 = f"{protection_name_2}-drpc"
        drpc_resources.append(resource_name_2)

        # VM 4: Shared with VM 2 (uses existing DRPC from VM 2)
        # TODO: The UI helper currently asserts exactly 1 Shared radio button, but at this
        # point 2 DRPCs exist in the namespace (resource_name_1 and resource_name_2).
        # The helper in dr_helpers_ui.py must be updated to select a specific DRPC by name
        # rather than asserting a single radio button before this step will pass.
        logger.info("DR protecting VM 4 with Shared protection (tied to VM 2)")
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[3]],
            managed_cluster_name=primary_cluster_name,
            standalone=False,
            protection_name=protection_name_2,
            namespace=workload_namespace,
        )

        logger.info(f"DRPC resources created: {drpc_resources}")

        # Get scheduling interval from first DRPC
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload_namespace,
            discovered_apps=True,
            resource_name=resource_name_1,
        )

        config.switch_to_cluster_by_name(primary_cluster_name)

        # Wait for all resources to be created (4 VMs total)
        total_pvc_count = sum(wl.workload_pvc_count for wl in all_cnv_workloads)
        total_pod_count = sum(wl.workload_pod_count for wl in all_cnv_workloads)

        for resource_name in drpc_resources:
            dr_helpers.wait_for_all_resources_creation(
                total_pvc_count,
                total_pod_count,
                workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
            )

        # Wait for all VMs to be running
        for cnv_wl in all_cnv_workloads:
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload_namespace,
            discovered_apps=True,
            resource_name=resource_name_1,
        )

        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()

        # Creating a file (file1) on all VMs and calculating MD5sum
        logger.info("Writing data to all VMs and calculating MD5sum")
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
        logger.info("Shutting down all the nodes of the primary managed cluster")
        nodes_multicluster[active_primary_index].stop_nodes(
            active_primary_cluster_node_objs
        )
        logger.info(
            f"All nodes of the primary managed cluster {primary_cluster_name} are powered off, "
            "waiting for cluster to be unreachable.."
        )
        sleep(300)

        # Failover all workloads (both DRPCs)
        logger.info("Failing over all workloads...")
        for resource_name in drpc_resources:
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=workload_namespace,
                discovered_apps=True,
                workload_placement_name=resource_name,
                old_primary=primary_cluster_name,
            )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for resource_name in drpc_resources:
            dr_helpers.wait_for_all_resources_creation(
                total_pvc_count,
                total_pod_count,
                workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
            )

        # Wait for all VMs to be running on secondary cluster
        for cnv_wl in all_cnv_workloads:
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        # Validating data integrity (file1) after failing-over VMs to secondary managed cluster
        logger.info("Validating data integrity after failover")
        validate_data_integrity_vm(
            all_cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )

        # Creating a file (file2) post failover on all VMs
        logger.info("Writing additional data post-failover")
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
        logger.info("Recover the down managed cluster")
        nodes_multicluster[active_primary_index].start_nodes(
            active_primary_cluster_node_objs
        )
        wait_for_nodes_status([node.name for node in active_primary_cluster_node_objs])
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)

        # Cleanup operations after successful failover
        logger.info("Doing Cleanup Operations after successful failover")
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

        logger.info("On UI, check if all VMs are running after failover")
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

        logger.info("Relocating all workloads back to primary cluster...")
        # Relocate workloads for first DRPC (VM 1 and VM 3)
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=workload_namespace,
            workload_placement_name=resource_name_1,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=all_cnv_workloads[0],
            workload_instances_shared=[all_cnv_workloads[0], all_cnv_workloads[2]],
        )

        # Relocate workloads for second DRPC (VM 2 and VM 4)
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=workload_namespace,
            workload_placement_name=resource_name_2,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=all_cnv_workloads[1],
            workload_instances_shared=[all_cnv_workloads[1], all_cnv_workloads[3]],
        )

        # Verify cleanup after relocate
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

        # Verify resources creation on primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        for resource_name in drpc_resources:
            dr_helpers.wait_for_all_resources_creation(
                total_pvc_count,
                total_pod_count,
                workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
            )

        # Wait for all VMs to be running on primary cluster
        for cnv_wl in all_cnv_workloads:
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        config.switch_acm_ctx()
        logger.info("On UI, check if all VMs are running after relocate")
        acm_obj.refresh_page()
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
        logger.info("Validating data integrity (file1) after relocate")
        validate_data_integrity_vm(
            all_cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )

        # Validating data integrity (file2) after relocating VMs back to primary managed cluster
        logger.info("Validating data integrity (file2) after relocate")
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
        # Step A: Remove protection from VM 3 (Shared with VM 1 / DRPC1).    #
        #   Validate: DRPC1 still exists, VM 1 is still protected.            #
        #                                                                      #
        # Step B: Remove protection from VM 2 (Standalone / DRPC2).          #
        #   Validate: DRPC2 is deleted.                                        #
        # ------------------------------------------------------------------ #
        config.switch_acm_ctx()
        assert navigate_using_fleet_virtualization(acm_obj)

        # Step A: remove the Shared VM (VM 3, index 2) from DRPC1
        logger.info(
            "Removing DR protection from Shared VM 3 "
            f"'{all_cnv_workloads[2].vm_name}' (tied to DRPC1)"
        )
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[2],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        # Validate DRPC1 still exists and VM 1 is still protected
        logger.info("Validating DRPC1 still exists after removing Shared VM 3")
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name_1,
            timeout=120,
            should_exist=True,
        )
        logger.info("Validating VM 1 is still running and protected (UI check)")
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[0]],
            protection_name=protection_name_1,
            namespace=workload_namespace,
            managed_cluster_name=primary_cluster_name,
            assign_policy=False,
        )
        logger.info("VM 3 removed from DRPC1; VM 1 remains protected under DRPC1")

        # Step B: remove the Standalone VM (VM 2, index 1) → DRPC2 deleted
        logger.info(
            "Removing DR protection from Standalone VM 2 "
            f"'{all_cnv_workloads[1].vm_name}' (DRPC2)"
        )
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[1],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        # Validate DRPC2 is deleted
        logger.info("Validating DRPC2 is deleted after removing Standalone VM 2")
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name_2,
            timeout=120,
            should_exist=False,
        )
        logger.info("Standalone VM 2 DR protection removed and DRPC2 deleted")

        logger.info(
            f"Test for mixed protection types (Standalone and Shared) "
            f"in namespace {workload_namespace} passed. "
            f"DRPC groups: {protection_name_1}, {protection_name_2}"
        )

    # TODO: Add Polarion ID when available
    @pytest.mark.polarion_id("OCS-wwww")
    def test_acm_kubevirt_remove_all_shared_protection_vms(
        self,
        setup_acm_ui,
        discovered_apps_dr_workload_cnv,
    ):
        """
        Verify that the DRPC is deleted when DR protection is removed from
        every VM enrolled under a shared protection group.

        Three VMs are deployed in the same namespace.  One is DR-protected as
        Standalone (this creates the DRPC), and the other two join the same
        DRPC as Shared.  The test then removes protection from the Shared VMs
        one at a time, asserting the DRPC is still present after each partial
        removal, and finally removes the Standalone VM's protection, asserting
        the DRPC is deleted once no VMs remain enrolled.

        Test steps:

        1. Deploy 3 CNV discovered workloads in a single namespace via CLI
        2. DR protect VM 1 as Standalone (creates the DRPC)
        3. DR protect VM 2 as Shared (joins VM 1's DRPC; only 1 DRPC exists)
        4. DR protect VM 3 as Shared (joins VM 1's DRPC; still only 1 DRPC)
        5. Verify all VMs are Running and replication resources are created
        6. Write data to all VMs and record md5sums
        7. Remove DR protection from VM 2 (Shared)
           - Verify DRPC still exists and VM 1 / VM 3 are still protected
        8. Remove DR protection from VM 3 (Shared)
           - Verify DRPC still exists and VM 1 is still protected
        9. Remove DR protection from VM 1 (Standalone – last enrolled VM)
           - Verify DRPC is deleted
        """

        vm_filepaths = ["/dd_file1.txt"]
        all_cnv_workloads = []

        logger.info("Deploy VM 1 (will be Standalone)")
        cnv_workload_1 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=False
        )
        all_cnv_workloads.extend(cnv_workload_1)

        logger.info("Deploy VM 2 in the same namespace (will be Shared)")
        cnv_workload_2 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        all_cnv_workloads.extend(cnv_workload_2)

        logger.info("Deploy VM 3 in the same namespace (will be Shared)")
        cnv_workload_3 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        all_cnv_workloads.extend(cnv_workload_3)

        assert all_cnv_workloads, "No discovered VMs found"
        assert (
            len(all_cnv_workloads) == 3
        ), f"Expected 3 VMs, found {len(all_cnv_workloads)}"

        config.switch_acm_ctx()
        workload_namespace = all_cnv_workloads[0].workload_namespace
        logger.info(f"All VMs deployed in namespace: {workload_namespace}")

        acm_obj = AcmAddClusters()
        primary_cluster_name = all_cnv_workloads[0].preferred_primary_cluster
        logger.info(f"Primary cluster: {primary_cluster_name}")

        assert navigate_using_fleet_virtualization(acm_obj)

        # VM 1: Standalone – creates the DRPC
        protection_name = workload_namespace
        logger.info("DR protecting VM 1 with Standalone protection")
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[0]],
            managed_cluster_name=primary_cluster_name,
            standalone=True,
            protection_name=protection_name,
            namespace=workload_namespace,
        )
        resource_name = f"{protection_name}-drpc"

        # VM 2: Shared – exactly 1 DRPC exists at this point
        logger.info("DR protecting VM 2 with Shared protection (tied to VM 1)")
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[1]],
            managed_cluster_name=primary_cluster_name,
            standalone=False,
            protection_name=protection_name,
            namespace=workload_namespace,
        )

        # VM 3: Shared – still only 1 DRPC, radio-button assertion holds
        logger.info("DR protecting VM 3 with Shared protection (tied to VM 1)")
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[2]],
            managed_cluster_name=primary_cluster_name,
            standalone=False,
            protection_name=protection_name,
            namespace=workload_namespace,
        )

        logger.info(f"All VMs enrolled under DRPC: {resource_name}")

        config.switch_to_cluster_by_name(primary_cluster_name)

        total_pvc_count = sum(wl.workload_pvc_count for wl in all_cnv_workloads)
        total_pod_count = sum(wl.workload_pod_count for wl in all_cnv_workloads)
        dr_helpers.wait_for_all_resources_creation(
            total_pvc_count,
            total_pod_count,
            workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )

        for cnv_wl in all_cnv_workloads:
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        # Download virtctl if not already present
        CNVInstaller().download_and_extract_virtctl_binary()

        # Write data to all VMs
        logger.info("Writing data to all VMs")
        for cnv_wl in all_cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[0],
                username=cnv_wl.vm_username,
                verify=True,
            )

        # ------------------------------------------------------------------ #
        # Step 7: Remove protection from VM 2 (Shared)                        #
        # ------------------------------------------------------------------ #
        config.switch_acm_ctx()
        assert navigate_using_fleet_virtualization(acm_obj)

        logger.info(
            f"Removing DR protection from Shared VM 2 '{all_cnv_workloads[1].vm_name}'"
        )
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[1],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        logger.info(
            "Validating DRPC still exists after removing VM 2 (VM 1 and VM 3 remain)"
        )
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name,
            timeout=120,
            should_exist=True,
        )
        logger.info(
            "Validating VM 1 and VM 3 are still Running and protected (UI check)"
        )
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[0], all_cnv_workloads[2]],
            protection_name=protection_name,
            namespace=workload_namespace,
            managed_cluster_name=primary_cluster_name,
            assign_policy=False,
        )
        logger.info("VM 2 removed; DRPC intact with VM 1 and VM 3 still enrolled")

        # ------------------------------------------------------------------ #
        # Step 8: Remove protection from VM 3 (Shared)                        #
        # ------------------------------------------------------------------ #
        assert navigate_using_fleet_virtualization(acm_obj)

        logger.info(
            f"Removing DR protection from Shared VM 3 '{all_cnv_workloads[2].vm_name}'"
        )
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[2],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        logger.info("Validating DRPC still exists after removing VM 3 (VM 1 remains)")
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name,
            timeout=120,
            should_exist=True,
        )
        logger.info("Validating VM 1 is still Running and protected (UI check)")
        assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[all_cnv_workloads[0]],
            protection_name=protection_name,
            namespace=workload_namespace,
            managed_cluster_name=primary_cluster_name,
            assign_policy=False,
        )
        logger.info("VM 3 removed; DRPC intact with VM 1 still enrolled")

        # ------------------------------------------------------------------ #
        # Step 9: Remove protection from VM 1 (Standalone – last enrollment)  #
        # DRPC must be deleted once no VMs remain enrolled.                   #
        # ------------------------------------------------------------------ #
        assert navigate_using_fleet_virtualization(acm_obj)

        logger.info(
            f"Removing DR protection from Standalone VM 1 '{all_cnv_workloads[0].vm_name}'"
            " (last VM in the group)"
        )
        assert remove_drprotection_for_discovered_vm_via_ui(
            acm_obj,
            vm=all_cnv_workloads[0],
            managed_cluster_name=primary_cluster_name,
            namespace=workload_namespace,
        )

        logger.info(
            "Validating DRPC is deleted now that all enrolled VMs have been removed"
        )
        config.switch_acm_ctx()
        wait_for_resource_existence(
            kind=constants.DRPC,
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name,
            timeout=300,
            should_exist=False,
        )
        logger.info(
            f"DRPC '{resource_name}' deleted as expected after removing "
            "all VMs from the shared protection group"
        )
