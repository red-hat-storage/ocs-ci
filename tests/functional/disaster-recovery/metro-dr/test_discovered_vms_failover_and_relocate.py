"""
Metro-DR test cases for CNV VM discovered applications

This module contains comprehensive test cases for Metro-DR operations
on CNV VMs enrolled as discovered applications, including:
- VM enrollment for DR protection
- Failover and relocate with primary cluster up
- Failover and relocate with primary cluster down
- Recipe-based protection
- Recipe with checkhooks
"""

import logging
import pytest
import time

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    turquoise_squad,
    mdr,
    tier1,
    tier2,
)
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.helpers.dr_helpers import (
    enable_fence,
    enable_unfence,
    get_fence_state,
    verify_fence_state,
    verify_cluster_data_protected_status,
    mdr_post_failover_check,
    gracefully_reboot_ocp_nodes,
    wait_for_vrg_state,
    wait_for_cnv_workload,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    failover,
    relocate,
    do_discovered_apps_cleanup,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


@mdr
@turquoise_squad
class TestMDRCNVDiscoveredVM:
    """
    Test Metro-DR operations for CNV VM discovered applications
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Teardown function: If fenced, un-fence the cluster and reboot nodes
        """
        self.primary_cluster_name = None

        def finalizer():
            if (
                self.primary_cluster_name is not None
                and get_fence_state(self.primary_cluster_name) == constants.ACTION_FENCE
            ):
                logger.info(
                    f"Unfencing cluster {self.primary_cluster_name} in teardown"
                )
                enable_unfence(self.primary_cluster_name)
                gracefully_reboot_ocp_nodes(
                    drcluster_name=self.primary_cluster_name, disable_eviction=True
                )

        request.addfinalizer(finalizer)

    @tier1
    @pytest.mark.polarion_id("OCS-7918")
    def test_enroll_discovered_vm_for_dr_protection(
        self,
        discovered_apps_dr_workload_cnv,
    ):
        """
        Test case 1: Enroll discovered VM for DR protection (MDR)

        Steps:
        1. Deploy CNV VM on primary
        2. Enroll as discovered app (DPA/DRPC/placement per docs)
        3. Verify VRG and protection state

        """
        logger.info("Starting test: Enroll discovered VM for DR protection")

        # Deploy CNV VM as discovered app
        logger.info("Deploying CNV VM as discovered application")
        cnv_workloads = discovered_apps_dr_workload_cnv(pvc_vm=1)

        # Get primary cluster information
        primary_cluster_name = get_current_primary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info(f"Primary cluster: {primary_cluster_name}")

        # Switch to primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Verify DRPC exists and is in proper state
        logger.info("Verifying DRPC resource")
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        assert drpc_obj.get(), "DRPC resource not found"
        logger.info(f"DRPC resource {drpc_obj.resource_name} exists")

        # Verify VRG (VolumeReplicationGroup) exists
        logger.info("Verifying VRG resource on primary cluster")
        wait_for_vrg_state(
            vrg_state="primary",
            vrg_namespace=constants.DR_OPS_NAMESPACE,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info("VRG is in Primary state")

        # Verify VM is running
        logger.info(f"Verifying VM {cnv_workloads[0].vm_name} is running")
        wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )
        logger.info(f"VM {cnv_workloads[0].vm_name} is running successfully")

        # Verify cluster data protected and peer ready status
        logger.info("Verifying cluster data protected and peer ready status")
        verify_cluster_data_protected_status(
            workload_type=constants.DISCOVERED_APPS,
            namespace=constants.DR_OPS_NAMESPACE,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info("Cluster data protected and peer ready status verified")

        logger.info("Test completed: VM successfully enrolled for DR protection")

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "recipe", "checkhooks"],
        argvalues=[
            pytest.param(
                False,
                False,
                False,
                marks=[tier1, pytest.mark.polarion_id("OCS-7919")],
                id="primary_up-no_recipe",
            ),
            pytest.param(
                True,
                False,
                False,
                marks=[tier1, pytest.mark.polarion_id("OCS-7920")],
                id="primary_down-no_recipe",
            ),
            pytest.param(
                False,
                True,
                False,
                marks=[tier2, pytest.mark.polarion_id("OCS-7921")],
                id="primary_up-with_recipe",
            ),
            pytest.param(
                True,
                True,
                False,
                marks=[tier2, pytest.mark.polarion_id("OCS-7922")],
                id="primary_down-with_recipe",
            ),
            pytest.param(
                False,
                True,
                True,
                marks=[tier2, pytest.mark.polarion_id("OCS-7923")],
                id="primary_up-recipe_with_checkhooks",
            ),
            pytest.param(
                True,
                True,
                True,
                marks=[tier2, pytest.mark.polarion_id("OCS-7924")],
                id="primary_down-recipe_with_checkhooks",
            ),
        ],
    )
    def test_discovered_vm_failover_and_relocate(
        self,
        primary_cluster_down,
        recipe,
        checkhooks,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,  # teardown-only: restarts nodes stopped during the test
    ):
        """
        Comprehensive test for discovered VM failover and relocate operations

        This test covers:
        - Test case 2: Discovered VM (CNV, RBD) failover and relocate (primary up)
        - Test case 3: Discovered VM (CNV, RBD) failover and relocate (primary down)
        - Test case 4: Discovered VM with recipe protection failover and relocate
        - Test case 5: Discovered VM with recipe with checkhooks

        Args:
            primary_cluster_down (bool): Whether to bring primary cluster down before failover
            recipe (bool): Whether to use recipe-based protection
            checkhooks (bool): Whether to use checkhooks in recipe

        Steps:
        1. Deploy CNV discovered VM (RBD-backed) on primary
        2. Write data to VM and calculate md5sum
        3. Verify cluster data protected status
        4. (If primary_cluster_down) Make primary managed cluster down
        5. Fence primary cluster
        6. Initiate failover to secondary
        7. Verify VM running and validate data on secondary
        8. Write additional data on secondary
        9. (If primary_cluster_down) Bring primary cluster up
        10. Unfence primary cluster
        11. Relocate back to primary
        12. Verify VM and data on primary

        """
        logger.info(
            f"Starting test: Discovered VM failover and relocate "
            f"(primary_down={primary_cluster_down}, recipe={recipe}, checkhooks={checkhooks})"
        )

        # File paths for data integrity verification
        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        # Deploy CNV VM as discovered app with optional recipe
        logger.info("Deploying CNV VM as discovered application")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1,
            recipe=recipe,
            checkhooks=checkhooks,
        )

        # Get cluster information
        self.primary_cluster_name = get_current_primary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info(f"Primary cluster before failover: {self.primary_cluster_name}")

        config.switch_to_cluster_by_name(self.primary_cluster_name)
        primary_cluster_index = config.cur_index

        # Download virtctl binary if needed
        CNVInstaller().download_and_extract_virtctl_binary()

        failover_cluster_name = get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info(f"Failover cluster: {failover_cluster_name}")

        # Step 2: Write data to VM and calculate md5sum
        logger.info("Writing initial data to VM and calculating md5sum")
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

        # Step 3: Verify hub ClusterDataProtected (DiscoveredApps DRPC)
        logger.info("Verifying cluster data protected status for discovered-apps DRPC")
        verify_cluster_data_protected_status(
            workload_type=constants.DISCOVERED_APPS,
            namespace=constants.DR_OPS_NAMESPACE,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info("Cluster data protected and peer ready status verified")

        # Wait before failover
        wait_time = 120
        logger.info(f"Waiting {wait_time} seconds before starting failover")
        time.sleep(wait_time)

        # Step 4: Make primary cluster down if required
        node_objs = get_node_objs()
        if primary_cluster_down:
            logger.info("Bringing primary managed cluster down")
            logger.info("Shutting down all nodes of the primary managed cluster")
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)
            logger.info("Primary cluster nodes powered off")

        # Step 5: Fence the primary managed cluster
        logger.info(f"Fencing primary cluster: {self.primary_cluster_name}")
        enable_fence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_FENCE
        ), f"DR cluster {self.primary_cluster_name} did not reach {constants.ACTION_FENCE} state"
        logger.info(f"Primary cluster {self.primary_cluster_name} fenced successfully")

        # Step 6: Initiate failover to secondary
        logger.info(
            f"Initiating failover to secondary cluster: {failover_cluster_name}"
        )
        failover(
            failover_cluster=failover_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=self.primary_cluster_name,
        )
        logger.info("Failover initiated successfully")

        # Step 7: Verify VM running and validate data on secondary
        logger.info("Verifying resources on secondary cluster after failover")
        config.switch_to_cluster_by_name(failover_cluster_name)

        wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info("All resources created on secondary cluster")

        wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )
        logger.info(f"VM {cnv_workloads[0].vm_name} is running on secondary cluster")

        # Validate data integrity after failover
        logger.info("Validating data integrity after failover")
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )
        logger.info("Data integrity validated successfully after failover")

        # Step 8: Write additional data on secondary
        logger.info("Writing additional data on secondary cluster after failover")
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
                f"Checksum of file written after failover: {vm_filepaths[1]} "
                f"on VM {cnv_wl.workload_name}: {md5sum}"
            )

        # Step 9: Bring primary cluster up if it was down
        wait_time = 120
        if primary_cluster_down:
            logger.info("Recovering the primary managed cluster")
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.info(f"Waiting {wait_time} seconds after starting nodes")
            time.sleep(wait_time)
            config.switch_to_cluster_by_name(self.primary_cluster_name)
            wait_for_nodes_status([node.name for node in node_objs])
            logger.info("Primary cluster nodes are ready")

        # Manually delete VM resources from recovered primary cluster
        logger.info("Manually deleting VM resources from recovered primary cluster")
        config.switch_to_cluster_by_name(self.primary_cluster_name)

        # Delete VM and related resources on recovered primary cluster
        do_discovered_apps_cleanup(
            drpc_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=self.primary_cluster_name,
            workload_namespace=cnv_workloads[0].workload_namespace,
            workload_dir=cnv_workloads[0].workload_dir,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
            skip_resource_deletion_verification=True,
        )
        mdr_post_failover_check(
            namespace=cnv_workloads[0].workload_namespace,
            timeout=1800,
        )

        # Step 10: Unfence the managed cluster
        logger.info(f"Unfencing cluster: {self.primary_cluster_name}")
        enable_unfence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_UNFENCE
        ), f"DR cluster {self.primary_cluster_name} did not reach {constants.ACTION_UNFENCE} state"
        logger.info(f"Cluster {self.primary_cluster_name} unfenced successfully")

        # Step 12: Gracefully reboot nodes after unfence
        logger.info("Gracefully rebooting nodes after unfence")
        gracefully_reboot_ocp_nodes(
            drcluster_name=self.primary_cluster_name, disable_eviction=True
        )
        logger.info("Nodes rebooted successfully")

        # Step 13: Validate all discovered apps cleanup completed after reboot
        logger.info(
            "Validating all discovered apps cleanup completed after node reboot"
        )
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        wait_for_all_resources_deletion(
            cnv_workloads[0].workload_namespace,
            timeout=1800,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info("All discovered apps cleanup validated and completed")

        # Step 14: Relocate back to primary
        logger.info("Initiating relocate back to primary cluster")

        relocate(
            preferred_cluster=self.primary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=failover_cluster_name,
            workload_instance=cnv_workloads[0],
        )
        logger.info("Relocate initiated successfully")

        # Verify resources deletion from failover cluster
        config.switch_to_cluster_by_name(failover_cluster_name)
        wait_for_all_resources_deletion(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info("All resources deleted from failover cluster after relocate")

        # Step 15: Verify VM and data on primary
        logger.info("Verifying resources on primary cluster after relocate")
        config.switch_to_cluster_by_name(self.primary_cluster_name)

        wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info("All resources created on primary cluster after relocate")

        wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )
        logger.info(f"VM {cnv_workloads[0].vm_name} is running on primary cluster")

        # Validate original data integrity after relocate
        logger.info("Validating original data integrity after relocate")
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )
        logger.info("Original data integrity validated successfully after relocate")

        # Validate data written after failover
        logger.info("Validating data written after failover")
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[1], md5sum_failover, "Relocate"
        )
        logger.info("Failover data integrity validated successfully after relocate")

        # Write final data to verify VM is fully functional
        logger.info("Writing final data to verify VM functionality")
        for cnv_wl in cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )
        logger.info("Final data write successful")

        logger.info(
            f"Test completed successfully: Discovered VM failover and relocate "
            f"(primary_down={primary_cluster_down}, recipe={recipe}, checkhooks={checkhooks})"
        )
