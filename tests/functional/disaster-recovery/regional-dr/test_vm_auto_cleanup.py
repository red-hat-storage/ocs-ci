import logging

from time import sleep

import pytest

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
    tier1,
    acceptance,
)
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.helpers.dr_helpers import wait_for_all_resources_deletion
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    check_or_assign_drpolicy_for_discovered_vms_via_ui,
    navigate_using_fleet_virtulization,
)
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@acceptance
@tier1
@turquoise_squad
@skipif_ocs_version("<4.21")
class TestVMAutoCleanUp:
    """
    Test automatic resource clean up on VM resources - RHSTOR-7242

    """

    @pytest.mark.parametrize(
        argnames=["protection_type", "primary_cluster_down"],
        argvalues=[
            pytest.param(
                "standalone",
                False,
                id="primary_up_standalone-protection",
            ),
            pytest.param(
                "standalone",
                True,
                id="primary_down_standalone-protection",
            ),
            pytest.param(
                "shared",
                False,
                id="primary_up_shared-protection",
            ),
            pytest.param(
                "shared",
                True,
                id="primary_down_shared-protection",
            ),
        ],
    )
    def test_vm_auto_cleanup(
        self,
        setup_acm_ui,
        protection_type,
        primary_cluster_down,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Test to validate automated resource clean up of VM resources such as VMs, PVC,
        DataVolume associated with the VMs that are protected via fleet virtualization.

        Test Steps:

        1. Deploy VMs using DVT via CLI
        2. DR Protect it from fleet virtualization Tab of ACM UI
        3. Validate the "k8s-resource-selector" label in VM and PVC
        4. Validate the creation of DR resources like VGR, VRG, VR
        5. Validate the lastGroupSyncTime
        6. Perform Failover
        7. Validate the DR resource creation on Secondary
        7. Validate automated resource clean up on the failed cluster
        8. Perform relocate
        9. Validate the DR resource creation on Primary
        10. Validate automated resource clean up on the secondary cluster

        Test has been parametrized to run with standalone and shared protection type.

        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        logger.info("Deploy 1st CNV workload")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1,
            dr_protect=False,
            shared_drpc_protection=False,
            vm_type=constants.VM_VOLUME_DVT,
        )

        if protection_type == "shared":
            # Deploy second workload for Shared protection (uses same namespace as first)
            # In case of Shared Protection, VMs under same namespace is protected via single DRPC
            logger.info("Deploy 2nd CNV workload in the existing namespace")
            cnv_workloads = discovered_apps_dr_workload_cnv(
                pvc_vm=1,
                dr_protect=False,
                shared_drpc_protection=True,
                vm_type=constants.VM_VOLUME_DVT,
            )

        assert cnv_workloads, "No discovered VM found"
        config.switch_acm_ctx()
        protection_name = cnv_workloads[0].workload_namespace
        logger.info(f"Protection name is {protection_name}")
        resource_name = cnv_workloads[0].discovered_apps_placement_name + "-drpc"

        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE, resource_name=resource_name
        )

        logger.info(f"CNV workloads instance is {cnv_workloads}")

        acm_obj = AcmAddClusters()
        primary_cluster_name = cnv_workloads[0].preferred_primary_cluster
        logger.info(
            f"Primary managed cluster name is {cnv_workloads[0].preferred_primary_cluster}"
        )

        assert navigate_using_fleet_virtulization(acm_obj)
        for i, vm in enumerate(cnv_workloads):
            standalone_flag = (not protection_type == "shared") or (i == 0)
            assert check_or_assign_drpolicy_for_discovered_vms_via_ui(
                acm_obj,
                vms=[vm],
                managed_cluster_name=primary_cluster_name,
                standalone=standalone_flag,
                protection_name=protection_name,
                namespace=cnv_workloads[0].workload_namespace,
            )

        config.switch_to_cluster_by_name(primary_cluster_name)

        # Validate the k8-resource-selector label on VMs and PVCs in the protected namespace
        dr_helpers.validate_protection_label(
            constants.VM,
            cnv_workloads[0].workload_namespace,
            protection_name=protection_name,
        )

        dr_helpers.validate_protection_label(
            constants.PVC, cnv_workloads[0].workload_namespace, protection_name
        )

        logger.info(
            f'Placement name is "{cnv_workloads[0].discovered_apps_placement_name}"'
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )

        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        logger.info("Checking for lastGroupSyncTime")
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
        logger.info(f"Primary cluster name before failover is {primary_cluster_name}")

        config.switch_to_cluster_by_name(primary_cluster_name)

        workload_pvc_count = (
            cnv_workloads[0].workload_pvc_count * 2
            if protection_type == "shared"
            else cnv_workloads[0].workload_pvc_count
        )
        workload_pod_count = (
            cnv_workloads[0].workload_pod_count * 2
            if protection_type == "shared"
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

        if primary_cluster_down:
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

        # Validate the k8-resource-selector label on VMs and PVCs in the protected namespace
        dr_helpers.validate_protection_label(
            constants.VM,
            cnv_workloads[0].workload_namespace,
            protection_name=protection_name,
        )

        dr_helpers.validate_protection_label(
            constants.PVC, cnv_workloads[0].workload_namespace, protection_name
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

        if primary_cluster_down:
            logger.info("Recover the failed managed cluster")
            nodes_multicluster[active_primary_index].start_nodes(
                active_primary_cluster_node_objs
            )
            wait_for_nodes_status(
                [node.name for node in active_primary_cluster_node_objs]
            )
            wait_for_pods_to_be_running(timeout=420, sleep=15)
            assert ceph_health_check(tries=10, delay=30)

        logger.info(
            "Validate Automatic resource cleanup of VM and its associated "
            "resources on failed clusters post failover"
        )
        wait_for_all_resources_deletion(
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_resource_existence(
                kind=constants.VM,
                namespace=cnv_wl.workload_namespace,
                should_exist=False,
            )
            dr_helpers.wait_for_resource_existence(
                kind=constants.VM_DATAVOLUME,
                namespace=cnv_wl.workload_namespace,
                should_exist=False,
            )

        config.switch_acm_ctx()
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        logger.info("Checking for lastGroupSyncTime")
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        logger.info(f"Primary cluster name before failover is {primary_cluster_name}")

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
            vm_auto_cleanup=True,
        )

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

        # Validate the k8-resource-selector label on VMs and PVCs in the protected namespace
        dr_helpers.validate_protection_label(
            constants.VM,
            cnv_workloads[0].workload_namespace,
            protection_name=protection_name,
        )

        dr_helpers.validate_protection_label(
            constants.PVC,
            cnv_workloads[0].workload_namespace,
            protection_name=protection_name,
        )

        # Verify automatic resources deletion on secondary managed cluster
        logger.info(
            "Validate Automatic resource cleanup of VM and its associated "
            "resources on failed clusters post relocate"
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        wait_for_all_resources_deletion(
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_resource_existence(
                kind=constants.VM,
                namespace=cnv_wl.workload_namespace,
                should_exist=False,
            )
            dr_helpers.wait_for_resource_existence(
                kind=constants.VM_DATAVOLUME,
                namespace=cnv_wl.workload_namespace,
                should_exist=False,
            )
        config.switch_acm_ctx()
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE, resource_name=resource_name
        )
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        logger.info("Checking for lastGroupSyncTime")
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        logger.info(f"Primary cluster name before failover is {primary_cluster_name}")

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

        logger.info("Checking for lastGroupSyncTime post relocate...")
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
