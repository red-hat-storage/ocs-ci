import logging

from time import sleep

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
from ocs_ci.helpers.dr_helpers import wait_for_all_resources_deletion
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    assign_drpolicy_for_discovered_vms_via_ui,
    verify_drpolicy_ui,
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

    # @pytest.mark.polarion_id("OCS-xxxx")
    # TODO: Add Polarion ID when available
    def test_acm_kubevirt_using_shared_protection(
        self,
        setup_acm_ui,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        DR operation on discovered VMs using Shared Protection type, both VMs are tied to a single DRPC
        in the same namespace where same DRPolicy is applied via UI to both the apps.

        Test steps:

        1. Deploy a CNV discovered workload in a test NS via CLI
        2. Deploy another CNV discovered workload in the same namespace via CLI
        3. Using ACM UI, DR protect the 1st workload from the VMs page using Standalone as Protection type
        4. Then DR protect 2nd workload from the VMs page using Shared option, which will use the existing DRPC
        of the 1st workload and gets tied to it.
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

        # Second workload (uses same namespace as first)
        logger.info("Deploy 2nd CNV workload in the existing namespace")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared_drpc_protection=True
        )
        logger.info(f"CNV workloads instance is {cnv_workloads}")

        acm_obj = AcmAddClusters()

        logger.info("Navigate to Virtual machines page on the ACM console")
        assert cnv_workloads, "No discovered VM found"
        config.switch_acm_ctx()
        protection_name = cnv_workloads[0].workload_namespace
        logger.info(f"Protection name is {protection_name}")
        assert assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[cnv_workloads[0].vm_name],
            protection_name=protection_name,
            namespace=cnv_workloads[0].workload_namespace,
        )
        assert assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj,
            vms=[cnv_workloads[1].vm_name],
            standalone=False,
            namespace=cnv_workloads[0].workload_namespace,
        )

        resource_name = cnv_workloads[0].discovered_apps_placement_name + "-drpc"
        logger.info(
            f'Placement name is "{cnv_workloads[0].discovered_apps_placement_name}"'
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )

        verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace,
                discovered_apps=True,
                resource_name=resource_name,
            )
        )
        logger.info(
            f"Primary cluster name before failover is {primary_cluster_name_before_failover}"
        )

        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count * 2,
            cnv_workloads[0].workload_pod_count * 2,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )

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
            f"All nodes of the primary managed cluster {primary_cluster_name_before_failover} are powered off, "
            "waiting for cluster to be unreachable.."
        )
        sleep(300)

        logger.info("FailingOver the workloads.....")
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=resource_name,
            old_primary=primary_cluster_name_before_failover,
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count * 2,
            cnv_workloads[0].workload_pod_count * 2,
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

        logger.info("Recover the down managed cluster")
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
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
                old_primary=primary_cluster_name_before_failover,
                workload_namespace=cnv_workloads[0].workload_namespace,
                workload_dir=cnv_wl.workload_dir,
                vrg_name=resource_name,
                skip_resource_deletion_verification=True,
            )

        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        wait_for_all_resources_deletion(
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        config.switch_acm_ctx()
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESAPCE, resource_name=resource_name
        )
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        # Doing Relocate in below code
        primary_cluster_name_after_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace,
                discovered_apps=True,
                resource_name=resource_name,
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        logger.info("Relocating the workloads.....")
        dr_helpers.relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            workload_placement_name=resource_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=cnv_workloads[0],
            workload_instances_shared=cnv_workloads,
        )
        # Cleanup is handled as part of the Relocate function and checks are done below
        config.switch_to_cluster_by_name(primary_cluster_name_after_failover)
        wait_for_all_resources_deletion(
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        config.switch_acm_ctx()
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESAPCE, resource_name=resource_name
        )
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        # Verify resources creation on primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count * 2,
            cnv_workloads[0].workload_pod_count * 2,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=resource_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

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
