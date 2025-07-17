import logging
import pytest

from time import sleep

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
)
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    assign_drpolicy_for_discovered_vms_via_ui,
)
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.19")
class TestACMKubevirtDRIntergration:
    """
    Test ACM Kubevirt DR Integration by DR Protecting Discovered VMs via UI as Standalone and Shared Protection type
    and perform DR operation on them- RHSTOR-6413

    """

    @pytest.mark.polarion_id("OCS-xxxx")
    # TODO: Add Polarion ID when available
    def test_acm_kubevirt_dr_intergration_ui(
        self, discovered_apps_dr_workload_cnv, setup_acm_ui
    ):
        """
        DR operation on discovered VMs using Standalone and Shared Protection type

        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        logger.info("Deploy 1st CNV workload")
        cnv_workloads1 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared=False
        )

        # Second workload (uses same namespace as first)
        logger.info("Deploy 2nd CNV workload in the existing namespace")
        cnv_workloads2 = discovered_apps_dr_workload_cnv(
            pvc_vm=1, dr_protect=False, shared=True
        )

        cnv_workloads = cnv_workloads1 + cnv_workloads2

        cnv_workloads[0].discovered_apps_placement_name = (
            f"{assign_drpolicy_for_discovered_vms_via_ui}-drpc"
        )
        logger.info(
            f'Placement name is "{assign_drpolicy_for_discovered_vms_via_ui}-drpc"'
        )

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace,
                discovered_apps=True,
                resource_name=cnv_workloads[0].discovered_apps_placement_name,
            )
        )

        acm_obj = AcmAddClusters()

        logger.info("Navigate to Virtual machines page on the ACM console")
        assert cnv_workloads, "No discovered VM found"
        config.switch_acm_ctx()
        assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj, vms=[cnv_workloads[0].vm_name]
        )
        assign_drpolicy_for_discovered_vms_via_ui(
            acm_obj, vms=[cnv_workloads[1].vm_name], standalone=False
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count * 2,
            cnv_workloads[0].workload_pod_count * 2,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=self.vm_name,
            namespace=self.workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
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

        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
        )
        logger.info("Doing Cleanup Operations")
        for cnv_wl in cnv_workloads:
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=cnv_wl.discovered_apps_placement_name,
                old_primary=primary_cluster_name_before_failover,
                workload_namespace=cnv_workloads[0].workload_namespace,
                workload_dir=cnv_wl.workload_dir,
                vrg_name=cnv_wl.discovered_apps_placement_name,
                shared=True,
            )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count * 2,
            cnv_workloads[0].workload_pod_count * 2,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
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

        # Doing Relocate
        primary_cluster_name_after_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace,
                discovered_apps=True,
                resource_name=cnv_workloads[0].discovered_apps_placement_name,
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        logger.info("Running Relocate Steps")
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        dr_helpers.relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=cnv_workloads[0],
        )
        # Verify resources creation on primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
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
