import logging
from time import sleep

import pytest

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import turquoise_squad, rdr
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@pytest.mark.polarion_id("OCS-6266")
class TestCNVFailoverAndRelocateWithDiscoveredApps:
    """
    Test CNV Failover and Relocate with Discovered Apps

    """

    def test_cnv_failover_and_relocate_discovered_apps(
        self, discovered_apps_dr_workload_cnv
    ):
        """
        Tests to verify cnv application failover and Relocate with Discovered Apps
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is UP
            2) Relocate back to primary

        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        cnv_workloads = discovered_apps_dr_workload_cnv(pvc_vm=1)

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace, discovered_apps=True
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace, discovered_apps=True
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

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace, discovered_apps=True
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
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
            workload_namespace=cnv_workloads[0].workload_namespace,
            workload_dir=cnv_workloads[0].workload_dir,
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
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
                cnv_workloads[0].workload_namespace, discovered_apps=True
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace, discovered_apps=True
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace, discovered_apps=True
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
