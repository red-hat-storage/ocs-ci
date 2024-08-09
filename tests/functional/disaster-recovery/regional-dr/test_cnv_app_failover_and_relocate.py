import logging
from time import sleep

import pytest

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@tier1
@turquoise_squad
class TestCnvApplicationRDR:
    """
    Includes tests related to CNV workloads on RDR environment.
    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            pytest.param(
                False,
                id="primary_up",
            ),
            pytest.param(
                True,
                id="primary_down",
            ),
        ],
    )
    def test_cnv_app_failover_and_relocate(
        self,
        primary_cluster_down,
        cnv_dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify CNV workload deployment using RBD PVC (Both Subscription and ApplicationSet based) and
        failover/relocate between managed clusters.

        """
        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()

        # Create CNV applications (Subscription and ApplicationSet)
        cnv_workloads = cnv_dr_workload(
            num_of_vm_subscription=1, num_of_vm_appset_push=1, num_of_vm_appset_pull=1
        )
        wl_namespace = cnv_workloads[0].workload_namespace

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            wl_namespace, cnv_workloads[0].workload_type
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index

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
            wl_namespace, cnv_workloads[0].workload_type
        )
        logger.info(f"Waiting for {scheduling_interval} minutes for sync to complete")
        sleep(scheduling_interval * 60)

        # Shutting down primary managed cluster nodes
        primary_cluster_nodes = get_node_objs()
        if primary_cluster_down:
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            wl_namespace, cnv_workloads[0].workload_type
        )

        # Failover the applications to secondary managed cluster
        for cnv_wl in cnv_workloads:
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=cnv_wl.cnv_workload_placement_name
                if cnv_wl.workload_type != constants.SUBSCRIPTION
                else None,
            )

        # Verify VM and its resources on secondary managed cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
            )
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
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

        # Verify resources are deleted from primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        # Start nodes if cluster is down
        if primary_cluster_down:
            logger.info(
                f"Waiting for {scheduling_interval} minutes before starting nodes of primary cluster: "
                f"{primary_cluster_name}"
            )
            sleep(scheduling_interval * 60)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
            logger.info("Wait for 180 seconds for pods to stabilize")
            sleep(180)
            logger.info(
                "Wait for all the pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking for Ceph Health OK")
            ceph_health_check()

        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_deletion(cnv_wl.workload_namespace)

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(
                [cnv_wl.workload_pvc_count for cnv_wl in cnv_workloads]
            )
        )

        logger.info(f"Waiting for {scheduling_interval} minutes for sync to complete")
        sleep(scheduling_interval * 60)

        # Relocate the applications back to primary managed cluster
        for cnv_wl in cnv_workloads:
            dr_helpers.relocate(
                preferred_cluster=primary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=cnv_wl.cnv_workload_placement_name
                if cnv_wl.workload_type != constants.SUBSCRIPTION
                else None,
            )

        # Verify resources deletion from secondary managed cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_deletion(cnv_wl.workload_namespace)

        # Verify resources creation on primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
            )
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(
                [cnv_wl.workload_pvc_count for cnv_wl in cnv_workloads]
            )
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
