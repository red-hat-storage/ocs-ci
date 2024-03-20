import logging
import pytest
import time

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework.pytest_customization.marks import tier2
from ocs_ci.framework import config
from ocs_ci.helpers.cnv_helpers import run_dd_io, cal_md5sum_vm
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.helpers.dr_helpers import (
    enable_fence,
    enable_unfence,
    get_fence_state,
    failover,
    relocate,
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    gracefully_reboot_ocp_nodes,
    wait_for_cnv_workload,
)

from ocs_ci.framework.pytest_customization.marks import turquoise_squad

logger = logging.getLogger(__name__)

polarion_id_cnv_primary_up = "OCS-5413"
polarion_id_cnv_primary_down = "OCS-5414"


@tier2
@turquoise_squad
class TestCnvApplicationMDR:
    """
    Includes tests related to CNV workloads on MDR environment.
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, cnv_dr_workload):
        """
        Teardown function: If fenced, un-fence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name
                and get_fence_state(self.primary_cluster_name) == "Fenced"
            ):
                enable_unfence(self.primary_cluster_name)
                gracefully_reboot_ocp_nodes(
                    drcluster_name=self.primary_cluster_name, disable_eviction=True
                )

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            pytest.param(
                False,
                marks=pytest.mark.polarion_id(polarion_id_cnv_primary_up),
                id="primary_up",
            ),
            pytest.param(
                True,
                marks=pytest.mark.polarion_id(polarion_id_cnv_primary_down),
                id="primary_down",
            ),
        ],
    )
    def test_cnv_app_failover_relocate(
        self,
        primary_cluster_down,
        nodes_multicluster,
        cnv_dr_workload,
        node_restart_teardown,
    ):
        """
        Tests to verify CNV based subscription and appset application deployment and
        fail-over/relocate between managed clusters.

        """
        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()

        # Create CNV applications(appset+sub)
        cnv_workloads = cnv_dr_workload(num_of_vm_subscription=1, num_of_vm_appset=1)
        self.wl_namespace = cnv_workloads[0].workload_namespace

        set_current_primary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        primary_cluster_index = config.cur_index

        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=self.wl_namespace, workload_type=cnv_workloads[0].workload_type
        )

        # Creating a file on VM and calculating its MD5sum
        for cnv_wl in cnv_workloads:
            md5sum_original.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[0],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        # Shutting down primary cluster nodes
        node_objs = get_node_objs()
        if primary_cluster_down:
            logger.info("Stopping primary cluster nodes")
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        # Fence the primary managed cluster
        enable_fence(drcluster_name=self.primary_cluster_name)

        secondary_cluster_name = get_current_secondary_cluster_name(
            self.wl_namespace, cnv_workloads[0].workload_type
        )

        # Fail-over the apps to secondary managed cluster
        for cnv_wl in cnv_workloads:
            failover(
                failover_cluster=secondary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=cnv_wl.cnv_workload_placement_name
                if cnv_wl.workload_type != constants.SUBSCRIPTION
                else None,
            )

        # Verify VM and its resources in secondary managed cluster
        set_current_primary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        for cnv_wl in cnv_workloads:
            wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
            )
            wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        for cnv_wl in cnv_workloads:
            md5sum_failover.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[1],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        # Validating data integrity after failing-over VMs to secondary managed cluster
        for count, cnv_wl in enumerate(cnv_workloads):
            md5sum_fail_out = cal_md5sum_vm(
                cnv_wl.vm_obj, file_path=vm_filepaths[0], username=cnv_wl.vm_username
            )
            logger.info(
                f"Validating MD5sum of file {vm_filepaths[0]} on VM: {cnv_wl.workload_name} after FailOver"
            )
            assert (
                md5sum_original[count] == md5sum_fail_out
            ), "Failed: MD5 comparison after FailOver"

        # Start nodes if cluster is down
        wait_time = 120
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} seconds before starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.info(
                f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            wait_for_nodes_status([node.name for node in node_objs])

        # Verify application are deleted from old managed cluster
        set_current_secondary_cluster_context(
            cnv_workloads[0].workload_namespace, cnv_workloads[0].workload_type
        )
        for cnv_wl in cnv_workloads:
            wait_for_all_resources_deletion(cnv_wl.workload_namespace)

        # Un-fence the managed cluster
        enable_unfence(drcluster_name=self.primary_cluster_name)

        # Reboot the nodes after unfenced
        gracefully_reboot_ocp_nodes(
            drcluster_name=self.primary_cluster_name, disable_eviction=True
        )

        secondary_cluster_name = get_current_secondary_cluster_name(
            self.wl_namespace, cnv_workloads[0].workload_type
        )

        # Relocate cnv apps back to primary managed cluster
        for cnv_wl in cnv_workloads:
            relocate(
                preferred_cluster=secondary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=cnv_wl.cnv_workload_placement_name
                if cnv_wl.workload_type != constants.SUBSCRIPTION
                else None,
            )

        set_current_secondary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        # Verify resources deletion from previous primary or current secondary cluster
        for cnv_wl in cnv_workloads:
            wait_for_all_resources_deletion(cnv_wl.workload_namespace)

        # Verify resource creation and VM status on relocated cluster
        set_current_primary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        for cnv_wl in cnv_workloads:
            wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
            )
            wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        # Validating data integrity(file1) after relocating VMs back to primary managed cluster
        for count, cnv_wl in enumerate(cnv_workloads):
            md5sum_org = cal_md5sum_vm(
                cnv_wl.vm_obj, file_path=vm_filepaths[0], username=cnv_wl.vm_username
            )
            logger.info(
                f"Validating MD5sum of file {vm_filepaths[0]} on VM: {cnv_wl.workload_name} after Relocate"
            )
            assert (
                md5sum_original[count] == md5sum_org
            ), f"Failed: MD5 comparison of {vm_filepaths[0]} after relocation"

        # Creating a file(file3) post relocate
        for cnv_wl in cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )

        # Validating data integrity(file2) after relocating VMs back to primary managed cluster
        for count, cnv_wl in enumerate(cnv_workloads):
            md5sum_fail = cal_md5sum_vm(
                cnv_wl.vm_obj, file_path=vm_filepaths[1], username=cnv_wl.vm_username
            )
            logger.info(
                f"Validating MD5sum of file {vm_filepaths[1]} on VM: {cnv_wl.workload_name} after Relocate"
            )
            assert (
                md5sum_failover[count] == md5sum_fail
            ), f"Failed: MD5 comparison of {vm_filepaths[1]}after relocation"
