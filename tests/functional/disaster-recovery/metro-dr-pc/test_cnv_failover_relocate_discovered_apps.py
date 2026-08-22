import logging
import pytest
import time

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import mdr, tier2, turquoise_squad
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.helpers.dr_helpers import (
    do_discovered_apps_cleanup,
    enable_fence,
    enable_unfence,
    failover,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_fence_state,
    gracefully_reboot_ocp_nodes,
    mdr_post_failover_check,
    relocate,
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    verify_cluster_data_protected_status,
    verify_fence_state,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    wait_for_cnv_workload,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status

logger = logging.getLogger(__name__)

polarion_id_cnv_primary_up = "OCS-XXXX"
polarion_id_cnv_primary_down = "OCS-XXXX"


@mdr
@tier2
@turquoise_squad
class TestCnvDiscoveredAppsMDR:
    """
    Includes tests related to CNV discovered application workloads on MDR environment.
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, discovered_apps_dr_workload_cnv):
        """
        Teardown function: If fenced, un-fence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name is not None
                and get_fence_state(self.primary_cluster_name) == "Fenced"
            ):
                enable_unfence(self.primary_cluster_name)
                gracefully_reboot_ocp_nodes(
                    drcluster_name=self.primary_cluster_name, disable_eviction=True
                )

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["vm_type", "primary_cluster_down"],
        argvalues=[
            pytest.param(
                constants.VM_VOLUME_PVC,
                False,
                marks=pytest.mark.polarion_id(polarion_id_cnv_primary_up),
                id="primary_up_vm-pvc",
            ),
            pytest.param(
                constants.VM_VOLUME_PVC,
                True,
                marks=pytest.mark.polarion_id(polarion_id_cnv_primary_down),
                id="primary_down_vm-pvc",
            ),
            pytest.param(
                constants.VM_VOLUME_DVT,
                False,
                marks=pytest.mark.polarion_id(polarion_id_cnv_primary_up),
                id="primary_up_vm-dvt",
            ),
            pytest.param(
                constants.VM_VOLUME_DVT,
                True,
                marks=pytest.mark.polarion_id(polarion_id_cnv_primary_down),
                id="primary_down_vm-dvt",
            ),
        ],
    )
    def test_cnv_failover_relocate_discovered_apps(
        self,
        vm_type,
        primary_cluster_down,
        nodes_multicluster,
        discovered_apps_dr_workload_cnv,
        node_restart_teardown,
    ):
        """
        Tests to verify CNV discovered application failover and relocate between
        managed clusters using the MDR fencing and unfencing workflow.
        """
        md5sum_original = []
        md5sum_failover = []
        self.primary_cluster_name = None
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        CNVInstaller().download_and_extract_virtctl_binary()

        cnv_workloads = discovered_apps_dr_workload_cnv(pvc_vm=1, vm_type=vm_type)
        self.wl_namespace = cnv_workloads[0].workload_namespace
        self.resource_name = cnv_workloads[0].discovered_apps_placement_name

        set_current_primary_cluster_context(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )
        primary_cluster_index = config.cur_index

        self.primary_cluster_name = get_current_primary_cluster_name(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )

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

        verify_cluster_data_protected_status(
            workload_type=constants.DISCOVERED_APPS,
            namespace=constants.DR_OPS_NAMESPACE,
            workload_placement_name=self.resource_name,
            drpc_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        wait_time = 120
        logger.info(
            f"Wait for {wait_time} seconds before starting Failover of application"
        )
        time.sleep(wait_time)

        node_objs = get_node_objs()
        if primary_cluster_down:
            logger.info("Stopping primary cluster nodes")
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        enable_fence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_FENCE
        ), f"DR cluster {self.primary_cluster_name} reached {constants.ACTION_FENCE} state"

        secondary_cluster_name = get_current_secondary_cluster_name(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )

        failover(
            failover_cluster=secondary_cluster_name,
            namespace=self.wl_namespace,
            discovered_apps=True,
            workload_placement_name=self.resource_name,
            old_primary=self.primary_cluster_name,
            skip_odf_cli_validation=primary_cluster_down,
        )

        set_current_primary_cluster_context(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )
        wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            self.wl_namespace,
            discovered_apps=True,
            vrg_name=self.resource_name,
        )
        wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=self.wl_namespace,
            phase=constants.STATUS_RUNNING,
        )

        if not primary_cluster_down:
            set_current_secondary_cluster_context(
                self.wl_namespace,
                discovered_apps=True,
                resource_name=self.resource_name,
            )
            mdr_post_failover_check(namespace=self.wl_namespace)

        set_current_primary_cluster_context(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
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

        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "FailOver"
        )

        if primary_cluster_down:
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.info(
                f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            wait_for_nodes_status([node.name for node in node_objs])

            set_current_secondary_cluster_context(
                self.wl_namespace,
                discovered_apps=True,
                resource_name=self.resource_name,
            )
            mdr_post_failover_check(namespace=self.wl_namespace)

        enable_unfence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_UNFENCE
        ), f"DR cluster {self.primary_cluster_name} reached {constants.ACTION_UNFENCE} state"

        gracefully_reboot_ocp_nodes(
            drcluster_name=self.primary_cluster_name, disable_eviction=True
        )

        do_discovered_apps_cleanup(
            drpc_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=self.primary_cluster_name,
            workload_namespace=self.wl_namespace,
            workload_dir=cnv_workloads[0].workload_dir,
            vrg_name=self.resource_name,
        )

        secondary_cluster_name = get_current_secondary_cluster_name(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )
        current_primary_cluster_name = get_current_primary_cluster_name(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )

        relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=self.wl_namespace,
            workload_placement_name=self.resource_name,
            discovered_apps=True,
            old_primary=current_primary_cluster_name,
            workload_instance=cnv_workloads[0],
        )

        set_current_secondary_cluster_context(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )
        wait_for_all_resources_deletion(self.wl_namespace)

        set_current_primary_cluster_context(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )
        wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            self.wl_namespace,
            discovered_apps=True,
            vrg_name=self.resource_name,
        )
        wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=self.wl_namespace,
            phase=constants.STATUS_RUNNING,
        )

        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[1], md5sum_failover, "Relocate"
        )

        for cnv_wl in cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )
