import logging
import pytest
import time

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mdr,
    tier2,
    turquoise_squad,
)
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.helpers.dr_helpers import (
    do_discovered_apps_cleanup_multi_ns,
    enable_fence,
    enable_unfence,
    failover,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_fence_state,
    gracefully_reboot_ocp_nodes,
    mdr_post_failover_check,
    relocate,
    verify_cluster_data_protected_status,
    verify_fence_state,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    wait_for_cnv_workload,
    wait_for_vrg_state,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


@mdr
@tier2
@turquoise_squad
class TestCnvFailoverRelocateDiscoveredAppsMultiNs:
    """
    Test failover and relocate with CNV discovered apps spread across multiple
    namespaces on metro-dr-pc.
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, discovered_apps_dr_workload_cnv):
        """
        Teardown function: If fenced, un-fence the cluster and reboot nodes.
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

        self.primary_cluster_name = None
        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            pytest.param(False, id="primary_up"),
            pytest.param(True, id="primary_down"),
        ],
    )
    def test_cnv_failover_relocate_discovered_apps_multi_ns(
        self,
        primary_cluster_down,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Verify CNV discovered app failover and relocate with one shared DRPC
        protecting VMs across multiple namespaces using the MDR fencing workflow.
        """
        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        CNVInstaller().download_and_extract_virtctl_binary()

        logger.test_step("Deploy CNV discovered apps across multiple namespaces")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=2, multi_ns=True, vm_type=constants.VM_VOLUME_PVC
        )
        resource_name = cnv_workloads[0].discovered_apps_placement_name
        primary_namespace = cnv_workloads[0].workload_namespace

        self.primary_cluster_name = get_current_primary_cluster_name(
            primary_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        secondary_cluster_name = get_current_secondary_cluster_name(
            primary_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )
        node_objs = get_node_objs()
        primary_cluster_index = config.cur_index

        for cnv_wl in cnv_workloads:
            md5sum_original.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[0],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        verify_cluster_data_protected_status(
            workload_type=constants.DISCOVERED_APPS,
            namespace=constants.DR_OPS_NAMESPACE,
            workload_placement_name=resource_name,
            drpc_name=resource_name,
        )

        wait_time = 120
        logger.test_step(
            f"Wait for {wait_time} seconds before starting Failover of application"
        )
        time.sleep(wait_time)

        if primary_cluster_down:
            logger.test_step("Stopping primary cluster nodes")
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        enable_fence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_FENCE
        ), f"DR cluster {self.primary_cluster_name} reached {constants.ACTION_FENCE} state"

        logger.test_step("Failover multi-ns CNV discovered apps to secondary")
        failover(
            failover_cluster=secondary_cluster_name,
            namespace=primary_namespace,
            discovered_apps=True,
            workload_placement_name=resource_name,
            old_primary=self.primary_cluster_name,
            skip_odf_cli_validation=primary_cluster_down,
        )

        logger.test_step("Verify workloads on secondary after failover")
        for cnv_wl in cnv_workloads:
            config.switch_to_cluster_by_name(secondary_cluster_name)
            wait_for_all_resources_creation(
                pvc_count=cnv_wl.workload_pvc_count,
                pod_count=cnv_wl.workload_pod_count,
                namespace=cnv_wl.workload_namespace,
                discovered_apps=True,
                skip_replication_resources=False,
                vrg_name=resource_name,
                skip_vrg_check=True,
            )
            wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        wait_for_vrg_state(
            vrg_state="primary",
            vrg_namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name,
        )

        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
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

        config.switch_to_cluster_by_name(self.primary_cluster_name)
        if primary_cluster_down:
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.test_step(
                f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            wait_for_nodes_status([node.name for node in node_objs])

        logger.test_step("Cleanup discovered apps on recovered primary")
        do_discovered_apps_cleanup_multi_ns(
            old_primary=self.primary_cluster_name,
            workload_instance=cnv_workloads,
            skip_resource_deletion_verification=True,
        )

        for cnv_wl in cnv_workloads:
            mdr_post_failover_check(namespace=cnv_wl.workload_namespace)

        enable_unfence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_UNFENCE
        ), f"DR cluster {self.primary_cluster_name} reached {constants.ACTION_UNFENCE} state"

        gracefully_reboot_ocp_nodes(
            drcluster_name=self.primary_cluster_name, disable_eviction=True
        )

        for cnv_wl in cnv_workloads:
            wait_for_all_resources_deletion(
                namespace=cnv_wl.workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
                skip_vrg_check=True,
            )

        wait_for_vrg_state(
            vrg_state="secondary",
            vrg_namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name,
        )

        with config.RunWithAcmConfigContext():
            drpc_obj = DRPC(
                namespace=constants.DR_OPS_NAMESPACE,
                resource_name=resource_name,
            )
            drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        logger.test_step(
            f"Wait for {wait_time} seconds before starting Relocate of application"
        )
        time.sleep(wait_time)

        primary_cluster_name_after_failover = get_current_primary_cluster_name(
            primary_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            primary_namespace,
            discovered_apps=True,
            resource_name=resource_name,
        )

        logger.test_step("Relocate multi-ns CNV discovered apps back to preferred")
        relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=primary_namespace,
            workload_placement_name=resource_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=cnv_workloads,
            multi_ns=True,
        )

        logger.test_step("Verify workloads after relocate")
        for cnv_wl in cnv_workloads:
            config.switch_to_cluster_by_name(self.primary_cluster_name)
            wait_for_all_resources_creation(
                pvc_count=cnv_wl.workload_pvc_count,
                pod_count=cnv_wl.workload_pod_count,
                namespace=cnv_wl.workload_namespace,
                discovered_apps=True,
                skip_replication_resources=False,
                vrg_name=resource_name,
                skip_vrg_check=True,
            )
            wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        wait_for_vrg_state(
            vrg_state="primary",
            vrg_namespace=constants.DR_OPS_NAMESPACE,
            resource_name=resource_name,
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

        with config.RunWithAcmConfigContext():
            drpc_obj = DRPC(
                namespace=constants.DR_OPS_NAMESPACE,
                resource_name=resource_name,
            )
            drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
