import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

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
    wait_for_vrg_state,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC

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
        logger.test_step(
            f"Wait for {wait_time} seconds before starting Failover of application"
        )
        time.sleep(wait_time)

        set_current_primary_cluster_context(
            self.wl_namespace,
            discovered_apps=True,
            resource_name=self.resource_name,
        )
        node_objs = get_node_objs()
        if primary_cluster_down:
            logger.test_step("Stopping primary cluster nodes")
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

        wait_for_vrg_state(
            vrg_state="primary",
            vrg_namespace=constants.DR_OPS_NAMESPACE,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        logger.test_step("Validate data integrity after failover")
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )

        logger.test_step("Add data to VMs post-failover")
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
                "Post-failover checksum: file=%s, vm=%s, md5=%s",
                vm_filepaths[1],
                cnv_wl.workload_name,
                md5sum,
            )

        config.switch_to_cluster_by_name(self.primary_cluster_name)
        if primary_cluster_down:
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.test_step(
                f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            wait_for_nodes_status([node.name for node in node_objs])

        do_discovered_apps_cleanup(
            drpc_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=self.primary_cluster_name,
            workload_namespace=self.wl_namespace,
            workload_dir=cnv_workloads[0].workload_dir,
            vrg_name=self.resource_name,
            skip_resource_deletion_verification=True,
        )

        mdr_post_failover_check(namespace=self.wl_namespace)

        enable_unfence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_UNFENCE
        ), f"DR cluster {self.primary_cluster_name} reached {constants.ACTION_UNFENCE} state"

        gracefully_reboot_ocp_nodes(
            drcluster_name=self.primary_cluster_name, disable_eviction=True
        )

        wait_for_all_resources_deletion(
            namespace=self.wl_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
            skip_vrg_check=True,
        )

        wait_for_vrg_state(
            vrg_state="secondary",
            vrg_namespace=constants.DR_OPS_NAMESPACE,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        with config.RunWithAcmConfigContext():
            drpc_obj = DRPC(
                namespace=constants.DR_OPS_NAMESPACE,
                resource_name=cnv_workloads[0].discovered_apps_placement_name,
            )
            drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

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
        wait_for_all_resources_deletion(
            namespace=self.wl_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
            skip_vrg_check=True,
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
            drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        logger.test_step("Add data to VMs post-relocate")
        for cnv_wl in cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )

    @pytest.mark.polarion_id(polarion_id_cnv_primary_down)
    def test_cnv_multi_recipe_failover_relocate_discovered_apps(
        self,
        nodes_multicluster,
        discovered_apps_dr_workload_cnv,
        node_restart_teardown,
    ):
        """
        Concurrent failover and relocate of multiple CNV discovered apps with
        primary down.

        Deploys one VM with kubeobject protection and two with recipe protection
        (with and without checkhooks), then fails over and relocates them
        concurrently.
        """
        self.primary_cluster_name = None
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]
        md5sum_original = {}
        md5sum_failover = {}

        CNVInstaller().download_and_extract_virtctl_binary()

        logger.test_step(
            "Deploy kubeobject and recipe (with and without checkhooks) CNV apps"
        )
        # Fixture returns the accumulated instance list; take only the newly
        # created workload from each factory call (pvc_vm=1).
        cnv_workloads = [
            discovered_apps_dr_workload_cnv(
                pvc_vm=1, vm_type=constants.VM_VOLUME_PVC, recipe=False
            )[-1],
            discovered_apps_dr_workload_cnv(
                pvc_vm=1,
                vm_type=constants.VM_VOLUME_PVC,
                recipe=True,
                checkhooks=False,
            )[-1],
            discovered_apps_dr_workload_cnv(
                pvc_vm=1,
                vm_type=constants.VM_VOLUME_PVC,
                recipe=True,
                checkhooks=True,
            )[-1],
        ]
        assert (
            len(cnv_workloads) == 3
        ), f"Expected 3 CNV workloads, got {len(cnv_workloads)}"

        primary_resource = cnv_workloads[0].discovered_apps_placement_name
        primary_namespace = cnv_workloads[0].workload_namespace
        set_current_primary_cluster_context(
            primary_namespace,
            discovered_apps=True,
            resource_name=primary_resource,
        )
        primary_cluster_index = config.cur_index
        self.primary_cluster_name = get_current_primary_cluster_name(
            primary_namespace,
            discovered_apps=True,
            resource_name=primary_resource,
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            primary_namespace,
            discovered_apps=True,
            resource_name=primary_resource,
        )

        for cnv_wl in cnv_workloads:
            md5sum_original[cnv_wl.discovered_apps_placement_name] = run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[0],
                username=cnv_wl.vm_username,
                verify=True,
            )
            verify_cluster_data_protected_status(
                workload_type=constants.DISCOVERED_APPS,
                namespace=constants.DR_OPS_NAMESPACE,
                workload_placement_name=cnv_wl.discovered_apps_placement_name,
                drpc_name=cnv_wl.discovered_apps_placement_name,
            )

        wait_time = 120
        logger.test_step(
            f"Wait for {wait_time} seconds before starting Failover of applications"
        )
        time.sleep(wait_time)

        set_current_primary_cluster_context(
            primary_namespace,
            discovered_apps=True,
            resource_name=primary_resource,
        )
        node_objs = get_node_objs()
        logger.test_step("Stopping primary cluster nodes")
        nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        enable_fence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_FENCE
        ), f"DR cluster {self.primary_cluster_name} reached {constants.ACTION_FENCE} state"

        logger.test_step(
            f"Concurrently failover {len(cnv_workloads)} CNV discovered apps"
        )

        config.switch_acm_ctx()
        failover_results = []
        with ThreadPoolExecutor() as executor:
            for cnv_wl in cnv_workloads:
                failover_results.append(
                    executor.submit(
                        failover,
                        failover_cluster=secondary_cluster_name,
                        namespace=cnv_wl.workload_namespace,
                        discovered_apps=True,
                        workload_placement_name=cnv_wl.discovered_apps_placement_name,
                        old_primary=self.primary_cluster_name,
                        skip_odf_cli_validation=True,
                    )
                )
                time.sleep(5)

        for result in failover_results:
            result.result()

        logger.test_step("Verify workloads on secondary after concurrent failover")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for cnv_wl in cnv_workloads:
            resource_name = cnv_wl.discovered_apps_placement_name
            wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
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
                [cnv_wl],
                vm_filepaths[0],
                [md5sum_original[resource_name]],
                "Failover",
            )
            md5sum_failover[resource_name] = run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[1],
                username=cnv_wl.vm_username,
                verify=True,
            )

        config.switch_to_cluster_by_name(self.primary_cluster_name)
        nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
        logger.test_step(
            f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
        )
        time.sleep(wait_time)
        wait_for_nodes_status([node.name for node in node_objs])

        logger.test_step("Cleanup discovered apps on recovered primary")
        for cnv_wl in cnv_workloads:
            do_discovered_apps_cleanup(
                drpc_name=cnv_wl.discovered_apps_placement_name,
                old_primary=self.primary_cluster_name,
                workload_namespace=cnv_wl.workload_namespace,
                workload_dir=cnv_wl.workload_dir,
                vrg_name=cnv_wl.discovered_apps_placement_name,
                skip_resource_deletion_verification=True,
            )

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
                vrg_name=cnv_wl.discovered_apps_placement_name,
                skip_vrg_check=True,
            )
            wait_for_vrg_state(
                vrg_state="secondary",
                vrg_namespace=constants.DR_OPS_NAMESPACE,
                resource_name=cnv_wl.discovered_apps_placement_name,
            )
            with config.RunWithAcmConfigContext():
                drpc_obj = DRPC(
                    namespace=constants.DR_OPS_NAMESPACE,
                    resource_name=cnv_wl.discovered_apps_placement_name,
                )
                drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        logger.test_step(
            f"Wait for {wait_time} seconds before starting Relocate of applications"
        )
        time.sleep(wait_time)

        logger.test_step(
            f"Concurrently relocate {len(cnv_workloads)} CNV discovered apps"
        )
        # vm_auto_cleanup=True skips in-helper cleanup so concurrent relocate
        # does not race on global config switches inside do_discovered_apps_cleanup.
        config.switch_acm_ctx()
        relocate_results = []
        with ThreadPoolExecutor() as executor:
            for cnv_wl in cnv_workloads:
                relocate_results.append(
                    executor.submit(
                        relocate,
                        preferred_cluster=self.primary_cluster_name,
                        namespace=cnv_wl.workload_namespace,
                        workload_placement_name=cnv_wl.discovered_apps_placement_name,
                        discovered_apps=True,
                        old_primary=secondary_cluster_name,
                        workload_instance=cnv_wl,
                        vm_auto_cleanup=True,
                    )
                )
                time.sleep(5)

        for result in relocate_results:
            result.result()

        logger.test_step("Cleanup discovered apps on secondary after relocate")
        for cnv_wl in cnv_workloads:
            do_discovered_apps_cleanup(
                drpc_name=cnv_wl.discovered_apps_placement_name,
                old_primary=secondary_cluster_name,
                workload_namespace=cnv_wl.workload_namespace,
                workload_dir=cnv_wl.workload_dir,
                vrg_name=cnv_wl.discovered_apps_placement_name,
                skip_resource_deletion_verification=True,
            )

        logger.test_step("Verify workloads after concurrent relocate")
        for cnv_wl in cnv_workloads:
            resource_name = cnv_wl.discovered_apps_placement_name
            config.switch_to_cluster_by_name(secondary_cluster_name)
            wait_for_all_resources_deletion(
                namespace=cnv_wl.workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
                skip_vrg_check=True,
            )
            config.switch_to_cluster_by_name(self.primary_cluster_name)
            wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
                discovered_apps=True,
                vrg_name=resource_name,
            )
            wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )
            validate_data_integrity_vm(
                [cnv_wl],
                vm_filepaths[0],
                [md5sum_original[resource_name]],
                "Relocate",
            )
            validate_data_integrity_vm(
                [cnv_wl],
                vm_filepaths[1],
                [md5sum_failover[resource_name]],
                "Relocate",
            )
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
