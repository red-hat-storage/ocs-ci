import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mdr,
    tier1,
    turquoise_squad,
)
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers import (
    do_discovered_apps_cleanup_multi_ns,
    enable_fence,
    enable_unfence,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_fence_state,
    gracefully_reboot_ocp_nodes,
    verify_fence_state,
    wait_for_vrg_state,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status

logger = logging.getLogger(__name__)


@mdr
@tier1
@turquoise_squad
class TestFailoverRelocateDiscoveredAppsMultiNs:
    """
    Test failover and relocate with discovered apps spread across multiple namespaces.
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, discovered_apps_dr_workload):
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

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface"],
        argvalues=[
            pytest.param(False, constants.CEPHBLOCKPOOL, id="primary_up-rbd"),
            pytest.param(True, constants.CEPHBLOCKPOOL, id="primary_down-rbd"),
            pytest.param(False, constants.CEPHFILESYSTEM, id="primary_up-cephfs"),
            pytest.param(True, constants.CEPHFILESYSTEM, id="primary_down-cephfs"),
        ],
    )
    def test_failover_relocate_discovered_apps_multi_ns(
        self,
        primary_cluster_down,
        pvc_interface,
        discovered_apps_dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify failover and relocate with discovered apps spread across
        multiple namespaces using the MDR workflow.

        """
        self.primary_cluster_name = None

        workloads = discovered_apps_dr_workload(
            kubeobject=2, multi_ns=True, pvc_interface=pvc_interface
        )
        self.primary_cluster_name = get_current_primary_cluster_name(
            workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=workloads[0].discovered_apps_placement_name,
        )
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        secondary_cluster_name = get_current_secondary_cluster_name(
            workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=workloads[0].discovered_apps_placement_name,
        )

        node_objs = get_node_objs()
        primary_cluster_index = config.cur_index

        wait_time = 120
        logger.info(
            f"Wait for {wait_time} seconds before starting Failover of application"
        )
        time.sleep(wait_time)

        if primary_cluster_down:
            logger.info("Stopping primary cluster nodes")
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        enable_fence(drcluster_name=self.primary_cluster_name)
        verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_FENCE
        )

        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=workloads[0].discovered_apps_placement_name,
            old_primary=self.primary_cluster_name,
            skip_odf_cli_validation=primary_cluster_down,
        )

        logger.info("Doing Cleanup Operations")
        do_discovered_apps_cleanup_multi_ns(
            old_primary=self.primary_cluster_name,
            workload_instance=workloads,
        )

        for workload in workloads:
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_all_resources_creation(
                pvc_count=workload.workload_pvc_count,
                pod_count=workload.workload_pod_count,
                namespace=workload.workload_namespace,
                discovered_apps=True,
                skip_replication_resources=False,
                vrg_name=workload.discovered_apps_placement_name,
                skip_vrg_check=True,
            )

        config.switch_to_cluster_by_name(secondary_cluster_name)
        wait_for_vrg_state(
            vrg_state="primary",
            vrg_namespace=constants.DR_OPS_NAMESPACE,
            resource_name=workloads[0].discovered_apps_placement_name,
        )

        if primary_cluster_down:
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.info(
                f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            wait_for_nodes_status([node.name for node in node_objs])

        enable_unfence(drcluster_name=self.primary_cluster_name)
        verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_UNFENCE
        )

        gracefully_reboot_ocp_nodes(
            drcluster_name=self.primary_cluster_name, disable_eviction=True
        )

        logger.info(
            f"Wait for {wait_time} seconds before starting Relocate of application"
        )
        time.sleep(wait_time)

        primary_cluster_name_after_failover = get_current_primary_cluster_name(
            workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=workloads[0].discovered_apps_placement_name,
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=workloads[0].discovered_apps_placement_name,
        )

        dr_helpers.relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=workloads[0].workload_namespace,
            workload_placement_name=workloads[0].discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=workloads,
            multi_ns=True,
        )

        for workload in workloads:
            config.switch_to_cluster_by_name(self.primary_cluster_name)
            dr_helpers.wait_for_all_resources_creation(
                pvc_count=workload.workload_pvc_count,
                pod_count=workload.workload_pod_count,
                namespace=workload.workload_namespace,
                discovered_apps=True,
                skip_replication_resources=False,
                vrg_name=workload.discovered_apps_placement_name,
                skip_vrg_check=True,
            )

        config.switch_to_cluster_by_name(self.primary_cluster_name)
        wait_for_vrg_state(
            vrg_state="primary",
            vrg_namespace=constants.DR_OPS_NAMESPACE,
            resource_name=workloads[0].discovered_apps_placement_name,
        )
