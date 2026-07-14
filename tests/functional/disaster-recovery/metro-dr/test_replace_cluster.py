import logging
import pytest
import time

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import turquoise_squad, mdr, tier4a
from ocs_ci.helpers.dr_helpers import get_active_acm_index
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs import constants
from ocs_ci.helpers.dr_helpers import (
    replace_cluster,
    enable_fence,
    enable_unfence,
    get_fence_state,
    failover,
    relocate,
    set_current_primary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    wait_for_all_resources_creation,
    gracefully_reboot_ocp_nodes,
    wait_for_all_resources_deletion,
    set_current_secondary_cluster_context,
)

logger = logging.getLogger(__name__)


@mdr
@tier4a
@turquoise_squad
class TestReplaceCluster:
    """
    Test for Recovery of replacement cluster
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
                    self.primary_cluster_name, disable_eviction=True
                )

        request.addfinalizer(finalizer)

    def test_replace_cluster(
        self,
        nodes_multicluster,
        dr_workload,
    ):
        """
        Tests to verify Recovery of replacement cluster
        """

        # Deploy Subscription based application
        sub = dr_workload(num_of_subscription=1)[0]
        self.namespace = sub.workload_namespace
        self.workload_type = sub.workload_type
        """
        # Deploy AppSet based application
        appset = dr_workload(num_of_subscription=0, num_of_appset=1)[1]
        """
        # Workloads list
        workload = [sub]

        # Create application on Primary managed cluster
        set_current_primary_cluster_context(self.namespace)
        primary_cluster_index = config.cur_index
        node_objs = get_node_objs()
        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=self.namespace
        )

        # Stop primary cluster nodes
        logger.info("Stopping primary cluster nodes")
        nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        # Fenced the primary managed cluster
        enable_fence(drcluster_name=self.primary_cluster_name)

        # Application Failover to Secondary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(self.namespace)
        failover_results = []
        with ThreadPoolExecutor() as executor:
            for wl in workload:
                failover_results.append(
                    executor.submit(
                        failover,
                        failover_cluster=secondary_cluster_name,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        switch_ctx=get_active_acm_index(),
                        workload_placement_name=(
                            wl.appset_placement_name
                            if wl.workload_type != constants.SUBSCRIPTION
                            else None
                        ),
                    )
                )
                time.sleep(10)

        # Wait for failover results
        for fl in failover_results:
            fl.result()

        # Verify application are running in other managedcluster
        # And not in previous cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workload:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Validate data integrity
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workload:
            validate_data_integrity(wl.workload_namespace)

        # Unfence cluster
        enable_unfence(self.primary_cluster_name)

        # Replace cluster configuration
        replace_cluster(workload, self.primary_cluster_name, secondary_cluster_name)

        # Application Relocate to Primary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(self.namespace)
        relocate_results = []
        with ThreadPoolExecutor() as executor:
            for wl in workload:
                relocate_results.append(
                    executor.submit(
                        relocate,
                        preferred_cluster=secondary_cluster_name,
                        namespace=wl.workload_namespace,
                        switch_ctx=get_active_acm_index(),
                        workload_placement_name=(
                            wl.appset_placement_name
                            if wl.workload_type != constants.SUBSCRIPTION
                            else None
                        ),
                    )
                )
                time.sleep(5)

        # Wait for relocate results
        for rl in relocate_results:
            rl.result()

        # Verify resources deletion from  secondary cluster
        for wl in workload:
            set_current_secondary_cluster_context(
                wl.workload_namespace, wl.workload_type
            )
            wait_for_all_resources_deletion(wl.workload_namespace)

            # Verify resources creation on preferredCluster
            set_current_primary_cluster_context(wl.workload_namespace, wl.workload_type)
            self.primary_cluster_name = get_current_primary_cluster_name(self.namespace)
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

            # Validate data integrityq
            set_current_primary_cluster_context(wl.workload_namespace, wl.workload_type)
            validate_data_integrity(wl.workload_namespace)
