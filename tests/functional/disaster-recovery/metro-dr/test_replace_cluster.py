import logging
import pytest
import time

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework import config
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs import constants

from ocs_ci.framework.pytest_customization.marks import tier1, turquoise_squad
from ocs_ci.helpers.dr_helpers import get_active_acm_index
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.helpers.dr_helpers_ui import (
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
)
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
)

logger = logging.getLogger(__name__)


@tier1
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
                    self.wl_namespace, self.primary_cluster_name
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

        # Failover apps followed by unfence

        acm_obj = AcmAddClusters()

        # Deploy Subscription based application
        sub = dr_workload(num_of_subscription=1, switch_ctx=get_active_acm_index())[0]
        self.namespace = sub.workload_namespace
        self.workload_type = sub.workload_type

        """
        # Deploy AppSet based application
        appset = dr_workload(
            num_of_subscription=0, num_of_appset=1, switch_ctx=get_active_acm_index()
        )[0]
        """
        # Workloads list
        # workload = [sub, appset]
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

        # Verify if cluster is marked unavailable on ACM console
        if config.RUN.get("mdr_failover_via_ui"):
            config.switch_acm_ctx()
            check_cluster_status_on_acm_console(
                acm_obj,
                down_cluster_name=self.primary_cluster_name,
                expected_text="Unknown",
            )
        elif config.RUN.get("mdr_failover_via_ui"):
            check_cluster_status_on_acm_console(acm_obj)

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
                        switch_ctx=get_active_acm_index(),
                    )
                )
                time.sleep(5)

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
        if (
            config.RUN.get("mdr_relocate_via_ui")
            and self.workload_type == constants.SUBSCRIPTION
        ):
            logger.info("Start the process of Relocate from ACM UI")
            # Relocate via ACM UI
            config.switch_ctx(get_active_acm_index())
            check_cluster_status_on_acm_console(acm_obj)
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{workload[0].workload_name}-1",
                policy_name=workload[0].dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
                action=constants.ACTION_RELOCATE,
            )
        else:
            relocate_results = []
            with ThreadPoolExecutor() as executor:
                for wl in workload:
                    relocate_results.append(
                        executor.submit(
                            relocate,
                            preferred_cluster=secondary_cluster_name,
                            namespace=wl.workload_namespace,
                            switch_ctx=get_active_acm_index(),
                        )
                    )
                    time.sleep(5)

            # Wait for relocate results
            for rl in relocate_results:
                rl.result()

        # Verify resources deletion from  secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workload:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Verify resources creation on preferredCluster
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        for wl in workload:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Validate data integrity
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        for wl in workload:
            validate_data_integrity(wl.workload_namespace)
