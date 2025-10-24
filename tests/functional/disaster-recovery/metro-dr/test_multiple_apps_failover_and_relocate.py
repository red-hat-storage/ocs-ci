import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import tier1, mdr
from ocs_ci.framework import config

from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs import constants
from ocs_ci.helpers.dr_helpers import (
    failover,
    enable_fence,
    enable_unfence,
    get_fence_state,
    set_current_primary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    wait_for_all_resources_creation,
    gracefully_reboot_ocp_nodes,
)
from ocs_ci.helpers.dr_helpers_ui import (
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)
from ocs_ci.framework.pytest_customization.marks import turquoise_squad

logger = logging.getLogger(__name__)


@mdr
@tier1
@turquoise_squad
class TestMultipleApplicationFailoverAndRelocate:
    """
    Deploy applications on both the managed clusters and test Failover and Relocate actions on them
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes_multicluster, dr_workloads_on_managed_clusters):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name is not None
                and get_fence_state(drcluster_name=self.primary_cluster_name)
                == constants.ACTION_FENCE
            ):
                enable_unfence(self.primary_cluster_name)
                gracefully_reboot_ocp_nodes(
                    drcluster_name=self.primary_cluster_name, disable_eviction=True
                )

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["workload_type"],
        argvalues=[
            pytest.param(
                constants.SUBSCRIPTION,
            ),
        ],
    )
    def test_application_failover_and_relocate(
        self,
        setup_acm_ui,
        nodes_multicluster,
        workload_type,
        dr_workloads_on_managed_clusters,
        node_restart_teardown,
    ):
        """
        Test to deploy applications on both the managed clusters and test Failover and Relocate actions on them

        """

        acm_obj = AcmAddClusters()
        primary_instances = []
        secondary_instances = []

        if workload_type == constants.SUBSCRIPTION:
            primary_instances, secondary_instances = dr_workloads_on_managed_clusters(
                num_of_subscription=1, primary_cluster=True, secondary_cluster=False
            )

        # Set Primary managed cluster
        set_current_primary_cluster_context(
            primary_instances[0].workload_namespace, workload_type
        )
        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=primary_instances[0].workload_namespace,
            workload_type=workload_type,
        )

        # Set secondary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(
            namespace=primary_instances[0].workload_namespace,
            workload_type=workload_type,
        )
        time.sleep(120)
        # Fence the primary managed cluster
        enable_fence(drcluster_name=self.primary_cluster_name)

        # Multiple applications Failover to Secondary managed cluster
        config.switch_acm_ctx()
        if (
            config.RUN.get("mdr_failover_via_ui")
            and workload_type == constants.SUBSCRIPTION
        ):
            logger.info(
                "Start the process of Failover of subscription based apps from ACM UI"
            )
            for instance in primary_instances:
                logger.info(f"Failing over app {instance.app_name} ")
                failover_relocate_ui(
                    acm_obj,
                    workload_to_move=instance.app_name,
                    policy_name=instance.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                )
        else:
            for instance in primary_instances:
                failover(
                    failover_cluster=secondary_cluster_name,
                    namespace=f"{instance.workload_namespace}",
                    workload_type=workload_type,
                )

        # Verify application are running in other managedcluster
        # And not in previous cluster
        set_current_primary_cluster_context(
            primary_instances[0].workload_namespace, workload_type
        )
        for instance in primary_instances:
            wait_for_all_resources_creation(
                instance.workload_pvc_count,
                instance.workload_pod_count,
                instance.workload_namespace,
            )

        # Verify the failover status from UI
        if config.RUN.get("mdr_failover_via_ui"):
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(acm_obj)

        # TODO Relocate of sub apps
        # TODO Failover of apps from c2 to c1
