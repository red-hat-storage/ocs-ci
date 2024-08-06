import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import tier1
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
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


@tier1
@turquoise_squad
class TestMultipleApplicationFailoverAndRelocate:
    """
    Test Failover and Relocate actions for application
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, dr_workloads_on_managed_clusters):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():

            if (
                self.primary_cluster_name is not None
                and get_fence_state(self.primary_cluster_name) == "Fenced"
            ):
                enable_unfence(self.primary_cluster_name)
                gracefully_reboot_ocp_nodes(self.primary_cluster_name)

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
        dr_workloads_on_managed_clusters,
        workload_type,
        node_restart_teardown,
    ):
        """
        Tests to failover and relocate of applications from both the managed clusters

        """
        acm_obj = AcmAddClusters()
        self.workload_namespaces = []
        if config.RUN.get("mdr_failover_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error(
                    "ODF/ACM version isn't supported for Failover/Relocate operation"
                )
                raise NotImplementedError

        acm_obj = AcmAddClusters()
        primary_instances = []
        secondary_instances = []

        if workload_type == constants.SUBSCRIPTION:
            primary_instances, secondary_instances = dr_workloads_on_managed_clusters(
                num_of_subscription=2, primary_cluster=True, secondary_cluster=True
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

        # Fence the primary managed cluster
        enable_fence(drcluster_name=self.primary_cluster_name)

        # Application Failover to Secondary managed cluster
        config.switch_acm_ctx()
        for instance in primary_instances:
            if (
                config.RUN.get("mdr_failover_via_ui")
                and workload_type == constants.SUBSCRIPTION
            ):
                logger.info(
                    "Start the process of Failover of subscription based app from ACM UI"
                )
                failover_relocate_ui(
                    acm_obj,
                    workload_to_move=f"{instance.app_name}-1",
                    policy_name=instance.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                )
            else:
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
