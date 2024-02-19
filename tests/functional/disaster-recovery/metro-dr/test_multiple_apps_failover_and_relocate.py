import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework import config

# from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs import constants
from ocs_ci.helpers.dr_helpers import (
    enable_fence,
    enable_unfence,
    get_fence_state,
    # failover,
    # relocate,
    set_current_primary_cluster_context,
    # set_current_secondary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    # wait_for_all_resources_creation,
    # wait_for_all_resources_deletion,
    gracefully_reboot_ocp_nodes,
)

# from ocs_ci.helpers.dr_helpers_ui import (
#     check_cluster_status_on_acm_console,
#     failover_relocate_ui,
#     verify_failover_relocate_status_ui,
# )
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
    def teardown(self, request, dr_workload):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name
                and get_fence_state(self.primary_cluster_name) == "Fenced"
            ):
                enable_unfence(self.primary_cluster_name)
                gracefully_reboot_ocp_nodes(self.namespace, self.primary_cluster_name)

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
    ):
        """
        Tests to failover and relocate of applications from both the managed clusters

        """
        self.workload_namespaces = []
        if config.RUN.get("mdr_failover_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error(
                    "ODF/ACM version isn't supported for Failover/Relocate operation"
                )
                raise NotImplementedError

        # acm_obj = AcmAddClusters()
        primary_instances = []
        secondary_instances = []

        if workload_type == constants.SUBSCRIPTION:
            primary_instances, secondary_instances = dr_workloads_on_managed_clusters(
                num_of_subscription=1, primary_cluster=True, secondary_cluster=True
            )

        # Set Primary managed cluster
        set_current_primary_cluster_context(
            primary_instances[0].workload_namespace, workload_type
        )
        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=primary_instances[0].workload_namespace,
            workload_type=workload_type,
        )
        logger.info(f"The primary cluster is {self.primary_cluster_name}")

        # Set secondary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(
            namespace=primary_instances[0].workload_namespace,
            workload_type=workload_type,
        )
        logger.info(f"The secondary cluster is {secondary_cluster_name}")

        # Fence the primary managed cluster
        enable_fence(drcluster_name=self.primary_cluster_name)

        # # Application Failover to Secondary managed cluster
        # for instance in primary_instances:
        #     if (
        #         config.RUN.get("mdr_failover_via_ui")
        #         and workload_type == constants.SUBSCRIPTION
        #     ):
        #         logger.info(
        #             "Start the process of Failover of subscription based app from ACM UI"
        #         )
        #         config.switch_acm_ctx()
        #         failover_relocate_ui(
        #             acm_obj,
        #             workload_to_move=f"{instance.workload_namespace}-1",
        #             policy_name=workload.dr_policy_name,
        #             failover_or_preferred_cluster=secondary_cluster_name,
        #         )
        # if workload_type == constants.APPLICATION_SET:
        #     # TODO: Failover appset based apps via UI
        #     # TODO: Failover of multiple apps to
        #     failover(
        #         failover_cluster=secondary_cluster_name,
        #         namespace=workload.workload_namespace,
        #         workload_type=workload_type,
        #         workload_placement_name=workload.appset_placement_name,
        #     )
        #
        # # Verify application are running in other managedcluster
        # # And not in previous cluster
        # set_current_primary_cluster_context(
        #     primary_instances[0].workload_namespace, workload_type
        # )
        # for instance in primary_instances:
        #     wait_for_all_resources_creation(
        #         workload.workload_pvc_count,
        #         workload.workload_pod_count,
        #         instance.workload_namespace,
        #     )
        #
        # # Verify the failover status from UI
        # if config.RUN.get("mdr_failover_via_ui"):
        #     config.switch_acm_ctx()
        #     verify_failover_relocate_status_ui(acm_obj)
