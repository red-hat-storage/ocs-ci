import logging
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import tier4a, turquoise_squad
from ocs_ci.framework import config
from ocs_ci.ocs.acm.acm import AcmAddClusters, validate_cluster_import
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs import constants
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.helpers.dr_helpers import (
    enable_fence,
    enable_unfence,
    get_fence_state,
    failover,
    relocate,
    restore_backup,
    create_backup_schedule,
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_passive_acm_index,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    gracefully_reboot_ocp_nodes,
    verify_drpolicy_cli,
)
from ocs_ci.helpers.dr_helpers_ui import (
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_active_acm_index
from ocs_ci.utility import version
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


@tier4a
@turquoise_squad
class TestApplicationFailoverAndRelocateWhenZoneDown:
    """
    Test failover and relocate all apps in a single zone after a zone disruption
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, dr_workload):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name
                and get_fence_state(
                    drcluster_name=self.primary_cluster_name,
                    switch_ctx=get_passive_acm_index(),
                )
                == "Fenced"
            ):
                enable_unfence(
                    drcluster_name=self.primary_cluster_name,
                    switch_ctx=get_passive_acm_index(),
                )
                gracefully_reboot_ocp_nodes(self.namespace, self.primary_cluster_name)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_application_failover_and_relocate(
        self,
        setup_acm_ui,
        nodes_multicluster,
        dr_workload,
    ):

        """
        Tests to verify failover and relocate all apps in a single zone after a zone disruption

        """

        if config.RUN.get("mdr_failover_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error(
                    "ODF/ACM version isn't supported for Failover/Relocate operation"
                )
                raise NotImplementedError

        acm_obj = AcmAddClusters()
        # ToDO: Create appset and multiple apps
        workload = dr_workload(num_of_subscription=1)[0]
        self.namespace = workload.workload_namespace

        # Create application on Primary managed cluster
        set_current_primary_cluster_context(workload.workload_namespace)
        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=workload.workload_namespace
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            workload.workload_namespace
        )

        # Create backup-schedule on active hub
        create_backup_schedule()
        # ToDo: To verfiy all the backups are taken
        wait_time = 300
        logger.info(f"Wait {wait_time} until backup is taken ")
        time.sleep(wait_time)

        # Install gitops operator on passive hub
        dep_obj = Deployment()
        dep_obj.deploy_gitops_operator(switch_ctx=get_passive_acm_index())

        # Get the active hub nodes
        config.switch_ctx(get_active_acm_index())
        active_hub_index = config.cur_index
        active_hub_cluster_node_objs = get_node_objs()

        # Shutdown active hub nodes
        logger.info("Shutting down all the nodes of active hub")
        nodes_multicluster[active_hub_index].stop_nodes(active_hub_cluster_node_objs)
        logger.info(
            "All nodes of active hub zone are powered off, "
            f"wait {wait_time} seconds before restoring in passive hub"
        )

        # Restore new hub
        restore_backup()
        logger.info(f"Wait {wait_time} until restores are taken ")
        time.sleep(wait_time)

        # Validate the clusters are imported
        clusters = [self.primary_cluster_name, secondary_cluster_name]
        for cluster in clusters:
            for sample in TimeoutSampler(
                timeout=1800,
                sleep=60,
                func=validate_cluster_import,
                cluster_name=cluster,
                switch_ctx=get_passive_acm_index(),
            ):
                if sample:
                    logger.info(
                        f"Cluster: {cluster} successfully imported post hub recovery"
                    )
                else:
                    logger.error(
                        f"import of cluster: {cluster} failed post hub recovery"
                    )

            # Validate klusterlet addons are running on managed cluster
            config.switch_to_cluster_by_name(cluster)
            wait_for_pods_to_be_running(
                namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
            )

        # Wait or verify the drpolicy is in validated state
        for sample in TimeoutSampler(
            timeout=1800,
            sleep=60,
            func=verify_drpolicy_cli(switch_ctx=get_passive_acm_index()),
        ):
            if sample:
                logger.info("Post hub recovery: DRPolicy validation succeeded")
            else:
                logger.error("Post hub recoevry: DRPolicy validation failed")
                raise UnexpectedBehaviour(
                    "Post hub recovery: DRPolicy is not in validated state"
                )

        # ToDo: Deploy application in both managed cluster and
        #  to verify the applications are present in secondary cluster

        # Fenced the primary managed cluster
        enable_fence(
            drcluster_name=self.primary_cluster_name,
            switch_ctx=get_passive_acm_index(),
        )

        # Application Failover to Secondary managed cluster
        if config.RUN.get("mdr_failover_via_ui"):
            logger.info("Start the process of Failover from ACM UI")
            config.switch_ctx(get_passive_acm_index())
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{workload.workload_name}-1",
                policy_name=workload.dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
            )
        else:
            failover(
                failover_cluster=secondary_cluster_name,
                namespace=workload.workload_namespace,
                switch_ctx=get_passive_acm_index(),
            )

        # Verify application are running in other managedcluster
        # And not in previous cluster
        set_current_primary_cluster_context(workload.workload_namespace)
        wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )

        # Verify the failover status from UI
        if config.RUN.get("mdr_failover_via_ui"):
            config.switch_ctx(get_passive_acm_index())
            verify_failover_relocate_status_ui(acm_obj)

        # Verify application are deleted from old cluster
        set_current_secondary_cluster_context(workload.workload_namespace)
        wait_for_all_resources_deletion(workload.workload_namespace)

        # Validate data integrity
        set_current_primary_cluster_context(workload.workload_namespace)
        validate_data_integrity(workload.workload_namespace)

        # Unfenced the managed cluster which was Fenced earlier
        enable_unfence(
            drcluster_name=self.primary_cluster_name,
            switch_ctx=get_passive_acm_index(),
        )

        # Reboot the nodes which unfenced
        gracefully_reboot_ocp_nodes(
            workload.workload_namespace, self.primary_cluster_name
        )

        # Application Relocate to Primary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(
            workload.workload_namespace
        )
        if config.RUN.get("mdr_relocate_via_ui"):
            logger.info("Start the process of Relocate from ACM UI")
            # Relocate via ACM UI
            config.switch_ctx(get_passive_acm_index())
            check_cluster_status_on_acm_console(acm_obj)
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{workload.workload_name}-1",
                policy_name=workload.dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
                action=constants.ACTION_RELOCATE,
            )
        else:
            relocate(
                secondary_cluster_name,
                workload.workload_namespace,
                switch_ctx=get_passive_acm_index(),
            )

        # Verify resources deletion from previous primary or current secondary cluster
        set_current_secondary_cluster_context(workload.workload_namespace)
        wait_for_all_resources_deletion(workload.workload_namespace)

        # Verify resources creation on preferredCluster
        set_current_primary_cluster_context(workload.workload_namespace)
        wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )

        # Verify Relocate status from UI
        if config.RUN.get("mdr_relocate_via_ui"):
            config.switch_ctx(get_passive_acm_index())
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )

        # Validate data integrity
        set_current_primary_cluster_context(workload.workload_namespace)
        validate_data_integrity(workload.workload_namespace)
