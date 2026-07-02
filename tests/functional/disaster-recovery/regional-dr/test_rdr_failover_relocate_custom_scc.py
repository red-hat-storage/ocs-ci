import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier2
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)

CUSTOM_WORKLOAD_REPO_URL = "https://github.com/pruthvitd/ocm-ramen-samples"
CUSTOM_WORKLOAD_REPO_BRANCH = "cephfs-selinux-relabel"


@pytest.fixture()
def custom_scc_workload_repo(request):
    """
    Temporarily override DR workload repo and branch for this test.

    The previous values are restored in teardown.
    """
    original_repo_url = config.ENV_DATA.get("dr_workload_repo_url")
    original_repo_branch = config.ENV_DATA.get("dr_workload_repo_branch")

    config.ENV_DATA["dr_workload_repo_url"] = CUSTOM_WORKLOAD_REPO_URL
    config.ENV_DATA["dr_workload_repo_branch"] = CUSTOM_WORKLOAD_REPO_BRANCH
    logger.info(
        "Using custom DR workload repo %s on branch %s",
        CUSTOM_WORKLOAD_REPO_URL,
        CUSTOM_WORKLOAD_REPO_BRANCH,
    )

    def _restore():
        if original_repo_url is None:
            config.ENV_DATA.pop("dr_workload_repo_url", None)
        else:
            config.ENV_DATA["dr_workload_repo_url"] = original_repo_url

        if original_repo_branch is None:
            config.ENV_DATA.pop("dr_workload_repo_branch", None)
        else:
            config.ENV_DATA["dr_workload_repo_branch"] = original_repo_branch

        logger.info("Restored original DR workload repo configuration")

    request.addfinalizer(_restore)


@rdr
@tier2
@turquoise_squad
class TestRDRFailoverRelocateCustomSCC:
    """
    Validate failover and relocate for a custom CephFS ApplicationSet workload.
    """

    def test_rdr_failover_relocate_custom_scc(
        self,
        dr_workload,
        custom_scc_workload_repo,
    ):
        """
        Deploy a CephFS ApplicationSet workload from a custom repo and branch,
        validate lastGroupSyncTime, then perform failover and relocate.
        """
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )
        workload = workloads[0]

        assert (
            getattr(workload, "workload_repo_url", None) == CUSTOM_WORKLOAD_REPO_URL
        ), "Custom workload repo URL was not applied to the workload"
        assert (
            getattr(workload, "workload_repo_branch", None)
            == CUSTOM_WORKLOAD_REPO_BRANCH
        ), "Custom workload repo branch was not applied to the workload"

        drpc_obj = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workload.appset_placement_name}-drpc",
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workload.workload_namespace, workload.workload_type
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload.workload_namespace, workload.workload_type
        )

        logger.info(
            "Custom workload deployed from repo %s branch %s in namespace %s",
            workload.workload_repo_url,
            workload.workload_repo_branch,
            workload.workload_namespace,
        )

        config.switch_to_cluster_by_name(secondary_cluster_name)
        if dr_helpers.is_cg_cephfs_enabled():
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=workload.workload_namespace,
                should_exist=True,
            )
            dr_helpers.wait_for_resource_count(
                kind=constants.VOLUMESNAPSHOT,
                namespace=workload.workload_namespace,
                expected_count=workload.workload_pvc_count,
            )
        dr_helpers.wait_for_replication_destinations_creation(
            workload.workload_pvc_count, workload.workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace, workload.workload_type
        )
        wait_time = 2 * scheduling_interval
        logger.info("Waiting for %s minutes to run IOs and sync data", wait_time)
        sleep(wait_time * 60)

        initial_last_group_sync_time = dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval
        )
        logger.info("Verified lastGroupSyncTime before failover")

        dr_helpers.failover(
            secondary_cluster_name,
            workload.workload_namespace,
            workload.workload_type,
            workload.appset_placement_name,
        )

        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            performed_dr_action=True,
        )

        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_deletion(workload.workload_namespace)

        config.switch_to_cluster_by_name(secondary_cluster_name)
        if dr_helpers.is_cg_cephfs_enabled():
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=workload.workload_namespace,
                should_exist=False,
            )
        dr_helpers.wait_for_replication_destinations_deletion(
            workload.workload_namespace
        )

        config.switch_to_cluster_by_name(primary_cluster_name)
        if dr_helpers.is_cg_cephfs_enabled():
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=workload.workload_namespace,
                should_exist=True,
            )
            dr_helpers.wait_for_resource_count(
                kind=constants.VOLUMESNAPSHOT,
                namespace=workload.workload_namespace,
                expected_count=workload.workload_pvc_count,
            )
        dr_helpers.wait_for_replication_destinations_creation(
            workload.workload_pvc_count, workload.workload_namespace
        )

        logger.info(
            "Waiting for %s minutes after failover for sync to complete", wait_time
        )
        sleep(wait_time * 60)

        post_failover_last_group_sync_time = dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval, initial_last_group_sync_time
        )
        logger.info("Verified lastGroupSyncTime after failover")

        dr_helpers.relocate(
            primary_cluster_name,
            workload.workload_namespace,
            workload.workload_type,
            workload.appset_placement_name,
        )

        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_deletion(workload.workload_namespace)

        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            performed_dr_action=True,
        )

        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_replication_destinations_deletion(
            workload.workload_namespace
        )
        if dr_helpers.is_cg_cephfs_enabled():
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=workload.workload_namespace,
                should_exist=False,
            )

        config.switch_to_cluster_by_name(secondary_cluster_name)
        if dr_helpers.is_cg_cephfs_enabled():
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=workload.workload_namespace,
                should_exist=True,
            )
            dr_helpers.wait_for_resource_count(
                kind=constants.VOLUMESNAPSHOT,
                namespace=workload.workload_namespace,
                expected_count=workload.workload_pvc_count,
            )
        dr_helpers.wait_for_replication_destinations_creation(
            workload.workload_pvc_count, workload.workload_namespace
        )

        logger.info(
            "Waiting for %s minutes after relocate for sync to complete", wait_time
        )
        sleep(wait_time * 60)

        dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval, post_failover_last_group_sync_time
        )
        logger.info("Verified lastGroupSyncTime after relocate")


# Made with Bob
