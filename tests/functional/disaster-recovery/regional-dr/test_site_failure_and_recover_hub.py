import logging
import time
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.pytest_customization.marks import tier4a, turquoise_squad
from ocs_ci.framework import config
from ocs_ci.ocs.acm.acm import validate_cluster_import
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.helpers.dr_helpers import (
    failover,
    restore_backup,
    create_backup_schedule,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_passive_acm_index,
    wait_for_all_resources_creation,
    verify_drpolicy_cli,
    verify_restore_is_completed,
    wait_for_all_resources_deletion,
    relocate,
    get_scheduling_interval,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_active_acm_index
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check

logger = logging.getLogger(__name__)


@tier4a
@turquoise_squad
class TestSiteFailureAndRecoverHub:
    """
    Perform site failure, recover hub by moving to passive hub using hub recovery and perform failover
    """

    def test_site_failure_and_failover(self, nodes_multicluster, dr_workload):
        """
        Tests to verify failover on all apps when active hub along with primary managed cluster is down
        """

        # Deploy Subscription and Appset based application
        rdr_workload = dr_workload(
            num_of_subscription=1, num_of_appset=1, switch_ctx=get_passive_acm_index()
        )
        logger.info(type(rdr_workload))
        primary_cluster_name = get_current_primary_cluster_name(
            rdr_workload[0].workload_namespace
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            rdr_workload[0].workload_namespace
        )
        scheduling_interval = get_scheduling_interval(
            rdr_workload[0].workload_namespace, rdr_workload[0].workload_type
        )
        # Create backup-schedule on active hub
        create_backup_schedule()
        two_times_scheduling_interval = 2 * scheduling_interval  # Time in minutes
        wait_time = 300
        logger.info(f"Wait {wait_time} until backup is taken ")
        time.sleep(wait_time)

        # Get the primary managed cluster nodes
        logger.info("Getting Primary managed cluster node details")
        config.switch_to_cluster_by_name(get_current_primary_cluster_name)
        active_primary_index = config.cur_index
        active_primary_cluster_node_objs = get_node_objs()

        # Get the active hub cluster nodes
        logger.info("Getting Active cluster node details")
        config.switch_ctx(get_active_acm_index())
        active_hub_index = config.cur_index
        active_hub_cluster_node_objs = get_node_objs()

        # ToDo Add verification for dpa and policy

        # Shutdown active hub and primary managed cluster nodes
        logger.info(
            "Shutting down all the nodes of primary managed cluster and active hub one after another"
        )
        nodes_multicluster[active_primary_index].stop_nodes(
            active_primary_cluster_node_objs
        )
        logger.info("All nodes of primary managed cluster are powered off")
        time.sleep(180)
        nodes_multicluster[active_hub_index].stop_nodes(active_hub_cluster_node_objs)
        logger.info(
            "All nodes of active hub cluster are powered off, "
            f"wait {wait_time} seconds before restoring backups on the passive hub"
        )

        config.switch_ctx(get_passive_acm_index())
        # Restore new hub
        restore_backup()
        logger.info(f"Wait {wait_time} until restores are taken ")
        time.sleep(wait_time)

        # Verify the restore is completed
        verify_restore_is_completed()

        # Validate the surviving managed cluster is successfully imported on the new hub
        for sample in TimeoutSampler(
            timeout=1800,
            sleep=60,
            func=validate_cluster_import,
            cluster_name=secondary_cluster_name,
            switch_ctx=get_passive_acm_index(),
        ):
            if sample:
                logger.info(
                    f"Cluster: {secondary_cluster_name} successfully imported post hub recovery"
                )
                # Validate klusterlet addons are running on managed cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                wait_for_pods_to_be_running(
                    namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
                )
                break
            else:
                logger.error(
                    f"import of cluster: {secondary_cluster_name} failed post hub recovery"
                )
                raise UnexpectedBehaviour(
                    f"import of cluster: {secondary_cluster_name} failed post hub recovery"
                )
            # Wait for drpolicy to be in validated state
        verify_drpolicy_cli(switch_ctx=get_passive_acm_index())

        logger.info(f"Wait for {wait_time} for drpc status to be restored")
        time.sleep(wait_time)

        # Failover action via CLI
        failover_results = []
        with ThreadPoolExecutor() as executor:
            for wl in rdr_workload:
                failover_results.append(
                    executor.submit(
                        failover,
                        failover_cluster=secondary_cluster_name,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        workload_placement_name=(
                            rdr_workload.appset_placement_name
                            if wl.workload_type != constants.SUBSCRIPTION
                            else None
                        ),
                        switch_ctx=get_passive_acm_index(),
                    )
                )
                time.sleep(60)

        # Wait for failover results
        for fl in failover_results:
            fl.result()

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in rdr_workload:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info("Recover managed cluster which went down during site-failure")
        nodes_multicluster[active_primary_index].start_nodes(
            active_primary_cluster_node_objs
        )
        wait_for_nodes_status([node.name for node in active_primary_cluster_node_objs])
        logger.info(
            "Check if recovered managed cluster is successfully imported on the new hub"
        )
        for sample in TimeoutSampler(
            timeout=900,
            sleep=60,
            func=validate_cluster_import,
            cluster_name=primary_cluster_name,
            switch_ctx=get_passive_acm_index(),
        ):
            if sample:
                logger.info(
                    f"Cluster: {primary_cluster_name} successfully imported post hub recovery"
                )
                # Validate klusterlet addons are running on managed cluster
                config.switch_to_cluster_by_name(primary_cluster_name)
                wait_for_pods_to_be_running(
                    namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
                )
                break
            else:
                logger.error(
                    f"Import of cluster: {primary_cluster_name} failed post hub recovery"
                )
                raise UnexpectedBehaviour(
                    f"Import of cluster: {primary_cluster_name} failed post hub recovery"
                )

        logger.info("Wait for approx. an hour to surpass 1hr eviction period timeout")
        time.sleep(3600)
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()

        # Verify application are deleted from old cluster
        for wl in rdr_workload:
            wait_for_all_resources_deletion(wl.workload_namespace)

        logger.info(f"Waiting for {two_times_scheduling_interval} minutes to run IOs")
        time.sleep(two_times_scheduling_interval * 60)

        relocate_results = []
        with ThreadPoolExecutor() as executor:
            for wl in rdr_workload:
                relocate_results.append(
                    executor.submit(
                        relocate,
                        preferred_cluster=primary_cluster_name,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        workload_placement_name=(
                            rdr_workload.appset_placement_name
                            if wl.workload_type != constants.SUBSCRIPTION
                            else None
                        ),
                        switch_ctx=get_passive_acm_index(),
                    )
                )
                time.sleep(60)

        # Wait for relocate results
        for rl in relocate_results:
            rl.result()

        # Verify resources creation on preferredCluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        for wl in rdr_workload:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Verify resources deletion from previous primary or current secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in rdr_workload:
            wait_for_all_resources_deletion(wl.workload_namespace)
