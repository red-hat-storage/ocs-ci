import logging

import tempfile
import time

from concurrent.futures import ThreadPoolExecutor

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier4a,
    turquoise_squad,
    dr_hub_recovery,
)
from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.acm.acm import (
    validate_cluster_import,
    get_clusters_env,
    copy_kubeconfig,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.helpers.dr_helpers import (
    failover,
    restore_backup,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_passive_acm_index,
    wait_for_all_resources_creation,
    verify_drpolicy_cli,
    verify_restore_is_completed,
    wait_for_all_resources_deletion,
    relocate,
    get_scheduling_interval,
    create_klusterlet_config,
    remove_parameter_klusterlet_config,
    configure_rdr_hub_recovery,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_active_acm_index, get_primary_cluster_config

from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, run_cmd

logger = logging.getLogger(__name__)


@tier4a
@turquoise_squad
@dr_hub_recovery
@pytest.mark.order("last-1")
class TestSiteFailureRecoveryAndFailover:
    """
    Perform site-failure by bringing down the active hub and the primary managed cluster, then perform hub recovery
    by moving to passive hub using backup and restore, and then failover all the DR protected workloads
    running on the down managed cluster to the secondary managed cluster.
    """

    def test_site_failure_and_failover(self, dr_workload, nodes_multicluster):
        """
        Test to verify failover of all workloads after site-failure where the active hub along with
        the primary managed cluster is down
        """

        # Deploy Subscription and Appset based application of both RBD and CephFS SC
        rdr_workload = dr_workload(
            num_of_subscription=1,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
            switch_ctx=get_passive_acm_index(),
        )
        dr_workload(
            num_of_subscription=1,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
            switch_ctx=get_passive_acm_index(),
        )
        drpc_objs = []
        for wl in rdr_workload:
            if wl.workload_type == constants.SUBSCRIPTION:
                drpc_objs.append(DRPC(namespace=wl.workload_namespace))
            else:
                drpc_objs.append(
                    DRPC(
                        namespace=constants.GITOPS_CLUSTER_NAMESPACE,
                        resource_name=f"{wl.appset_placement_name}-drpc",
                    )
                )

        primary_cluster_name = get_current_primary_cluster_name(
            rdr_workload[0].workload_namespace
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            rdr_workload[0].workload_namespace
        )

        # Verify the creation of ReplicationDestination resources on secondary cluster in case of CephFS
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in rdr_workload:
            if wl.pvc_interface == constants.CEPHFILESYSTEM:
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        scheduling_interval = get_scheduling_interval(
            rdr_workload[0].workload_namespace, rdr_workload[0].workload_type
        )

        two_times_scheduling_interval = 2 * scheduling_interval  # Time in minutes
        wait_time = 420

        assert configure_rdr_hub_recovery()

        # Get the primary managed cluster nodes
        logger.info("Getting Primary managed cluster node details")
        config.switch_to_cluster_by_name(primary_cluster_name)
        active_primary_index = config.cur_index
        active_primary_cluster_node_objs = get_node_objs()

        # Get the active hub cluster nodes
        logger.info("Getting Active Hub cluster node details")
        config.switch_ctx(get_active_acm_index())
        active_hub_index = config.cur_index
        active_hub_cluster_node_objs = get_node_objs()

        drpc_cmd = run_cmd("oc get drpc -o wide -A")
        logger.info("DRPC output from current active hub cluster before site-failure")
        logger.info(drpc_cmd)

        # Shutdown active hub and primary managed cluster nodes
        logger.info(
            "Shutting down all the nodes of primary managed cluster and active hub one after another"
        )
        nodes_multicluster[active_primary_index].stop_nodes(
            active_primary_cluster_node_objs
        )
        logger.info("All nodes of primary managed cluster are powered off")
        time.sleep(300)
        nodes_multicluster[active_hub_index].stop_nodes(active_hub_cluster_node_objs)
        logger.info(
            "All nodes of active hub cluster are powered off, "
            f"wait {wait_time} seconds before restoring backups on the passive hub"
        )
        time.sleep(wait_time)

        config.switch_ctx(get_passive_acm_index())
        # Create KlusterletConfig
        logger.info("Create klusterletconfig on passive hub")
        create_klusterlet_config()

        # Restore new hub
        logger.info("Restore backups on the passive hub cluster")
        restore_backup()
        logger.info(f"Wait {wait_time} until restores are taken ")
        time.sleep(wait_time)

        # Verify the restore is completed
        logger.info("Verify if backup restore is successful or not")
        verify_restore_is_completed()

        # Validate the surviving managed cluster is successfully imported on the new hub
        for sample in TimeoutSampler(
            timeout=1800,
            sleep=15,
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
        logger.info("Verify status of DR Policy on the new hub")
        verify_drpolicy_cli(switch_ctx=get_passive_acm_index())

        logger.info(f"Wait for {wait_time} for drpc status to be restored")
        time.sleep(wait_time)

        config.switch_ctx(get_passive_acm_index())
        drpc_cmd = run_cmd("oc get drpc -o wide -A")
        logger.info("DRPC output from new hub cluster before failover")
        logger.info(drpc_cmd)

        # Failover action via CLI
        logger.info("Failover workloads after hub recovery")
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
                            wl.appset_placement_name
                            if wl.workload_type != constants.SUBSCRIPTION
                            else None
                        ),
                        switch_ctx=get_passive_acm_index(),
                    )
                )
                time.sleep(5)

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

        config.switch_ctx(get_passive_acm_index())
        drpc_cmd = run_cmd("oc get drpc -o wide -A")
        logger.info("DRPC output from new hub cluster after successful failover")
        logger.info(drpc_cmd)

        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info("Recover managed cluster which went down during site-failure")
        nodes_multicluster[active_primary_index].start_nodes(
            active_primary_cluster_node_objs
        )
        wait_for_nodes_status([node.name for node in active_primary_cluster_node_objs])
        config.switch_ctx(get_passive_acm_index())
        logger.info(
            "Check if recovered managed cluster is successfully imported on the new hub"
        )
        logger.info("Create and apply the auto-import-secret.yaml")
        clusters_env = get_clusters_env()
        primary_index = get_primary_cluster_config().MULTICLUSTER["multicluster_index"]
        down_cluster_name = clusters_env.get(f"cluster_name_{primary_index}")
        down_cluster_kubeconfig = copy_kubeconfig(
            file=clusters_env.get(f"kubeconfig_location_c{primary_index}"),
            return_str=True,
        )
        auto_import_secret = templating.load_yaml(
            "ocs_ci/templates/acm-deployment/auto-import-secret.yaml"
        )
        auto_import_secret["metadata"]["namespace"] = down_cluster_name
        auto_import_secret["stringData"]["autoImportRetry"] = "50"
        auto_import_secret["stringData"]["kubeconfig"] = down_cluster_kubeconfig
        auto_import_secret_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="auto-import", delete=False
        )
        templating.dump_data_to_temp_yaml(
            auto_import_secret, auto_import_secret_yaml.name
        )
        config.switch_ctx(get_passive_acm_index())
        run_cmd(f"oc apply -f {auto_import_secret_yaml.name}")

        for sample in TimeoutSampler(
            timeout=900,
            sleep=15,
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

        # Edit the global KlusterletConfig on the new hub and remove
        # the parameter appliedManifestWorkEvictionGracePeriod and its value.
        # appliedManifestWorkEvictionGracePeriod should only be removed if
        # no DRPCs are in the Paused `PROGRESSION` or if `PROGRESSION` is in Cleaning Up state in case workloads are
        # successfully FailedOver or Relocated after hub recovery was performed`
        logger.info(
            "Edit the global KlusterletConfig on the new hub and "
            "remove the parameter appliedManifestWorkEvictionGracePeriod and its value."
        )
        remove_parameter_klusterlet_config()

        logger.info(
            "Wait for approx. an hour to surpass 1hr of default eviction period timeout"
        )
        time.sleep(3600)

        config.switch_to_cluster_by_name(primary_cluster_name)

        # Verify application are deleted from old cluster
        for wl in rdr_workload:
            wait_for_all_resources_deletion(wl.workload_namespace, timeout=1800)

        for wl in rdr_workload:
            if wl.pvc_interface == constants.CEPHFILESYSTEM:
                # Verify the deletion of ReplicationDestination resources on secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                # Verify the creation of ReplicationDestination resources on primary cluster
                config.switch_to_cluster_by_name(primary_cluster_name)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(
                [
                    wl.workload_pvc_count
                    for wl in rdr_workload
                    if wl.pvc_interface == constants.CEPHBLOCKPOOL
                ]
            )
        )

        logger.info(f"Waiting for {two_times_scheduling_interval} minutes to run IOs")
        time.sleep(two_times_scheduling_interval * 60)

        config.switch_ctx(get_passive_acm_index())
        logger.info(
            "DRPC output from new hub cluster after successful failover and cleanup"
        )
        drpc_cmd = run_cmd("oc get drpc -o wide -A")
        logger.info(drpc_cmd)

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
                            wl.appset_placement_name
                            if wl.workload_type != constants.SUBSCRIPTION
                            else None
                        ),
                        switch_ctx=get_passive_acm_index(),
                    )
                )
                time.sleep(5)

        # Wait for relocate results
        for rl in relocate_results:
            rl.result()

        config.switch_ctx(get_passive_acm_index())
        drpc_cmd = run_cmd("oc get drpc -o wide -A")
        logger.info("DRPC output from new hub cluster after relocate")
        logger.info(drpc_cmd)

        # Verify resources creation on preferredCluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        for wl in rdr_workload:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        for wl in rdr_workload:
            if wl.pvc_interface == constants.CEPHFILESYSTEM:
                # Verify the deletion of ReplicationDestination resources on primary cluster
                config.switch_to_cluster_by_name(primary_cluster_name)
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                # Verify the creation of ReplicationDestination resources on secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(
                [
                    wl.workload_pvc_count
                    for wl in rdr_workload
                    if wl.pvc_interface == constants.CEPHBLOCKPOOL
                ]
            )
        )

        # Verify resources deletion from previous primary or current secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in rdr_workload:
            wait_for_all_resources_deletion(wl.workload_namespace)

        logger.info("Relocate successful")
