import logging
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import acceptance, tier1, tier4, skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers import (
    wait_for_replication_destinations_creation,
    wait_for_replication_destinations_deletion,
    is_cg_cephfs_enabled,
)
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


def process_single_workload(
    rdr_workload,
    primary_cluster_down,
    pvc_interface,
    nodes_multicluster,
    primary_cluster_name_before_failover,
    primary_cluster_name_before_failover_index,
    primary_cluster_name_before_failover_nodes,
):
    """
    Process a single workload through failover and relocate operations.
    This function is designed to be called in parallel for multiple workloads.

    Args:
        rdr_workload: Workload object containing workload details
        primary_cluster_down (bool): Whether to simulate primary cluster down
        pvc_interface (str): PVC interface type (RBD or CephFS)
        nodes_multicluster: Multicluster nodes object
        primary_cluster_name_before_failover (str): Primary cluster name
        primary_cluster_name_before_failover_index (int): Index of primary cluster
        primary_cluster_name_before_failover_nodes (list): List of primary cluster nodes

    Returns:
        dict: Result dictionary with status and workload info
    """
    workload_name = rdr_workload.discovered_apps_placement_name
    try:
        logger.info(
            f"[{workload_name}] ========== Starting workload processing =========="
        )
        logger.info(
            f"[{workload_name}] Primary cluster: {primary_cluster_name_before_failover}"
        )

        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )
        logger.info(f"[{workload_name}] Secondary cluster: {secondary_cluster_name}")

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )
        logger.info(
            f"[{workload_name}] Scheduling interval: {scheduling_interval} minutes"
        )

        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )
        logger.info(
            f"[{workload_name}] DRPC object created for namespace: {constants.DR_OPS_NAMESPACE}"
        )

        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"[{workload_name}] ========== Phase 1: Initial IO Wait ==========")
        logger.info(f"[{workload_name}] Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        logger.info(f"[{workload_name}] IO wait completed")

        if pvc_interface == constants.CEPHFILESYSTEM:
            logger.info(
                f"[{workload_name}] ========== Verifying CephFS Replication Resources =========="
            )
            # Verify the creation of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            logger.info(
                f"[{workload_name}] Switched to secondary cluster: {secondary_cluster_name}"
            )
            # Verifying the existence of replication group destination and volume snapshots
            cg_enabled = is_cg_cephfs_enabled()
            logger.info(f"[{workload_name}] CephFS CG enabled: {cg_enabled}")
            if cg_enabled:
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=rdr_workload.workload_namespace,
                    should_exist=True,
                )
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=rdr_workload.workload_namespace,
                    expected_count=rdr_workload.workload_pvc_count,
                )
                logger.info(
                    f"[{workload_name}] Replication group destination and volume snapshots verified"
                )

            logger.info(
                f"[{workload_name}] Waiting for {rdr_workload.workload_pvc_count} replication destinations"
            )
            wait_for_replication_destinations_creation(
                rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
            )
            logger.info(
                f"[{workload_name}] Replication destinations created successfully"
            )

        logger.info(
            f"[{workload_name}] ========== Verifying KubeObject Protection =========="
        )
        logger.info(f"[{workload_name}] Checking for lastKubeObjectProtectionTime")
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, rdr_workload.kubeobject_capture_interval_int
        )
        logger.info(f"[{workload_name}] KubeObject protection time verified")

        if primary_cluster_down:
            logger.info(
                f"[{workload_name}] ========== Phase 2: Simulating Primary Cluster Down =========="
            )
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            logger.info(
                f"[{workload_name}] Stopping {len(primary_cluster_name_before_failover_nodes)} nodes \
                    of primary cluster: {primary_cluster_name_before_failover}"
            )
            nodes_multicluster[primary_cluster_name_before_failover_index].stop_nodes(
                primary_cluster_name_before_failover_nodes
            )
            logger.info(f"[{workload_name}] Primary cluster nodes stopped successfully")

        logger.info(
            f"[{workload_name}] ========== Phase 3: Performing Failover =========="
        )
        logger.info(
            f"[{workload_name}] Initiating failover from {primary_cluster_name_before_failover} \
                to {secondary_cluster_name}"
        )
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=rdr_workload.workload_namespace,
            discovered_apps=True,
            workload_placement_name=rdr_workload.discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
        )
        logger.info(f"[{workload_name}] Failover completed successfully")

        if primary_cluster_down:
            logger.info(
                f"[{workload_name}] ========== Phase 4: Recovering Primary Cluster =========="
            )
            logger.info(
                f"[{workload_name}] Waiting for {wait_time} minutes before starting nodes "
                f"of primary cluster: {primary_cluster_name_before_failover}"
            )
            sleep(wait_time * 60)
            logger.info(
                f"[{workload_name}] Wait completed, starting primary cluster nodes"
            )
            nodes_multicluster[primary_cluster_name_before_failover_index].start_nodes(
                primary_cluster_name_before_failover_nodes
            )
            logger.info(f"[{workload_name}] Primary cluster nodes started")
            wait_for_nodes_status(
                [node.name for node in primary_cluster_name_before_failover_nodes]
            )
            logger.info(f"[{workload_name}] All nodes are in Ready state")

            logger.info(
                f"[{workload_name}] Waiting for all pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), f"[{workload_name}] Not all the pods reached running state"
            logger.info(f"[{workload_name}] All pods are running")

            logger.info(f"[{workload_name}] Checking for Ceph Health OK")
            ceph_health_check()
            logger.info(f"[{workload_name}] Ceph cluster is healthy")

        logger.info(
            f"[{workload_name}] ========== Phase 5: Cleanup Operations =========="
        )
        logger.info(f"[{workload_name}] Starting cleanup operations")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=rdr_workload.discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
            workload_namespace=rdr_workload.workload_namespace,
            workload_dir=rdr_workload.workload_dir,
            vrg_name=rdr_workload.discovered_apps_placement_name,
        )
        logger.info(f"[{workload_name}] Cleanup operations completed")

        # Verify resources creation on secondary cluster (failoverCluster)
        logger.info(
            f"[{workload_name}] ========== Phase 6: Verifying Failover Resources =========="
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        logger.info(
            f"[{workload_name}] Switched to failover cluster: {secondary_cluster_name}"
        )
        logger.info(
            f"[{workload_name}] Verifying {rdr_workload.workload_pvc_count} PVCs \
                and {rdr_workload.workload_pod_count} pods"
        )
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=rdr_workload.discovered_apps_placement_name,
            performed_dr_action=True,
        )
        logger.info(
            f"[{workload_name}] All resources created successfully on failover cluster"
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            logger.info(
                f"[{workload_name}] ========== Verifying CephFS Replication After Failover =========="
            )
            # verify the deletion of replication destination resources
            # on the old secondary cluster
            logger.info(
                f"[{workload_name}] Verifying deletion of replication destinations on old secondary"
            )
            config.switch_to_cluster_by_name(secondary_cluster_name)
            wait_for_replication_destinations_deletion(rdr_workload.workload_namespace)
            logger.info(
                f"[{workload_name}] Replication destinations deleted on old secondary"
            )

            cg_enabled = is_cg_cephfs_enabled()
            if cg_enabled:
                logger.info(
                    f"[{workload_name}] Verifying replication group destination deletion"
                )
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=rdr_workload.workload_namespace,
                    should_exist=False,
                )
                logger.info(f"[{workload_name}] Replication group destination deleted")

            # Verify the creation of ReplicationDestination resources on
            # the new secondary cluster
            logger.info(
                f"[{workload_name}] Verifying replication destinations on new secondary"
            )
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            dr_helpers.wait_for_replication_destinations_creation(
                rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
            )
            logger.info(
                f"[{workload_name}] Replication destinations created on new secondary"
            )

            if cg_enabled:
                logger.info(
                    f"[{workload_name}] Verifying replication group destination and volume snapshots"
                )
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=rdr_workload.workload_namespace,
                    should_exist=True,
                )

                # Verify the creation of Volume Snapshot
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=rdr_workload.workload_namespace,
                    expected_count=rdr_workload.workload_pvc_count,
                )
                logger.info(
                    f"[{workload_name}] Replication group destination and volume snapshots verified"
                )

        # Doing Relocate
        logger.info(
            f"[{workload_name}] ========== Phase 7: Preparing for Relocate =========="
        )
        primary_cluster_name_after_failover = (
            dr_helpers.get_current_primary_cluster_name(
                rdr_workload.workload_namespace,
                discovered_apps=True,
                resource_name=rdr_workload.discovered_apps_placement_name,
            )
        )
        logger.info(
            f"[{workload_name}] Current primary after failover: {primary_cluster_name_after_failover}"
        )

        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )
        logger.info(
            f"[{workload_name}] New secondary cluster: {secondary_cluster_name}"
        )

        logger.info(
            f"[{workload_name}] ========== Phase 8: IO Wait Before Relocate =========="
        )
        logger.info(
            f"[{workload_name}] Waiting for {wait_time} minutes to run IOs before relocate"
        )
        sleep(wait_time * 60)
        logger.info(f"[{workload_name}] IO wait completed")

        logger.info(
            f"[{workload_name}] ========== Verifying KubeObject Protection Before Relocate =========="
        )
        logger.info(f"[{workload_name}] Checking for lastKubeObjectProtectionTime")
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, rdr_workload.kubeobject_capture_interval_int
        )
        logger.info(f"[{workload_name}] KubeObject protection time verified")

        logger.info(
            f"[{workload_name}] ========== Phase 9: Performing Relocate =========="
        )
        logger.info(
            f"[{workload_name}] Initiating relocate from {primary_cluster_name_after_failover} \
                to {secondary_cluster_name}"
        )
        dr_helpers.relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=rdr_workload.workload_namespace,
            workload_placement_name=rdr_workload.discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=rdr_workload,
        )
        logger.info(f"[{workload_name}] Relocate completed successfully")

        logger.info(
            f"[{workload_name}] ========== Verifying KubeObject Protection After Relocate =========="
        )
        logger.info(
            f"[{workload_name}] Checking for lastKubeObjectProtectionTime post relocate"
        )
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, rdr_workload.kubeobject_capture_interval_int
        )
        logger.info(
            f"[{workload_name}] KubeObject protection time verified after relocate"
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        logger.info(
            f"[{workload_name}] ========== Phase 10: Verifying Relocate Resources =========="
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        logger.info(
            f"[{workload_name}] Switched to cluster: {primary_cluster_name_before_failover}"
        )
        logger.info(
            f"[{workload_name}] Verifying {rdr_workload.workload_pvc_count} PVCs \
                and {rdr_workload.workload_pod_count} pods after relocate"
        )
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=rdr_workload.discovered_apps_placement_name,
            performed_dr_action=True,
        )
        logger.info(
            f"[{workload_name}] All resources created successfully after relocate"
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            logger.info(
                f"[{workload_name}] ========== Verifying CephFS Replication After Relocate =========="
            )
            # Verify the deletion of replication destination resources
            # On the old secondary cluster
            logger.info(
                f"[{workload_name}] Verifying deletion of replication destinations on old secondary"
            )
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            wait_for_replication_destinations_deletion(rdr_workload.workload_namespace)
            logger.info(
                f"[{workload_name}] Replication destinations deleted on old secondary"
            )

            cg_enabled = is_cg_cephfs_enabled()
            if cg_enabled:
                logger.info(
                    f"[{workload_name}] Verifying replication group destination deletion"
                )
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=rdr_workload.workload_namespace,
                    should_exist=False,
                )
                logger.info(f"[{workload_name}] Replication group destination deleted")

            # Verify the creation of ReplicationDestination resources on
            # the current secondary cluster
            logger.info(
                f"[{workload_name}] Verifying replication destinations on current secondary"
            )
            config.switch_to_cluster_by_name(primary_cluster_name_after_failover)
            dr_helpers.wait_for_replication_destinations_creation(
                rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
            )
            logger.info(
                f"[{workload_name}] Replication destinations created on current secondary"
            )

            if cg_enabled:
                logger.info(
                    f"[{workload_name}] Verifying replication group destination and volume snapshots"
                )
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=rdr_workload.workload_namespace,
                    should_exist=True,
                )

                # Verify the creation of Volume Snapshot
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=rdr_workload.workload_namespace,
                    expected_count=rdr_workload.workload_pvc_count,
                )
                logger.info(
                    f"[{workload_name}] Replication group destination and volume snapshots verified"
                )

        logger.info(
            f"[{workload_name}] ========== Workload Processing Completed Successfully =========="
        )
        return {"status": "success", "workload": workload_name}

    except Exception as e:
        logger.error(
            f"[{workload_name}] ========== ERROR: Workload Processing Failed =========="
        )
        logger.error(f"[{workload_name}] Error details: {str(e)}")
        logger.exception(f"[{workload_name}] Full exception traceback:")
        return {"status": "failed", "workload": workload_name, "error": str(e)}


@rdr
@turquoise_squad
@skipif_ocs_version("<4.16")
class TestFailoverAndRelocateWithDiscoveredApps:
    """
    Test Failover and Relocate with Discovered Apps

    """

    @pytest.mark.parametrize(
        argnames=[
            "primary_cluster_down",
            "pvc_interface",
            "kubeobject",
            "recipe",
            "iterations",
        ],
        argvalues=[
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                1,
                marks=[tier1, acceptance],
                id="primary_up-rbd",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                1,
                marks=tier4,
                id="primary_down-rbd",
            ),
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                3,
                marks=tier4,
                id="primary_up-rbd-multiple-iterations",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                3,
                marks=tier4,
                id="primary_down-rbd-multiple-iterations",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                1,
                1,
                1,
                marks=[skipif_ocs_version("<4.19"), tier1, acceptance],
                id="primary_up-cephfs",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                1,
                1,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_down-cephfs",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                1,
                1,
                3,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_up-cephfs-multiple-iterations",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                1,
                3,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_down-cephfs-multiple-iterations",
            ),
        ],
    )
    def test_failover_and_relocate_discovered_apps(
        self,
        discovered_apps_dr_workload,
        primary_cluster_down,
        pvc_interface,
        nodes_multicluster,
        kubeobject,
        recipe,
        iterations,
    ):
        """
        Tests to verify application failover and Relocate with Discovered Apps
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is UP
            2) Relocate back to primary

        """
        rdr_workloads = discovered_apps_dr_workload(
            pvc_interface=pvc_interface, kubeobject=kubeobject, recipe=recipe
        )
        iteration = 1
        while iteration <= iterations:
            logger.info("=" * 80)
            logger.info(f"STARTING ITERATION {iteration} OF {iterations}")
            logger.info("=" * 80)

            # Get cluster info once for all workloads (they share the same clusters)
            if not rdr_workloads:
                logger.warning("No workloads to process, exiting iteration loop")
                break

            logger.info(f"Total workloads to process: {len(rdr_workloads)}")
            first_workload = rdr_workloads[0]
            primary_cluster_name_before_failover = (
                dr_helpers.get_current_primary_cluster_name(
                    first_workload.workload_namespace,
                    discovered_apps=True,
                    resource_name=first_workload.discovered_apps_placement_name,
                )
            )
            logger.info(
                f"Primary cluster before failover: {primary_cluster_name_before_failover}"
            )

            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            primary_cluster_name_before_failover_index = config.cur_index
            primary_cluster_name_before_failover_nodes = get_node_objs()
            logger.info(
                f"Primary cluster index: {primary_cluster_name_before_failover_index}"
            )
            logger.info(
                f"Number of nodes in primary cluster: {len(primary_cluster_name_before_failover_nodes)}"
            )

            # Process all workloads in parallel using ThreadPoolExecutor
            # This allows recipe and kubeobject workloads to run simultaneously
            max_workers = len(rdr_workloads)  # One thread per workload
            logger.info("=" * 80)
            logger.info(
                f"PARALLEL PROCESSING: Starting {max_workers} workload(s) concurrently"
            )
            logger.info(
                f"Workloads: {[w.discovered_apps_placement_name for w in rdr_workloads]}"
            )
            logger.info("=" * 80)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                logger.info(f"ThreadPoolExecutor created with {max_workers} workers")

                # Submit all workload processing tasks
                future_to_workload = {
                    executor.submit(
                        process_single_workload,
                        rdr_workload,
                        primary_cluster_down,
                        pvc_interface,
                        nodes_multicluster,
                        primary_cluster_name_before_failover,
                        primary_cluster_name_before_failover_index,
                        primary_cluster_name_before_failover_nodes,
                    ): rdr_workload
                    for rdr_workload in rdr_workloads
                }
                logger.info(
                    f"Submitted {len(future_to_workload)} workload tasks to thread pool"
                )

                # Wait for all workloads to complete and collect results
                logger.info("Waiting for all workload tasks to complete...")
                results = []
                completed_count = 0
                for future in as_completed(future_to_workload):
                    workload = future_to_workload[future]
                    completed_count += 1
                    try:
                        result = future.result()
                        results.append(result)
                        if result["status"] == "success":
                            logger.info(
                                f"[{completed_count}/{max_workers}] Workload {result['workload']} \
                                    completed successfully"
                            )
                        else:
                            logger.error(
                                f"[{completed_count}/{max_workers}] Workload {result['workload']} FAILED: \
                                    {result.get('error', 'Unknown error')}"
                            )
                    except Exception as exc:
                        logger.error(
                            f"[{completed_count}/{max_workers}] Workload {workload.discovered_apps_placement_name} "
                            f"generated an exception: {exc}"
                        )
                        logger.exception(
                            f"Full exception for {workload.discovered_apps_placement_name}:"
                        )
                        results.append(
                            {
                                "status": "failed",
                                "workload": workload.discovered_apps_placement_name,
                                "error": str(exc),
                            }
                        )

            logger.info("=" * 80)
            logger.info(f"All {max_workers} workload tasks completed")
            logger.info("=" * 80)

            # Check if all workloads completed successfully
            failed_workloads = [r for r in results if r["status"] == "failed"]
            successful_workloads = [r for r in results if r["status"] == "success"]

            logger.info(
                f"Results Summary: {len(successful_workloads)} successful, {len(failed_workloads)} failed"
            )

            if failed_workloads:
                failed_names = [r["workload"] for r in failed_workloads]
                logger.error("=" * 80)
                logger.error(f"ITERATION {iteration} FAILED")
                logger.error(f"Failed workloads: {failed_names}")
                logger.error("=" * 80)
                raise AssertionError(
                    f"The following workloads failed: {failed_names}. "
                    f"Check logs above for detailed error information."
                )

            logger.info("=" * 80)
            logger.info(f"ITERATION {iteration} COMPLETED SUCCESSFULLY")
            logger.info(
                f"All {len(successful_workloads)} workloads processed successfully"
            )
            logger.info("=" * 80)
            iteration += 1
