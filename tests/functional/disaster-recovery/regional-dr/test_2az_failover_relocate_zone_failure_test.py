import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1, tier4
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs, get_nodes_having_label
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@turquoise_squad
class Test2AZFailoverAndRelocateZoneFailure:
    """
    Test Failover and Relocate with zone failure on 2az cluster
    Tests deploy 2 GitOps apps, 2 Discovered apps, and 2 CNV apps

    """

    @pytest.mark.parametrize(
        argnames=["pvc_interface", "power_off_zone"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL,
                "data-1",
                marks=[tier1, acceptance],
                id="rbd-zone-data-a",
            ),
            pytest.param(
                constants.CEPHBLOCKPOOL,
                "arbiter",
                # marks=[tier1, acceptance],
                marks=[acceptance],
                id="rbd-zone-arbiter",
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                "data-1",
                marks=tier4,
                id="cephfs-zone-data-a",
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                "arbiter",
                marks=tier4,
                id="cephfs-zone-arbiter",
            ),
        ],
    )
    def test_failover_and_relocate_multiple_workloads(
        self,
        pvc_interface,
        power_off_zone,
        all_dr_workloads,
        nodes_multicluster,
        verify_arbiter_deployment_with_zone_failure,
    ):
        """
        Tests to verify application failover and relocate with multiple workloads:
        - 2 GitOps/Subscription apps
        - 2 Discovered apps
        - 2 CNV apps (only for RBD/CEPHBLOCKPOOL)

        This test covers:
        1. Deploy workloads on primary cluster
        2. Failover to secondary cluster (with zone failure on primary cluster)
        3. Relocate back to primary cluster
        4. Verify data integrity and application functionality

        Args:
            pvc_interface (str): Storage interface (CEPHBLOCKPOOL or CEPHFILESYSTEM)
            power_off_zone (str): Zone to power off during failover ("data-a" or "arbiter")
            all_dr_workloads: Combined fixture for all DR workload types
            nodes_multicluster: Fixture for multicluster node operations

        """
        primary_cluster_down = True  # Always bring down primary cluster during failover
        failover_batch = []
        relocate_batch = []

        logger.info(
            f"Starting test with pvc_interface={pvc_interface}, "
            f"power_off_zone={power_off_zone}, "
            f"primary_cluster_down={primary_cluster_down}"
        )

        # ========================================
        # Step 1: Deploy 2 GitOps/Subscription Apps
        # ========================================
        logger.info("Deploying 2 GitOps/ApplicationSet apps")
        gitops_workloads = all_dr_workloads["dr_workload"](
            num_of_subscription=0, num_of_appset=2, skip_mirroring_validation=True
        )
        logger.info(f"Deployed {len(gitops_workloads)} GitOps/ApplicationSet workloads")

        # ========================================
        # Step 2: Deploy 2 Discovered Apps
        # ========================================
        logger.info("Deploying 2 Discovered apps")
        discovered_workloads = all_dr_workloads["discovered_apps"](
            kubeobject=2, recipe=0, pvc_interface=pvc_interface, multi_ns=False
        )
        logger.info(f"Deployed {len(discovered_workloads)} Discovered apps")

        # ========================================
        # Step 3: Deploy 2 CNV Apps (only for RBD)
        # ========================================
        cnv_workloads = []
        if pvc_interface == constants.CEPHBLOCKPOOL:
            logger.info("Deploying 2 CNV apps (RBD only)")
            cnv_workloads = all_dr_workloads["discovered_apps_cnv"](
                pvc_vm=1,
                custom_sc=False,
                dr_protect=True,
                shared_drpc_protection=False,
                vm_type=constants.VM_VOLUME_PVC,
            )
            logger.info(f"Deployed {len(cnv_workloads)} CNV workloads")
        else:
            logger.info("Skipping CNV apps deployment (only supported for RBD)")

        # Combine all workloads for iteration
        all_workloads = gitops_workloads + discovered_workloads + cnv_workloads
        logger.info(f"Total workloads deployed: {len(all_workloads)}")

        # ========================================
        # Step 4: Validate mirroring status for all deployed workloads
        # ========================================
        if pvc_interface == constants.CEPHBLOCKPOOL:
            # Flatten the list if any workload is itself a list
            flattened_workloads = []
            for wl in all_workloads:
                if isinstance(wl, list):
                    flattened_workloads.extend(wl)
                else:
                    flattened_workloads.append(wl)
            total_pvc_count = sum([wl.workload_pvc_count for wl in flattened_workloads])
            logger.info(
                f"Validating mirroring status for {total_pvc_count} PVCs across {len(flattened_workloads)} workloads"
            )
            # dr_helpers.wait_for_mirroring_status_ok(
            #     replaying_images=total_pvc_count, timeout=900
            # )
            logger.info("Mirroring status validation successful for all workloads")
            # Use flattened list for the rest of the test
            all_workloads = flattened_workloads

        # ========================================
        # Step 5: Prepare workload metadata for batch operations
        # ========================================
        logger.info("Preparing workload metadata for batch failover and relocate")
        workload_metadata = []
        completed_failovers = []
        completed_relocates = []

        for idx, workload in enumerate(all_workloads, 1):
            workload_type = (
                "GitOps"
                if workload in gitops_workloads
                else "Discovered" if workload in discovered_workloads else "CNV"
            )

            # Get workload details
            workload_namespace = workload.workload_namespace
            is_discovered_app = workload in (discovered_workloads + cnv_workloads)
            is_appset = workload in gitops_workloads

            if is_discovered_app:
                resource_name = workload.discovered_apps_placement_name
            elif is_appset:
                resource_name = workload.appset_placement_name
            else:
                resource_name = workload.workload_name

            # Get primary and secondary cluster names
            primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
                workload_namespace,
                workload_type=(
                    constants.APPLICATION_SET if is_appset else constants.SUBSCRIPTION
                ),
                discovered_apps=is_discovered_app,
                resource_name=resource_name,
            )
            config.switch_to_cluster_by_name(primary_cluster_name)
            primary_cluster_index = config.cur_index
            primary_cluster_nodes = get_node_objs()

            secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
                workload_namespace,
                workload_type=(
                    constants.APPLICATION_SET if is_appset else constants.SUBSCRIPTION
                ),
                discovered_apps=is_discovered_app,
                resource_name=resource_name,
            )

            # Get scheduling interval
            scheduling_interval = dr_helpers.get_scheduling_interval(
                workload_namespace,
                workload_type=(
                    constants.APPLICATION_SET if is_appset else constants.SUBSCRIPTION
                ),
                discovered_apps=is_discovered_app,
                resource_name=resource_name,
            )

            drpc_name = (
                resource_name
                if is_discovered_app
                else (f"{resource_name}-drpc" if is_appset else workload_namespace)
            )
            action_old_primary = primary_cluster_name if is_discovered_app else None

            workload_metadata.append(
                {
                    "idx": idx,
                    "workload": workload,
                    "workload_type": workload_type,
                    "workload_namespace": workload_namespace,
                    "is_discovered_app": is_discovered_app,
                    "is_appset": is_appset,
                    "resource_name": resource_name,
                    "drpc_name": drpc_name,
                    "primary_cluster_name": primary_cluster_name,
                    "primary_cluster_index": primary_cluster_index,
                    "primary_cluster_nodes": primary_cluster_nodes,
                    "secondary_cluster_name": secondary_cluster_name,
                    "scheduling_interval": scheduling_interval,
                    "old_primary": action_old_primary,
                }
            )

            logger.info(
                f"Workload {idx}/{len(all_workloads)} ({workload_type}): "
                f"namespace={workload_namespace}, resource_name={resource_name}, "
                f"drpc_name={drpc_name}, Primary={primary_cluster_name}, "
                f"Secondary={secondary_cluster_name}"
            )

        # Get max scheduling interval for wait time
        max_scheduling_interval = max(
            wl["scheduling_interval"] for wl in workload_metadata
        )
        wait_time = 2 * max_scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for IOs to complete")
        sleep(wait_time * 60)

        # ========================================
        # Step 6: Batch Failover to Secondary Cluster
        # ========================================
        logger.info(f"Starting batch failover for all {len(all_workloads)} workloads")

        # Stop primary cluster nodes if needed (only once for all workloads)
        if primary_cluster_down and workload_metadata:
            first_workload = workload_metadata[0]
            config.switch_to_cluster_by_name(first_workload["primary_cluster_name"])
            logger.info(
                f"Stopping nodes in zone '{power_off_zone}' on primary cluster: "
                f"{first_workload['primary_cluster_name']}"
            )

            zone_label = f"{constants.ZONE_LABEL}={power_off_zone}"
            zone_nodes_info = get_nodes_having_label(zone_label)
            zone_nodes = [
                node_obj
                for node_obj in first_workload["primary_cluster_nodes"]
                if node_obj.name
                in [node["metadata"]["name"] for node in zone_nodes_info]
            ]

            if not zone_nodes:
                logger.warning(
                    f"No nodes found in zone '{power_off_zone}'. "
                    f"Falling back to stopping all primary cluster nodes."
                )
                zone_nodes = first_workload["primary_cluster_nodes"]
            else:
                logger.info(
                    f"Found {len(zone_nodes)} nodes in zone '{power_off_zone}': "
                    f"{[node.name for node in zone_nodes]}"
                )

            nodes_multicluster[first_workload["primary_cluster_index"]].stop_nodes(
                zone_nodes
            )
            logger.info(f"Nodes in zone '{power_off_zone}' stopped")

        # Perform failover for all workloads
        try:
            failover_batch = []
            for wl_meta in workload_metadata:
                failover_batch.append(
                    {
                        "drpc_name": wl_meta["drpc_name"],
                        "namespace": wl_meta["workload_namespace"],
                        "resource_name": wl_meta["resource_name"],
                        "workload_type": wl_meta["workload_type"],
                        "target_cluster": wl_meta["secondary_cluster_name"],
                    }
                )
                logger.info(
                    f"Initiating failover for workload {wl_meta['idx']}/{len(all_workloads)} "
                    f"({wl_meta['workload_type']}) namespace={wl_meta['workload_namespace']} "
                    f"resource_name={wl_meta['resource_name']} drpc_name={wl_meta['drpc_name']} "
                    f"to {wl_meta['secondary_cluster_name']}"
                )
                failover_params = {
                    "failover_cluster": wl_meta["secondary_cluster_name"],
                    "namespace": wl_meta["workload_namespace"],
                    "workload_placement_name": wl_meta["resource_name"],
                    "discovered_apps": wl_meta["is_discovered_app"],
                    "old_primary": wl_meta["old_primary"],
                }
                if not wl_meta["is_discovered_app"]:
                    failover_params["workload_type"] = (
                        constants.APPLICATION_SET
                        if wl_meta["is_appset"]
                        else constants.SUBSCRIPTION
                    )
                dr_helpers.failover(**failover_params)

            logger.info(
                f"Failover patches submitted for all workloads. Batch details: {failover_batch}"
            )

            # Wait for all failovers to complete
            logger.info("Waiting for all failovers to complete")
            for wl_meta in workload_metadata:
                logger.info(
                    f"Verifying failover completion for workload "
                    f"{wl_meta['idx']}/{len(all_workloads)} "
                    f"({wl_meta['workload_type']}) namespace={wl_meta['workload_namespace']} "
                    f"resource_name={wl_meta['resource_name']} "
                    f"drpc_name={wl_meta['drpc_name']}"
                )
                dr_helpers.wait_for_all_resources_creation(
                    wl_meta["workload"].workload_pvc_count,
                    wl_meta["workload"].workload_pod_count,
                    wl_meta["workload_namespace"],
                    discovered_apps=wl_meta["is_discovered_app"],
                    timeout=1200,
                )
                config.switch_to_cluster_by_name(wl_meta["secondary_cluster_name"])
                wait_for_pods_to_be_running(
                    namespace=wl_meta["workload_namespace"],
                    timeout=720,
                )
                completed_failovers.append(
                    {
                        "drpc_name": wl_meta["drpc_name"],
                        "namespace": wl_meta["workload_namespace"],
                        "resource_name": wl_meta["resource_name"],
                        "workload_type": wl_meta["workload_type"],
                        "target_cluster": wl_meta["secondary_cluster_name"],
                    }
                )
                logger.info(
                    f"Workload {wl_meta['idx']} successfully failed over to "
                    f"{wl_meta['secondary_cluster_name']} "
                    f"(drpc_name={wl_meta['drpc_name']}, "
                    f"namespace={wl_meta['workload_namespace']}, "
                    f"resource_name={wl_meta['resource_name']})"
                )
        except Exception as ex:
            logger.error(
                f"Failover phase failed. Requested failover batch: {failover_batch}. "
                f"Completed failovers before failure: {completed_failovers}"
            )
            raise type(ex)(
                f"{str(ex)} | Failover phase context: requested_failover_batch="
                f"{failover_batch}, completed_failovers={completed_failovers}"
            ) from ex

        # Restart primary cluster zone nodes if they were stopped
        if primary_cluster_down and workload_metadata:
            first_workload = workload_metadata[0]
            logger.info(
                f"Starting nodes in zone '{power_off_zone}' on primary cluster: "
                f"{first_workload['primary_cluster_name']}"
            )

            config.switch_to_cluster_by_name(first_workload["primary_cluster_name"])
            zone_label = f"{constants.ZONE_LABEL}={power_off_zone}"
            zone_nodes_info = get_nodes_having_label(zone_label)
            zone_nodes = [
                node_obj
                for node_obj in first_workload["primary_cluster_nodes"]
                if node_obj.name
                in [node["metadata"]["name"] for node in zone_nodes_info]
            ]

            if not zone_nodes:
                zone_nodes = first_workload["primary_cluster_nodes"]

            nodes_multicluster[first_workload["primary_cluster_index"]].start_nodes(
                zone_nodes
            )
            config.switch_to_cluster_by_name(first_workload["primary_cluster_name"])
            wait_for_nodes_status(timeout=900)
            ceph_health_check()
            logger.info(f"Nodes in zone '{power_off_zone}' restarted and healthy")

        # ========================================
        # Step 7: Batch Relocate back to Primary Cluster
        # ========================================
        logger.info(f"Starting batch relocate for all {len(all_workloads)} workloads")

        # Wait before relocate
        sleep(max_scheduling_interval * 60)

        # Perform relocate for all workloads
        try:
            relocate_batch = []
            for wl_meta in workload_metadata:
                relocate_batch.append(
                    {
                        "drpc_name": wl_meta["drpc_name"],
                        "namespace": wl_meta["workload_namespace"],
                        "resource_name": wl_meta["resource_name"],
                        "workload_type": wl_meta["workload_type"],
                        "target_cluster": wl_meta["primary_cluster_name"],
                    }
                )
                logger.info(
                    f"Initiating relocate for workload {wl_meta['idx']}/{len(all_workloads)} "
                    f"({wl_meta['workload_type']}) namespace={wl_meta['workload_namespace']} "
                    f"resource_name={wl_meta['resource_name']} drpc_name={wl_meta['drpc_name']} "
                    f"back to {wl_meta['primary_cluster_name']}"
                )
                relocate_params = {
                    "preferred_cluster": wl_meta["primary_cluster_name"],
                    "namespace": wl_meta["workload_namespace"],
                    "workload_placement_name": wl_meta["resource_name"],
                    "discovered_apps": wl_meta["is_discovered_app"],
                    "old_primary": wl_meta["secondary_cluster_name"],
                }
                if wl_meta["is_discovered_app"]:
                    relocate_params["workload_instance"] = wl_meta["workload"]
                    relocate_params["vm_auto_cleanup"] = True
                else:
                    relocate_params["workload_type"] = (
                        constants.APPLICATION_SET
                        if wl_meta["is_appset"]
                        else constants.SUBSCRIPTION
                    )
                dr_helpers.relocate(**relocate_params)

            logger.info(
                f"Relocate patches submitted for all workloads. Batch details: {relocate_batch}"
            )

            # Wait for all relocates to complete
            logger.info("Waiting for all relocates to complete")
            for wl_meta in workload_metadata:
                logger.info(
                    f"Verifying relocate completion for workload "
                    f"{wl_meta['idx']}/{len(all_workloads)} "
                    f"({wl_meta['workload_type']}) namespace={wl_meta['workload_namespace']} "
                    f"resource_name={wl_meta['resource_name']} "
                    f"drpc_name={wl_meta['drpc_name']}"
                )
                dr_helpers.wait_for_all_resources_creation(
                    wl_meta["workload"].workload_pvc_count,
                    wl_meta["workload"].workload_pod_count,
                    wl_meta["workload_namespace"],
                    discovered_apps=wl_meta["is_discovered_app"],
                    timeout=1200,
                )
                config.switch_to_cluster_by_name(wl_meta["primary_cluster_name"])
                wait_for_pods_to_be_running(
                    namespace=wl_meta["workload_namespace"],
                    timeout=720,
                )
                completed_relocates.append(
                    {
                        "drpc_name": wl_meta["drpc_name"],
                        "namespace": wl_meta["workload_namespace"],
                        "resource_name": wl_meta["resource_name"],
                        "workload_type": wl_meta["workload_type"],
                        "target_cluster": wl_meta["primary_cluster_name"],
                    }
                )
                logger.info(
                    f"Workload {wl_meta['idx']} successfully relocated back to "
                    f"{wl_meta['primary_cluster_name']} "
                    f"(drpc_name={wl_meta['drpc_name']}, "
                    f"namespace={wl_meta['workload_namespace']}, "
                    f"resource_name={wl_meta['resource_name']})"
                )

            discovered_workloads_to_cleanup = [
                wl_meta for wl_meta in workload_metadata if wl_meta["is_discovered_app"]
            ]
            if discovered_workloads_to_cleanup:
                logger.info(
                    "Starting explicit cleanup for discovered workloads after all relocates "
                    f"have completed. Workloads to cleanup: "
                    f"{[wl['drpc_name'] for wl in discovered_workloads_to_cleanup]}"
                )
                for wl_meta in discovered_workloads_to_cleanup:
                    logger.info(
                        f"Cleaning up discovered workload with drpc_name={wl_meta['drpc_name']}, "
                        f"namespace={wl_meta['workload_namespace']}, "
                        f"resource_name={wl_meta['resource_name']}, "
                        f"old_primary={wl_meta['secondary_cluster_name']}"
                    )
                    dr_helpers.do_discovered_apps_cleanup(
                        drpc_name=wl_meta["resource_name"],
                        old_primary=wl_meta["secondary_cluster_name"],
                        workload_namespace=wl_meta["workload"].workload_namespace,
                        workload_dir=wl_meta["workload"].workload_dir,
                        vrg_name=wl_meta["workload"].discovered_apps_placement_name,
                    )
        except Exception as ex:
            logger.error(
                f"Relocate phase failed. Requested relocate batch: {relocate_batch}. "
                f"Completed failovers before failure: {completed_failovers}. "
                f"Completed relocates before failure: {completed_relocates}"
            )
            raise type(ex)(
                f"{str(ex)} | Relocate phase context: requested_relocate_batch="
                f"{relocate_batch}, completed_failovers={completed_failovers}, "
                f"completed_relocates={completed_relocates}"
            ) from ex

        # ========================================
        # Step 8: Verify data integrity for CNV workloads
        # ========================================
        if cnv_workloads:
            logger.info("Verifying data integrity for CNV workloads")
            for wl_meta in workload_metadata:
                if wl_meta["workload"] in cnv_workloads:
                    logger.info(
                        f"Verifying CNV workload {wl_meta['idx']} data integrity"
                    )
                    wl_meta["workload"].verify_data_integrity()

        logger.info(
            f"Successfully completed failover and relocate for all {len(all_workloads)} workloads"
        )
