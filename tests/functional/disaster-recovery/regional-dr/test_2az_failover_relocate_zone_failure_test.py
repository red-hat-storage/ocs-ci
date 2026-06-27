import logging
import random
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_node_objs,
    get_nodes,
    get_nodes_having_label,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.retry import retry

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
                "osd_zone",
                marks=[tier1, acceptance],
                id="rbd-zone-osd",
            ),
            pytest.param(
                constants.CEPHBLOCKPOOL,
                constants.ARBITER_ZONE_LABEL[0],
                marks=[tier1],
                id="rbd-zone-arbiter",
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                "osd_zone",
                marks=[tier1, acceptance],
                id="cephfs-zone-osd",
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                constants.ARBITER_ZONE_LABEL[0],
                marks=[tier1],
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
        node_restart_teardown,
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

        # Resolve power_off_zone to the real topology.kubernetes.io/zone label
        # on this cluster (equivalent to: oc get nodes --show-labels | grep zone).
        # There are always 3 zones total: 1 arbiter + 2 data zones.
        with config.RunWithPrimaryConfigContext():
            arbiter_zone = config.DEPLOYMENT.get(
                "arbiter_zone", constants.ARBITER_ZONE_LABEL[0]
            )
            logger.debug(f"Configured arbiter_zone='{arbiter_zone}'")

            worker_nodes = get_nodes()
            all_zone_labels = {
                node.data["metadata"]["labels"][constants.ZONE_LABEL]
                for node in worker_nodes
                if constants.ZONE_LABEL in node.data["metadata"]["labels"]
            }
            logger.debug(
                f"Found {len(worker_nodes)} worker nodes across "
                f"{len(all_zone_labels)} zone(s): {sorted(all_zone_labels)}"
            )

            if power_off_zone == constants.ARBITER_ZONE_LABEL[0]:
                # Use the real arbiter zone name as read from the node labels
                power_off_zone = arbiter_zone
                logger.debug(
                    f"Zone type='arbiter' resolved to arbiter_zone='{power_off_zone}'"
                )
            else:
                # "osd_zone" — exclude arbiter, pick randomly from the 2 remaining data zones
                data_zones = list(all_zone_labels - {arbiter_zone})
                logger.debug(
                    f"Zone type='osd_zone': available data zones "
                    f"(arbiter='{arbiter_zone}' excluded): {sorted(data_zones)}"
                )
                logger.assertion(
                    f"Data zones available on cluster: expected>0, actual={len(data_zones)}"
                )
                assert data_zones, "No data zones found on cluster nodes"
                power_off_zone = random.choice(data_zones)
                logger.debug(
                    f"Randomly selected power_off_zone='{power_off_zone}' "
                    f"from data zones {sorted(data_zones)}"
                )

        logger.info(
            f"Resolved power_off_zone='{power_off_zone}', "
            f"pvc_interface={pvc_interface}"
        )

        # ========================================
        # Step 1: Deploy GitOps/ApplicationSet App
        # ========================================
        logger.test_step("Deploy GitOps/ApplicationSet workload")
        gitops_workloads = all_dr_workloads["dr_workload"](
            num_of_subscription=0, num_of_appset=1, skip_mirroring_validation=True
        )
        logger.info(
            f"Deployed {len(gitops_workloads)} GitOps/ApplicationSet workload(s): "
            f"{[w.workload_namespace for w in gitops_workloads]}"
        )

        # ========================================
        # Step 2: Deploy Discovered App
        # ========================================
        logger.test_step("Deploy Discovered workload")
        discovered_workloads = all_dr_workloads["discovered_apps"](
            kubeobject=1, recipe=0, pvc_interface=pvc_interface, multi_ns=False
        )
        logger.info(
            f"Deployed {len(discovered_workloads)} Discovered workload(s): "
            f"{[w.workload_namespace for w in discovered_workloads]}"
        )

        # ========================================
        # Step 3: Deploy CNV Apps (RBD only)
        # ========================================
        cnv_workloads = []
        if pvc_interface == constants.CEPHBLOCKPOOL:
            logger.test_step("Deploy CNV workload (RBD only)")
            cnv_workloads = all_dr_workloads["discovered_apps_cnv"](
                pvc_vm=1,
                custom_sc=False,
                dr_protect=True,
                shared_drpc_protection=False,
                vm_type=constants.VM_VOLUME_PVC,
            )
            logger.info(
                f"Deployed {len(cnv_workloads)} CNV workload(s): "
                f"{[w.workload_namespace for w in cnv_workloads]}"
            )
        else:
            logger.info(
                f"Skipping CNV workload deployment: pvc_interface={pvc_interface}, "
                f"CNV requires CEPHBLOCKPOOL"
            )

        # Combine all workloads for iteration
        all_workloads = gitops_workloads + discovered_workloads + cnv_workloads
        logger.info(
            f"Total workloads: {len(all_workloads)} "
            f"(GitOps={len(gitops_workloads)}, "
            f"Discovered={len(discovered_workloads)}, "
            f"CNV={len(cnv_workloads)})"
        )

        # ========================================
        # Step 4: Validate mirroring status (RBD only)
        # ========================================
        if pvc_interface == constants.CEPHBLOCKPOOL:
            logger.test_step("Validate RBD mirroring status for all deployed workloads")
            # Flatten the list if any workload is itself a list
            flattened_workloads = []
            for wl in all_workloads:
                if isinstance(wl, list):
                    flattened_workloads.extend(wl)
                else:
                    flattened_workloads.append(wl)
            total_pvc_count = sum([wl.workload_pvc_count for wl in flattened_workloads])
            logger.info(
                f"Waiting for mirroring status OK: "
                f"{total_pvc_count} PVC(s) across {len(flattened_workloads)} workload(s) "
                f"(timeout=900s)"
            )
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=total_pvc_count, timeout=900
            )
            logger.info(f"Mirroring status OK for all {total_pvc_count} PVC(s)")
            # Use flattened list for the rest of the test
            all_workloads = flattened_workloads
        else:
            logger.info(f"Skipping mirroring validation: pvc_interface={pvc_interface}")

        # ========================================
        # Step 5: Collect workload metadata
        # ========================================
        logger.test_step(f"Collect DR metadata for {len(all_workloads)} workload(s)")
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

            logger.debug(
                f"Workload {idx}/{len(all_workloads)}: type={workload_type}, "
                f"namespace={workload_namespace}, resource_name={resource_name}, "
                f"primary={primary_cluster_name}, secondary={secondary_cluster_name}"
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

            logger.debug(
                f"Workload {idx}/{len(all_workloads)}: drpc_name={drpc_name}, "
                f"scheduling_interval={scheduling_interval}min"
            )

        logger.info(f"Metadata collected for {len(workload_metadata)} workload(s)")

        # Get max scheduling interval for wait time
        max_scheduling_interval = max(
            wl["scheduling_interval"] for wl in workload_metadata
        )
        wait_time = 2 * max_scheduling_interval
        logger.info(
            f"Waiting {wait_time} min (2x max scheduling interval of "
            f"{max_scheduling_interval} min) for IOs to settle before failover"
        )
        sleep(wait_time * 60)
        logger.info("IO settle wait complete")

        # ========================================
        # Step 6: Sequential Failover to Secondary Cluster
        # ========================================
        logger.test_step(
            f"Failover {len(all_workloads)} workload(s) to secondary cluster "
            f"with zone failure on '{power_off_zone}'"
        )

        # Stop primary cluster nodes in the target zone (once for all workloads)
        if primary_cluster_down and workload_metadata:
            first_workload = workload_metadata[0]
            config.switch_to_cluster_by_name(first_workload["primary_cluster_name"])
            logger.info(
                f"Simulating zone failure: stopping nodes in zone '{power_off_zone}' "
                f"on primary cluster '{first_workload['primary_cluster_name']}'"
            )

            zone_label = f"{constants.ZONE_LABEL}={power_off_zone}"
            logger.info(f"Looking up nodes with label: {zone_label}")
            zone_nodes_info = get_nodes_having_label(zone_label)
            zone_nodes = [
                node_obj
                for node_obj in first_workload["primary_cluster_nodes"]
                if node_obj.name
                in [node["metadata"]["name"] for node in zone_nodes_info]
            ]

            if not zone_nodes:
                logger.warning(
                    f"No nodes found with label '{zone_label}'. "
                    f"Falling back to stopping all {len(first_workload['primary_cluster_nodes'])} "
                    f"primary cluster nodes."
                )
                zone_nodes = first_workload["primary_cluster_nodes"]
            else:
                logger.info(
                    f"Found {len(zone_nodes)} node(s) in zone '{power_off_zone}': "
                    f"{[node.name for node in zone_nodes]}"
                )

            logger.info(
                f"Stopping {len(zone_nodes)} node(s): "
                f"{[node.name for node in zone_nodes]}"
            )
            nodes_multicluster[first_workload["primary_cluster_index"]].stop_nodes(
                zone_nodes
            )
            logger.info(
                f"Node(s) in zone '{power_off_zone}' stopped successfully. "
                f"Waiting 10 min for OpenShift to detect the node failure ..."
            )
            sleep(600)  # 10 minutes = 600 seconds
            logger.info("Node failure detection wait complete")

        # Perform failover for workloads one by one
        try:
            failover_batch = []
            for wl_meta in workload_metadata:
                logger.info(
                    f"Failover [{wl_meta['idx']}/{len(all_workloads)}] "
                    f"type={wl_meta['workload_type']}, "
                    f"drpc_name={wl_meta['drpc_name']}, "
                    f"namespace={wl_meta['workload_namespace']}, "
                    f"target={wl_meta['secondary_cluster_name']}"
                )

                # Initiate failover for this workload
                # Use extended timeout for zone failure scenarios (1800 seconds = 30 minutes)
                failover_params = {
                    "failover_cluster": wl_meta["secondary_cluster_name"],
                    "namespace": wl_meta["workload_namespace"],
                    "workload_placement_name": wl_meta["resource_name"],
                    "discovered_apps": wl_meta["is_discovered_app"],
                    "old_primary": wl_meta["old_primary"],
                    "timeout": 1800,
                }
                if not wl_meta["is_discovered_app"]:
                    failover_params["workload_type"] = (
                        constants.APPLICATION_SET
                        if wl_meta["is_appset"]
                        else constants.SUBSCRIPTION
                    )
                dr_helpers.failover(**failover_params)

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
                    f"Waiting for resources on secondary cluster "
                    f"'{wl_meta['secondary_cluster_name']}': "
                    f"pvc_count={wl_meta['workload'].workload_pvc_count}, "
                    f"pod_count={wl_meta['workload'].workload_pod_count}, "
                    f"timeout=1200s"
                )

                config.switch_to_cluster_by_name(wl_meta["secondary_cluster_name"])
                dr_helpers.wait_for_all_resources_creation(
                    wl_meta["workload"].workload_pvc_count,
                    wl_meta["workload"].workload_pod_count,
                    wl_meta["workload_namespace"],
                    discovered_apps=wl_meta["is_discovered_app"],
                    timeout=1200,
                )
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
                    f"Failover complete [{wl_meta['idx']}/{len(all_workloads)}]: "
                    f"drpc_name={wl_meta['drpc_name']}, "
                    f"cluster={wl_meta['secondary_cluster_name']}, "
                    f"namespace={wl_meta['workload_namespace']}"
                )
        except Exception as ex:
            logger.exception(
                f"Failover phase failed at workload "
                f"{len(completed_failovers)+1}/{len(all_workloads)}: "
                f"completed={completed_failovers}, batch={failover_batch}"
            )
            raise type(ex)(
                f"{str(ex)} | Failover phase context: requested_failover_batch="
                f"{failover_batch}, completed_failovers={completed_failovers}"
            ) from ex

        # Restart primary cluster zone nodes
        if primary_cluster_down and workload_metadata:
            first_workload = workload_metadata[0]
            logger.info(
                f"Restoring zone '{power_off_zone}': starting stopped node(s) "
                f"on primary cluster '{first_workload['primary_cluster_name']}'"
            )

            config.switch_to_cluster_by_name(first_workload["primary_cluster_name"])
            zone_label = f"{constants.ZONE_LABEL}={power_off_zone}"

            # Retry get_nodes_having_label as it may fail with CommandFailed
            @retry(CommandFailed, tries=5, delay=10, backoff=1)
            def get_zone_nodes_with_retry():
                return get_nodes_having_label(zone_label)

            zone_nodes_info = get_zone_nodes_with_retry()
            zone_nodes = [
                node_obj
                for node_obj in first_workload["primary_cluster_nodes"]
                if node_obj.name
                in [node["metadata"]["name"] for node in zone_nodes_info]
            ]

            if not zone_nodes:
                logger.warning(
                    f"No nodes found with label '{zone_label}' during restart; "
                    f"falling back to all primary cluster nodes"
                )
                zone_nodes = first_workload["primary_cluster_nodes"]
            else:
                logger.info(
                    f"Starting {len(zone_nodes)} node(s) in zone '{power_off_zone}': "
                    f"{[node.name for node in zone_nodes]}"
                )

            nodes_multicluster[first_workload["primary_cluster_index"]].start_nodes(
                zone_nodes
            )
            logger.info(
                f"Start signal sent for {len(zone_nodes)} node(s). "
                f"Waiting for nodes to reach Ready state (timeout=900s) ..."
            )
            config.switch_to_cluster_by_name(first_workload["primary_cluster_name"])
            wait_for_nodes_status([node.name for node in zone_nodes], timeout=900)
            logger.info(
                f"All node(s) in zone '{power_off_zone}' are Ready. "
                f"Waiting for pods to be running (timeout=720s) ..."
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"

            logger.info(
                f"Zone '{power_off_zone}' fully restored: "
                f"{len(zone_nodes)} node(s) Ready, all pods running"
            )

            # Cleanup discovered apps after failover and node restart
            discovered_workloads_after_failover = [
                wl_meta for wl_meta in workload_metadata if wl_meta["is_discovered_app"]
            ]
            if discovered_workloads_after_failover:
                logger.info(
                    f"Cleaning up {len(discovered_workloads_after_failover)} discovered "
                    f"workload(s) after failover and zone restore: "
                    f"{[wl['drpc_name'] for wl in discovered_workloads_after_failover]}"
                )
                for wl_meta in discovered_workloads_after_failover:
                    logger.debug(
                        f"Discovered-app cleanup: drpc_name={wl_meta['drpc_name']}, "
                        f"namespace={wl_meta['workload_namespace']}, "
                        f"old_primary={wl_meta['primary_cluster_name']}"
                    )
                    dr_helpers.do_discovered_apps_cleanup(
                        drpc_name=wl_meta["resource_name"],
                        old_primary=wl_meta["primary_cluster_name"],
                        workload_namespace=wl_meta["workload"].workload_namespace,
                        workload_dir=wl_meta["workload"].workload_dir,
                        vrg_name=wl_meta["workload"].discovered_apps_placement_name,
                    )

        # ========================================
        # Step 7: Sequential Relocate back to Primary Cluster
        # ========================================
        logger.test_step(
            f"Relocate {len(all_workloads)} workload(s) back to primary cluster"
        )

        # Wait one scheduling interval before relocate
        logger.info(
            f"Waiting {max_scheduling_interval} min (1x scheduling interval) "
            f"before starting relocate"
        )
        sleep(max_scheduling_interval * 60)
        logger.info("Pre-relocate wait complete")

        # Perform relocate for workloads one by one
        try:
            relocate_batch = []
            for wl_meta in workload_metadata:
                logger.info(
                    f"Relocate [{wl_meta['idx']}/{len(all_workloads)}] "
                    f"type={wl_meta['workload_type']}, "
                    f"drpc_name={wl_meta['drpc_name']}, "
                    f"namespace={wl_meta['workload_namespace']}, "
                    f"target={wl_meta['primary_cluster_name']}"
                )

                # Initiate relocate for this workload
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

                relocate_batch.append(
                    {
                        "drpc_name": wl_meta["drpc_name"],
                        "namespace": wl_meta["workload_namespace"],
                        "resource_name": wl_meta["resource_name"],
                        "workload_type": wl_meta["workload_type"],
                        "target_cluster": wl_meta["primary_cluster_name"],
                    }
                )
                # For discovered apps, perform cleanup before verifying resources
                if wl_meta["is_discovered_app"]:
                    logger.debug(
                        f"Discovered-app cleanup before resource verification: "
                        f"drpc_name={wl_meta['drpc_name']}, "
                        f"namespace={wl_meta['workload_namespace']}, "
                        f"old_primary={wl_meta['secondary_cluster_name']}"
                    )
                    dr_helpers.do_discovered_apps_cleanup(
                        drpc_name=wl_meta["resource_name"],
                        old_primary=wl_meta["secondary_cluster_name"],
                        workload_namespace=wl_meta["workload"].workload_namespace,
                        workload_dir=wl_meta["workload"].workload_dir,
                        vrg_name=wl_meta["workload"].discovered_apps_placement_name,
                    )

                logger.info(
                    f"Waiting for resources on primary cluster "
                    f"'{wl_meta['primary_cluster_name']}': "
                    f"pvc_count={wl_meta['workload'].workload_pvc_count}, "
                    f"pod_count={wl_meta['workload'].workload_pod_count}, "
                    f"timeout=1200s"
                )
                config.switch_to_cluster_by_name(wl_meta["primary_cluster_name"])
                dr_helpers.wait_for_all_resources_creation(
                    wl_meta["workload"].workload_pvc_count,
                    wl_meta["workload"].workload_pod_count,
                    wl_meta["workload_namespace"],
                    discovered_apps=wl_meta["is_discovered_app"],
                    timeout=1200,
                )
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
                    f"Relocate complete [{wl_meta['idx']}/{len(all_workloads)}]: "
                    f"drpc_name={wl_meta['drpc_name']}, "
                    f"cluster={wl_meta['primary_cluster_name']}, "
                    f"namespace={wl_meta['workload_namespace']}"
                )

        except Exception as ex:
            logger.exception(
                f"Relocate phase failed at workload "
                f"{len(completed_relocates)+1}/{len(all_workloads)}: "
                f"completed_relocates={completed_relocates}, "
                f"completed_failovers={completed_failovers}, "
                f"batch={relocate_batch}"
            )
            raise type(ex)(
                f"{str(ex)} | Relocate phase context: requested_relocate_batch="
                f"{relocate_batch}, completed_failovers={completed_failovers}, "
                f"completed_relocates={completed_relocates}"
            ) from ex

        logger.info(
            f"Failover and relocate complete for all {len(all_workloads)} workload(s): "
            f"pvc_interface={pvc_interface}, power_off_zone='{power_off_zone}', "
            f"failovers={len(completed_failovers)}, relocates={len(completed_relocates)}"
        )
