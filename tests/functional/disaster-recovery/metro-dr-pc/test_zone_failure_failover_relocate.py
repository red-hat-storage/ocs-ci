import logging
import random
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import mdr, turquoise_squad
from ocs_ci.framework.testlib import tier2
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_node_objs,
    get_nodes,
    get_nodes_having_label,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


@mdr
@turquoise_squad
class TestZoneFailureFailoverRelocate:
    """
    Test failover when one zone fails and relocate when the failed zone become ready

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, node_restart_teardown):
        """
        Teardown function: If fenced, un-fence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name is not None
                and dr_helpers.get_fence_state(self.primary_cluster_name) == "Fenced"
            ):
                dr_helpers.enable_unfence(self.primary_cluster_name)
                dr_helpers.gracefully_reboot_ocp_nodes(
                    drcluster_name=self.primary_cluster_name, disable_eviction=True
                )

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["power_off_zone"],
        argvalues=[
            pytest.param(
                "osd_zone",
                marks=[tier2],
                id="zone-osd",
            ),
            pytest.param(
                constants.ARBITER_ZONE_LABEL[0],
                marks=[tier2],
                id="zone-arbiter",
            ),
        ],
    )
    def test_zone_failure_failover_relocate_multiple_workloads(
        self,
        power_off_zone,
        all_dr_workloads,
        nodes_multicluster,
    ):
        """
        Test to verify application failover and relocate with multiple workloads when one zone in down:
        - 1 GitOps app each for RBD and CephFS
        - 1 Discovered app each for RBD and CephFS
        - 1 CNV discovered app (RBD)

        Args:
            power_off_zone (str): Zone to power off during failover
            all_dr_workloads: Combined fixture for all DR workload types
            nodes_multicluster: Fixture for multicluster node operations

        """
        failover_batch = []
        relocate_batch = []

        # Resolve power_off_zone to the real topology.kubernetes.io/zone label
        # on this cluster (equivalent to: oc get nodes --show-labels | grep zone).
        # There are always 3 zones total: 1 arbiter + 2 data zones.
        with config.RunWithPrimaryConfigContext():
            self.provider_cluster_index = config.get_provider_index()
        with config.RunWithConfigContext(self.provider_cluster_index):
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
                # TODO: Choose zone where primary cluster is present
                assert data_zones, "No data zones found on cluster nodes"
                power_off_zone = random.choice(data_zones)
                logger.debug(
                    f"Randomly selected power_off_zone='{power_off_zone}' "
                    f"from data zones {sorted(data_zones)}"
                )

        logger.info(f"Resolved power_off_zone is '{power_off_zone}'")

        # ========================================
        # Step 1: Deploy GitOps/ApplicationSet App
        # ========================================
        gitops_workloads = []
        logger.test_step("Deploy GitOps/ApplicationSet workload")
        for pvc_interface in (constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM):
            gitops_workload = all_dr_workloads["dr_workload"](
                num_of_subscription=0,
                num_of_appset=1,
                pvc_interface=pvc_interface,
                skip_mirroring_validation=True,
            )
            gitops_workloads.extend(gitops_workload)
        logger.info(
            f"Deployed {len(gitops_workloads)} GitOps/ApplicationSet workload(s): "
            f"{[w.workload_namespace for w in gitops_workloads]}"
        )

        # ========================================
        # Step 2: Deploy Discovered App
        # ========================================
        discovered_workloads = []
        logger.test_step("Deploy Discovered workloads")
        for pvc_interface in (constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM):
            discovered_workload = all_dr_workloads["discovered_apps"](
                kubeobject=1,
                recipe=0,
                pvc_interface=pvc_interface,
                multi_ns=False,
                skip_mirroring_validation=True,
            )
            discovered_workloads.extend(discovered_workload)
        logger.info(
            f"Deployed {len(discovered_workloads)} Discovered workload(s): "
            f"{[w.workload_namespace for w in discovered_workloads]}"
        )

        # ========================================
        # Step 3: Deploy CNV Apps (RBD only)
        # ========================================
        logger.test_step("Deploy CNV discovered apps workload ")
        cnv_workloads = all_dr_workloads["discovered_apps_cnv"](
            pvc_vm=1,
            custom_sc=False,
            dr_protect=True,
            shared_drpc_protection=False,
            vm_type=constants.VM_VOLUME_PVC,
            skip_mirroring_validation=True,
        )
        logger.info(
            f"Deployed {len(cnv_workloads)} CNV workload(s): "
            f"{[w.workload_namespace for w in cnv_workloads]}"
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
        # Step 4: Collect workload metadata
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
                f"Workload drpc_name={drpc_name}, namespace={workload_namespace}"
            )

        logger.info(f"Metadata collected for {len(workload_metadata)} workload(s)")

        self.primary_cluster_name = config.get_cluster_name_by_index(
            self.provider_cluster_index
        )

        # Ensure that the primary cluster in config is used and same for all workloads
        assert all(
            [
                workload_item["primary_cluster_name"] == self.primary_cluster_name
                for workload_item in workload_metadata
            ]
        ), f"Primary cluster is not same as in the config for some workloads. Workload details:\n{workload_metadata}"

        wait_time = 180
        logger.test_step(f"Wait {wait_time} seconds for IO")
        sleep(wait_time)

        # ========================================
        # Step 5: Stop the nodes in the target zone
        # ========================================
        logger.test_step(f"Stop the zone {power_off_zone}")
        with config.RunWithConfigContext(self.provider_cluster_index):
            logger.info(
                f"Simulating zone failure: stopping nodes in zone '{power_off_zone}' on base cluster"
            )

            zone_label = f"{constants.ZONE_LABEL}={power_off_zone}"
            logger.info(f"Looking up nodes with label: {zone_label}")
            zone_nodes_info = get_nodes_having_label(zone_label)
            zone_nodes = [
                node_obj
                for node_obj in get_node_objs()
                if node_obj.name
                in [node["metadata"]["name"] for node in zone_nodes_info]
            ]

            assert zone_nodes, f"No nodes found with label '{zone_label}'"
            logger.info(
                f"Found {len(zone_nodes)} node(s) in zone '{power_off_zone}': "
                f"{[node.name for node in zone_nodes]}"
            )

            logger.info(
                f"Stopping {len(zone_nodes)} node(s): {[node.name for node in zone_nodes]}"
            )
            nodes_multicluster[self.provider_cluster_index].stop_nodes(zone_nodes)
            logger.info(
                f"Node(s) in zone '{power_off_zone}' stopped successfully. "
                f"Waiting 10 min for OpenShift to detect the node failure."
            )
            sleep(600)
            logger.info("Node failure detection wait complete")

        # TODO: Poweroff primary cluster nodes if needed
        # ========================================
        # Step 6: Enable Fence
        # ========================================
        logger.test_step("Enable fence in primary cluster drcluster")
        dr_helpers.enable_fence(drcluster_name=self.primary_cluster_name)
        dr_helpers.verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_FENCE
        )

        config.switch_acm_ctx()
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
                    "skip_odf_cli_validation": False,
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
        except Exception:
            logger.exception(
                f"Failover phase failed at workload "
                f"{len(completed_failovers)+1}/{len(all_workloads)}: "
                f"completed={completed_failovers}, batch={failover_batch}"
            )
            raise

        # ========================================
        # Step 7: Start target zone nodes
        # ========================================
        logger.test_step(
            f"Start stopped node(s) {zone_nodes} on base cluster zone '{power_off_zone}'"
        )
        with config.RunWithConfigContext(self.provider_cluster_index):
            nodes_multicluster[self.provider_cluster_index].start_nodes(zone_nodes)
            wait_for_nodes_status([node.name for node in zone_nodes], timeout=600)
        logger.info(
            f"All node(s) in zone '{power_off_zone}' are Ready. "
            f"Waiting for pods to be running (timeout=720s) ..."
        )

        # TODO: wait_for_pods_to_be_running()
        logger.info(
            f"Zone '{power_off_zone}' fully restored: {len(zone_nodes)} node(s) Ready"
        )

        with config.RunWithPrimaryConfigContext():
            wait_for_nodes_status()
        with config.RunWithPrimaryConfigContext():
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
                        skip_resource_deletion_verification=True,
                    )
                    dr_helpers.mdr_post_failover_check(
                        namespace=wl_meta["workload"].workload_namespace
                    )

        # ========================================
        # Step 8: Enable unfence
        # ========================================
        logger.test_step("Unfence primary cluster")
        dr_helpers.enable_unfence(drcluster_name=self.primary_cluster_name)
        assert dr_helpers.verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_UNFENCE
        )

        with config.RunWithPrimaryConfigContext():
            dr_helpers.gracefully_reboot_ocp_nodes(
                drcluster_name=self.primary_cluster_name, disable_eviction=True
            )
            for wl_meta in workload_metadata:
                dr_helpers.wait_for_all_resources_deletion(
                    namespace=wl_meta["workload_namespace"],
                    discovered_apps=wl_meta["is_discovered_app"],
                    skip_vrg_check=True,
                )

        for wl_meta in workload_metadata:
            drpc_obj = DRPC(
                namespace=(
                    constants.DR_OPS_NAMESPACE
                    if wl_meta["is_discovered_app"]
                    else constants.GITOPS_CLUSTER_NAMESPACE
                ),
                resource_name=wl_meta["drpc_name"],
            )
            drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        # ========================================
        # Step 9: Sequential Relocate back to Primary Cluster
        # ========================================
        logger.test_step(f"Relocate {len(all_workloads)} workloads to primary cluster")
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
                    performed_dr_action=True,
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
                with config.RunWithConfigContext(
                    config.get_cluster_index_by_name(wl_meta["secondary_cluster_name"])
                ):
                    dr_helpers.mdr_post_failover_check(
                        namespace=wl_meta["workload"].workload_namespace
                    )
                    dr_helpers.wait_for_all_resources_deletion(
                        namespace=wl_meta["workload_namespace"],
                        discovered_apps=wl_meta["is_discovered_app"],
                        skip_vrg_check=True,
                    )
                logger.info(
                    f"Relocate complete [{wl_meta['idx']}/{len(all_workloads)}]: "
                    f"drpc_name={wl_meta['drpc_name']}, "
                    f"cluster={wl_meta['primary_cluster_name']}, "
                    f"namespace={wl_meta['workload_namespace']}"
                )
        except Exception:
            logger.exception(
                f"Relocate phase failed at workload "
                f"{len(completed_relocates)+1}/{len(all_workloads)}: "
                f"completed_relocates={completed_relocates}, "
                f"completed_failovers={completed_failovers}, "
                f"batch={relocate_batch}"
            )
            raise

        for wl_meta in workload_metadata:
            drpc_obj = DRPC(
                namespace=(
                    constants.DR_OPS_NAMESPACE
                    if wl_meta["is_discovered_app"]
                    else constants.GITOPS_CLUSTER_NAMESPACE
                ),
                resource_name=wl_meta["drpc_name"],
            )
            drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

        logger.info(
            f"Failover and relocate complete for all {len(all_workloads)} workload(s). "
            f"power_off_zone is '{power_off_zone}', "
            f"failovers={len(completed_failovers)}, relocates={len(completed_relocates)}"
        )
