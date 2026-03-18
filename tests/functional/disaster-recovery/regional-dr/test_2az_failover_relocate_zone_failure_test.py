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
                "data-a",
                marks=[tier1, acceptance],
                id="rbd-zone-data-a",
            ),
            pytest.param(
                constants.CEPHBLOCKPOOL,
                "arbiter",
                marks=[tier1, acceptance],
                id="rbd-zone-arbiter",
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                "data-a",
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
        node_restart_teardown,
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
            node_restart_teardown: Fixture for node restart cleanup

        """
        primary_cluster_down = True  # Always bring down primary cluster during failover

        logger.info(
            f"Starting test with pvc_interface={pvc_interface}, "
            f"power_off_zone={power_off_zone}, "
            f"primary_cluster_down={primary_cluster_down}"
        )

        # ========================================
        # Step 1: Deploy 2 GitOps/Subscription Apps
        # ========================================
        logger.info("Deploying 2 GitOps/Subscription apps")
        gitops_workload_1 = all_dr_workloads["dr_workload"]()
        gitops_workload_2 = all_dr_workloads["dr_workload"]()
        gitops_workloads = [gitops_workload_1, gitops_workload_2]
        logger.info(f"Deployed {len(gitops_workloads)} GitOps workloads")

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
                pvc_vm=2,
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
        # Step 4: Perform Failover and Relocate for each workload
        # ========================================
        for idx, workload in enumerate(all_workloads, 1):
            workload_type = (
                "GitOps"
                if workload in gitops_workloads
                else "Discovered" if workload in discovered_workloads else "CNV"
            )
            logger.info(
                f"Processing workload {idx}/{len(all_workloads)} - Type: {workload_type}"
            )

            # Get workload details
            workload_namespace = workload.workload_namespace
            is_discovered_app = workload in (discovered_workloads + cnv_workloads)

            if is_discovered_app:
                resource_name = workload.discovered_apps_placement_name
            else:
                resource_name = workload.workload_name

            # Get primary and secondary cluster names
            primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
                workload_namespace,
                discovered_apps=is_discovered_app,
                resource_name=resource_name,
            )
            config.switch_to_cluster_by_name(primary_cluster_name)
            primary_cluster_index = config.cur_index
            primary_cluster_nodes = get_node_objs()

            secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
                workload_namespace,
                discovered_apps=is_discovered_app,
                resource_name=resource_name,
            )

            logger.info(
                f"Workload {idx}: Primary={primary_cluster_name}, "
                f"Secondary={secondary_cluster_name}"
            )

            # Get scheduling interval and wait for IOs
            scheduling_interval = dr_helpers.get_scheduling_interval(
                workload_namespace,
                discovered_apps=is_discovered_app,
                resource_name=resource_name,
            )

            wait_time = 2 * scheduling_interval
            logger.info(f"Waiting {wait_time} minutes for IOs to complete")
            sleep(wait_time * 60)

            # ========================================
            # Step 5: Failover to Secondary Cluster
            # ========================================
            logger.info(
                f"Starting failover for workload {idx} to {secondary_cluster_name}"
            )

            if primary_cluster_down:
                # Stop primary cluster nodes in the specified zone
                config.switch_to_cluster_by_name(primary_cluster_name)
                logger.info(
                    f"Stopping nodes in zone '{power_off_zone}' on primary cluster: {primary_cluster_name}"
                )

                # Get nodes in the specified zone
                zone_label = f"{constants.ZONE_LABEL}={power_off_zone}"
                zone_nodes_info = get_nodes_having_label(zone_label)
                zone_nodes = [
                    node_obj
                    for node_obj in primary_cluster_nodes
                    if node_obj.name
                    in [node["metadata"]["name"] for node in zone_nodes_info]
                ]

                if not zone_nodes:
                    logger.warning(
                        f"No nodes found in zone '{power_off_zone}'. "
                        f"Falling back to stopping all primary cluster nodes."
                    )
                    zone_nodes = primary_cluster_nodes
                else:
                    logger.info(
                        f"Found {len(zone_nodes)} nodes in zone '{power_off_zone}': "
                        f"{[node.name for node in zone_nodes]}"
                    )

                nodes_multicluster[primary_cluster_index].stop_nodes(zone_nodes)
                logger.info(f"Nodes in zone '{power_off_zone}' stopped")

            # Perform failover via CLI
            logger.info("Performing failover via CLI")
            failover_params = {
                "failover_cluster": secondary_cluster_name,
                "namespace": workload_namespace,
                "workload_placement_name": resource_name,
                "discovered_apps": is_discovered_app,
            }
            if not is_discovered_app:
                failover_params["workload_type"] = constants.SUBSCRIPTION
            dr_helpers.failover(**failover_params)

            # Wait for failover to complete
            dr_helpers.wait_for_all_resources_creation(
                workload.workload_pvc_count,
                workload.workload_pod_count,
                workload_namespace,
                discovered_apps=is_discovered_app,
            )

            # Verify workload on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            wait_for_pods_to_be_running(
                namespace=workload_namespace,
                timeout=720,
            )
            logger.info(
                f"Workload {idx} successfully failed over to {secondary_cluster_name}"
            )

            # Restart primary cluster zone nodes if they were stopped
            if primary_cluster_down:
                logger.info(
                    f"Starting nodes in zone '{power_off_zone}' on primary cluster: {primary_cluster_name}"
                )

                # Get the same zone nodes that were stopped
                config.switch_to_cluster_by_name(primary_cluster_name)
                zone_label = f"{constants.ZONE_LABEL}={power_off_zone}"
                zone_nodes_info = get_nodes_having_label(zone_label)
                zone_nodes = [
                    node_obj
                    for node_obj in primary_cluster_nodes
                    if node_obj.name
                    in [node["metadata"]["name"] for node in zone_nodes_info]
                ]

                if not zone_nodes:
                    zone_nodes = primary_cluster_nodes

                nodes_multicluster[primary_cluster_index].start_nodes(zone_nodes)
                config.switch_to_cluster_by_name(primary_cluster_name)
                wait_for_nodes_status(timeout=900)
                ceph_health_check()
                logger.info(f"Nodes in zone '{power_off_zone}' restarted and healthy")

            # ========================================
            # Step 6: Relocate back to Primary Cluster
            # ========================================
            logger.info(
                f"Starting relocate for workload {idx} back to {primary_cluster_name}"
            )

            # Wait before relocate
            sleep(scheduling_interval * 60)

            # Perform relocate via CLI
            logger.info("Performing relocate via CLI")
            relocate_params = {
                "preferred_cluster": primary_cluster_name,
                "namespace": workload_namespace,
                "workload_placement_name": resource_name,
                "discovered_apps": is_discovered_app,
            }
            if not is_discovered_app:
                relocate_params["workload_type"] = constants.SUBSCRIPTION
            dr_helpers.relocate(**relocate_params)

            # Wait for relocate to complete
            dr_helpers.wait_for_all_resources_creation(
                workload.workload_pvc_count,
                workload.workload_pod_count,
                workload_namespace,
                discovered_apps=is_discovered_app,
            )

            # Verify workload on primary cluster
            config.switch_to_cluster_by_name(primary_cluster_name)
            wait_for_pods_to_be_running(
                namespace=workload_namespace,
                timeout=720,
            )
            logger.info(
                f"Workload {idx} successfully relocated back to {primary_cluster_name}"
            )

            # Verify data integrity for CNV workloads
            if workload in cnv_workloads:
                logger.info(f"Verifying CNV workload {idx} data integrity")
                workload.verify_data_integrity()

        logger.info(
            f"Successfully completed failover and relocate for all {len(all_workloads)} workloads"
        )
