import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import acceptance, tier1, skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@acceptance
@tier1
@turquoise_squad
@skipif_ocs_version("<4.16")
class TestFailoverAndRelocateWithDiscoveredApps:
    """
    Test Failover and Relocate with Discovered Apps

    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface", "kubeobject", "recipe"],
        argvalues=[
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                0,
                1,
                id="primary_up-rbd-recipe",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                0,
                1,
                id="primary_down-rbd-recipe",
            ),
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                1,
                0,
                id="primary_up-rbd-kubeobject",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                1,
                0,
                id="primary_down-rbd-kubeobject",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                1,
                0,
                marks=[skipif_ocs_version("<4.19")],
                id="primary_up-cephfs-kubeobject",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                0,
                marks=[skipif_ocs_version("<4.19")],
                id="primary_down-cephfs-kubeobject",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                0,
                1,
                marks=[skipif_ocs_version("<4.19")],
                id="primary_up-cephfs-recipe",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                0,
                1,
                marks=[skipif_ocs_version("<4.19")],
                id="primary_down-cephfs-recipe",
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
    ):
        """
        Tests to verify application failover and Relocate with Discovered Apps
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is UP
            2) Relocate back to primary

        """
        rdr_workload = discovered_apps_dr_workload(
            pvc_interface=pvc_interface, kubeobject=kubeobject, recipe=recipe
        )[0]

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                rdr_workload.workload_namespace,
                discovered_apps=True,
                resource_name=rdr_workload.discovered_apps_placement_name,
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        primary_cluster_name_before_failover_index = config.cur_index
        primary_cluster_name_before_failover_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )
        drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESAPCE)
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify the creation of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_creation(
                rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
            )

        logger.info("Checking for lastKubeObjectProtectionTime")
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, rdr_workload.kubeobject_capture_interval_int
        )

        if primary_cluster_down:
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            logger.info(
                f"Stopping nodes of primary cluster: {primary_cluster_name_before_failover}"
            )
            nodes_multicluster[primary_cluster_name_before_failover_index].stop_nodes(
                primary_cluster_name_before_failover_nodes
            )

        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=rdr_workload.workload_namespace,
            discovered_apps=True,
            workload_placement_name=rdr_workload.discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
        )

        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} minutes before starting nodes "
                f"of primary cluster: {primary_cluster_name_before_failover}"
            )
            sleep(wait_time * 60)
            nodes_multicluster[primary_cluster_name_before_failover_index].start_nodes(
                primary_cluster_name_before_failover_nodes
            )
            wait_for_nodes_status(
                [node.name for node in primary_cluster_name_before_failover_nodes]
            )
            logger.info("Wait for 180 seconds for pods to stabilize")
            sleep(180)
            logger.info(
                "Wait for all the pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking for Ceph Health OK")
            ceph_health_check()

        logger.info("Doing Cleanup Operations")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=rdr_workload.discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
            workload_namespace=rdr_workload.workload_namespace,
            workload_dir=rdr_workload.workload_dir,
            vrg_name=rdr_workload.discovered_apps_placement_name,
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=rdr_workload.discovered_apps_placement_name,
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_deletion(
                rdr_workload.workload_namespace
            )
            # Verify the creation of ReplicationDestination resources on primary cluster
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            dr_helpers.wait_for_replication_destinations_creation(
                rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
            )
        # Doing Relocate
        primary_cluster_name_after_failover = (
            dr_helpers.get_current_primary_cluster_name(
                rdr_workload.workload_namespace,
                discovered_apps=True,
                resource_name=rdr_workload.discovered_apps_placement_name,
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )

        logger.info("Running Relocate Steps")
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        logger.info("Checking for lastKubeObjectProtectionTime")
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, rdr_workload.kubeobject_capture_interval_int
        )

        dr_helpers.relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=rdr_workload.workload_namespace,
            workload_placement_name=rdr_workload.discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=rdr_workload,
        )

        logger.info("Checking for lastKubeObjectProtectionTime post Relocate Operation")
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, rdr_workload.kubeobject_capture_interval_int
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=rdr_workload.discovered_apps_placement_name,
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            dr_helpers.wait_for_replication_destinations_deletion(
                rdr_workload.workload_namespace
            )
            # Verify the creation of ReplicationDestination resources on primary cluster
            config.switch_to_cluster_by_name(primary_cluster_name_after_failover)
            dr_helpers.wait_for_replication_destinations_creation(
                rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
            )
