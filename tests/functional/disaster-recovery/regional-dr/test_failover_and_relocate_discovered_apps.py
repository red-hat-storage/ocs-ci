import logging
from time import sleep


from ocs_ci.framework import config
from ocs_ci.framework.testlib import acceptance, tier1, skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC

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

    def test_failover_and_relocate_discovered_apps(self, discovered_apps_dr_workload):
        """
        Tests to verify application failover and Relocate with Discovered Apps
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is UP
            2) Relocate back to primary

        """

        rdr_workload = discovered_apps_dr_workload()[0]

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                rdr_workload.workload_namespace, discovered_apps=True
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace, discovered_apps=True
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace, discovered_apps=True
        )
        drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESAPCE)
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        logger.info("Checking for lastKubeObjectProtectionTime")
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, rdr_workload.kubeobject_capture_interval_int
        )

        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=rdr_workload.workload_namespace,
            discovered_apps=True,
            workload_placement_name=rdr_workload.discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
        )
        logger.info("Doing Cleanup Operations")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=rdr_workload.discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
            workload_namespace=rdr_workload.workload_namespace,
            workload_dir=rdr_workload.workload_dir,
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
            discovered_apps=True,
            vrg_name=rdr_workload.discovered_apps_placement_name
        )

        # Doing Relocate
        primary_cluster_name_after_failover = (
            dr_helpers.get_current_primary_cluster_name(
                rdr_workload.workload_namespace, discovered_apps=True
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace, discovered_apps=True
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace, discovered_apps=True
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

        # TODO: Add data integrity checks
