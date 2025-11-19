import logging
from time import sleep

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


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.16")
class TestFailoverAndRelocateWithDiscoveredApps:
    """
    Test Failover and Relocate with Discovered Apps and
    custom CephFS SC and Pool

    """

    @pytest.mark.parametrize(
        argnames=[
            "primary_cluster_down",
            "pvc_interface",
            "kubeobject",
            "recipe",
            "iterations",
            "custom_sc",
            "replica",
            "compression",
        ],
        argvalues=[
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                1,
                False,
                None,
                None,
                marks=[tier1, acceptance],
                id="primary_up-rbd",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                1,
                False,
                None,
                None,
                marks=tier4,
                id="primary_down-rbd",
            ),
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                3,
                False,
                None,
                None,
                marks=tier4,
                id="primary_up-rbd-multiple-iterations",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                3,
                False,
                None,
                None,
                marks=tier4,
                id="primary_down-rbd-multiple-iterations",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                1,
                1,
                1,
                False,
                None,
                None,
                marks=[skipif_ocs_version("<4.19"), tier1, acceptance],
                id="primary_up-cephfs",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                1,
                1,
                False,
                None,
                None,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_down-cephfs",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                1,
                1,
                3,
                False,
                None,
                None,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_up-cephfs-multiple-iterations",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                1,
                3,
                False,
                None,
                None,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_down-cephfs-multiple-iterations",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                0,
                1,
                True,
                2,
                None,
                marks=[skipif_ocs_version("<4.21")],
                id="custom_pool_replica2_without_compression_with_primary-down",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                0,
                1,
                True,
                3,
                "aggressive",
                marks=[skipif_ocs_version("<4.21")],
                id="custom_pool_replica3_with_compression_with_primary-down",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                0,
                1,
                True,
                2,
                "aggressive",
                marks=[skipif_ocs_version("<4.21")],
                id="custom_pool_replica2_with_compression_with_primary-down",
            ),
        ],
    )
    def test_failover_and_relocate_discovered_apps(
        self,
        primary_cluster_down,
        pvc_interface,
        discovered_apps_dr_workload,
        nodes_multicluster,
        node_restart_teardown,
        kubeobject,
        recipe,
        iterations,
        custom_sc,
        replica,
        compression,
        cephfs_custom_storage_class,
    ):
        """
        Tests to verify application failover and relocate with discovered applications
        Covers primary cluster up or down scenarios.

        Test is parametrized to run with Custom CephFS Storage Class and Pool of Replica-2.
        """
        if custom_sc:
            logger.info("Calling fixture to create Custom Pool/SC..")
            cephfs_custom_storage_class(replica=replica, compression=compression)

        rdr_workloads = discovered_apps_dr_workload(
            pvc_interface=pvc_interface,
            kubeobject=kubeobject,
            recipe=recipe,
            custom_sc=custom_sc,
        )
        first_workload = rdr_workloads[0]
        drpc_objs = [
            DRPC(
                namespace=constants.DR_OPS_NAMESPACE,
                resource_name=wl.discovered_apps_placement_name,
            )
            for wl in rdr_workloads
        ]

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                first_workload.workload_namespace,
                discovered_apps=True,
                resource_name=first_workload.discovered_apps_placement_name,
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        primary_cluster_name_before_failover_index = config.cur_index
        primary_cluster_name_before_failover_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            first_workload.workload_namespace,
            discovered_apps=True,
            resource_name=first_workload.discovered_apps_placement_name,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            first_workload.workload_namespace,
            discovered_apps=True,
            resource_name=first_workload.discovered_apps_placement_name,
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            cg_cephfs_enabled = is_cg_cephfs_enabled()
            # Verify the creation of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            for rdr_workload in rdr_workloads:
                if cg_cephfs_enabled:
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
                wait_for_replication_destinations_creation(
                    rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
                )

        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
            logger.info(
                "Checking for lastKubeObjectProtectionTime before Failover Operation"
            )
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

        for rdr_workload in rdr_workloads:
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
            logger.info(
                "Wait for all the pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking for Ceph Health OK")
            ceph_health_check()

        for rdr_workload in rdr_workloads:
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
        for rdr_workload in rdr_workloads:
            dr_helpers.wait_for_all_resources_creation(
                rdr_workload.workload_pvc_count,
                rdr_workload.workload_pod_count,
                rdr_workload.workload_namespace,
                timeout=1200,
                discovered_apps=True,
                vrg_name=rdr_workload.discovered_apps_placement_name,
                performed_dr_action=True,
            )

        if pvc_interface == constants.CEPHFILESYSTEM:
            for rdr_workload in rdr_workloads:
                # verify the deletion of replication destination resources
                # on the old secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                wait_for_replication_destinations_deletion(
                    rdr_workload.workload_namespace
                )
                if cg_cephfs_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=rdr_workload.workload_namespace,
                        should_exist=False,
                    )

                # Verify the creation of ReplicationDestination resources on
                # the new secondary cluster
                config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
                wait_for_replication_destinations_creation(
                    rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
                )
                if cg_cephfs_enabled:
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

        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
            logger.info(
                "Checking for lastKubeObjectProtectionTime after Failover Operation"
            )
            dr_helpers.verify_last_kubeobject_protection_time(
                drpc_obj, rdr_workload.kubeobject_capture_interval_int
            )

        logger.info("Running Relocate Steps")
        for rdr_workload in rdr_workloads:
            dr_helpers.relocate(
                preferred_cluster=primary_cluster_name_before_failover,
                namespace=rdr_workload.workload_namespace,
                workload_placement_name=rdr_workload.discovered_apps_placement_name,
                discovered_apps=True,
                old_primary=secondary_cluster_name,
                workload_instance=rdr_workload,
            )

        # Verify resources creation on primary cluster (preferredCluster)
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        for rdr_workload in rdr_workloads:
            dr_helpers.wait_for_all_resources_creation(
                rdr_workload.workload_pvc_count,
                rdr_workload.workload_pod_count,
                rdr_workload.workload_namespace,
                timeout=1200,
                discovered_apps=True,
                vrg_name=rdr_workload.discovered_apps_placement_name,
                performed_dr_action=True,
            )

        if pvc_interface == constants.CEPHFILESYSTEM:
            for rdr_workload in rdr_workloads:
                # Verify the deletion of replication destination resources
                # On the old secondary cluster
                config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
                wait_for_replication_destinations_deletion(
                    rdr_workload.workload_namespace
                )
                if cg_cephfs_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=rdr_workload.workload_namespace,
                        should_exist=False,
                    )

                # Verify the creation of ReplicationDestination resources on
                # the current secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                wait_for_replication_destinations_creation(
                    rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
                )
                if cg_cephfs_enabled:
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

        for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
            logger.info(
                "Checking for lastKubeObjectProtectionTime post Relocate Operation"
            )
            dr_helpers.verify_last_kubeobject_protection_time(
                drpc_obj, rdr_workload.kubeobject_capture_interval_int
            )
