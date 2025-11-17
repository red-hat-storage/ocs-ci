import logging
import pytest
import copy
from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC


logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestDisableDR:
    """
    Test Disable Disaster Recovery

    """

    @pytest.mark.parametrize(
        argnames=["pvc_interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL,
                marks=pytest.mark.polarion_id("OCS-6209"),
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-6241"),
            ),
        ],
    )
    def test_disable_dr(self, pvc_interface, dr_workload, discovered_apps_dr_workload):
        """
        Test to verify disable DR of application

        """
        discovered_apps = False

        rdr_workload = dr_workload(
            num_of_subscription=1, num_of_appset=1, pvc_interface=pvc_interface
        )

        drpc_subscription = DRPC(namespace=rdr_workload[0].workload_namespace)
        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{rdr_workload[1].appset_placement_name}-drpc",
        )

        drpc_objs = [drpc_subscription, drpc_appset]
        rdr_workloads = copy.copy(rdr_workload)

        if constants.CEPHFILESYSTEM not in pvc_interface:
            logger.info("Discovered apps")
            rdr_workload_discovered_apps = discovered_apps_dr_workload()[0]
            rdr_workloads.append(rdr_workload_discovered_apps)
            drpc_discovered_apps = DRPC(namespace=constants.DR_OPS_NAMESAPCE)
            drpc_objs.append(drpc_discovered_apps)
            discovered_apps = True

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workloads[0].workload_namespace,
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Check lastGroupSyncTime
        for drpc_obj in drpc_objs:
            dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        logger.info("Verified the lastGroupSyncTime before disabling the DR")

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workloads[0].workload_namespace,
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workloads[0].workload_namespace, rdr_workloads[0].workload_type
        )
        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify the creation of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            for wl in rdr_workloads:
                # Verifying the existence of replication group destination and volume snapshots
                cg_enabled = dr_helpers.is_cg_cephfs_enabled()
                if cg_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=wl.workload_namespace,
                        should_exist=True,
                    )
                    dr_helpers.wait_for_resource_count(
                        kind=constants.VOLUMESNAPSHOT,
                        namespace=wl.workload_namespace,
                        expected_count=wl.workload_pvc_count,
                    )
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        # Disable DR
        dr_helpers.disable_dr_rdr(discovered_apps=discovered_apps)

        # Verify resources deletion from primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Verify replication resource deletion on primary cluster
        for workload in rdr_workloads:
            logger.info(
                f"Validating replication resource deletion in namespace {workload.workload_namespace}..."
            )
            dr_helpers.wait_for_replication_resources_deletion(
                workload.workload_namespace,
                timeout=300,
                check_state=False,
            )
            # Verify pod status on primary cluster
            logger.info(
                f"Validate pods and pvc in {workload.workload_namespace} be in Running state"
            )
            dr_helpers.wait_for_all_resources_creation(
                workload.workload_pvc_count,
                workload.workload_pod_count,
                workload.workload_namespace,
                skip_replication_resources=True,
            )

        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify the deletion of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_deletion(wl.workload_namespace)

            for wl in rdr_workloads:
                # Verifying the existence of replication group destination
                cg_enabled = dr_helpers.is_cg_cephfs_enabled()
                if cg_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=wl.workload_namespace,
                        should_exist=False,
                    )
