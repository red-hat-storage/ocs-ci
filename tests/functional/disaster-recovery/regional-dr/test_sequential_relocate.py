import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers import (
    wait_for_replication_destinations_creation,
    wait_for_replication_destinations_deletion,
    is_cg_cephfs_enabled,
)
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@pytest.mark.parametrize(
    argnames=["pvc_interface"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL],
            marks=pytest.mark.polarion_id("OCS-4772"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM],
            marks=pytest.mark.polarion_id("OCS-4735"),
        ),
    ],
)
class TestSequentialRelocate:
    """
    Test Sequential Relocate actions

    """

    def test_sequential_relocate_to_secondary(self, pvc_interface, dr_workload):
        """
        Test to verify relocate action for multiple workloads one after another from primary to secondary cluster

        """
        workloads = dr_workload(
            num_of_subscription=2, num_of_appset=3, pvc_interface=pvc_interface
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workloads[0].workload_namespace
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workloads[0].workload_namespace
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify the creation of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            for wl in workloads:
                # Verifying the existence of replication group destination and volume snapshots
                if is_cg_cephfs_enabled():
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
                wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace
        )
        wait_time = 1.5 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        time.sleep(wait_time * 60)

        # Initiate relocate for all the workloads one after another
        config.switch_acm_ctx()
        relocate_results = []
        with ThreadPoolExecutor() as executor:
            for wl in workloads:
                relocate_results.append(
                    executor.submit(
                        dr_helpers.relocate,
                        preferred_cluster=secondary_cluster_name,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        workload_placement_name=(
                            wl.appset_placement_name
                            if wl.workload_type == constants.APPLICATION_SET
                            else None
                        ),
                    )
                )
                time.sleep(5)

        # Wait for relocate results
        for relocate in relocate_results:
            relocate.result()

        # Verify resources deletion from primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        # Verify resources creation on secondary cluster (preferredCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                performed_dr_action=True,
            )

        if pvc_interface == constants.CEPHFILESYSTEM:
            for wl in workloads:
                # Verify the deletion of Replication Group Destination resources
                # on the old secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                wait_for_replication_destinations_deletion(wl.workload_namespace)

                cg_enabled = is_cg_cephfs_enabled()
                if cg_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=wl.workload_namespace,
                        should_exist=False,
                    )

                # Verify the creation of Replication Group Destination resources
                # on the current secondary cluster
                config.switch_to_cluster_by_name(primary_cluster_name)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )
                if cg_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=wl.workload_namespace,
                        should_exist=True,
                    )
                    # Verify the creation of Volume Snapshot
                    dr_helpers.wait_for_resource_count(
                        kind=constants.VOLUMESNAPSHOT,
                        namespace=wl.workload_namespace,
                        expected_count=wl.workload_pvc_count,
                    )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum([wl.workload_pvc_count for wl in workloads])
            )
