import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters

logger = logging.getLogger(__name__)


@rdr
@acceptance
@tier1
@turquoise_squad
class TestRelocate:
    """
    Test Relocate action via CLI and UI

    """

    @pytest.mark.parametrize(
        argnames=["pvc_interface", "via_ui"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL],
                False,
                marks=pytest.mark.polarion_id("OCS-4425"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM],
                False,
                marks=pytest.mark.polarion_id("OCS-4725"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL],
                True,
                marks=pytest.mark.polarion_id("OCS-4744"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM],
                True,
                marks=pytest.mark.polarion_id("OCS-6862"),
            ),
        ],
    )
    def test_relocate(self, pvc_interface, via_ui, setup_acm_ui, dr_workload):
        """
        Tests to verify application relocate between managed clusters.

        This test will run twice both via CLI and UI

        """
        if via_ui:
            acm_obj = AcmAddClusters()

        workloads = dr_workload(
            num_of_subscription=1, num_of_appset=1, pvc_interface=pvc_interface
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workloads[0].workload_namespace, workloads[0].workload_type
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workloads[0].workload_namespace, workloads[0].workload_type
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify the creation of ReplicationDestination resources on secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster_name)
            for wl in workloads:
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace, workloads[0].workload_type
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        for wl in workloads:
            if via_ui:
                logger.info("Start the process of Relocate from ACM UI")
                config.switch_acm_ctx()
                dr_submariner_validation_from_ui(acm_obj)
                check_cluster_status_on_acm_console(acm_obj)
                # Relocate via ACM UI
                failover_relocate_ui(
                    acm_obj,
                    scheduling_interval=scheduling_interval,
                    workload_to_move=f"{wl.workload_name}-1",
                    policy_name=wl.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                    action=constants.ACTION_RELOCATE,
                )
            else:
                # Relocate action via CLI
                dr_helpers.relocate(
                    secondary_cluster_name,
                    wl.workload_namespace,
                    wl.workload_type,
                    (
                        wl.appset_placement_name
                        if wl.workload_type == constants.APPLICATION_SET
                        else None
                    ),
                )

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
            )

        if pvc_interface == constants.CEPHFILESYSTEM:
            for wl in workloads:
                # Verify the deletion of ReplicationDestination resources on secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                # Verify the creation of ReplicationDestination resources on primary cluster
                config.switch_to_cluster_by_name(primary_cluster_name)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum([wl.workload_pvc_count for wl in workloads])
            )

        if via_ui:
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )
