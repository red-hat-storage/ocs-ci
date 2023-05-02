import logging
import pytest

from time import sleep

from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.framework import config
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.utility import version
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)

polarion_id_relocate = "OCS-4425"
if config.RUN.get("rdr_relocate_via_ui"):
    polarion_id_relocate = "OCS-4744"


@acceptance
@tier1
@pytest.mark.polarion_id(polarion_id_relocate)
class TestRelocate:
    """
    Test Relocate action

    """

    def test_relocate(self, setup_acm_ui, rdr_workload):
        """
        Test to verify relocation of application between managed clusters

        This test is also compatible to be run from ACM UI,
        pass the yaml conf/ocsci/dr_ui.yaml to trigger it.

        """
        if config.RUN.get("rdr_relocate_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error("ODF/ACM version isn't supported for Relocate operation")
                raise NotImplementedError

        acm_obj = AcmAddClusters()

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )

        if config.RUN.get("rdr_relocate_via_ui"):
            logger.info("Start the process of Relocate from ACM UI")
            config.switch_acm_ctx()
            dr_submariner_validation_from_ui(acm_obj)
            check_cluster_status_on_acm_console(acm_obj)
            # Relocate via ACM UI
            failover_relocate_ui(
                acm_obj,
                scheduling_interval=scheduling_interval,
                workload_to_move=f"{rdr_workload.workload_name}-1",
                policy_name=rdr_workload.dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
                action=constants.ACTION_RELOCATE,
            )
        else:
            # Relocate action via CLI
            dr_helpers.relocate(secondary_cluster_name, rdr_workload.workload_namespace)

        # Verify resources deletion from previous primary or current secondary cluster
        dr_helpers.set_current_secondary_cluster_context(
            rdr_workload.workload_namespace
        )
        dr_helpers.wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        # Verify resources creation on new primary cluster (preferredCluster)
        dr_helpers.set_current_primary_cluster_context(rdr_workload.workload_namespace)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        dr_helpers.wait_for_mirroring_status_ok()

        if config.RUN.get("rdr_relocate_via_ui"):
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )

        # TODO: Add data integrity checks
