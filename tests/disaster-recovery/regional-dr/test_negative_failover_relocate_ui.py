import logging
import pytest

from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
)
from ocs_ci.framework.testlib import skipif_ocs_version

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.13")
class TestNegativeFailoverRelocate:
    """
    Test Failover/Relocate action to same cluster where workloads are already  running
    (primary up in case of Failover)

    """

    @pytest.mark.polarion_id("OCS-4746")
    def test_failover_to_same_cluster(
        self,
        setup_acm_ui,
        nodes_multicluster,
        dr_workload,
        node_restart_teardown,
    ):
        """
        Tests to verify if application failover to same cluster where it's running is blocked
        Pass the yaml conf/ocsci/dr_ui.yaml to trigger UI actions.

        """
        if config.RUN.get("rdr_failover_via_ui"):
            acm_obj = AcmAddClusters()
            rdr_workload = dr_workload(num_of_subscription=1)[0]

            dr_helpers.set_current_primary_cluster_context(
                rdr_workload.workload_namespace
            )

            scheduling_interval = dr_helpers.get_scheduling_interval(
                rdr_workload.workload_namespace
            )
            wait_time = 1  # Time in minutes
            logger.info(f"Waiting for {wait_time} minutes to run IOs")
            sleep(1)

            primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
                rdr_workload.workload_namespace
            )

            logger.info("Start the process of Failover from ACM UI")
            config.switch_acm_ctx()
            dr_submariner_validation_from_ui(acm_obj)
            check_cluster_status_on_acm_console(acm_obj)

            # Failover via ACM UI
            failover_relocate_ui(
                acm_obj,
                scheduling_interval=scheduling_interval,
                workload_to_move=f"{rdr_workload.workload_name}-1",
                policy_name=rdr_workload.dr_policy_name,
                failover_or_preferred_cluster=primary_cluster_name,
                move_workloads_to_same_cluster=True,
            )

    # @pytest.mark.polarion_id("OCS-4747")
    # def test_relocate_to_same_cluster(self, setup_acm_ui, rdr_workload):
    #     """
    #     Tests to verify if application relocate to same cluster where it's running is blocked
    #     Pass the yaml conf/ocsci/dr_ui.yaml to trigger UI actions.
    #
    #     """
    #     if config.RUN.get("rdr_relocate_via_ui"):
    #         acm_obj = AcmAddClusters()
    #
    #         scheduling_interval = dr_helpers.get_scheduling_interval(
    #             rdr_workload.workload_namespace
    #         )
    #
    #         wait_time = 1  # Time in minutes
    #         logger.info(f"Waiting for {wait_time} minutes to run IOs")
    #         sleep(1)
    #
    #         primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
    #             rdr_workload.workload_namespace
    #         )
    #
    #         logger.info("Start the process of Relocate from ACM UI")
    #         config.switch_acm_ctx()
    #         dr_submariner_validation_from_ui(acm_obj)
    #         check_cluster_status_on_acm_console(acm_obj)
    #         # Relocate via ACM UI
    #         failover_relocate_ui(
    #             acm_obj,
    #             scheduling_interval=scheduling_interval,
    #             workload_to_move=f"{rdr_workload.workload_name}-1",
    #             policy_name=rdr_workload.dr_policy_name,
    #             failover_or_preferred_cluster=primary_cluster_name,
    #             action=constants.ACTION_RELOCATE,
    #             move_workloads_to_same_cluster=True,
    #         )
