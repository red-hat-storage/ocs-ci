import logging
import pytest

from time import sleep

from ocs_ci.framework import config

from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
)

logger = logging.getLogger(__name__)


@tier1
@turquoise_squad
@skipif_ocs_version("<4.16")
class TestRDRMonitoringDashboardUI:
    """
    Test class for RDR monitoring dashboard validation

    """

    @pytest.mark.polarion_id("XXXX")
    def test_rdr_monitoring_dashboard_ui(
        self,
        setup_acm_ui,
        dr_workload,
    ):
        """
        Test to verify the presence of RDR monitoring dashboard and various workloads
        and their status on it

        """

        acm_obj = AcmAddClusters()
        rdr_workload = dr_workload(num_of_subscription=1, num_of_appset=1)[0]

        dr_helpers.set_current_primary_cluster_context(rdr_workload.workload_namespace)

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload.workload_namespace
        )

        logger.info("Navigate to ACM console")
        config.switch_acm_ctx()
        check_cluster_status_on_acm_console(acm_obj)

        # Failover via ACM UI
        result = failover_relocate_ui(
            acm_obj,
            scheduling_interval=scheduling_interval,
            workload_to_move=f"{rdr_workload.workload_name}-1",
            policy_name=rdr_workload.dr_policy_name,
            failover_or_preferred_cluster=primary_cluster_name,
            move_workloads_to_same_cluster=True,
        )
        assert result, "Failover negative scenario test via ACM UI failed"
        logger.info("Failover negative scenario test via ACM UI passed")
