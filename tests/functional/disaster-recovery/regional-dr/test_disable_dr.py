import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@tier1
@turquoise_squad
class TestDisableDR:
    """
    Test Disable Disaster Recovery

    """

    @pytest.mark.parametrize(
        argnames=["workload_type"],
        argvalues=[
            pytest.param(
                *[constants.SUBSCRIPTION],
                marks=pytest.mark.polarion_id("OCS-6209"),
            ),
            pytest.param(
                *[constants.APPLICATION_SET],
                marks=pytest.mark.polarion_id("OCS-6209"),
            ),
        ],
    )
    def test_disable_dr(self, workload_type, dr_workload):
        """
        Test to verify disable DR of application

        """

        if workload_type == constants.SUBSCRIPTION:
            rdr_workload = dr_workload(num_of_subscription=1)[0]

        if workload_type == constants.APPLICATION_SET:
            rdr_workload = dr_workload(
                num_of_subscription=0, num_of_appset=1
            )[0]

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload.workload_namespace, workload_type
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace, workload_type
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Disable DR
        dr_helpers.disable_dr_rdr(workload_type)

        # Verify resources deletion from primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Verify pods and pvc on primary cluster
        logger.info(f"Validating pod,pvc on primary cluster - {primary_cluster_name}")
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
            skip_replication_resources=True,
        )

        # Verify replication resource deletion on primary cluster
        logger.info("Validating replication resource deletion...")
        dr_helpers.wait_for_replication_resources_deletion(
            rdr_workload.workload_namespace,
            timeout=300,
            check_state=False,
        )
