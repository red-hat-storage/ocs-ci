import logging
from time import sleep
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.resources.pod import check_pods_in_statuses
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


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
    def test_disable_dr(self, pvc_interface, dr_workload):
        """
        Test to verify disable DR of application

        """

        rdr_workload = dr_workload(
            num_of_subscription=1, num_of_appset=1, pvc_interface=pvc_interface
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload[0].workload_namespace,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload[0].workload_namespace,
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Disable DR
        dr_helpers.disable_dr_rdr()

        # Verify resources deletion from primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Verify replication resource deletion on primary cluster
        for workload in rdr_workload:
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
                f"Wait for all the pods in {workload.workload_namespace} to be in running state"
            )
            assert check_pods_in_statuses(
                expected_statuses="Running",
                namespace=workload.workload_namespace,
            ), "Not all the pods in running state"
