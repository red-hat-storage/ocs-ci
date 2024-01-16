import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.utility import version
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)

logger = logging.getLogger(__name__)


if config.RUN.get("rdr_relocate_via_ui"):
    polarion_id_relocate = "OCS-5034"
else:
    polarion_id_relocate = "OCS-4772"


@tier1
@turquoise_squad
@pytest.mark.polarion_id("OCS-4772")
@pytest.mark.polarion_id(polarion_id_relocate)
class TestSequentialRelocate:
    """
    Test Sequential Relocate actions.
    This test is also compatible to be run from ACM UI. Pass the yaml conf/ocsci/dr_ui.yaml to test from UI.

    """

    def test_sequential_relocate_to_secondary(self, setup_acm_ui, dr_workload):
        """
        Test to verify relocate action for multiple workloads one after another from primary to secondary cluster

        """
        if config.RUN.get("rdr_relocate_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error("ODF/ACM version isn't supported for Relocate operation")
                raise NotImplementedError
        acm_obj = AcmAddClusters()

        workloads = dr_workload(num_of_subscription=5)

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workloads[0].workload_namespace
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workloads[0].workload_namespace
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        time.sleep(wait_time * 60)

        config.switch_acm_ctx()

        if config.RUN.get("rdr_relocate_via_ui"):
            dr_submariner_validation_from_ui(acm_obj)
            check_cluster_status_on_acm_console(acm_obj)
            logger.info("Start the process of Relocate from UI")
        else:
            logger.info("Start the process of Relocate from CLI")

        # Initiate relocate for all the workloads one after another
        relocate_results = []
        with ThreadPoolExecutor() as executor:
            for wl in workloads:
                if config.RUN.get("rdr_relocate_via_ui"):
                    # Relocate via ACM UI
                    failover_relocate_ui(
                        acm_obj,
                        scheduling_interval=scheduling_interval,
                        workload_to_move=f"{wl.workload_name}-1",
                        policy_name=wl.dr_policy_name,
                        failover_or_preferred_cluster=secondary_cluster_name,
                        action=constants.ACTION_RELOCATE,
                    )
                relocate_results.append(
                    executor.submit(
                        dr_helpers.relocate,
                        preferred_cluster=secondary_cluster_name,
                        namespace=wl.workload_namespace,
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
            )

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum([wl.workload_pvc_count for wl in workloads])
        )

        if config.RUN.get("rdr_relocate_via_ui"):
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )
