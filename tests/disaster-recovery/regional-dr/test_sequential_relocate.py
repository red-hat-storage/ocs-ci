import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.helpers import dr_helpers

logger = logging.getLogger(__name__)


@tier1
@pytest.mark.polarion_id("OCS-4772")
class TestSequentialRelocate:
    """
    Test Sequential Relocate actions

    """

    def test_sequential_relocate_to_secondary(self, dr_workload):
        """
        Test to verify relocate action for multiple workloads one after another from primary to secondary cluster

        """
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
