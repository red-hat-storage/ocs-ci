import logging
import time

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
)
from ocs_ci.helpers.ceph_helpers import (
    wait_for_ceph_used_capacity_reached,
)
from ocs_ci.ocs.cluster import CephCluster, get_ceph_used_capacity
from ocs_ci.ocs.fill_pool_job import (
    _apply_incompressible_resource_floors,
    _memory_bytes,
)

log = logging.getLogger(__name__)


@libtest
@ignore_leftovers
class TestFillPoolJob(ManageTest):
    """
    Test the Fill Pool Job functionalities
    """

    def test_memory_bytes_fractional_binary_units(self):
        """_memory_bytes must parse fractional Ki/Mi/Gi/Ti quantities."""
        quantity_per_expected_bytes = {
            "1.5Ki": 1.5 * 1024,
            "1.5Mi": 1.5 * 1024**2,
            "1.5Gi": 1.5 * 1024**3,
            "1.5Ti": 1.5 * 1024**4,
            "512Mi": 512 * 1024**2,
            "2Gi": 2 * 1024**3,
        }
        for quantity, expected_bytes in quantity_per_expected_bytes.items():
            actual_bytes = _memory_bytes(quantity)
            assert actual_bytes == expected_bytes, (
                f"_memory_bytes must parse {quantity} as {expected_bytes} bytes; "
                f"got {actual_bytes}"
            )

        _, _, mem_request, mem_limit = _apply_incompressible_resource_floors(
            "100m", "500m", "1.5Gi", "1.5Gi"
        )
        assert mem_request == "1.5Gi", (
            "incompressible mem_request of 1.5Gi already exceeds the 512Mi floor "
            f"and must be kept; got {mem_request}"
        )
        assert mem_limit == "2Gi", (
            "incompressible mem_limit of 1.5Gi is below the 2Gi floor "
            f"and must be raised to 2Gi; got {mem_limit}"
        )

    def test_fill_pool_job_incompressible(self, fill_job_factory):
        """
        Run Fill Pool Job with incompressible data to fill the cluster to a
        target usage. Verifies that used capacity increases and logs elapsed time.

        """
        ceph_cluster = CephCluster()
        ceph_capacity = ceph_cluster.get_ceph_capacity()
        orig_ceph_used_capacity = get_ceph_used_capacity()
        log.info(
            f"Ceph Cluster capacity: {ceph_capacity}GiB, "
            f"Original used capacity: {orig_ceph_used_capacity}GiB"
        )
        if ceph_capacity > 500:
            storage_to_fill = 240  # in GiB
            # 240Gi at ~220MiB/s is ~18min; 40min leaves margin for slower clusters.
            timeout = 2400
        else:
            storage_to_fill = ceph_capacity / 2  # in GiB
            timeout = 1800

        log.info(f"Total storage to fill the cluster: {storage_to_fill}Gi")

        start = time.time()
        fill_job_factory(
            fill_mode="incompressible",
            storage=f"{int(storage_to_fill)}Gi",
            block_size="4M",
        )

        gap_difference = storage_to_fill * 0.1  # 10% gap
        # Calculate the expected used capacity after filling the cluster
        expected_used_capacity = (
            orig_ceph_used_capacity + storage_to_fill - gap_difference
        )
        wait_for_ceph_used_capacity_reached(
            expected_used_capacity=expected_used_capacity,
            timeout=timeout,
            sleep=20,
        )

        end = time.time()
        fill_up_time = end - start
        used_capacity = get_ceph_used_capacity()
        log.info(
            f"Fill Pool Job workload completed. Total used capacity: {used_capacity}GiB. "
            f"Elapsed time: {fill_up_time} seconds."
        )

    def test_manual_fill_pool_job_cleanup(self, fill_job_factory):
        """
        Run Fill Pool Job and then manually delete it to verify cleanup works as expected.

        """
        orig_ceph_used_capacity = get_ceph_used_capacity()
        log.info(f"Original used capacity: {orig_ceph_used_capacity}GiB")

        fill_job_obj = fill_job_factory(
            fill_mode="zero",
            storage="50Gi",
        )
        log.info("Manually deleting the Fill Pool Job")
        fill_job_obj.cleanup()
        log.info("Fill Pool Job deleted successfully")

        timeout = 30
        log.info(f"Wait {timeout} seconds for any capacity changes to reflect")
        time.sleep(timeout)

        log.info(
            "Verifying that used capacity remains unchanged after Fill Pool Job deletion"
        )
        used_capacity = get_ceph_used_capacity()
        gap_diff_range = 10  # 10Gi gap
        if (
            not (orig_ceph_used_capacity - gap_diff_range)
            <= used_capacity
            <= (orig_ceph_used_capacity + gap_diff_range)
        ):
            raise AssertionError(
                f"Used capacity changed after Fill Pool Job deletion. "
                f"Original: {orig_ceph_used_capacity}GiB, Current: {used_capacity}GiB"
            )
        log.info("Used capacity remains unchanged after Fill Pool Job deletion")
