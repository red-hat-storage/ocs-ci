import logging
import time

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
)
from ocs_ci.helpers.ceph_helpers import (
    wait_for_ceph_used_capacity_reached,
    get_ceph_used_capacity,
)
from ocs_ci.ocs.cluster import CephCluster

log = logging.getLogger(__name__)


@libtest
@ignore_leftovers
class TestFillPoolPod(ManageTest):
    """
    Test the Fill Pool Pod functionalities
    """

    def test_fill_pool_pod_with_both_modes(self, fill_pod_factory):
        """
        Run Fill Pool Pod using both modes to fill the cluster to a target usage.
        Verifies that the workload completes, logs capacity usage and elapsed time.

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
            timeout = 1200
        else:
            storage_to_fill = ceph_capacity / 2  # in GiB
            timeout = 600

        # Divide the storage to fill between zero and random modes. The random mode will
        # fill 25% of the total. This is to optimize the time taken to fill the cluster,
        # as the zero mode is faster.
        storage_to_fill_random_mode = int(storage_to_fill // 4)
        storage_to_fill_zero_mode = storage_to_fill - storage_to_fill_random_mode
        log.info(
            f"Total storage to fill the cluster: {storage_to_fill}Gi, "
            f"Storage to fill in zero mode: {storage_to_fill_zero_mode}Gi, "
            f"Storage to fill in random mode: {storage_to_fill_random_mode}Gi"
        )

        start = time.time()
        fill_pod_factory(
            fill_mode="zero",
            storage=f"{storage_to_fill_zero_mode}Gi",
        )
        fill_pod_factory(
            fill_mode="random",
            storage=f"{storage_to_fill_random_mode}Gi",
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
            f"Fill Pool Pod workload completed. Total used capacity: {used_capacity}GiB. "
            f"Elapsed time: {fill_up_time} seconds."
        )

    def test_manual_fill_pool_pod_cleanup(self, fill_pod_factory):
        """
        Run Fill Pool Pod and then manually delete it to verify cleanup works as expected.

        """
        orig_ceph_used_capacity = get_ceph_used_capacity()
        log.info(f"Original used capacity: {orig_ceph_used_capacity}GiB")

        fill_pod_obj = fill_pod_factory(
            fill_mode="zero",
            storage="50Gi",
        )
        log.info("Manually deleting the Fill Pool Pod")
        fill_pod_obj.cleanup()
        log.info("Fill Pool Pod deleted successfully")

        timeout = 30
        log.info(f"Wait {timeout} seconds for any capacity changes to reflect")
        time.sleep(timeout)

        log.info(
            "Verifying that used capacity remains unchanged after Fill Pool Pod deletion"
        )
        used_capacity = get_ceph_used_capacity()
        gap_diff_range = 10  # 10Gi gap
        if (
            not (orig_ceph_used_capacity - gap_diff_range)
            <= used_capacity
            <= (orig_ceph_used_capacity + gap_diff_range)
        ):
            raise AssertionError(
                f"Used capacity changed after Fill Pool Pod deletion. "
                f"Original: {orig_ceph_used_capacity}GiB, Current: {used_capacity}GiB"
            )
        log.info("Used capacity remains unchanged after Fill Pool Pod deletion")
