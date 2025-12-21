import logging
import time
from uuid import uuid4

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
)
from ocs_ci.ocs.cluster import get_percent_used_capacity

log = logging.getLogger(__name__)


@libtest
@ignore_leftovers
class TestBenchmarkOperator(ManageTest):
    """
    Test the Benchmark Operator FIO Class functionalities
    """

    def test_benchmark_workload_storageutilization_default_values(
        self, benchmark_workload_storageutilization
    ):
        """
        Run FIO benchmark using default parameters to fill the cluster to a target usage.
        Verifies that the workload completes, logs capacity usage and elapsed time.
        """
        start = time.time()
        benchmark_workload_storageutilization(target_percentage=30, is_completed=True)
        end = time.time()
        fill_up_time = end - start
        used_capacity = get_percent_used_capacity()
        log.info(
            f"The current percent used capacity = {used_capacity}%, "
            f"The time took to fill up the cluster = {fill_up_time}"
        )

    def test_benchmark_workload_storageutilization_picked_values(
        self, benchmark_workload_storageutilization
    ):
        """
        Run FIO benchmark using picked values to fill the cluster to a target usage.
        Verifies that the workload completes, logs capacity usage and elapsed time.
        """
        start = time.time()
        benchmark_name = f"fio-benchmark{uuid4().hex[:4]}"
        benchmark_workload_storageutilization(
            target_percentage=20,
            bs="2048KiB",
            benchmark_name=benchmark_name,
            is_completed=True,
        )
        end = time.time()
        fill_up_time = end - start
        used_capacity = get_percent_used_capacity()
        log.info(
            f"The current percent used capacity = {used_capacity}%, "
            f"The time took to fill up the cluster = {fill_up_time}"
        )

    def test_benchmark_workload_storageutilization_fill_up_quickly(
        self, benchmark_workload_storageutilization
    ):
        """
        Run FIO benchmark with high-performance FIO settings to quickly fill up to target usage.
        Verifies that the workload completes, logs capacity usage and elapsed time.
        """
        benchmark_name = f"fio-benchmark{uuid4().hex[:4]}"
        log.info(f"Starting benchmark {benchmark_name} with fast fill settings")
        start = time.time()

        benchmark_workload_storageutilization(
            target_percentage=30,
            bs="4096KiB",
            benchmark_name=benchmark_name,
            is_completed=True,
            numjobs=4,
            iodepth=64,
            max_servers=60,
        )
        end = time.time()
        fill_up_time = end - start
        used_capacity = get_percent_used_capacity()
        log.info(
            f"The current percent used capacity = {used_capacity}%, "
            f"The time took to fill up the cluster = {fill_up_time}"
        )
