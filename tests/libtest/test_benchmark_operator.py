import logging
from uuid import uuid4

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
)

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
        Create a new benchmark operator with the default values
        """
        benchmark_workload_storageutilization(target_percentage=25, is_completed=True)

    def test_benchmark_workload_storageutilization_picked_values(
        self, benchmark_workload_storageutilization
    ):
        """
        Create a new benchmark operator with picked values
        """
        benchmark_name = f"fio-benchmark{uuid4().hex[:4]}"
        benchmark_workload_storageutilization(
            target_percentage=20,
            bs="2048KiB",
            benchmark_name=benchmark_name,
            is_completed=True,
        )
