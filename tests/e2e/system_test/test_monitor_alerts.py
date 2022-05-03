import logging

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.benchmark_operator_fio import get_file_size

log = logging.getLogger(__name__)


class TestFullClusterMonitoring(E2ETest):
    def test_full_cluster_monitoring(self, benchmark_fio_factory_fixture):
        size = get_file_size(50)
        benchmark_fio_factory_fixture(total_size=size)
