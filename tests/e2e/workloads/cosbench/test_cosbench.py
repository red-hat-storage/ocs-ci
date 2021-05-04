import logging
import pytest

from ocs_ci.framework.testlib import tier2, E2ETest
from ocs_ci.ocs.cosbench import Cosbench

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def cosbench(request):

    cosbench = Cosbench()

    def teardown():
        cosbench.cosbench_teardown()

    request.addfinalizer(teardown)
    return cosbench


@tier2
class TestCosbenchWorkload(E2ETest):
    """
    Test cosbench workloads on MCG
    """

    @pytest.fixture()
    def cosbench_setup(self, cosbench):
        """
        Cosbench test setup
        """
        # Deployment of cosbench
        cosbench.setup_cosbench()

    @pytest.mark.usefixtures(cosbench_setup.__name__)
    def test_cosbench_workload_simple(self, cosbench):
        """
        Tests basic cosbench workload.
        Creates and deletes objects and buckets.
        """
        bucket_prefix = "mcg-bucket-"

        # Deployment of cosbench
        cosbench.setup_cosbench()

        # Create initial containers and objects
        cosbench.run_init_workload(
            prefix=bucket_prefix, containers=5, objects=10, validate=True
        )

        # Dispose containers and objects
        cosbench.run_cleanup_workload(
            prefix=bucket_prefix, containers=5, objects=10, validate=True
        )

    def test_cosbench_main_workload(self):
        """
        Performs Reads and writes objects and buckets.

        """
        bucket_prefix = "bucket-"
        operations = {"read": 50, "write": 50}

        # Deployment of cosbench
        cosbench.setup_cosbench()

        # Create initial containers and objects
        cosbench.run_init_workload(
            prefix=bucket_prefix, containers=10, objects=50, validate=True
        )

        # Run main workload
        cosbench.run_main_workload(
            operation_type=operations,
            prefix=bucket_prefix,
            containers=10,
            objects=50,
            validate=True,
        )

        # Dispose containers and objects
        cosbench.run_cleanup_workload(
            prefix=bucket_prefix, containers=10, objects=50, validate=True
        )
