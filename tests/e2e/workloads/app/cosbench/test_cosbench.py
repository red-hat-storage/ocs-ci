import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.cosbench import Cosbench

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def cosbench(request):

    cosbench = Cosbench()

    def teardown():
        cosbench.cosbench_teardown()

    request.addfinalizer(teardown)
    return cosbench


@workloads
@pytest.mark.polarion_id("OCS-2529")
class TestCosbenchWorkload(E2ETest):
    """
    Test cosbench workloads on MCG
    """

    def test_cosbench_workload_simple(self, cosbench):
        """
        Tests basic Cosbench workload.
        Creates and deletes objects and buckets.
        """
        bucket_prefix = "mcg-bucket-"
        buckets = 5
        objects = 10

        # Deployment of cosbench
        cosbench.setup_cosbench()

        # Create initial containers and objects
        cosbench.run_init_workload(
            prefix=bucket_prefix, containers=buckets, objects=objects, validate=True
        )

        # Dispose containers and objects
        cosbench.run_cleanup_workload(
            prefix=bucket_prefix, containers=buckets, objects=objects, validate=True
        )

    @pytest.mark.parametrize(
        argnames="objects, size, size_unit",
        argvalues=[
            pytest.param(
                *[10000, 4, "KB"],
            ),
            pytest.param(
                *[1000, 2, "MB"],
            ),
            pytest.param(
                *[500, 8, "MB"],
            ),
        ],
    )
    def test_cosbench_workload_operations(self, cosbench, objects, size, size_unit):
        """
        Test to perform reads and writes on objects.

        Write example:
            total objects = buckets * objects
        """
        bucket_prefix = "bucket-"
        buckets = 10

        # Operations to perform and its ratio(%)
        operations = {"read": 50, "write": 50}

        # Deployment of cosbench
        cosbench.setup_cosbench()

        # Create initial containers and objects
        cosbench.run_init_workload(
            prefix=bucket_prefix,
            containers=buckets,
            objects=objects,
            validate=True,
            size=size,
            size_unit=size_unit,
        )

        # Run main workload
        cosbench.run_main_workload(
            operation_type=operations,
            prefix=bucket_prefix,
            containers=buckets,
            objects=objects,
            validate=True,
            size=size,
            size_unit=size_unit,
        )

        # Dispose containers and objects
        cosbench.run_cleanup_workload(
            prefix=bucket_prefix, containers=buckets, objects=objects, validate=True
        )
