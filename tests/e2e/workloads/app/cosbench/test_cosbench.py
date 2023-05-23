import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import bugzilla
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.ocs.cosbench import Cosbench

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def cosbench(request):

    cosbench = Cosbench()

    def teardown():
        cosbench.cleanup()

    request.addfinalizer(teardown)
    return cosbench


@workloads
@ignore_leftovers
@pytest.mark.polarion_id("OCS-2529")
class TestCosbenchWorkload(E2ETest):
    """
    Test Cosbench workloads on MCG

    """

    @bugzilla("2010453")
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
    def test_cosbench_workload_simple(self, cosbench, objects, size, size_unit):
        """
        Tests basic Cosbench workload.
        Creates and deletes objects and buckets.

        """
        bucket_prefix = "mcg-bucket-"
        buckets = 5

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
            timeout=1200,
            sleep=30,
        )

        # Dispose containers and objects
        cosbench.run_cleanup_workload(
            prefix=bucket_prefix,
            containers=buckets,
            objects=objects,
            validate=True,
            timeout=1200,
            sleep=30,
        )

    def test_cosbench_workload_operations(self, cosbench):
        """
        Test to perform reads and writes on objects.

        """
        bucket_prefix = "bucket-"
        buckets = 10
        objects = 50

        # Operations to perform and its ratio(%)
        operations = {"read": 50, "write": 50}

        # Deployment of cosbench
        cosbench.setup_cosbench()

        # Create initial containers and objects
        cosbench.run_init_workload(
            prefix=bucket_prefix, containers=buckets, objects=objects, validate=True
        )

        # Run main workload
        cosbench.run_main_workload(
            operation_type=operations,
            prefix=bucket_prefix,
            containers=buckets,
            objects=objects,
            validate=True,
        )

        # Dispose containers and objects
        cosbench.run_cleanup_workload(
            prefix=bucket_prefix, containers=buckets, objects=objects, validate=True
        )
