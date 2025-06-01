import logging
import pytest
from time import sleep
from ocs_ci.ocs import warp
from ocs_ci.utility import utils
from ocs_ci.ocs.scale_noobaa_lib import (
    get_noobaa_pods_status,
    check_memory_leak_in_noobaa_endpoint_log,
)
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    orange_squad,
    mcg,
)

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def warps3(request):
    warps3 = warp.Warp()
    warps3.create_resource_warp()

    def teardown():
        warps3.cleanup(multi_client=False)

    request.addfinalizer(teardown)
    return warps3


@mcg
@orange_squad
@scale
@ignore_leftovers
class TestWarp(E2ETest):
    """
    Test running workload Warp S3 benchmark to generate load for a period of time
    to ensure that noobaa pods are still in a running state
    """

    @pytest.mark.polarion_id("OCS-4001")
    @pytest.mark.parametrize(
        argnames="amount,interface,bucketclass_dict",
        argvalues=[
            pytest.param(
                *[1, "OC", None],
            ),
        ],
        ids=[
            "OC-DEFAULT-BACKINGSTORE",
        ],
    )
    def test_s3_benchmark_warp(
        self,
        warps3,
        mcg_obj,
        bucket_factory,
        amount,
        interface,
        bucketclass_dict,
    ):
        """
        Test flow:
        * Create a single object bucket
        * Verify noobaa pods status before running Wrap
        * Perform Warp workload for period of time (5 hours)
        * Check for memory leak in noobaa pods after running Wrap
        """

        # Create an Object bucket
        object_bucket = bucket_factory(
            amount, interface, bucketclass=bucketclass_dict, verify_health=False
        )[0]
        object_bucket.verify_health(timeout=180)

        # Check noobaa pods status before running Warp benchmark
        get_noobaa_pods_status()

        # Sleeping script for 1 minute before triggering warp workload
        sleep(60)

        # Running warp s3 benchmark
        warps3.run_benchmark(
            bucket_name=object_bucket.name,
            access_key=mcg_obj.access_key_id,
            secret_key=mcg_obj.access_key,
            duration="300m",
            concurrent=256,
            obj_size="4KB",
            validate=True,
            timeout=25000,
            multi_client=False,
        )

        # Check ceph health status
        utils.ceph_health_check()

        # Check noobaa pods status after running Warp benchmark
        get_noobaa_pods_status()

        # Check noobaa endpoint logs
        check_memory_leak_in_noobaa_endpoint_log()
