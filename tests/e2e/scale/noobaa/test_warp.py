import logging
import pytest
from ocs_ci.ocs import warp
from ocs_ci.utility import utils
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.framework.pytest_customization.marks import (
    vsphere_platform_required,
)

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def warps3(request):

    warps3 = warp.Warp()
    warps3.create_resource_warp()
    warps3.install_warp()

    def teardown():
        warps3.cleanup()

    request.addfinalizer(teardown)
    return warps3


@scale
class TestWarp(E2ETest):
    """
    Test writing one million S3 objects to a single bucket
    """

    @vsphere_platform_required
    @pytest.mark.polarion_id("OCS-0000")
    def test_s3_benchmark_warp(self, warps3, mcg_obj, bucket_factory):
        """
        Test case to test one million objects in a single bucket:
        * Create RGW user
        * Create test pod
        * Install hs S3 benchmark
        * Run hs S3 benchmark to create 1M objects
        """

        # Create an Object bucket
        object_bucket = bucket_factory(amount=1, interface="OC", verify_health=False)[0]
        object_bucket.verify_health(timeout=180)

        # Running warp s3 benchmark
        warps3.run_benchmark(
            bucket_name=object_bucket.name,
            access_key=mcg_obj.access_key_id,
            secret_key=mcg_obj.access_key,
            duration="1m",
            concurrent=20,
            objects=100,
            obj_size="1.5MiB",
            timeout=7200,
        )

        # Check ceph health status
        utils.ceph_health_check()
