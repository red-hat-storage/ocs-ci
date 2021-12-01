import logging
import pytest
from ocs_ci.ocs import hsbench
from ocs_ci.utility import utils
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.framework.pytest_customization.marks import (
    vsphere_platform_required,
    bugzilla,
    skipif_ocs_version,
)

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def hsbenchs3(request):

    hsbenchs3 = hsbench.HsBench()

    def teardown():
        hsbenchs3.delete_test_user()
        hsbenchs3.cleanup()

    request.addfinalizer(teardown)
    return hsbenchs3


@pytest.fixture(scope="function")
def s3bench(request):

    s3bench = hsbench.HsBench()
    s3bench.create_resource_hsbench()
    s3bench.install_hsbench()

    def teardown():
        s3bench.cleanup()

    request.addfinalizer(teardown)
    return s3bench


@scale
class TestHsBench(E2ETest):
    """
    Test writing one million S3 objects to a single bucket
    """

    @vsphere_platform_required
    @pytest.mark.polarion_id("OCS-2321")
    def test_s3_benchmark_hsbench(self, hsbenchs3):
        """
        Test case to test one million objects in a single bucket:
        * Create RGW user
        * Create test pod
        * Install hs S3 benchmark
        * Run hs S3 benchmark to create 1M objects
        """
        # Create RGW user
        hsbenchs3.create_test_user()

        # Create resource for hsbench
        hsbenchs3.create_resource_hsbench()

        # Install hsbench
        hsbenchs3.install_hsbench()

        # Running hsbench
        hsbenchs3.run_benchmark(num_obj=1000000, timeout=7200)

        # Validate hsbench created objects
        hsbenchs3.validate_s3_objects()

        # Validate reshard process
        hsbenchs3.validate_reshard_process()

        # Check ceph health status
        utils.ceph_health_check()

    @bugzilla("1998680")
    @skipif_ocs_version("<4.9")
    @pytest.mark.polarion_id("OCS-2698")
    def test_s3_benchmark_object_bucket(self, s3bench, mcg_obj, bucket_factory):
        """
        Test case to test one million objects in a single bucket:
        * Create an OBC
        * Run hs S3 benchmark to create 700k objects on the object bucket
        * Post writing objects verify OBC creation
        """
        # Create an Object bucket
        object_bucket = bucket_factory(amount=1, interface="OC")[0]

        # Write 1M objects to the object bucket
        s3bench.run_benchmark(
            num_obj=700000,
            timeout=12000,
            access_key=mcg_obj.access_key_id,
            secret_key=mcg_obj.access_key,
            end_point=f"http://s3.openshift-storage.svc/{object_bucket.name}",
        )

        # Create new OBC and verify it is bound
        bucket_factory(interface="OC")
