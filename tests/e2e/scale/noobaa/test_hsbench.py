import logging
import pytest
from ocs_ci.ocs import hsbench
from ocs_ci.utility import utils
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.framework.pytest_customization.marks import vsphere_platform_required

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def hsbenchs3(request):

    hsbenchs3 = hsbench.HsBench()

    def teardown():
        hsbenchs3.delete_test_user()
        hsbenchs3.cleanup()

    request.addfinalizer(teardown)
    return hsbenchs3


@scale
@vsphere_platform_required
@pytest.mark.polarion_id("OCS-2321")
class TestHsBench(E2ETest):
    """
    Test writing one million S3 objects to a single bucket
    """

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
