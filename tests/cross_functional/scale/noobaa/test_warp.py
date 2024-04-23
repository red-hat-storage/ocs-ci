import logging
import pytest
from ocs_ci.ocs import warp
from ocs_ci.utility import utils
from ocs_ci.ocs.constants import DEFAULT_STORAGECLASS_RBD
from ocs_ci.ocs.scale_noobaa_lib import (
    get_noobaa_pods_status,
    check_memory_leak_in_noobaa_endpoint_log,
)
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    bugzilla,
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

    @bugzilla("2089630")
    @pytest.mark.polarion_id("OCS-4001")
    @pytest.mark.parametrize(
        argnames="amount,interface,bucketclass_dict",
        argvalues=[
            pytest.param(
                *[1, "OC", None],
            ),
            pytest.param(
                *[
                    1,
                    "OC",
                    {
                        "interface": "OC",
                        "backingstore_dict": {
                            "pv": [(1, 100, DEFAULT_STORAGECLASS_RBD)]
                        },
                    },
                ],
            ),
            pytest.param(
                *[
                    1,
                    "OC",
                    {
                        "interface": "OC",
                        "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                ],
            ),
            pytest.param(
                *[
                    1,
                    "OC",
                    {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                ],
            ),
            pytest.param(
                *[
                    1,
                    "OC",
                    {"interface": "OC", "backingstore_dict": {"gcp": [(1, None)]}},
                ],
            ),
            pytest.param(
                *[
                    1,
                    "OC",
                    {"interface": "OC", "backingstore_dict": {"ibmcos": [(1, None)]}},
                ],
            ),
        ],
        ids=[
            "OC-DEFAULT-BACKINGSTORE",
            "OC-PVPOOL",
            "OC-AWS",
            "OC-AZURE",
            "OC-GCP",
            "OC-IBMCOS",
        ],
    )
    def test_s3_benchmark_warp(
        self,
        warps3,
        mcg_obj,
        backingstore_factory,
        bucket_class_factory,
        bucket_factory,
        amount,
        interface,
        bucketclass_dict,
    ):
        """
        Test flow:
        * Create a single object bucket
        * Verify noobaa pods status before running Wrap
        * Perform Warp workload for period of time (60 mins)
        * Verify noobaa pods status after running Wrap
        """

        # Create an Object bucket
        object_bucket = bucket_factory(
            amount, interface, bucketclass=bucketclass_dict, verify_health=False
        )[0]
        object_bucket.verify_health(timeout=180)

        # Check noobaa pods status before running Warp benchmark
        get_noobaa_pods_status()

        # Running warp s3 benchmark
        warps3.run_benchmark(
            bucket_name=object_bucket.name,
            access_key=mcg_obj.access_key_id,
            secret_key=mcg_obj.access_key,
            duration="60m",
            concurrent=20,
            objects=5000,
            obj_size="4KB",
            validate=True,
            timeout=4000,
            multi_client=False,
        )

        # Check ceph health status
        utils.ceph_health_check()

        # Check noobaa pods status after running Warp benchmark
        get_noobaa_pods_status()

        # Check noobaa endpoint logs
        check_memory_leak_in_noobaa_endpoint_log()
