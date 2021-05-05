import logging
import pytest

from ocs_ci.framework.testlib import (
    MCGTest,
    skipif_ocs_version,
    scale,
)
from ocs_ci.ocs import hsbench

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def s3bench(request):

    s3bench = hsbench.HsBench()

    def teardown():
        s3bench.cleanup()

    request.addfinalizer(teardown)
    return s3bench


@scale
class TestScaleNamespace(MCGTest):
    """
    Test creation of a namespace scale resource
    """

    @skipif_ocs_version("<4.7")
    @pytest.mark.polarion_id("OCS-2518")
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                }
            ),
        ],
    )
    def test_scale_namespace_bucket_creation_crd(
        self,
        mcg_obj,
        bucket_factory,
        bucketclass_dict,
        s3bench,
    ):
        """
        Test namespace bucket creation using the MCG CRDs.
        Create 50 namespace resources
        For each namespace resource, create namespace bucket and start hsbench benchmark

        """
        num_scale_namespace = 50
        num_s3_obj = 100000

        # Create hs s3 benchmark
        s3bench.create_resource_hsbench()
        s3bench.install_hsbench()

        for i in range(int(num_scale_namespace)):
            # Create the namespace bucket on top of the namespace resource
            ns_bucket = bucket_factory(
                amount=1,
                interface=bucketclass_dict["interface"],
                bucketclass=bucketclass_dict,
            )[0]

            logger.info(f"Bucket Name: {ns_bucket.name}")
            end_point = f"http://s3.openshift-storage.svc/{ns_bucket.name}"

            s3bench.run_benchmark(
                num_obj=num_s3_obj,
                timeout=7200,
                access_key=mcg_obj.access_key_id,
                secret_key=mcg_obj.access_key,
                end_point=end_point,
                run_mode="ipg",
            )
