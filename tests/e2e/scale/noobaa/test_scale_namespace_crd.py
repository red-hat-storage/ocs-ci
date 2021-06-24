import logging
import pytest

from ocs_ci.framework.testlib import (
    E2ETest,
    skipif_ocs_version,
    on_prem_platform_required,
    scale,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import hsbench

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def s3bench(request):
    # Create hs s3 benchmark
    s3bench = hsbench.HsBench()
    s3bench.create_resource_hsbench()
    s3bench.install_hsbench()

    def teardown():
        s3bench.cleanup()

    request.addfinalizer(teardown)
    return s3bench


@scale
class TestScaleNamespace(E2ETest):
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
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"rgw": [(1, None)]},
                    },
                },
                marks=[
                    on_prem_platform_required,
                ],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Multi",
                        "namespacestore_dict": {
                            "aws": [(1, "us-east-2")],
                            "azure": [(1, None)],
                        },
                    },
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Multi",
                        "namespacestore_dict": {
                            "rgw": [(2, None)],
                        },
                    },
                },
                marks=[
                    on_prem_platform_required,
                ],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 60000,
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                },
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
        num_s3_obj = 10
        ns_bucket_list = []
        for _ in range(50):
            ns_bucket_list.append(
                bucket_factory(
                    amount=1,
                    interface=bucketclass_dict["interface"],
                    bucketclass=bucketclass_dict,
                )[0]
            )

        for ns_bucket in ns_bucket_list:
            s3bench.run_benchmark(
                num_obj=num_s3_obj,
                timeout=7200,
                access_key=mcg_obj.access_key_id,
                secret_key=mcg_obj.access_key,
                end_point=f"http://s3.openshift-storage.svc/{ns_bucket.name}",
                run_mode="ipg",
            )
