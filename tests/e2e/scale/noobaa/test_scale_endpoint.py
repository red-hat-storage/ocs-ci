import pytest
import logging
from ocs_ci.framework.testlib import MCGTest, scale, skipif_ocs_version
from ocs_ci.ocs import constants, defaults, ocp, scale_pgsql
from ocs_ci.utility import utils

log = logging.getLogger(__name__)

option1 = {
    'create': [('name', 'job1'), ('runtime', '900')],
    'job1': [('iodepth', '4'), ('rw', 'randrw'), ('bs', '32k'), ('size', '64m'), ('numjobs', '4')]
    }
option2 = {
    'create': [('name', 'job1'), ('name', 'job2'), ('name', 'job3'), ('runtime', '900')],
    'job1': [('iodepth', '4'), ('rw', 'randrw'), ('bs', '32k'), ('size', '64m'), ('numjobs', '4')],
    'job2': [('iodepth', '16'), ('rw', 'randrw'), ('bs', '64k'), ('size', '512m'), ('numjobs', '4')],
    'job3': [('iodepth', '32'), ('rw', 'randrw'), ('bs', '128k'), ('size', '1024m'), ('numjobs', '4')]
    }


@scale
@skipif_ocs_version('<4.5')
@pytest.mark.parametrize(
    argnames=[
        "test_options", "endpoint1", "endpoint2",
    ],
    argvalues=[
        pytest.param(
            *[option1, 1, 1], marks=pytest.mark.polarion_id("OCS-2420")
        ),
        pytest.param(
            *[option2, 1, 2], marks=pytest.mark.polarion_id("OCS-2402")
        )
    ]
)
class TestScaleEndpointAutoScale(MCGTest):
    """
    Test MCG endpoint auto-scaling
    """

    # This will ensure the test will start
    # with an autoscaling conifguration of 1-2
    MIN_ENDPOINT_COUNT = 1
    MAX_ENDPOINT_COUNT = 2

    def _assert_endpoint_count(self, desired_count):
        pod = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)

        assert pod.wait_for_resource(
            resource_count=desired_count,
            condition=constants.STATUS_RUNNING,
            selector=constants.NOOBAA_ENDPOINT_POD_LABEL,
            dont_allow_other_resources=True,
            timeout=500,
        )

    def test_scale_endpoint(
        self, mcg_job_factory, test_options, endpoint1, endpoint2
    ):
        # Add workers node to cluster
        scale_pgsql.add_worker_node()

        # Check autoscale endpoint count before start s3 load
        self._assert_endpoint_count(endpoint1)

        # Create s3 workload using mcg_job_factory
        job = mcg_job_factory(custom_options=test_options)

        # Validate autoscale endpoint count
        self._assert_endpoint_count(endpoint2)

        # Delete mcg_job_factory
        job.delete()
        job.ocp.wait_for_delete(resource_name=job.name, timeout=60)

        # Validate autoscale endpoint count
        self._assert_endpoint_count(endpoint1)

        # Delete workers node in the cluster
        scale_pgsql.delete_worker_node()

        # Check ceph health status
        utils.ceph_health_check()
