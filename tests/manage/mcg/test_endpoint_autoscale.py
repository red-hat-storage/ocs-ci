# import pytest

from ocs_ci.framework.testlib import MCGTest, tier1, skipif_ocs_version
from ocs_ci.ocs import constants, defaults, ocp


# @pytest.mark.polarion_id("OCS-XXXX")
@tier1
class TestEndpointAutoScale(MCGTest):
    """
    Test MCG endpoint auto-scaling

    """

    # This will ensure the test will start
    # with an autoscaling conifguration of 1-2
    MIN_ENDPOINT_COUNT = 1
    MAX_ENDPOINT_COUNT = 2

    @skipif_ocs_version('<4.5')
    def test_scaling_under_load(self, mcg_job_factory):
        self._assert_endpoint_count(1)

        options = {'create': [('name', 'job1'), ('name', 'job2'),
                              ('name', 'job3'), ('runtime', '900')]}
        job = mcg_job_factory(custom_options=options)
        self._assert_endpoint_count(2)

        job.delete()
        job.ocp.wait_for_delete(resource_name=job.name, timeout=60)
        self._assert_endpoint_count(1)

    def _assert_endpoint_count(self, desired_count):
        pod = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)

        assert pod.wait_for_resource(
            resource_count=desired_count,
            condition=constants.STATUS_RUNNING,
            selector=constants.NOOBAA_ENDPOINT_POD_LABEL,
            dont_allow_other_resources=True,
            timeout=500,
        )
