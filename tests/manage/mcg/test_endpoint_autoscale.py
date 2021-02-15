import pytest

from ocs_ci.framework.testlib import MCGTest, tier1, skipif_ocs_version
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated


# @pytest.mark.polarion_id("OCS-XXXX")
@skipif_openshift_dedicated
@tier1
class TestEndpointAutoScale(MCGTest):
    """
    Test MCG endpoint auto-scaling

    """

    # This will ensure the test will start
    # with an autoscaling conifguration of 1-2
    MIN_ENDPOINT_COUNT = 1
    MAX_ENDPOINT_COUNT = 2

    @skipif_ocs_version("<4.5")
    @pytest.mark.skip(
        reason="Skipped because of https://github.com/red-hat-storage/ocs-ci/issues/3720"
    )
    def test_scaling_under_load(self, mcg_job_factory):
        self._assert_endpoint_count(1)

        options = {
            "create": [
                ("name", "job1"),
                ("name", "job2"),
                ("name", "job3"),
                ("runtime", "900"),
            ],
            "job1": [
                ("iodepth", "4"),
                ("rw", "randrw"),
                ("bs", "32k"),
                ("size", "64m"),
                ("numjobs", "4"),
            ],
            "job2": [
                ("iodepth", "16"),
                ("rw", "randrw"),
                ("bs", "64k"),
                ("size", "512m"),
                ("numjobs", "4"),
            ],
            "job3": [
                ("iodepth", "32"),
                ("rw", "randrw"),
                ("bs", "128k"),
                ("size", "1024m"),
                ("numjobs", "4"),
            ],
        }
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
