import time
import logging
from ocs_ci.framework import config
from ocs_ci.framework.testlib import MCGTest, tier1, skipif_ocs_version
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    red_squad,
)
from ocs_ci.ocs.scale_noobaa_lib import get_endpoint_pod_count, get_hpa_utilization

log = logging.getLogger(__name__)

# @pytest.mark.polarion_id("OCS-XXXX")
# Skipped above 4.6 because of https://github.com/red-hat-storage/ocs-ci/issues/4129


@red_squad
@skipif_ocs_version(["<4.5", "<4.14"])
@skipif_managed_service
@tier1
class TestEndpointAutoScale(MCGTest):
    """
    Test MCG endpoint auto-scaling

    """

    # This will ensure the test will start
    # with an autoscaling conifguration of 1-2
    MIN_ENDPOINT_COUNT = 1
    MAX_ENDPOINT_COUNT = 2
    options = {
        "create": [
            ("name", "job1"),
            ("name", "job2"),
            ("name", "job3"),
            ("runtime", "1200"),
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

    def test_scaling_under_load(self, mcg_job_factory):
        self._assert_endpoint_count(self.MIN_ENDPOINT_COUNT)
        endpoint_cnt = get_endpoint_pod_count(config.ENV_DATA["cluster_namespace"])
        get_hpa_utilization(config.ENV_DATA["cluster_namespace"])
        job_cnt = 0
        wait_time = 30
        job_list = list()
        while endpoint_cnt < self.MAX_ENDPOINT_COUNT:
            exec(f"job{job_cnt} = mcg_job_factory(custom_options=options)")
            job_list.append(f"job{job_cnt}")
            time.sleep(wait_time)
            endpoint_cnt = get_endpoint_pod_count(config.ENV_DATA["cluster_namespace"])
            hpa_cpu_utilization = get_hpa_utilization(
                config.ENV_DATA["cluster_namespace"]
            )
            log.info(
                f"HPA CPU utilization by noobaa-endpoint is {hpa_cpu_utilization}%"
            )
            if endpoint_cnt == self.MAX_ENDPOINT_COUNT:
                break
            job_cnt += 1
        for i in job_list:
            exec(f"{i}.delete()")
            exec(f"{i}.ocp.wait_for_delete(resource_name={i}.name, timeout=60)")
        self._assert_endpoint_count(self.MIN_ENDPOINT_COUNT)

    def _assert_endpoint_count(self, desired_count):
        pod = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )

        assert pod.wait_for_resource(
            resource_count=desired_count,
            condition=constants.STATUS_RUNNING,
            selector=constants.NOOBAA_ENDPOINT_POD_LABEL,
            dont_allow_other_resources=True,
            timeout=900,
        )
