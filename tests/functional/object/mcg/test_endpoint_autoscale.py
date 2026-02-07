import time
import logging
from ocs_ci.framework import config
from ocs_ci.framework.testlib import MCGTest, tier2, skipif_ocs_version
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.ocs.scale_noobaa_lib import get_endpoint_pod_count, get_hpa_utilization

log = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
@skipif_ocs_version(["<4.5", "<4.14"])
@skipif_managed_service
@tier2
class TestEndpointAutoScale(MCGTest):
    """
    Test MCG endpoint auto-scaling

    """

    # This will ensure the test will start
    # with an autoscaling configuration of 1-2
    MIN_ENDPOINT_COUNT = 1
    MAX_ENDPOINT_COUNT = 2
    options = {
        "create": [
            ("name", "job"),
        ],
        "job": [
            ("time_based", "1"),
            ("runtime", f"{2 * 60}"),
            ("size", "10G"),
            ("iodepth", "64"),
            ("rw", "randrw"),
            ("rwmixread", "50"),
            ("bs", "128k"),
            ("numjobs", "32"),
        ],
    }

    def test_scaling_under_load(self, mcg_job_factory):
        self._assert_endpoint_count(self.MIN_ENDPOINT_COUNT)
        endpoint_cnt = get_endpoint_pod_count(config.ENV_DATA["cluster_namespace"])
        get_hpa_utilization(config.ENV_DATA["cluster_namespace"])

        max_wait_time = 600
        start_time = time.time()
        elapsed_time = 0

        job_cnt = 0
        job_list = list()
        max_utilization = 0

        try:
            while endpoint_cnt < self.MAX_ENDPOINT_COUNT:
                exec(f"job{job_cnt} = mcg_job_factory(custom_options=self.options)")
                job_list.append(f"job{job_cnt}")
                job_cnt += 1

                endpoint_cnt = get_endpoint_pod_count(
                    config.ENV_DATA["cluster_namespace"]
                )
                hpa_cpu_utilization = get_hpa_utilization(
                    config.ENV_DATA["cluster_namespace"]
                )
                log.info(
                    f"HPA CPU utilization by noobaa-endpoint is {hpa_cpu_utilization}%"
                )
                max_utilization = max(max_utilization, hpa_cpu_utilization)
                if endpoint_cnt == self.MAX_ENDPOINT_COUNT:
                    break

                elapsed_time = time.time() - start_time
                if elapsed_time >= max_wait_time:
                    raise TimeoutError(
                        (
                            f"NooBaa endpoints did not scale up in {max_wait_time} seconds"
                            f"Max utilization reached: {max_utilization}%"
                        )
                    )
        finally:
            for job in job_list:
                exec(f"{job}.delete()")
                exec(f"{job}.ocp.wait_for_delete(resource_name={job}.name, timeout=60)")

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
