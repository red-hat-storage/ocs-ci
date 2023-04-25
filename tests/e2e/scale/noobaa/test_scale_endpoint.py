import pytest
import logging
import time

from ocs_ci.framework import config
from ocs_ci.framework.testlib import MCGTest, scale, skipif_ocs_version
from ocs_ci.ocs import constants, ocp, scale_pgsql
from ocs_ci.utility import utils
from ocs_ci.helpers import disruption_helpers
from ocs_ci.ocs.scale_noobaa_lib import get_endpoint_pod_count, get_hpa_utilization

log = logging.getLogger(__name__)

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


@pytest.fixture(scope="function")
def worker_node(request):
    def teardown():
        scale_pgsql.delete_worker_node()

    request.addfinalizer(teardown)
    return worker_node


@scale
@skipif_ocs_version("<4.5")
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(*["mgr"], marks=pytest.mark.polarion_id("OCS-2402")),
        pytest.param(*["mon"], marks=pytest.mark.polarion_id("OCS-2420")),
        pytest.param(*["osd"], marks=pytest.mark.polarion_id("OCS-2446")),
        pytest.param(*["mds"], marks=pytest.mark.polarion_id("OCS-2447")),
    ],
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

    def test_scale_endpoint_and_respin_ceph_pods(
        self,
        mcg_job_factory,
        resource_to_delete,
        worker_node,
    ):
        """
        Generate S3 workload to trigger autoscale to increase from 1 to 2 endpoint
        then respin ceph pods
        """
        # Add workers node to cluster
        scale_pgsql.add_worker_node()

        # Check autoscale endpoint count before start s3 load
        self._assert_endpoint_count(desired_count=self.MIN_ENDPOINT_COUNT)

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

        # Validate autoscale endpoint count
        self._assert_endpoint_count(desired_count=self.MAX_ENDPOINT_COUNT)

        # Respin ceph pods
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        number_of_resource = disruption.resource_count
        for i in range(0, number_of_resource):
            disruption.delete_resource(resource_id=i)

        # Delete mcg_job_factory
        for job in job_list:
            exec(f"{job}.delete()")
            exec(f"{job}.ocp.wait_for_delete(resource_name={job}.name, timeout=60)")

        # Validate autoscale endpoint count
        self._assert_endpoint_count(desired_count=self.MIN_ENDPOINT_COUNT)

        # Check ceph health status
        utils.ceph_health_check()
