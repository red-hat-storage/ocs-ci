import pytest
import logging

from ocs_ci.framework.testlib import (
    MCGTest,
    tier1,
    tier4,
    tier4a,
    skipif_ocs_version,
    polarion_id,
)
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod, get_pod_node
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs import cluster

logger = logging.getLogger(__name__)


class TestEndpointAutoScale(MCGTest):
    """
    Test MCG endpoint auto-scaling

    """

    # This will ensure the test will start
    # with an autoscaling conifguration of 1-2
    MIN_ENDPOINT_COUNT = 1
    MAX_ENDPOINT_COUNT = 2

    @pytest.fixture()
    def options(self):
        return {
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

    @tier1
    @skipif_ocs_version("<4.5")
    @polarion_id("OCS-2402")
    def test_scaling_under_load(self, mcg_job_factory, options):
        self._assert_endpoint_count(1)

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

    @tier4
    @tier4a
    @polarion_id("OCS-2422")
    def test_auto_scale_with_stop_and_start_node(self, mcg_job_factory, nodes, options):
        """"""
        cl_obj = cluster.CephCluster()

        self._assert_endpoint_count(1)
        job = mcg_job_factory(custom_options=options)
        self._assert_endpoint_count(2)

        nodes.stop_nodes()

        ep_pod_objs = get_pods_having_label(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        )
        ep_pod_obj = Pod(**ep_pod_objs[0])
        # Retrieve the node object on which the pod resides
        node = get_pod_node(ep_pod_obj)
        # Drain the node
        nodes.stop_nodes([node])

        wait_for_nodes_status(
            [node.get().get("metadata").get("name")], status=constants.NODE_NOT_READY
        )
        # Retrieve the new pod that should've been created post node power off

        # Wait for the endpoint pod to reach Terminating status
        logger.info(f"Waiting for pod {ep_pod_obj.name} to reach status Terminating")
        assert ep_pod_obj.ocp.wait_for_resource(
            timeout=600,
            resource_name=ep_pod_obj.name,
            condition=constants.STATUS_TERMINATING,
        ), f"Noobaa endpoint pod {ep_pod_obj.name} failed to reach status Terminating"
        logger.info(f"Pod {ep_pod_obj.name} has reached status Terminating")

        # Wait for 2 Noobaa endpoint pods to be started and reach running status
        logger.info("Waiting for 2 Noobaa endpoint pods to reach status Running")

        self._assert_endpoint_count(2)

        logger.info("2 Noobaa endpoint pods failed to reach status Running")
        nodes.start_nodes([node])

        wait_for_nodes_status(
            [node.get().get("metadata").get("name")], status=constants.NODE_READY
        )

        # Check the NB status to verify the system is healthy
        cl_obj.wait_for_noobaa_health_ok()

        job.delete()
        job.ocp.wait_for_delete(resource_name=job.name, timeout=60)
        self._assert_endpoint_count(1)
