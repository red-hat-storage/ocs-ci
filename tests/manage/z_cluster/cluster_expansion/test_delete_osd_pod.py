import pytest
import logging
import time


from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4a
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.cluster import get_percent_used_capacity
from ocs_ci.utility.utils import ceph_health_check
from tests.disruption_helpers import Disruptions


@pytest.mark.parametrize(
    argnames=["workload_storageutilization_rbd"],
    argvalues=[
        pytest.param(
            *[(0.11, True, 120)],
            marks=pytest.mark.polarion_id("OCS-1203")
        ),
    ],
    indirect=["workload_storageutilization_rbd"],
)
@ignore_leftovers
@tier4a
class TestAddCapacityWithOSDPodDelete(ManageTest):
    """
    Test add capacity when one of the osd pods gets deleted
    in the middle of the process.
    """

    def test_add_capacity_osd_pod_delete(self, workload_storageutilization_rbd):
        """
        Test add capacity when one of the osd pods gets deleted
        in the middle of the process.
        """
        used_percentage = get_percent_used_capacity()
        logging.info(f"storageutilization is completed. used capacity = {used_percentage}")

        max_osds = 15
        osd_pods_before = pod_helpers.get_osd_pods()
        if len(osd_pods_before) >= max_osds:
            pytest.skip("We have maximum of osd's in the cluster")

        d = Disruptions()
        d.set_resource('osd')

        osd_size = storage_cluster.get_osd_size()
        logging.info(f"Adding one new set of OSDs. osd size = {osd_size}")
        result = storage_cluster.add_capacity(osd_size)
        logging.info("Adding one new set of OSDs was issued without problems")

        time_to_wait_before_delete_osd_pod = 20
        time.sleep(time_to_wait_before_delete_osd_pod)
        logging.info("Delete an osd pod while storage capacity is getting increased")
        d.delete_resource(1)

        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )

        pod.wait_for_resource(
            timeout=420,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=result * 3
        )

        logging.info("Finished verifying add capacity osd storage with node restart")
        logging.info("Waiting for ceph health check to finished...")
        ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace'], tries=80
        )
