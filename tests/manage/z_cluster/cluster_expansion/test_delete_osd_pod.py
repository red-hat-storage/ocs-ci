import pytest
import logging


from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4a
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.cluster_load import ClusterLoad
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

    def test_add_capacity_osd_pod_delete(
        self, nodes, multi_pvc_factory, pvc_factory, pod_factory, workload_storageutilization_rbd
    ):
        """ Test add capacity when one of the osd pods gets deleted
        in the middle of the process.
        """
        logging.info("Condition 1 to start the test is met: storageutilization is completed")
        # Please notice: When the branch 'wip-add-capacity-e_e' will be merged into master
        # the test will include more much data both before, and after calling 'add_capacity'function.

        max_osds = 12
        osd_pods_before = pod_helpers.get_osd_pods()
        assert len(osd_pods_before) < max_osds, (
            "Condition 2 to start test failed: We have maximum of osd's in the cluster")
        logging.info("All start conditions are met!")

        logging.info("Perform some IO operations...")
        cluster_load = ClusterLoad()
        cluster_load.reach_cluster_load_percentage_in_throughput(pvc_factory, pod_factory, target_percentage=0.3)

        d = Disruptions()
        d.set_resource('osd')
        osd_size = storage_cluster.get_osd_size()
        logging.info("Calling add_capacity function...")
        result = storage_cluster.add_capacity(osd_size)

        logging.info("Delete osd resource")
        d.delete_resource(1)
        if result:
            logging.info("add capacity finished successfully")
        else:
            logging.info("add capacity failed")

        # The exit criteria verification conditions here are not complete. When the branch
        # 'wip-add-capacity-e_e' will be merged into master I will use the functions from this branch.

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
