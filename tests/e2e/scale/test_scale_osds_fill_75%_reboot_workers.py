"""
This testcase starts with minimum number of osds(one osd)
on each node and slowly scaling it into 6 osds and then reboot worker
nodes
"""

import logging
import pytest

from ocs_ci.ocs.cluster import CephCluster, is_flexible_scaling_enabled
from ocs_ci.ocs.cluster import count_cluster_osd, validate_osd_utilization
from ocs_ci.framework import config
from ocs_ci.ocs.node import get_nodes, wait_for_nodes_status
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants, platform_nodes
from ocs_ci.ocs.resources.pod import wait_for_dc_app_pods_to_reach_running_state
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.framework.pytest_customization.marks import (
    orange_squad,
    skipif_external_mode,
    skipif_aws_i3,
)


logger = logging.getLogger(__name__)


@orange_squad
@scale
@ignore_leftovers
@skipif_external_mode
@pytest.mark.skip(
    reason="Skipped due to failure in 75% filling-up cluster "
    "which created more PODs and failed for memory issue"
)
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2117")
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-2117")
        ),
    ],
)
@skipif_aws_i3
class TestScaleOSDsRebootNodes(E2ETest):
    """
    Add first set of OSD to a minimum cluster with 50%
    of storage utilization and wait for rebalance
    Reboot worker nodes after rebalance
    """

    num_of_pvcs = 10
    pvc_size = 5

    def test_scale_osds_reboot_nodes(
        self, interface, project_factory, multi_pvc_factory, dc_pod_factory
    ):
        """
        Check storage utilization, if its less then runs IO,
        Scale osds from 3-6, check for rebalance and reboot workers
        """
        current_osd_count = count_cluster_osd()
        proj_obj = project_factory()
        if current_osd_count == 3:
            while not validate_osd_utilization(osd_used=10):
                # Create pvc
                pvc_objs = multi_pvc_factory(
                    project=proj_obj,
                    interface=interface,
                    size=self.pvc_size,
                    num_of_pvc=self.num_of_pvcs,
                )

                dc_pod_objs = list()
                for pvc_obj in pvc_objs:
                    dc_pod_objs.append(dc_pod_factory(pvc=pvc_obj))

                wait_for_dc_app_pods_to_reach_running_state(dc_pod_objs, timeout=1200)

                for pod_obj in dc_pod_objs:
                    pod_obj.run_io(
                        storage_type="fs",
                        size="3G",
                        runtime="60",
                        fio_filename=f"{pod_obj.name}_io",
                    )

        # Add capacity
        osd_size = storage_cluster.get_osd_size()
        count = storage_cluster.add_capacity(osd_size)
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        if is_flexible_scaling_enabled():
            replica_count = 1
        else:
            replica_count = 3
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=count * replica_count,
        )
        assert ceph_health_check(
            delay=120, tries=50
        ), "New OSDs failed to reach running state"

        cluster = CephCluster()

        # Get rebalance status
        rebalance_status = cluster.get_rebalance_status()
        logger.info(rebalance_status)
        if rebalance_status:
            time_taken = cluster.time_taken_to_complete_rebalance()
            logger.info(f"The time taken to complete rebalance {time_taken}")

        # Rolling reboot on worker nodes
        worker_nodes = get_nodes(node_type="worker")

        factory = platform_nodes.PlatformNodesFactory()
        nodes = factory.get_nodes_platform()

        for node in worker_nodes:
            nodes.restart_nodes(nodes=[node])
            wait_for_nodes_status()

        assert ceph_health_check(
            delay=180
        ), "Failed, Ceph health bad after nodes reboot"
