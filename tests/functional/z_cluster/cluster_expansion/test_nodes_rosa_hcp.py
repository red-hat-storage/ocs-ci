import random
import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    rosa_hcp_required,
    tier4a,
    polarion_id,
    brown_squad,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.machinepool import NodeConf, MachinePools
from ocs_ci.ocs.node import unschedule_nodes, schedule_nodes, get_node_pods
from ocs_ci.ocs import node
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.utility.utils import get_random_str, ceph_health_check

log = logging.getLogger(__name__)


def select_osd_node_name():
    """
    select randomly one of the osd nodes

    Returns:
        str: the selected osd node name

    """
    osd_node_names = node.get_osd_running_nodes()
    osd_node_name = random.choice(osd_node_names)
    log.info(f"Selected OSD is {osd_node_name}")
    return osd_node_name


def get_osd_pod_name(osd_node_name):
    """
    get the osd pod name from the osd node name

    Args:
        osd_node_name (str): the osd node name

    Returns:
        Pod: the osd pod object

    """
    osd_pods = get_osd_pods()
    osd_pod_name = get_node_pods(osd_node_name, osd_pods)[0]
    log.info(f"OSD pod name is {osd_pod_name}")
    return osd_pod_name


class TestAddDifferentInstanceTypeNode(ManageTest):
    @pytest.fixture
    def setup(self, request):
        """
        Method to set test variables
        """
        self.osd_node_name = select_osd_node_name()
        self.osd_pod = get_osd_pod_name(self.osd_node_name)
        self.machine_pool_new = f"workers-{get_random_str(3)}"
        log.info(f"New machine pool name is {self.machine_pool_new}")
        log.info(f"OSD node name is {self.osd_node_name}")

        def finalizer():
            """
            Teardown function to schedule initial node back
            """
            schedule_nodes([self.osd_node_name])

        request.addfinalizer(finalizer)

    @tier4a
    @brown_squad
    @rosa_hcp_required
    @polarion_id("OCS-6270")
    def test_add_ocs_node_non_default_machinepool(self, setup, add_nodes):
        """
        Test to add 1 ocs node and wait till rebalance is completed

        Compute nodes minimal requirements are:
        Compute node instance type m5.xlarge (4 vCPU 16, GiB RAM)

        Steps:
        1. Run create machinepool with node and label it with "openshif-storage" tag
        2. Select any node with osd and cordon it
        3. delete OSD pod on unscheduled node
        4. verify all OSD pods are running
        5. verify data rebalancing complete in reasonable time
        """

        instance_types = ["m5.xlarge", "m5.4xlarge", "m5.8xlarge", "m5.12xlarge"]
        cluster_name = config.ENV_DATA["cluster_name"]
        namespace = config.ENV_DATA["cluster_namespace"]
        ceph_health_tries = 40
        machine_pools = MachinePools(cluster_name=cluster_name)
        machine_pool = machine_pools.filter(
            machinepool_id=config.ENV_DATA["machine_pool"], pick_first=True
        )
        alt_inst_type = random.choice(
            (
                [
                    i_type
                    for i_type in instance_types
                    if i_type != machine_pool.instance_type
                ]
            )
        )

        node_conf = NodeConf(
            **{"machinepool_id": self.machine_pool_new, "instance_type": alt_inst_type}
        )
        add_nodes(ocs_nodes=True, node_count=1, node_conf=node_conf)

        unschedule_nodes([self.osd_node_name])
        self.osd_pod.delete(wait=True)

        ceph_health_check(namespace=namespace, tries=ceph_health_tries, delay=60)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"
