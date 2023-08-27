import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    skipif_bm,
)
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.helpers.helpers import get_failure_domin
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.node import (
    get_node_rack_or_zone,
    get_node_rack_or_zone_dict,
    get_node_names,
    get_node_objs,
)
from ocs_ci.helpers.helpers import (
    verify_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running,
)
from ocs_ci.framework.testlib import (
    tier2,
    ignore_leftovers,
    ManageTest,
    bugzilla,
    skipif_external_mode,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@tier2
@ignore_leftovers
@bugzilla("1898808")
@skipif_bm
@skipif_external_mode
@skipif_managed_service
@pytest.mark.polarion_id("OCS-2594")
class TestAddNodeCrashCollector(ManageTest):
    """
    Add node with OCS label and verify crashcollector created on new node

    Test Procedure:
    1.Add worker node with OCS label
    2.Drain the 'old' node located in the same rack of ​​the new node
    3.Wait for 3 mon pods to be on running state
    4.Verify ceph-crashcollector pod running on worker node where "rook-ceph" pods are running.
    5.Schedule worker-node-x
    6.Wait for 3 osd pods to be on running state
    7.Verify ceph-crashcollector pod running on worker node where "rook-ceph" pods are running.

    """

    def is_node_rack_or_zone_exist(self, failure_domain, node_name):
        """
        Check if the node rack/zone exist

        Args:
            failure_domain (str): The failure domain
            node_name (str): The node name

        Returns:
            bool: True if the node rack/zone exist. False otherwise

        """
        node_obj = get_node_objs([node_name])[0]
        return get_node_rack_or_zone(failure_domain, node_obj) is not None

    def test_crashcollector_pod_existence_on_ceph_pods_running_nodes(
        self, add_nodes, node_drain_teardown
    ):
        """
        Add node with OCS label and verify crashcollector created on new node

        """
        failure_domain = get_failure_domin()
        logger.info(f"The failure domain is {failure_domain}")

        if failure_domain in ("zone", "rack"):
            old_node_rack_zone_dict = get_node_rack_or_zone_dict(failure_domain)
            logger.info(f"The old node rack/zone dict is {old_node_rack_zone_dict}")

        old_nodes = get_node_names()

        logger.info("Add one worker node with OCS label")
        add_nodes(ocs_nodes=True, node_count=1)

        new_node_name = list(set(get_node_names()) - set(old_nodes))[0]
        new_node = get_node_objs([new_node_name])[0]
        logger.info(f"New worker node is {new_node_name}")

        logger.info(f"Checking if the rack/zone of the node {new_node_name} is exist")
        timeout = 120
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=10,
            func=self.is_node_rack_or_zone_exist,
            node_name=new_node_name,
            failure_domain=failure_domain,
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Didn't find the node rack/zone after {timeout} seconds"

        if failure_domain in ("zone", "rack"):
            new_node_rack_zone_dict = get_node_rack_or_zone_dict(failure_domain)
            logger.info(f"The new node rack/zone dict is {new_node_rack_zone_dict}")

            new_rack_zone = get_node_rack_or_zone(failure_domain, new_node)
            logger.info(f"New worker node {new_node_name} in zone/rack {new_rack_zone}")

            for node, rack_zone in old_node_rack_zone_dict.items():
                if rack_zone == new_rack_zone:
                    drain_node = node
        else:
            drain_node = old_nodes[0]

        drain_nodes([drain_node])

        logger.info("Wait for 3 mon pods to be on running state")
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.MON_APP_LABEL,
            resource_count=3,
            timeout=1400,
        )
        assert verify_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running()

        schedule_nodes([drain_node])

        logger.info("Wait for 3 osd pods to be on running state")
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.OSD_APP_LABEL,
            resource_count=3,
            timeout=600,
        )

        assert verify_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running()
