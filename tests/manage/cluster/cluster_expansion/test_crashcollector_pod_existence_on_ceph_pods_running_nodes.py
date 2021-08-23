import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated
from ocs_ci.ocs.node import drain_nodes, schedule_nodes, get_node_zone
from ocs_ci.helpers.helpers import get_failure_domin
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.node import (
    get_nodes_where_ocs_pods_running,
    get_node_rack,
    get_node_names,
    get_crashcollector_nodes,
)
from ocs_ci.framework.testlib import (
    tier2,
    ignore_leftovers,
    ManageTest,
    bugzilla,
    skipif_external_mode,
)

logger = logging.getLogger(__name__)


@tier2
@ignore_leftovers
@bugzilla("1898808")
@skipif_external_mode
@skipif_openshift_dedicated
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

    def test_crashcollector_pod_existence_on_ceph_pods_running_nodes(
        self, add_nodes, node_drain_teardown
    ):
        """
        Add node with OCS label and verify crashcollector created on new node

        """
        failure_domain = get_failure_domin()
        logger.info(f"The failure domain is {failure_domain}")

        if failure_domain in ("zone", "rack"):
            old_node_rack_zone = (
                get_node_zone() if failure_domain.lower() == "zone" else get_node_rack()
            )
            logger.info(f"The old node rack/zone is {old_node_rack_zone}")

        old_nodes = get_node_names()

        logger.info("Add one worker node with OCS label")
        add_nodes(ocs_nodes=True, node_count=1)

        new_node = list(set(get_node_names()) - set(old_nodes))
        logger.info(f"New worker node is {new_node[0]}")

        if failure_domain in ("zone", "rack"):
            new_node_rack_zone = (
                get_node_zone() if failure_domain.lower() == "zone" else get_node_rack()
            )
            logger.info(f"The new node rack/zone is {new_node_rack_zone}")

            new_rack_zone = new_node_rack_zone[new_node[0]]
            logger.info(f"New worker node {new_node[0]} in zone/rack {new_rack_zone}")

            for node, rack_zone in old_node_rack_zone.items():
                if rack_zone == new_rack_zone:
                    drain_node = node
        else:
            drain_node = old_nodes[0]

        drain_nodes([drain_node])

        logging.info("Wait for 3 mon pods to be on running state")
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.MON_APP_LABEL,
            resource_count=3,
            timeout=1400,
        )
        logger.info(
            "Verify rook-ceph-crashcollector pod running on worker node"
            " where rook-ceph pods are running."
        )
        assert sorted(get_crashcollector_nodes()) == sorted(
            get_nodes_where_ocs_pods_running()
        ), (
            f"The crashcollector pod exists on "
            f"{get_crashcollector_nodes() - get_nodes_where_ocs_pods_running()} "
            f"even though rook-ceph pods are not running on this node"
        )

        schedule_nodes([drain_node])

        logging.info("Wait for 3 osd pods to be on running state")
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.OSD_APP_LABEL,
            resource_count=3,
            timeout=600,
        )

        logger.info(
            "Verify rook-ceph-crashcollector pod running on worker node where rook-ceph pods are running."
        )
        assert sorted(get_crashcollector_nodes()) == sorted(
            get_nodes_where_ocs_pods_running()
        ), (
            f"The crashcollector pod exists on "
            f"{get_crashcollector_nodes() - get_nodes_where_ocs_pods_running()} "
            f"even though rook-ceph pods are not running on this node"
        )
