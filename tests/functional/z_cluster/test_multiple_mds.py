"""
Test cases for multiple mds support
"""

import logging
import random

import pytest

from ocs_ci.framework.pytest_customization.marks import brown_squad, tier4c
from ocs_ci.helpers.helpers import verify_storagecluster_nodetopology
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import (
    adjust_active_mds_count,
    get_active_mds_count,
    get_active_mds_pods,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import node
from tests.functional.z_cluster.nodes.test_node_replacement_proactive import (
    delete_and_create_osd_node,
)

log = logging.getLogger(__name__)


@brown_squad
@tier4c
class TestMultipleMds:
    """
    Tests for support multiple mds

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure mds pod count is set to original.

        """

        def finalizer():
            adjust_active_mds_count(1), "Failed to set active mds count to 1"

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_multiple_mds(self, cluster):
        """
        1. Trigger the scale-up process to add new pods.
        2. Verify active and standby-replay mds count is same.
        3. Perform node replacement on a newly added mds pod running node.
        4. Make sure all the active mds pods come to active state.

        """
        original_active_count = get_active_mds_count()

        # Scale up active mds pods from 1 to 2.
        new_active_mds_count = original_active_count + 1
        adjust_active_mds_count(new_active_mds_count)

        # Scale up active mds pods from 2 to 3.
        new_active_mds_count = original_active_count + 1
        adjust_active_mds_count(new_active_mds_count)

        # Verify active and standby-replay mds counts.
        ct_pod = pod.get_ceph_tools_pod()
        ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")
        # Extract the mdsmap list from the data
        ceph_mdsmap = ceph_mdsmap["mdsmap"]
        # Counting active MDS daemons
        active_count = sum(1 for mds in ceph_mdsmap if mds["state"] == "active")

        standby_replay = sum(
            1 for mds in ceph_mdsmap if mds["state"] == "standby-replay"
        )

        log.info(f"Number of active MDS daemons:{active_count}")
        log.info(f"Number of standby MDS daemons:{standby_replay}")
        assert (
            active_count == new_active_mds_count
        ), "Active mds counts did not increased"
        assert (
            standby_replay == new_active_mds_count
        ), "Standby replay mds counts did not increased"

        # Replace node
        active_mds_pods = get_active_mds_pods()
        active_mds_pod = random.choice(active_mds_pods)
        active_mds_node_name = active_mds_pod.data["spec"].get("nodeName")
        log.info(f"Replacing active mds node : {active_mds_node_name}")
        delete_and_create_osd_node(active_mds_node_name)

        toolbox_pod = pod.get_ceph_tools_pod()
        tree_output = toolbox_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
        log.info(f"ceph osd tree output:{tree_output}")

        assert not (
            active_mds_node_name in str(tree_output)
        ), f"Deleted host {active_mds_node_name} still exist in ceph osd tree after node replacement"

        assert (
            verify_storagecluster_nodetopology
        ), "Storagecluster node topology is having an entry of non ocs node"

        assert (
            active_count == new_active_mds_count
        ), "Active mds counts did not match after node replacement"
        assert (
            standby_replay == new_active_mds_count
        ), "Standby replay mds counts did not match after node replacement"

    def test_fault_tolerance_multiple_mds(self):
        """
        1. Trigger the scale-up process to add new pods.
        2. Drain active mds pod running node.
        3. Verify active and standby-replay mds count is same.
        4. Fail one active mds pod [out of two] and standby pod changes to active.

        """

        original_active_count = get_active_mds_count()

        # Scale up active mds pods from 1 to 2.
        new_active_mds_count = original_active_count + 1
        adjust_active_mds_count(new_active_mds_count)

        # Get active mds node name
        active_mds_pods = get_active_mds_pods()
        active_mds_pod = random.choice(active_mds_pods)
        active_mds_pod_name = active_mds_pod.name
        selected_pod_obj = pod.get_pod_obj(
            name=active_mds_pod_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        active_mds_node_name = selected_pod_obj.data["spec"].get("nodeName")

        # Drain active mds pod running node
        node.drain_nodes([active_mds_node_name])

        # Make the node schedulable again
        node.schedule_nodes([active_mds_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=40)

        # Verify active and standby-replay mds counts.
        ct_pod = pod.get_ceph_tools_pod()
        ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")
        # Extract the mdsmap list from the data
        ceph_mdsmap = ceph_mdsmap["mdsmap"]
        # Counting active MDS daemons
        active_count = sum(1 for mds in ceph_mdsmap if mds["state"] == "active")

        standby_replay = sum(
            1 for mds in ceph_mdsmap if mds["state"] == "standby-replay"
        )

        log.info(f"Number of active MDS daemons:{active_count}")
        log.info(f"Number of standby MDS daemons:{standby_replay}")
        assert (
            active_count == new_active_mds_count
        ), "Active mds counts did not increased"
        assert (
            standby_replay == new_active_mds_count
        ), "Standby replay mds counts did not increased"

        # Fail one active mds pod [out of two]
        rand = random.randint(0, 1)
        ct_pod = pod.get_ceph_tools_pod()
        ct_pod.exec_ceph_cmd(f"ceph mds fail {rand}")
        # Verify active and standby-replay mds counts.
        ct_pod = pod.get_ceph_tools_pod()
        ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")
        # Extract the mdsmap list from the data
        ceph_mdsmap = ceph_mdsmap["mdsmap"]
        # Counting active MDS daemons
        active_count = sum(1 for mds in ceph_mdsmap if mds["state"] == "active")

        standby_replay = sum(
            1 for mds in ceph_mdsmap if mds["state"] == "standby-replay"
        )

        log.info(f"Number of active MDS daemons:{active_count}")
        log.info(f"Number of standby MDS daemons:{standby_replay}")
        assert (
            active_count == new_active_mds_count
        ), "Active mds counts did not increased"
        assert (
            standby_replay == new_active_mds_count
        ), "Standby replay mds counts did not increased"
