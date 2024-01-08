import logging
import pytest
import random


from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4a,
    tier4b,
    ignore_leftovers,
    ManageTest,
    bugzilla,
    hci_provider_and_client_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import HCI_PROVIDER
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.node import (
    get_node_objs,
    recover_node_to_ready_state,
    get_osd_running_nodes,
    get_node_osd_ids,
    wait_for_osd_ids_come_up_on_node,
    wait_for_nodes_status,
    get_nodes,
    wait_for_node_count_to_reach_status,
    drain_nodes,
    schedule_nodes,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.cluster import (
    ceph_health_check,
)
from ocs_ci.framework import config
from ocs_ci.utility.utils import switch_to_correct_cluster_at_setup

logger = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@hci_provider_and_client_required
class TestNodesRestartHCI(ManageTest):
    """
    Test nodes restart scenarios when using HCI platform
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Initialize Sanity instance, and create pods and PVCs factory

        """
        self.orig_index = config.cur_index
        switch_to_correct_cluster_at_setup(request)
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again

        """

        def finalizer():
            ocp_nodes = get_node_objs()
            for n in ocp_nodes:
                recover_node_to_ready_state(n)

            logger.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)
            ceph_health_check()

        request.addfinalizer(finalizer)

    @tier4a
    @pytest.mark.polarion_id("OCS-3980")
    @pytest.mark.parametrize(
        "cluster_type",
        [HCI_PROVIDER],
    )
    def test_osd_node_restart_and_check_osd_pods_status(self, cluster_type, nodes):
        """
        1) Restart one of the osd nodes.
        2) Check that the osd pods associated with the node should change to a Terminating state.
        3) Wait for the node to reach Ready state.
        4) Check that the new osd pods with the same ids start on the same node.

        """
        osd_node_name = random.choice(get_osd_running_nodes())
        osd_node = get_node_objs([osd_node_name])[0]

        old_osd_pod_ids = get_node_osd_ids(osd_node_name)
        logger.info(f"osd pod ids: {old_osd_pod_ids}")
        node_osd_pods = pod.get_osd_pods_having_ids(old_osd_pod_ids)
        node_osd_pod_names = [p.name for p in node_osd_pods]

        logger.info(f"Going to restart the node {osd_node_name}")
        nodes.restart_nodes(nodes=[osd_node], wait=False)

        logger.info("Verify the node osd pods go into a Terminating state")
        res = pod.wait_for_pods_to_be_in_statuses(
            [constants.STATUS_TERMINATING], node_osd_pod_names
        )
        assert res, "Not all the node osd pods are in a Terminating state"

        wait_for_nodes_status(node_names=[osd_node_name], timeout=300)

        if len(get_nodes(constants.WORKER_MACHINE)) <= 3:
            assert wait_for_osd_ids_come_up_on_node(
                osd_node_name, old_osd_pod_ids, timeout=300
            )
            logger.info(
                f"the osd ids {old_osd_pod_ids} Successfully come up on the node {osd_node_name}"
            )
        else:
            new_osd_pods = pod.wait_for_osd_pods_having_ids(osd_ids=old_osd_pod_ids)
            new_osd_pod_names = [p.name for p in new_osd_pods]
            logger.info(
                f"Wait for the new osd pods with the ids {old_osd_pod_ids} to be running"
            )
            res = pod.wait_for_pods_to_be_in_statuses(
                constants.STATUS_RUNNING,
                new_osd_pod_names,
                raise_pod_not_found_error=True,
            )
            assert res, "Not all the node osd pods are in a Running state"

    @tier4a
    @pytest.mark.parametrize(
        argnames=["cluster_type", "node_type"],
        argvalues=[
            pytest.param(
                *[HCI_PROVIDER, constants.WORKER_MACHINE],
            ),
            pytest.param(
                *[HCI_PROVIDER, constants.MASTER_MACHINE],
            ),
        ],
    )
    def test_nodes_restart(self, cluster_type, nodes, node_type):
        """
        Test nodes restart (from the platform layer)

        """
        node_count = len(get_nodes(node_type=node_type))
        ocp_nodes = get_nodes(node_type=node_type)
        ocp_node = random.choice(ocp_nodes)

        nodes.restart_nodes(nodes=[ocp_node])
        wait_for_node_count_to_reach_status(node_count=node_count, node_type=node_type)
        ceph_health_check()

    @tier4b
    @bugzilla("1754287")
    @pytest.mark.polarion_id("OCS-2015")
    @pytest.mark.parametrize(
        argnames=["cluster_type", "node_type"],
        argvalues=[
            pytest.param(*[HCI_PROVIDER, constants.WORKER_MACHINE]),
        ],
    )
    def test_rolling_nodes_restart(self, cluster_type, nodes, node_type):
        """
        Test restart nodes one after the other and check health status in between

        """
        node_count = len(get_nodes(node_type))
        ocp_nodes = get_nodes(node_type=node_type)

        for node in ocp_nodes:
            nodes.restart_nodes(nodes=[node])
            wait_for_node_count_to_reach_status(
                node_count=node_count, node_type=node_type
            )
            ceph_health_check(tries=40)

    @tier4a
    @pytest.mark.parametrize(
        argnames=["cluster_type", "node_type"],
        argvalues=[
            pytest.param(*[HCI_PROVIDER, constants.WORKER_MACHINE]),
        ],
    )
    def test_node_maintenance_restart(self, cluster_type, nodes, node_type):
        """
        - Mark as unschedulable and drain 1 worker node in the provider cluster
        - Restart the node
        - Mark the node as schedulable

        """
        typed_nodes = get_nodes(node_type=node_type)
        assert typed_nodes, f"Failed to find a {node_type} node."
        typed_node = random.choice(typed_nodes)
        typed_node_name = typed_node.name

        # Get the current reboot events from the node
        reboot_events_cmd = (
            f"get events -A --field-selector involvedObject.name="
            f"{typed_node_name},reason=Rebooted -o yaml"
        )

        # Find the number of reboot events in the node
        num_events = len(typed_node.ocp.exec_oc_cmd(reboot_events_cmd)["items"])

        # Unschedule and drain the node
        drain_nodes([typed_node_name])
        # Wait for the node to be unschedule
        wait_for_nodes_status(
            node_names=[typed_node_name],
            status=constants.NODE_READY_SCHEDULING_DISABLED,
        )

        # Restart the node
        nodes.restart_nodes(nodes=typed_nodes, wait=False)

        # Verify that the node restarted
        try:
            wait_for_nodes_status(
                node_names=[typed_node_name],
                status=constants.NODE_NOT_READY_SCHEDULING_DISABLED,
            )
        except ResourceWrongStatusException:
            # Sometimes, the node will be back to running state quickly so
            # that the status change won't be detected. Verify the node was
            # actually restarted by checking the reboot events count
            new_num_events = len(
                typed_nodes[0].ocp.exec_oc_cmd(reboot_events_cmd)["items"]
            )
            assert new_num_events > num_events, (
                f"Reboot event not found." f"Node {typed_node_name} did not restart."
            )

        # Wait for the node to be Ready
        wait_for_nodes_status(
            node_names=[typed_node_name],
            status=constants.NODE_READY_SCHEDULING_DISABLED,
        )

        # Mark the node as schedulable
        schedule_nodes([typed_node_name])

        self.sanity_helpers.health_check()
