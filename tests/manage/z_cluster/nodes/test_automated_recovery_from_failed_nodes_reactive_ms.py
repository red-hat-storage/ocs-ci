import logging
import pytest
from ocs_ci.framework.testlib import (
    tier4,
    tier4a,
    ManageTest,
    managed_service_required,
    skipif_ms_consumer,
    ignore_leftovers,
)

from ocs_ci.ocs import machine, constants
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    get_osd_pod_id,
    get_osd_pods_having_ids,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import wait_for_rook_ceph_pod_status

from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_node_objs,
    get_nodes,
    get_node_pods,
    wait_for_nodes_status,
    recover_node_to_ready_state,
)

log = logging.getLogger(__name__)


@ignore_leftovers
@tier4
@tier4a
@managed_service_required
@skipif_ms_consumer
class TestAutomatedRecoveryFromStoppedNodes(ManageTest):

    osd_worker_node = None
    extra_node = False
    machineset_name = None
    start_ready_replica_count = None

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        def finalizer():
            log.info("Verify that all the worker nodes are in a Ready state")
            wnodes = get_nodes(node_type=constants.WORKER_MACHINE)
            for wnode in wnodes:
                is_recovered = recover_node_to_ready_state(wnode)
                if not is_recovered:
                    log.warning(f"The node {wnode.name} has failed to recover")

            ceph_health_check()

        request.addfinalizer(finalizer)

    def test_automated_recovery_from_stopped_node_and_start(self, nodes):
        """
        1) Stop node.
        2) The osd pods associated with the node should change to a Terminating state.
        3) The node should power on automatically.
        4) The new osd pods should start on the stopped node after it powered on.
        """
        wnode_name = get_osd_running_nodes()[0]
        machine_name = machine.get_machine_from_node_name(wnode_name)
        self.machineset_name = machine.get_machineset_from_machine_name(machine_name)
        self.osd_worker_node = get_node_objs([wnode_name])[0]

        osd_pods = get_node_pods(self.osd_worker_node, pods_to_search=get_osd_pods())
        osd_pod_ids = [get_osd_pod_id(p) for p in osd_pods]
        log.info(f"osd pod ids: {osd_pod_ids}")

        nodes.stop_nodes([self.osd_worker_node], wait=True)
        log.info(f"Successfully powered off node: {self.osd_worker_node[0].name}")

        log.info("Verify the osd pods go into a Terminating state")
        timeout = 180
        for osd_pod in osd_pods:
            assert wait_for_rook_ceph_pod_status(
                osd_pod, constants.STATUS_TERMINATING, timeout
            ), (
                f"The pod {osd_pod.name} didn't reach the status {constants.STATUS_TERMINATING} "
                f"after {timeout} seconds"
            )

        log.info(f"Wait for the node: {self.osd_worker_node.name} to power on")
        wait_for_nodes_status([self.osd_worker_node])
        log.info(f"Successfully powered on node {self.osd_worker_node.name}")

        new_osd_pods = get_node_pods(
            self.osd_worker_node, pods_to_search=get_osd_pods()
        )
        osd_pods_with_start_ids = get_osd_pods_having_ids(osd_pod_ids)
        osd_pods_with_start_id_names = [p.name for p in osd_pods_with_start_ids]

        for new_osd_pod in new_osd_pods:
            assert new_osd_pod.name in osd_pods_with_start_id_names

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check()
