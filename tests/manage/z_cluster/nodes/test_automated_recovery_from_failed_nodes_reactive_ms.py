import logging
import pytest
import random

from ocs_ci.framework.testlib import (
    tier4b,
    ManageTest,
    managed_service_required,
    skipif_ms_consumer,
    ignore_leftovers,
)

from ocs_ci.ocs import machine, constants
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses,
    check_pods_after_node_replacement,
    get_osd_pods_having_ids,
    delete_pods,
    wait_for_osd_pods_having_ids,
)
from ocs_ci.utility.utils import ceph_health_check

from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_worker_nodes,
    get_node_objs,
    get_nodes,
    get_node_osd_ids,
    wait_for_nodes_status,
    recover_node_to_ready_state,
    get_ocs_nodes,
    get_node_rook_ceph_pod_names,
    verify_worker_nodes_security_groups,
    wait_for_osd_ids_come_up_on_node,
    unschedule_nodes,
    schedule_nodes,
)

log = logging.getLogger(__name__)


def get_node_pod_names_expected_to_terminate(node_name):
    """
    Get the node pod names expected to be in a Terminating state

    Args:
        node_name (str): The node name

    Returns:
        list: list of the node pod names expected to be in a Terminating state

    """
    node_rook_ceph_pod_names = get_node_rook_ceph_pod_names(node_name)
    return [
        pod_name
        for pod_name in node_rook_ceph_pod_names
        if not pod_name.startswith(("rook-ceph-operator", "rook-ceph-osd-prepare"))
    ]


def get_all_pod_names_expected_to_terminate():
    """
    Get the pod names of all the ocs nodes, that expected to be in a Terminating state

    Returns:
         list: list of the pod names of all the ocs nodes, that expected to be in a Terminating state

    """
    ocs_nodes = get_ocs_nodes()
    ocs_node_names = [n.name for n in ocs_nodes]
    pod_names_expected_to_terminate = []
    for node_name in ocs_node_names:
        pod_names_expected_to_terminate.extend(
            get_node_pod_names_expected_to_terminate(node_name)
        )

    return pod_names_expected_to_terminate


def check_automated_recovery_from_stopped_node(nodes):
    """
    1) Stop node.
    2) The rook ceph pods associated with the node should change to a Terminating state.
    3) The node should power on automatically.
    4) The new osd pods with the same ids should start on the stopped node after it powered on.

    """
    osd_node_name = random.choice(get_osd_running_nodes())
    osd_node = get_node_objs([osd_node_name])[0]

    old_osd_pod_ids = get_node_osd_ids(osd_node_name)
    log.info(f"osd pod ids: {old_osd_pod_ids}")

    pod_names_expected_to_terminate = get_node_pod_names_expected_to_terminate(
        osd_node_name
    )

    nodes.stop_nodes([osd_node], wait=True)
    log.info(f"Successfully powered off node: {osd_node_name}")

    log.info("Verify the node rook ceph pods go into a Terminating state")
    res = wait_for_pods_to_be_in_statuses(
        [constants.STATUS_TERMINATING], pod_names_expected_to_terminate
    )
    assert res, "Not all the node rook ceph pods are in a Terminating state"

    # This is a workaround until we find what should be the behavior
    # when shutting down a worker node
    nodes.start_nodes(nodes=[osd_node])

    log.info(f"Wait for the node: {osd_node_name} to power on")
    wait_for_nodes_status([osd_node_name])
    log.info(f"Successfully powered on node {osd_node_name}")

    assert wait_for_osd_ids_come_up_on_node(osd_node_name, old_osd_pod_ids)
    log.info(
        f"the osd ids {old_osd_pod_ids} Successfully come up on the node {osd_node_name}"
    )


def check_automated_recovery_from_terminated_node(nodes):
    """
    1) Terminate node.
    2) The rook ceph pods associated with the node should change to a Terminating state.
    3) A new node should be created automatically
    4) The new osd pods with the same ids of the terminated node should start on the new osd node.

    """
    old_wnodes = get_worker_nodes()
    log.info(f"start worker nodes: {old_wnodes}")

    old_osd_node_names = get_osd_running_nodes()
    old_osd_nodes = get_node_objs(old_osd_node_names)
    osd_node = random.choice(old_osd_nodes)
    log.info(f"osd node name: {osd_node.name}")

    machine_name = machine.get_machine_from_node_name(osd_node.name)
    machineset = machine.get_machineset_from_machine_name(machine_name)
    log.info(f"machineset name: {machineset}")

    old_osd_pod_ids = get_node_osd_ids(osd_node.name)
    log.info(f"osd pod ids: {old_osd_pod_ids}")

    pod_names_expected_to_terminate = get_node_pod_names_expected_to_terminate(
        osd_node.name
    )

    nodes.terminate_nodes([osd_node], wait=True)
    log.info(f"Successfully terminated the node: {osd_node.name}")

    log.info("Verify the node rook ceph pods go into a Terminating state")
    res = wait_for_pods_to_be_in_statuses(
        [constants.STATUS_TERMINATING], pod_names_expected_to_terminate
    )
    assert res, "Not all the node rook ceph pods are in a Terminating state"

    machine.wait_for_new_node_to_be_ready(machineset, timeout=900)
    new_wnode_names = list(set(get_worker_nodes()) - set(old_wnodes))
    new_wnode = get_node_objs(new_wnode_names)[0]
    log.info(f"Successfully created a new node {new_wnode.name}")

    wait_for_nodes_status([new_wnode.name])
    log.info(f"The new worker node {new_wnode.name} is in a Ready state!")

    wait_for_osd_ids_come_up_on_node(new_wnode.name, old_osd_pod_ids, timeout=300)
    log.info(
        f"the osd ids {old_osd_pod_ids} Successfully come up on the node {osd_node.name}"
    )


def check_automated_recovery_from_drain_node(nodes):
    """
    1) Drain one worker node.
    2) Delete the OSD pods associated with the node.
    3) The new OSD pods with the same ids that come up, should be in a Pending state.
    4) Schedule the worker node.
    5) The OSD pods associated with the node, should back into a Running state, and come up
        on the same node.

    """
    osd_node_name = random.choice(get_osd_running_nodes())
    old_osd_pod_ids = get_node_osd_ids(osd_node_name)
    log.info(f"osd pod ids: {old_osd_pod_ids}")
    node_osd_pods = get_osd_pods_having_ids(old_osd_pod_ids)

    unschedule_nodes([osd_node_name])
    log.info(f"Successfully unschedule the node: {osd_node_name}")

    log.info("Delete the node osd pods")
    delete_pods(node_osd_pods)

    new_osd_pods = wait_for_osd_pods_having_ids(osd_ids=old_osd_pod_ids)
    new_osd_pod_names = [p.name for p in new_osd_pods]
    log.info(f"Verify the new osd pods {new_osd_pod_names} go into a Pending state")
    res = wait_for_pods_to_be_in_statuses(
        [constants.STATUS_PENDING],
        new_osd_pod_names,
        raise_pod_not_found_error=True,
    )
    assert res, "Not all the node osd pods are in a Pending state"

    log.info(f"Wait for the node: {osd_node_name} to be scheduled")
    schedule_nodes([osd_node_name])
    log.info(f"Successfully scheduled the node {osd_node_name}")

    assert wait_for_osd_ids_come_up_on_node(osd_node_name, old_osd_pod_ids)
    log.info(
        f"the osd ids {old_osd_pod_ids} Successfully come up on the node {osd_node_name}"
    )


FAILURE_TYPE_FUNC_CALL_DICT = {
    "stopped_node": check_automated_recovery_from_stopped_node,
    "terminate_node": check_automated_recovery_from_terminated_node,
    "drain_node": check_automated_recovery_from_drain_node,
}


@ignore_leftovers
@tier4b
@managed_service_required
@skipif_ms_consumer
class TestAutomatedRecoveryFromFailedNodeReactiveMS(ManageTest):
    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        def finalizer():
            log.info("Verify that all the worker nodes are in a Ready state")
            wnodes = get_nodes(node_type=constants.WORKER_MACHINE)
            for wnode in wnodes:
                is_recovered = recover_node_to_ready_state(wnode)
                if not is_recovered:
                    log.warning(f"The node {wnode.name} has failed to recover")

            log.info("Verify again that the ceph health is OK")
            ceph_health_check()

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["failure"],
        argvalues=[
            pytest.param("stopped_node"),
            pytest.param("terminate_node"),
            pytest.param("drain_node"),
        ],
    )
    def test_automated_recovery_from_failed_nodes_reactive_ms(
        self,
        nodes,
        failure,
    ):
        """
        We have 3 test cases to check:
            A) Automated recovery from stopped worker node
            B) Automated recovery from termination of a worker node
            C) Automated recovery from unschedule and reschedule a worker node.
        """
        log.info("Start executing the node test function on the provider...")
        FAILURE_TYPE_FUNC_CALL_DICT[failure](nodes)

        # Verification steps after the automated recovery.
        assert check_pods_after_node_replacement(), "Not all the pods are running"
        assert (
            verify_worker_nodes_security_groups()
        ), "Not all the worker nodes security groups set correctly"

        log.info("Checking that the ceph health is ok")
        ceph_health_check()
