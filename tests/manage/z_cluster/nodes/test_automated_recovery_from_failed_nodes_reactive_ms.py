import logging
import pytest
import random

from ocs_ci.framework.testlib import (
    tier4b,
    ManageTest,
    managed_service_required,
    ignore_leftovers,
)

from ocs_ci.framework import config
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
    get_node_rook_ceph_pod_names,
    verify_worker_nodes_security_groups,
    wait_for_osd_ids_come_up_on_node,
    unschedule_nodes,
    schedule_nodes,
    wait_for_new_worker_node_ipi,
)
from ocs_ci.ocs.cephfs_workload import LogReaderWriterParallel
from ocs_ci.ocs.exceptions import ResourceWrongStatusException

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


def check_automated_recovery_from_stopped_node(nodes):
    """
    1) Stop node.
    2) The rook ceph pods associated with the node should change to a Terminating state.
    3) The node should power on automatically, or if removed from the cluster,
       a new node should create automatically.
    4) The new osd pods with the same ids should start on the stopped node after it powered on,
       or to start on the new osd node.

    """
    old_wnodes = get_worker_nodes()
    log.info(f"Current worker nodes: {old_wnodes}")

    osd_node_name = random.choice(get_osd_running_nodes())
    osd_node = get_node_objs([osd_node_name])[0]

    machine_name = machine.get_machine_from_node_name(osd_node_name)
    machineset = machine.get_machineset_from_machine_name(machine_name)
    log.info(f"machineset name: {machineset}")

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

    try:
        log.info(f"Wait for the node: {osd_node_name} to power on")
        wait_for_nodes_status([osd_node_name])
        log.info(f"Successfully powered on node {osd_node_name}")
    except ResourceWrongStatusException as e:
        log.info(
            f"The worker node {osd_node_name} didn't start due to the exception {str(e)} "
            f"Probably it has been removed from the cluster. Waiting for a new node to come up..."
        )
        new_wnode = wait_for_new_worker_node_ipi(machineset, old_wnodes)
        osd_node_name = new_wnode.name

    assert wait_for_osd_ids_come_up_on_node(osd_node_name, old_osd_pod_ids, timeout=300)
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
    log.info(f"Current worker nodes: {old_wnodes}")

    osd_node_name = random.choice(get_osd_running_nodes())
    osd_node = get_node_objs([osd_node_name])[0]

    machine_name = machine.get_machine_from_node_name(osd_node_name)
    machineset = machine.get_machineset_from_machine_name(machine_name)
    log.info(f"machineset name: {machineset}")

    old_osd_pod_ids = get_node_osd_ids(osd_node_name)
    log.info(f"osd pod ids: {old_osd_pod_ids}")

    pod_names_expected_to_terminate = get_node_pod_names_expected_to_terminate(
        osd_node.name
    )

    nodes.terminate_nodes([osd_node], wait=True)
    log.info(f"Successfully terminated the node: {osd_node_name}")

    log.info("Verify the node rook ceph pods go into a Terminating state")
    res = wait_for_pods_to_be_in_statuses(
        [constants.STATUS_TERMINATING], pod_names_expected_to_terminate
    )
    assert res, "Not all the node rook ceph pods are in a Terminating state"

    new_wnode = wait_for_new_worker_node_ipi(machineset, old_wnodes)

    wait_for_osd_ids_come_up_on_node(new_wnode.name, old_osd_pod_ids, timeout=300)
    log.info(
        f"the osd ids {old_osd_pod_ids} Successfully come up on the node {new_wnode.name}"
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

    wnodes = get_worker_nodes()
    if len(wnodes) <= 3:
        expected_pods_status = constants.STATUS_PENDING
    else:
        expected_pods_status = constants.STATUS_RUNNING

    log.info(
        f"Verify the new osd pods {new_osd_pod_names} go into a {expected_pods_status} state"
    )
    res = wait_for_pods_to_be_in_statuses(
        [expected_pods_status],
        new_osd_pod_names,
        raise_pod_not_found_error=True,
    )
    assert res, f"Not all the node osd pods are in a {expected_pods_status} state"

    log.info(f"Wait for the node: {osd_node_name} to be scheduled")
    schedule_nodes([osd_node_name])
    log.info(f"Successfully scheduled the node {osd_node_name}")

    if len(wnodes) <= 3:
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
class TestAutomatedRecoveryFromFailedNodeReactiveMS(ManageTest):
    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        def finalizer():
            config.switch_to_provider()
            log.info(
                "Verify that all the worker nodes are in a Ready state on the provider"
            )
            wnodes = get_nodes(node_type=constants.WORKER_MACHINE)
            for wnode in wnodes:
                is_recovered = recover_node_to_ready_state(wnode)
                if not is_recovered:
                    log.warning(f"The node {wnode.name} has failed to recover")

            log.info("Verify again that the ceph health is OK")
            ceph_health_check()

            config.switch_to_consumer()
            log.info("Verify that the ceph health is OK on consumer")
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
        project,
        tmp_path,
    ):
        """
        We have 3 test cases to check when running IO in the background:
            A) Automated recovery from stopped worker node
            B) Automated recovery from termination of a worker node
            C) Automated recovery from unschedule and reschedule a worker node.
        """
        config.switch_to_consumer()
        ceph_health_check()
        log_read_write = LogReaderWriterParallel(project, tmp_path)
        log_read_write.log_reader_writer_parallel()

        config.switch_to_provider()
        log.info("Start executing the node test function on the provider...")
        FAILURE_TYPE_FUNC_CALL_DICT[failure](nodes)

        # Verification steps after the automated recovery.
        assert check_pods_after_node_replacement(), "Not all the pods are running"
        assert (
            verify_worker_nodes_security_groups()
        ), "Not all the worker nodes security groups set correctly"

        log.info("Checking that the ceph health is ok on the provider")
        ceph_health_check()

        config.switch_to_consumer()
        log.info("Validate the data on the consumer")
        log_read_write.fetch_and_validate_data()

        log.info("Checking that the ceph health is ok on the consumer")
        ceph_health_check()
