import logging
import pytest

from ocs_ci.framework.testlib import (
    tier4,
    tier4a,
    ManageTest,
    managed_service_required,
    ignore_leftovers,
)

from ocs_ci.framework import config
from ocs_ci.ocs import machine, constants, cephfs_workload
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses,
    check_pods_after_node_replacement,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.helpers.sanity_helpers import Sanity

from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_worker_nodes,
    get_node_objs,
    get_nodes,
    get_node_osd_ids,
    wait_for_nodes_status,
    recover_node_to_ready_state,
    label_nodes,
    get_ocs_nodes,
    get_osd_ids_per_node,
    get_node_rook_ceph_pod_names,
    verify_worker_nodes_security_groups,
    wait_for_osd_ids_come_up_on_node,
    wait_for_all_osd_ids_come_up_on_nodes,
)

log = logging.getLogger(__name__)


def get_node_pod_names_expect_to_terminate(node_name):
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


def get_all_pod_names_expect_to_terminate():
    """
    Get the pod names of all the ocs nodes, that expected to be in a Terminating state

    Returns:
         list: list of the pod names of all the ocs nodes, that expected to be in a Terminating state

    """
    ocs_nodes = get_ocs_nodes()
    pod_names_expect_to_terminate = []
    for n in ocs_nodes:
        pod_names_expect_to_terminate.extend(get_node_pod_names_expect_to_terminate(n))

    return pod_names_expect_to_terminate


def check_automated_recovery_from_stopped_node(nodes):
    """
    1) Stop node.
    2) The rook ceph pods associated with the node should change to a Terminating state.
    3) The node should power on automatically.
    4) The new osd pods with the same ids should start on the stopped node after it powered on.

    """
    osd_node_name = get_osd_running_nodes()[0]
    osd_node = get_node_objs([osd_node_name])[0]

    old_osd_pod_ids = get_node_osd_ids(osd_node_name)
    log.info(f"osd pod ids: {old_osd_pod_ids}")

    pod_names_expect_to_terminate = get_node_pod_names_expect_to_terminate(
        osd_node_name
    )

    nodes.stop_nodes([osd_node], wait=True)
    log.info(f"Successfully powered off node: {osd_node_name}")

    log.info("Verify the node rook ceph pods go into a Terminating state")
    res = wait_for_pods_to_be_in_statuses(
        [constants.STATUS_TERMINATING], pod_names_expect_to_terminate
    )
    assert res, "Not all the node rook ceph pods are in a Terminating state"

    log.info(f"Wait for the node: {osd_node_name} to power on")
    wait_for_nodes_status([osd_node_name])
    log.info(f"Successfully powered on node {osd_node_name}")

    assert wait_for_osd_ids_come_up_on_node(osd_node_name, old_osd_pod_ids)
    log.info(
        f"the osd ids {old_osd_pod_ids} Successfully come up on the node {osd_node_name}"
    )


def check_automated_recovery_from_terminate_node(nodes):
    """
    1) Terminate node.
    2) The rook ceph pods associated with the node should change to a Terminating state.
    3) A new node should be created automatically
    4) The new osd pods with the same ids of the terminated node should start on the new osd node.

    """
    old_wnodes = get_worker_nodes()
    log.info(f"start worker nodes: {old_wnodes}")
    old_osd_node_names = get_osd_running_nodes()
    osd_node = get_node_objs(old_osd_node_names)[0]
    log.info(f"osd node name: {osd_node.name}")

    machine_name = machine.get_machine_from_node_name(osd_node.name)
    machineset = machine.get_machineset_from_machine_name(machine_name)
    log.info(f"machineset name: {machineset}")

    old_osd_pod_ids = get_node_osd_ids(osd_node.name)
    log.info(f"osd pod ids: {old_osd_pod_ids}")

    pod_names_expect_to_terminate = get_node_pod_names_expect_to_terminate(
        osd_node.name
    )

    nodes.stop_nodes([osd_node], wait=True)
    log.info(f"Successfully powered off node: {osd_node.name}")

    log.info("Verify the node rook ceph pods go into a Terminating state")
    res = wait_for_pods_to_be_in_statuses(
        [constants.STATUS_TERMINATING], pod_names_expect_to_terminate
    )
    assert res, "Not all the node rook ceph pods are in a Terminating state"

    machine.wait_for_new_node_to_be_ready(machineset, timeout=360)
    new_wnode_names = list(set(get_worker_nodes()) - set(old_wnodes))
    new_wnode = get_node_objs(new_wnode_names)[0]
    log.info(f"Successfully created a new node {new_wnode.name}")

    wait_for_nodes_status([new_wnode.name])
    log.info(f"The new worker node {new_wnode.name} is in a Ready state!")
    label_nodes([new_wnode])

    wait_for_osd_ids_come_up_on_node(new_wnode.name, old_osd_pod_ids, timeout=300)
    log.info(
        f"the osd ids {old_osd_pod_ids} Successfully come up on the node {osd_node.name}"
    )


def check_automated_recovery_from_full_cluster_shutdown(nodes):
    """
    1) Stop all the worker nodes.
    2) The rook ceph pods associated with the osd nodes should change to a Terminating state.
    3) The worker nodes should be powered on automatically.
    4) The new osd pods with the same ids should start on the same worker nodes.

    """
    old_osd_ids_per_node = get_osd_ids_per_node()
    log.info(f"old osd ids per node: {old_osd_ids_per_node}")

    wnode_names = get_worker_nodes()
    wnodes = get_node_objs(wnode_names)

    pod_names_expect_to_terminate = get_all_pod_names_expect_to_terminate()

    nodes.stop_nodes(wnodes)
    log.info(f"Successfully stopped the worker nodes: {wnode_names}")

    res = wait_for_pods_to_be_in_statuses(
        [constants.STATUS_TERMINATING], pod_names_expect_to_terminate
    )
    assert res, "Not all the rook ceph pods are in a Terminating state"

    wait_for_nodes_status(wnode_names, timeout=360)
    log.info("All the worker nodes are in a Ready state!")

    assert wait_for_all_osd_ids_come_up_on_nodes(old_osd_ids_per_node)
    log.info("The osd ids successfully come up on the osd nodes")


FAILURE_TYPE_FUNC_CALL_DICT = {
    "stopped_node": check_automated_recovery_from_stopped_node,
    "terminate_node": check_automated_recovery_from_terminate_node,
    "full_cluster_shutdown": check_automated_recovery_from_full_cluster_shutdown,
}


@ignore_leftovers
@tier4
@tier4a
@managed_service_required
class TestAutomatedRecoveryFromFailedNodeReactiveMS(ManageTest):
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        config.switch_to_consumer()
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        def finalizer():
            config.switch_to_provider()
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
            pytest.param("full_cluster_shutdown"),
        ],
    )
    def test_automated_recovery_from_failed_nodes_reactive_ms(
        self,
        nodes,
        project,
        tmp_path,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
        failure,
    ):
        """
        We have 3 test cases to check:
            A) Automated recovery from stopped worker node
            B) Automated recovery from termination of a worker node
            C) Automated recovery from full cluster shutdown
        """

        log_read_write = cephfs_workload.LogReaderWriterParallel(project, tmp_path)
        log.info("Start read and write to cephfs on consumer")
        log_read_write.log_reader_writer_parallel()

        config.switch_to_provider()

        log.info("Start executing the node test function on the provider...")
        FAILURE_TYPE_FUNC_CALL_DICT[failure](nodes)

        # Verification steps after the automated recovery.
        assert check_pods_after_node_replacement(), "Not all the pods are running"
        assert (
            verify_worker_nodes_security_groups()
        ), "Not all the worker nodes security groups set correctly"

        config.switch_to_consumer()
        log.info("Fetch and validate data on consumer")
        log_read_write.fetch_and_validate_data()

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        # Deleting Resources
        self.sanity_helpers.delete_resources()

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check()
