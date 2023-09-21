import logging
import pytest
import random

from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4b,
    ManageTest,
    managed_service_required,
    ignore_leftovers,
)

from ocs_ci.framework import config
from ocs_ci.helpers.managed_services import verify_osd_distribution_on_provider
from ocs_ci.ocs import machine, constants
from ocs_ci.ocs.cluster import is_ms_provider_cluster
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses,
    check_pods_after_node_replacement,
    get_osd_pods_having_ids,
    delete_pods,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility.retry import retry

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
    unschedule_nodes,
    schedule_nodes,
    wait_for_new_worker_node_ipi,
    consumers_verification_steps_after_provider_node_replacement,
)
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
    2) The node should power on automatically, or if removed from the cluster,
       a new node should create automatically.
    3) The new osd pods with the same ids should start on the stopped node after it powered on,
       or to start on the new osd node or another node in the same zone

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

    nodes.stop_nodes([osd_node], wait=False)
    nodes.wait_for_nodes_to_stop_or_terminate([osd_node])
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

    # Wait for correct OSD distribution. This will also wait for the necessary number of OSD pods to be Running
    retry((AssertionError), tries=10, delay=60)(verify_osd_distribution_on_provider)

    # Verify that OSD pods are running on the node
    osd_ids_on_node = get_node_osd_ids(osd_node_name)
    assert osd_ids_on_node, f"No OSD pod is running on the node {osd_node_name}."
    log.info(f"OSD {osd_ids_on_node} are running on the node {osd_node_name}.")

    # The OSD IDs that were present on the stopped should be in running state now
    osds_with_old_id = get_osd_pods_having_ids(old_osd_pod_ids)
    assert len(set(osds_with_old_id)) == len(
        set(old_osd_pod_ids)
    ), f"One or more of the OSD IDs {old_osd_pod_ids} which were running on the stopped node are not present now"


def check_automated_recovery_from_terminated_node(nodes):
    """
    1) Terminate node.
    2) A new node should be created automatically
    3) The new osd pods with the same ids of the terminated node should start on the new osd node or on another node
       in the same zone

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

    # Wait for correct OSD distribution. This will also wait for the necessary number of OSD pods to be Running
    retry((AssertionError), tries=10, delay=60)(verify_osd_distribution_on_provider)

    # Verify that OSD pods are running on the new node
    osd_ids_on_node = get_node_osd_ids(new_wnode.name)
    assert osd_ids_on_node, f"No OSD pod is running on the node {new_wnode.name}."
    log.info(f"OSD {osd_ids_on_node} are running on the node {new_wnode.name}.")

    # The OSD IDs that were present on the terminated should be in running state now
    osds_with_old_id = get_osd_pods_having_ids(old_osd_pod_ids)
    assert len(set(osds_with_old_id)) == len(
        set(old_osd_pod_ids)
    ), f"One or more of the OSD IDs {old_osd_pod_ids} which were running on the terminated node are not present now"


def check_automated_recovery_from_drain_node(nodes):
    """
    1) Drain one worker node.
    2) Delete the OSD pods associated with the node.
    3) Schedule the worker node.
    4) The OSD pods associated with the node, should back into a Running state, and come up
       on the same node or a different node in the same zone.

    """
    osd_node_name = random.choice(get_osd_running_nodes())
    old_osd_pod_ids = get_node_osd_ids(osd_node_name)
    log.info(f"osd pod ids: {old_osd_pod_ids}")
    node_osd_pods = get_osd_pods_having_ids(old_osd_pod_ids)

    unschedule_nodes([osd_node_name])
    log.info(f"Successfully unschedule the node: {osd_node_name}")

    log.info("Delete the node osd pods")
    delete_pods(node_osd_pods)

    log.info(f"Wait for the node: {osd_node_name} to be scheduled")
    schedule_nodes([osd_node_name])
    log.info(f"Successfully scheduled the node {osd_node_name}")

    # Wait for correct OSD distribution. This will also wait for the necessary number of OSD pods to be Running
    retry((AssertionError), tries=10, delay=60)(verify_osd_distribution_on_provider)

    # Verify that OSD pods are running on the node
    osd_ids_on_node = get_node_osd_ids(osd_node_name)
    assert osd_ids_on_node, f"No OSD pod is running on the node {osd_node_name}."
    log.info(f"OSD {osd_ids_on_node} are running on the node {osd_node_name}.")

    # The OSD IDs that were present on the node should be in running state now
    osds_with_old_id = get_osd_pods_having_ids(old_osd_pod_ids)
    assert len(set(osds_with_old_id)) == len(
        set(old_osd_pod_ids)
    ), f"One or more of the OSD IDs {old_osd_pod_ids} which were running on the node are not present now"


FAILURE_TYPE_FUNC_CALL_DICT = {
    "stopped_node": check_automated_recovery_from_stopped_node,
    "terminate_node": check_automated_recovery_from_terminated_node,
    "drain_node": check_automated_recovery_from_drain_node,
}


@brown_squad
@ignore_leftovers
@tier4b
@managed_service_required
class TestAutomatedRecoveryFromFailedNodeReactiveMS(ManageTest):
    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Initialize the create pods and PVCs factory, save the original index

        """
        self.orig_index = config.cur_index
        self.create_pods_and_pvcs_factory = (
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
        )

    def create_resources(self):
        """
        Create resources on the consumers and run IO

        """
        self.create_pods_and_pvcs_factory()

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

            config.switch_ctx(self.orig_index)
            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                log.info(
                    "Execute the the consumers verification steps before starting the next test"
                )
                consumers_verification_steps_after_provider_node_replacement()

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
        We have 3 test cases to check when running IO in the background:
            A) Automated recovery from stopped worker node
            B) Automated recovery from termination of a worker node
            C) Automated recovery from unschedule and reschedule a worker node.
        """
        self.create_resources()

        config.switch_to_provider()
        log.info("Start executing the node test function on the provider...")
        FAILURE_TYPE_FUNC_CALL_DICT[failure](nodes)

        # Verification steps after the automated recovery.
        assert check_pods_after_node_replacement(), "Not all the pods are running"
        assert (
            verify_worker_nodes_security_groups()
        ), "Not all the worker nodes security groups set correctly"

        # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
        if is_ms_provider_cluster() and config.is_consumer_exist():
            assert consumers_verification_steps_after_provider_node_replacement()

        log.info("Checking that the ceph health is ok on the provider")
        ceph_health_check()

        log.info("Checking that the ceph health is ok on the consumers")
        consumer_indexes = config.get_consumer_indexes_list()
        for i in consumer_indexes:
            config.switch_ctx(i)
            ceph_health_check()
