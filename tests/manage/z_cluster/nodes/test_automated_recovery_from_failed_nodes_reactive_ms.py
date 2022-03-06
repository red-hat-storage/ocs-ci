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
    get_mon_pods,
    get_mgr_pods,
    wait_for_pods_terminating,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.helpers.sanity_helpers import Sanity

from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_worker_nodes,
    get_node_objs,
    get_nodes,
    get_node_pods,
    get_node_osd_ids,
    wait_for_nodes_status,
    recover_node_to_ready_state,
    label_nodes,
    wait_for_new_osd_node,
    get_osd_ids_per_node,
)

log = logging.getLogger(__name__)


@ignore_leftovers
@tier4
@tier4a
@managed_service_required
@skipif_ms_consumer
class TestAutomatedRecoveryFromFailedNodeReactive(ManageTest):
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

        osd_node_name = get_osd_running_nodes()[0]
        log.info(f"osd node name = {osd_node_name}")
        self.osd_node = get_node_objs([osd_node_name])[0]
        machine_name = machine.get_machine_from_node_name(osd_node_name)
        self.machineset_name = machine.get_machineset_from_machine_name(machine_name)
        log.info(f"machineset name: {self.machineset_name}")
        self.start_ready_replica_count = machine.get_ready_replica_count(
            self.machineset_name
        )
        self.start_osd_pod_ids = get_node_osd_ids(osd_node_name)
        log.info(f"osd pod ids associated with the node are: {self.start_osd_pod_ids}")

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

    def test_automated_recovery_from_stopped_node(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
    ):
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

        rook_ceph_pods = get_osd_pods() + get_mon_pods() + get_mgr_pods()
        node_rook_ceph_pods = get_node_pods(osd_node_name, rook_ceph_pods)

        nodes.stop_nodes([osd_node], wait=True)
        log.info(f"Successfully powered off node: {osd_node_name}")

        log.info("Verify the node rook ceph pods go into a Terminating state")
        assert wait_for_pods_terminating(
            node_rook_ceph_pods
        ), "Not all the pods are in a Terminating state"

        log.info(f"Wait for the node: {osd_node_name} to power on")
        wait_for_nodes_status([osd_node_name])
        log.info(f"Successfully powered on node {osd_node_name}")

        new_osd_pod_ids = get_node_osd_ids(osd_node_name)
        log.info(f"new osd pod ids: {new_osd_pod_ids}")
        assert (
            old_osd_pod_ids == new_osd_pod_ids
        ), "New osd pod ids are not equal to the old osd pod ids"

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

    def test_automated_recovery_from_terminate_node(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        1) Terminate node.
        2) The rook ceph pods associated with the node should change to a Terminating state.
        3) A new node should be created automatically
        4) The new osd pods with the same ids of the terminated node should start on
           the new created.
        """
        wnodes = set(get_worker_nodes())
        old_osd_node_names = get_osd_running_nodes()
        osd_node = get_node_objs(old_osd_node_names)[0]
        log.info(f"osd node name: {osd_node.name}")

        machine_name = machine.get_machine_from_node_name(osd_node.name)
        machineset = machine.get_machineset_from_machine_name(machine_name)
        log.info(f"machineset name: {machineset}")

        old_osd_pod_ids = get_node_osd_ids(osd_node.name)
        log.info(f"osd pod ids: {old_osd_pod_ids}")

        rook_ceph_pods = get_osd_pods() + get_mon_pods() + get_mgr_pods()
        node_rook_ceph_pods = get_node_pods(osd_node.name, rook_ceph_pods)

        nodes.terminate_nodes([osd_node], wait=True)
        log.info(f"Successfully terminated node: {osd_node.name}")

        log.info("Verify the node rook ceph pods go into a Terminating state")
        assert wait_for_pods_terminating(
            node_rook_ceph_pods
        ), "Not all the pods are in a Terminating state"

        machine.wait_for_new_node_to_be_ready(machineset, timeout=360)
        new_wnode_names = list(set(wnodes) - set(get_worker_nodes()))
        new_wnode = get_node_objs(new_wnode_names)[0]
        log.info(f"Successfully created a new node {new_wnode}")

        wait_for_nodes_status([new_wnode.name])
        log.info(f"The new worker node {new_wnode} is in a Ready state!")
        label_nodes([new_wnode])

        new_osd_node_name = wait_for_new_osd_node(old_osd_node_names)
        log.info(f"New osd node name: {new_osd_node_name}")
        new_osd_node = get_node_objs([new_osd_node_name])

        new_osd_pod_ids = get_node_osd_ids(new_osd_node)
        log.info(f"new osd pod ids: {new_osd_pod_ids}")
        assert (
            old_osd_pod_ids == new_osd_pod_ids
        ), "New osd pod ids are not equal to the old osd pod ids"

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

    def test_automated_recovery_from_full_cluster_shutdown(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        1) Stop all the worker nodes.
        2) The rook ceph pods associated with the osd nodes should change to a Terminating state.
        3) The worker nodes should be powered on automatically
        4) The new osd pods with the same ids should start on the same worker nodes.
        5) Ceph health should be OK
        """
        old_osd_ids_per_node = get_osd_ids_per_node()
        log.info(f"old osd ids per node: {old_osd_ids_per_node}")

        wnode_names = get_worker_nodes()
        wnodes = get_node_objs(wnode_names)

        nodes.stop_nodes(wnodes)
        log.info(f"Successfully stopped the worker nodes: {wnode_names}")

        wait_for_nodes_status(wnode_names, timeout=360)
        log.info("All the worker nodes are in a Ready state!")

        new_osd_ids_per_node = get_osd_ids_per_node()
        log.info(f"new osd ids per node: {new_osd_ids_per_node}")

        assert (
            old_osd_ids_per_node == new_osd_ids_per_node
        ), "The osd ids per node have changed"

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
