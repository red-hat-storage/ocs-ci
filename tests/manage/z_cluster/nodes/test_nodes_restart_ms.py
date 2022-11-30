import logging
import pytest
import random


from ocs_ci.framework.testlib import (
    tier4a,
    tier4b,
    ignore_leftovers,
    ManageTest,
    bugzilla,
    managed_service_required,
    polarion_id,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    get_node_objs,
    recover_node_to_ready_state,
    get_osd_running_nodes,
    get_node_osd_ids,
    wait_for_osd_ids_come_up_on_node,
    wait_for_nodes_status,
    verify_worker_nodes_security_groups,
    get_nodes,
    wait_for_node_count_to_reach_status,
    drain_nodes,
    schedule_nodes,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.cluster import (
    ceph_health_check,
    is_ms_consumer_cluster,
)
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


@ignore_leftovers
@managed_service_required
class TestNodesRestartMS(ManageTest):
    """
    Test nodes restart scenarios when using managed service
    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Initialize Sanity instance, and create pods and PVCs factory

        """
        self.orig_index = config.cur_index
        self.sanity_helpers = Sanity()
        self.create_pods_and_pvcs_factory = (
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
        )

    def create_resources(self):
        """
        Create resources on the consumers and run IO

        """
        if is_ms_consumer_cluster():
            consumer_indexes = [config.cur_index]
        else:
            consumer_indexes = config.get_consumer_indexes_list()

        self.create_pods_and_pvcs_factory(consumer_indexes=consumer_indexes)

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
    def test_osd_node_restart_and_check_osd_pods_status(self, nodes):
        """
        1) Restart one of the osd nodes.
        2) Check that the osd pods associated with the node should change to a Terminating state.
        3) Wait for the node to reach Ready state.
        4) Check that the new osd pods with the same ids start on the same node.
        5) Check the worker nodes security groups.
        """
        # This is a workaround due to the issue https://github.com/red-hat-storage/ocs-ci/issues/6162
        if is_ms_consumer_cluster():
            logger.info(
                "The test is applicable only for an MS provider cluster. "
                "Switching to the provider cluster..."
            )
            config.switch_to_provider()

        self.create_resources()

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

        wait_for_nodes_status(node_names=[osd_node_name])
        assert wait_for_osd_ids_come_up_on_node(
            osd_node_name, old_osd_pod_ids, timeout=300
        )
        logger.info(
            f"the osd ids {old_osd_pod_ids} Successfully come up on the node {osd_node_name}"
        )

        logger.info("Verify the worker nodes security groups on the provider...")
        assert verify_worker_nodes_security_groups()

    @tier4a
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(
                *[constants.WORKER_MACHINE], marks=pytest.mark.polarion_id("OCS-3982")
            ),
            pytest.param(
                *[constants.MASTER_MACHINE], marks=pytest.mark.polarion_id("OCS-3981")
            ),
        ],
    )
    def test_nodes_restart(self, nodes, node_type):
        """
        Test nodes restart (from the platform layer)

        """
        node_count = len(get_nodes(node_type=node_type))
        if node_type == constants.WORKER_MACHINE:
            ocp_nodes = get_nodes(node_type=node_type)
        else:
            ocp_nodes = get_nodes(node_type=node_type, num_of_nodes=2)

        nodes.restart_nodes(nodes=ocp_nodes, wait=False)
        wait_for_node_count_to_reach_status(node_count=node_count, node_type=node_type)
        self.sanity_helpers.health_check()
        self.create_resources()

    @tier4b
    @bugzilla("1754287")
    @pytest.mark.polarion_id("OCS-2015")
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(constants.WORKER_MACHINE),
            pytest.param(constants.MASTER_MACHINE),
        ],
    )
    def test_rolling_nodes_restart(self, nodes, node_type):
        """
        Test restart nodes one after the other and check health status in between

        """
        ocp_nodes = get_nodes(node_type=node_type)
        for node in ocp_nodes:
            nodes.restart_nodes(nodes=[node], wait=False)
            wait_for_node_count_to_reach_status(
                node_count=len(ocp_nodes), node_type=node_type
            )
            self.sanity_helpers.health_check(cluster_check=False, tries=60)

        if is_ms_consumer_cluster():
            logger.info(
                "Verify that the nodes are ready before start creating resources"
            )
            wait_for_node_count_to_reach_status(
                node_count=len(ocp_nodes), node_type=node_type
            )

        self.create_resources()

    @tier4a
    @polarion_id("OCS-4482")
    def test_node_maintenance_restart(self, nodes, pvc_factory, pod_factory):
        """
        - Mark as unschedulable and drain 1 worker node in the provider cluster
        - Check cluster functionality by creating resources from the consumer cluster
          (PVCs, pods - both CephFS and RBD)
        - Restart the node
        - Mark the node as schedulable
        - Verify storagecluster, managedocs and cephcluster
        - Check cluster functionality by creating resources from the consumer cluster
          (PVCs, pods - both CephFS and RBD)

        """
        # Switch to provider cluster for the test
        if is_ms_consumer_cluster():
            logger.info(
                "The test is applicable only for an MS provider cluster. "
                "Switching to the provider cluster..."
            )
            config.switch_to_provider()

        self.create_resources()

        # Get 1 worker node
        typed_nodes = get_nodes(node_type=constants.WORKER_MACHINE, num_of_nodes=1)
        assert typed_nodes, f"Failed to find a {constants.WORKER_MACHINE} node."
        typed_node_name = typed_nodes[0].name

        # Get the current reboot events from the node
        reboot_events_cmd = (
            f"get events -A --field-selector involvedObject.name="
            f"{typed_node_name},reason=Rebooted -o yaml"
        )

        # Find the number of reboot events in the node
        num_events = len(typed_nodes[0].ocp.exec_oc_cmd(reboot_events_cmd)["items"])

        # Unschedule and drain the node
        drain_nodes([typed_node_name])

        # Create PVCs and pods
        self.create_resources()

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

        # Performs necessary checks in the clusters
        for cluster_index in [
            config.get_provider_index()
        ] + config.get_consumer_indexes_list():
            config.switch_ctx(cluster_index)

            # Verify storagecluster status
            logger.info("Verifying storagecluster status")
            verify_storage_cluster()

            # Verify managedocs components are Ready
            logger.info("Verifying managedocs components state")
            managedocs_obj = OCP(
                kind="managedocs",
                resource_name="managedocs",
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            for component in {"alertmanager", "prometheus", "storageCluster"}:
                assert (
                    managedocs_obj.get()["status"]["components"][component]["state"]
                    == "Ready"
                ), f"{component} status is {managedocs_obj.get()['status']['components'][component]['state']}"

            # Verify the phase of ceph cluster
            logger.info("Verify the phase of ceph cluster")
            cephcluster = OCP(
                kind="CephCluster", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            cephcluster_yaml = cephcluster.get().get("items")[0]
            expected_phase = (
                "Ready" if cluster_index == config.get_provider_index() else "Connected"
            )
            assert (
                cephcluster_yaml["status"]["phase"] == expected_phase
            ), f"Status of cephcluster {cephcluster_yaml['metadata']['name']} is {cephcluster_yaml['status']['phase']}"

        # Create PVCs and pods
        self.create_resources()
