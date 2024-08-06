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
    provider_client_platform_required,
    polarion_id,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import HCI_PROVIDER
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.node import (
    get_node_objs,
    recover_node_to_ready_state,
    get_osd_running_nodes,
    get_node_osd_ids,
    wait_for_nodes_status,
    get_nodes,
    wait_for_node_count_to_reach_status,
    drain_nodes,
    schedule_nodes,
    get_node_by_internal_ip,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import SanityProviderMode
from ocs_ci.ocs.cluster import (
    ceph_health_check,
)
from ocs_ci.framework import config
from ocs_ci.utility.utils import switch_to_correct_cluster_at_setup
from ocs_ci.ocs.resources.storage_cluster import (
    get_client_storage_provider_endpoint,
    wait_for_storage_client_connected,
)
from ocs_ci.utility.decorators import switch_to_client_for_function


logger = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@provider_client_platform_required
class TestNodesRestartHCI(ManageTest):
    """
    Test nodes restart scenarios when using HCI platform
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, create_scale_pods_and_pvcs_using_kube_job_on_hci_clients):
        """
        Initialize Sanity instance, and create pods and PVCs factory

        """
        self.orig_index = config.cur_index
        switch_to_correct_cluster_at_setup(request)
        self.sanity_helpers = SanityProviderMode(
            create_scale_pods_and_pvcs_using_kube_job_on_hci_clients
        )

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
        4) Wait for the node to reach the Ready state.
        5) Wait for the new osd pods with the old ids to be running

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
            [constants.STATUS_TERMINATING], node_osd_pod_names, timeout=480, sleep=20
        )
        assert res, "Not all the node osd pods are in a Terminating state"

        logger.info(f"Wait for the node {osd_node_name} to be ready")
        wait_for_nodes_status(node_names=[osd_node_name], timeout=720, sleep=20)

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
                marks=pytest.mark.polarion_id("OCS-5420"),
            ),
            pytest.param(
                *[HCI_PROVIDER, constants.MASTER_MACHINE],
                marks=pytest.mark.polarion_id("OCS-5420"),
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

        nodes.restart_nodes(nodes=[ocp_node], wait=True)
        logger.info("Wait for the expected node count to be ready...")
        wait_for_node_count_to_reach_status(node_count=node_count, node_type=node_type)
        self.sanity_helpers.health_check_provider_mode(tries=40)
        self.sanity_helpers.create_resources_on_clients()

    @tier4a
    @pytest.mark.parametrize(
        argnames=["cluster_type", "node_type"],
        argvalues=[
            pytest.param(
                *[HCI_PROVIDER, constants.WORKER_MACHINE],
                marks=pytest.mark.polarion_id("OCS-5421"),
            ),
            pytest.param(
                *[HCI_PROVIDER, constants.MASTER_MACHINE],
                marks=pytest.mark.polarion_id("OCS-5421"),
            ),
        ],
    )
    def test_nodes_restart_by_stop_and_start(self, cluster_type, nodes, node_type):
        """
        Test nodes restart by stop and start (from the platform layer)

        """
        node_count = len(get_nodes(node_type=node_type))
        ocp_nodes = get_nodes(node_type=node_type)
        ocp_node = random.choice(ocp_nodes)

        nodes.restart_nodes_by_stop_and_start(nodes=[ocp_node], wait=True)
        logger.info("Wait for the expected node count to be ready...")
        wait_for_node_count_to_reach_status(node_count=node_count, node_type=node_type)
        self.sanity_helpers.health_check(tries=40)
        self.sanity_helpers.create_resources_on_clients()

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

        self.sanity_helpers.health_check(tries=20)
        self.sanity_helpers.create_resources_on_clients()

    @tier4a
    @polarion_id("OCS-4482")
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
        nodes.restart_nodes(nodes=[typed_node], wait=False)

        # Verify that the node restarted
        try:
            wait_for_nodes_status(
                node_names=[typed_node_name],
                status=constants.NODE_NOT_READY_SCHEDULING_DISABLED,
                timeout=180,
                sleep=5,
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
            timeout=720,
            sleep=20,
        )

        # Mark the node as schedulable
        schedule_nodes([typed_node_name])

        self.sanity_helpers.health_check(tries=40)
        self.sanity_helpers.create_resources_on_clients()

    @tier4a
    @polarion_id("OCS-5802")
    @pytest.mark.parametrize(
        "cluster_type",
        [HCI_PROVIDER],
    )
    def test_stop_provider_endpoint_node_and_check_client_connection(
        self, cluster_type, nodes
    ):
        """
        Test stop the provider storage endpoint node and check the client connection is successfully
        The test will implement the following steps:
        1. Get the client "storageProviderEndpoint" param in the storage-client resource
        2. Get the provider endpoint node by the client "storageProviderEndpoint" in the previous step,
        and stop the provider endpoint node.
        3. Verify the client storage-client is connected to the provider.
        4. Try to create and delete resources from the client/s.
        5. Start the provider endpoint node
        6. Check the cluster health is ok for the provider and clients.
        7. Check the client storage endpoint ip.

        """
        logger.info("Get the 'storageProviderEndpoint' node")
        storage_provider_endpoint = switch_to_client_for_function(
            get_client_storage_provider_endpoint
        )()
        endpoint_ip = storage_provider_endpoint.split(":")[0]
        logger.info(f"client storage endpoint ip = {endpoint_ip}")
        endpoint_node = get_node_by_internal_ip(endpoint_ip)
        assert endpoint_node, "Didn't find the endpoint node in the provider"

        logger.info(f"Stopping the endpoint node {endpoint_node.name}")
        nodes.stop_nodes(nodes=[endpoint_node], wait=True)

        logger.info("Check the client connection is OK")
        switch_to_client_for_function(wait_for_storage_client_connected)(timeout=300)
        logger.info("Try to create and delete resources on the clients")
        self.sanity_helpers.create_resources_on_clients()
        self.sanity_helpers.delete_resources_on_clients()
        logger.info("Check again that the client connection is OK")
        switch_to_client_for_function(wait_for_storage_client_connected)(timeout=120)

        logger.info(f"Starting the endpoint node {endpoint_node.name}")
        nodes.start_nodes(nodes=[endpoint_node], wait=True)
        self.sanity_helpers.health_check_provider_mode(tries=40)

        storage_provider_endpoint = switch_to_client_for_function(
            get_client_storage_provider_endpoint
        )()
        endpoint_ip = storage_provider_endpoint.split(":")[0]
        logger.info(
            f"client storage endpoint ip after starting the endpoint node = {endpoint_ip}"
        )

    @tier4a
    @polarion_id("OCS-5803")
    @pytest.mark.parametrize(
        "cluster_type",
        [HCI_PROVIDER],
    )
    def test_stop_provider_mgr_node_and_check_client_connection(
        self, cluster_type, nodes
    ):
        """
        Test stop the provider MGR node and check the client connection is successfully
        The test will implement the following steps:
        1. Pick randomly one of the provider MGR nodes.
        2. Stop the MGR node.
        3. Verify the client storage-client is connected to the provider.
        4. Try to create and delete resources from the client/s.
        5. Start the MGR node
        6. Check the cluster health is ok for the provider and clients.

        """
        mgr_pod = random.choice(pod.get_mgr_pods())
        mgr_node = pod.get_pod_node(mgr_pod)

        logger.info(f"Stopping the MGR node {mgr_node.name}")
        nodes.stop_nodes(nodes=[mgr_node], wait=True)

        logger.info("Check the client connection is OK")
        switch_to_client_for_function(wait_for_storage_client_connected)(timeout=300)
        logger.info("Try to create and delete resources on the clients")
        self.sanity_helpers.create_resources_on_clients()
        self.sanity_helpers.delete_resources_on_clients()
        logger.info("Check again that the client connection is OK")
        switch_to_client_for_function(wait_for_storage_client_connected)(timeout=120)

        logger.info(f"Starting the MGR node {mgr_node.name}")
        nodes.start_nodes(nodes=[mgr_node], wait=True)
        self.sanity_helpers.health_check_provider_mode(tries=40)
