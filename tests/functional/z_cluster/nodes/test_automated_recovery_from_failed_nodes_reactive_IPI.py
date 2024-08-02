import logging
import pytest
from ocs_ci.framework.pytest_customization.marks import brown_squad, skipif_compact_mode
from ocs_ci.framework.testlib import (
    tier4a,
    tier4b,
    ManageTest,
    ipi_deployment_required,
    ignore_leftovers,
    skipif_external_mode,
    skipif_ibm_cloud,
)
from ocs_ci.framework import config
from ocs_ci.ocs import machine, constants
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import get_all_pods, get_osd_pods, get_pod_node
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import (
    label_worker_node,
    remove_label_from_worker_node,
    wait_for_resource_state,
    get_failure_domain,
)
from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_app_pod_running_nodes,
    get_both_osd_and_app_pod_running_node,
    get_node_objs,
    add_new_node_and_label_it,
    get_worker_nodes,
    recover_node_to_ready_state,
    add_new_nodes_and_label_after_node_failure_ipi,
    get_another_osd_node_in_same_rack_or_zone,
    get_node_pods,
    wait_for_nodes_racks_or_zones,
    wait_for_nodes_status,
    wait_for_node_count_to_reach_status,
)
from ocs_ci.ocs.exceptions import ResourceWrongStatusException

log = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@tier4b
@ipi_deployment_required
class TestAutomatedRecoveryFromFailedNodes(ManageTest):
    """
    Knip-678 Automated recovery from failed nodes - Reactive
    """

    threads = []

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            worker_nodes = get_worker_nodes()
            # Removing created label on all worker nodes
            remove_label_from_worker_node(worker_nodes, label_key="dc")
            for thread in self.threads:
                thread.join()

            log.info("Get the machine set name from one of the worker node names")
            machine_name = machine.get_machine_from_node_name(worker_nodes[0])
            machineset_name = machine.get_machineset_from_machine_name(machine_name)
            log.info(
                "Verify that the current replica count is equal to the ready replica count"
            )
            machine.change_current_replica_count_to_ready_replica_count(machineset_name)

            ceph_health_check()

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["interface", "failure"],
        argvalues=[
            pytest.param(
                *["rbd", "shutdown"],
                marks=[
                    pytest.mark.polarion_id("OCS-2102"),
                    pytest.mark.bugzilla("1845666"),
                ],
            ),
            pytest.param(
                *["rbd", "terminate"], marks=pytest.mark.polarion_id("OCS-2103")
            ),
            pytest.param(
                *["cephfs", "shutdown"],
                marks=[
                    pytest.mark.polarion_id("OCS-2104"),
                    pytest.mark.bugzilla("1845666"),
                ],
            ),
            pytest.param(
                *["cephfs", "terminate"], marks=pytest.mark.polarion_id("OCS-2105")
            ),
        ],
    )
    def test_automated_recovery_from_failed_nodes_IPI_reactive(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        failure,
        dc_pod_factory,
        interface,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Reactive case - IPI
        """
        # Get OSD running nodes
        osd_running_nodes = get_osd_running_nodes()
        log.info(f"OSDs are running on nodes {osd_running_nodes}")
        # Label osd nodes with fedora app
        label_worker_node(osd_running_nodes, label_key="dc", label_value="fedora")

        # Create DC app pods
        log.info("Creating DC based app pods")
        if interface == "rbd":
            interface = constants.CEPHBLOCKPOOL
        elif interface == "cephfs":
            interface = constants.CEPHFILESYSTEM
        dc_pod_obj = []
        for i in range(2):
            dc_pod = dc_pod_factory(interface=interface, node_selector={"dc": "fedora"})
            self.threads.append(pod.run_io_in_bg(dc_pod, fedora_dc=True))
            dc_pod_obj.append(dc_pod)

        # Get app pods running nodes
        dc_pod_node_name = get_app_pod_running_nodes(dc_pod_obj)
        log.info(f"DC app pod running nodes are {dc_pod_node_name}")

        # Get both osd and app pod running node
        common_nodes = get_both_osd_and_app_pod_running_node(
            osd_running_nodes, dc_pod_node_name
        )
        log.info(f"Both OSD and app pod is running on nodes {common_nodes}")

        # Get the machine name using the node name
        machine_name = machine.get_machine_from_node_name(common_nodes[0])
        log.info(f"{common_nodes[0]} associated machine is {machine_name}")

        # Get the machineset name using machine name
        machineset_name = machine.get_machineset_from_machine_name(machine_name)
        log.info(f"{common_nodes[0]} associated machineset is {machineset_name}")

        # Add a new node and label it
        new_ocs_node_names = add_new_node_and_label_it(machineset_name)
        failure_domain = get_failure_domain()
        log.info("Wait for the nodes racks or zones to appear...")
        wait_for_nodes_racks_or_zones(failure_domain, new_ocs_node_names)

        new_ocs_node = get_node_objs(new_ocs_node_names)[0]
        osd_node_in_same_rack_or_zone = get_another_osd_node_in_same_rack_or_zone(
            failure_domain, new_ocs_node, common_nodes
        )
        # Get the failure node obj
        failure_node_obj = get_node_objs([osd_node_in_same_rack_or_zone.name])

        # Induce failure on the selected failure node
        log.info(f"Inducing failure on node {failure_node_obj[0].name}")
        if failure == "shutdown":
            nodes.stop_nodes(failure_node_obj, wait=True)
            log.info(f"Successfully powered off node: " f"{failure_node_obj[0].name}")
        elif failure == "terminate":
            nodes.terminate_nodes(failure_node_obj, wait=True)
            log.info(
                f"Successfully terminated node : "
                f"{failure_node_obj[0].name} instance"
            )

        try:
            # DC app pods on the failed node will get automatically created on other
            # running node. Waiting for all dc app pod to reach running state
            pod.wait_for_dc_app_pods_to_reach_running_state(dc_pod_obj, timeout=720)
            log.info("All the dc pods reached running state")
            pod.wait_for_storage_pods(timeout=300)

        except ResourceWrongStatusException:
            if failure == "shutdown":
                nodes.terminate_nodes(failure_node_obj, wait=True)
                log.info(
                    f"Successfully terminated node : "
                    f"{failure_node_obj[0].name} instance"
                )
            raise

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            tries = 200
        else:
            tries = 40

        self.sanity_helpers.health_check(tries=tries)


@brown_squad
@ignore_leftovers
@tier4a
@ipi_deployment_required
@skipif_ibm_cloud
@skipif_compact_mode
class TestAutomatedRecoveryFromStoppedNodes(ManageTest):

    osd_worker_node = None
    extra_node = False
    machineset_name = None
    start_ready_replica_count = None

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        def finalizer():
            if self.extra_node:
                nodes.terminate_nodes([self.osd_worker_node], wait=True)
                log.info(
                    f"Successfully terminated node : "
                    f"{self.osd_worker_node.name} instance"
                )
            else:
                is_recovered = recover_node_to_ready_state(self.osd_worker_node)
                if not is_recovered:
                    log.warning(
                        f"The recovery of the osd worker node "
                        f"{self.osd_worker_node.name} failed. Adding a new OCS worker node..."
                    )
                    add_new_nodes_and_label_after_node_failure_ipi(self.machineset_name)

            log.info("Wait for node count to be equal to original count")
            wait_for_node_count_to_reach_status(node_count=initial_node_count)
            log.info("Node count matched")
            ceph_health_check()

            machine.wait_for_ready_replica_count_to_reach_expected_value(
                self.machineset_name,
                expected_value=self.start_ready_replica_count,
                timeout=420,
            )
            log.info(
                "Verify that the current replica count is equal to the ready replica count"
            )
            ready_replica_count = machine.get_ready_replica_count(self.machineset_name)
            res = machine.wait_for_current_replica_count_to_reach_expected_value(
                self.machineset_name, expected_value=ready_replica_count
            )
            if not res:
                machine.change_current_replica_count_to_ready_replica_count(
                    self.machineset_name
                )

            log.info("Check again that the Ceph Health is Health OK")
            ceph_health_check()

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["additional_node"],
        argvalues=[
            pytest.param(
                True,
                marks=pytest.mark.polarion_id("OCS-2191"),
            ),
            pytest.param(False, marks=pytest.mark.polarion_id("OCS-2190")),
        ],
    )
    @skipif_external_mode
    def test_automated_recovery_from_stopped_node_and_start(
        self, nodes, additional_node
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Reactive case - IPI

        0) A - add new node, B - don't add new node
        1) Stop node
        2) Validate result:
             A - pods should respin on the new node
             B - pods should remain in Pending state on the stopped node
        3) Start node
        4) Validate result:
             A - pods should start on the new node
             B - pods should start on the stopped node after starting it
        """
        wnode_name = get_worker_nodes()[0]
        machine_name = machine.get_machine_from_node_name(wnode_name)
        self.machineset_name = machine.get_machineset_from_machine_name(machine_name)
        self.start_ready_replica_count = machine.get_ready_replica_count(
            self.machineset_name
        )

        global initial_node_count
        initial_node_count = len(get_worker_nodes())
        log.info(f"Initial node count is {initial_node_count}")

        if additional_node:
            new_ocs_node_names = add_new_node_and_label_it(self.machineset_name)
            failure_domain = get_failure_domain()
            log.info("Wait for the nodes racks or zones to appear...")
            wait_for_nodes_racks_or_zones(failure_domain, new_ocs_node_names)

            new_ocs_node = get_node_objs(new_ocs_node_names)[0]
            log.info(f"Successfully created a new OCS node '{new_ocs_node.name}'")
            self.extra_node = True
            log.info("Get another OSD node in the same rack or zone...")
            self.osd_worker_node = get_another_osd_node_in_same_rack_or_zone(
                failure_domain, new_ocs_node
            )
            assert (
                self.osd_worker_node
            ), "Didn't find another osd node in the same rack or zone"
        else:
            osd_node_names = get_osd_running_nodes()
            self.osd_worker_node = get_node_objs(osd_node_names)[0]

        osd_pods = get_osd_pods()
        temp_osd = get_node_pods(self.osd_worker_node.name, pods_to_search=osd_pods)[0]
        osd_real_name = "-".join(temp_osd.name.split("-")[:-1])

        nodes.stop_nodes([self.osd_worker_node])
        log.info(f"Successfully powered off node: {self.osd_worker_node.name}")

        timeout = 420
        assert pod.wait_for_pods_to_be_in_statuses(
            [constants.STATUS_TERMINATING], [temp_osd.name], timeout=timeout
        ), (
            f"The pod {osd_real_name} didn't reach the status {constants.STATUS_TERMINATING} "
            f"after {timeout} seconds"
        )

        # Validate that the OSD in terminate state has a new OSD in Pending
        all_pod_obj = get_all_pods(namespace=config.ENV_DATA["cluster_namespace"])
        new_osd = None
        for pod_obj in all_pod_obj:
            if osd_real_name == "-".join(pod_obj.name.split("-")[:-1]) and (
                temp_osd.name != pod_obj.name
            ):
                new_osd = pod_obj
                break

        nodes.start_nodes(nodes=[self.osd_worker_node], wait=True)
        log.info(f"Successfully powered on node: {self.osd_worker_node.name}")
        wait_for_nodes_status(timeout=600)
        wait_for_resource_state(new_osd, constants.STATUS_RUNNING, timeout=360)
        if additional_node:
            new_osd_node = get_pod_node(new_osd)
            assert (
                new_osd_node.name != self.osd_worker_node.name
            ), "New OSD is expected to run on the new additional node"
