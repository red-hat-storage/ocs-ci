import logging
import pytest

import time

from subprocess import TimeoutExpired

from ocs_ci.ocs.exceptions import (
    CephHealthException,
    ResourceWrongStatusException,
    ResourceNotFoundError,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check_base, TimeoutSampler

from ocs_ci.ocs import constants, machine, ocp, defaults
from ocs_ci.ocs.resources.pod import get_pods_having_label, wait_for_pods_to_be_running
from ocs_ci.ocs.node import (
    drain_nodes,
    schedule_nodes,
    get_nodes,
    wait_for_nodes_status,
    remove_nodes,
    get_osd_running_nodes,
    get_node_objs,
    add_new_node_and_label_it,
)
from ocs_ci.ocs.cluster import validate_existence_of_blocking_pdb
from ocs_ci.framework.testlib import (
    tier1,
    tier2,
    tier3,
    tier4a,
    ManageTest,
    aws_based_platform_required,
    ignore_leftovers,
    ipi_deployment_required,
    skipif_bm,
    bugzilla,
    skipif_managed_service,
    skipif_more_than_three_workers,
)
from ocs_ci.helpers.sanity_helpers import Sanity, SanityExternalCluster
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import (
    label_worker_node,
    remove_label_from_worker_node,
    storagecluster_independent_check,
    verify_pdb_mon,
)
from ocs_ci.helpers import helpers


log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def teardown(request):
    """
    Tear down function

    """

    def finalizer():
        """
        Make sure that all cluster's nodes are in 'Ready' state and if not,
        change them back to 'Ready' state by marking them as schedulable
        """
        scheduling_disabled_nodes = [
            n.name
            for n in get_node_objs()
            if n.ocp.get_resource_status(n.name)
            == constants.NODE_READY_SCHEDULING_DISABLED
        ]
        if scheduling_disabled_nodes:
            schedule_nodes(scheduling_disabled_nodes)

        # Remove label created for DC app pods on all worker nodes
        node_objs = get_node_objs()
        for node_obj in node_objs:
            if "dc" in node_obj.get().get("metadata").get("labels").keys():
                remove_label_from_worker_node([node_obj.name], label_key="dc")

    request.addfinalizer(finalizer)


@ignore_leftovers
class TestNodesMaintenance(ManageTest):
    """
    Test basic flows of maintenance (unschedule and drain) and
    activate operations, followed by cluster functionality and health checks

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        if storagecluster_independent_check():
            self.sanity_helpers = SanityExternalCluster()
        else:
            self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def health_checker(self):
        """
        Check Ceph health

        """
        try:
            status = ceph_health_check_base()
            if status:
                log.info("Health check passed")
        except CephHealthException as e:
            # skip because ceph is not in good health
            pytest.skip(str(e))

    @tier1
    @skipif_managed_service
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*["worker"], marks=pytest.mark.polarion_id("OCS-1269")),
            pytest.param(*["master"], marks=pytest.mark.polarion_id("OCS-1272")),
        ],
    )
    def test_node_maintenance(
        self,
        reduce_and_resume_cluster_load,
        node_type,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        OCS-1269/OCS-1272:
        - Maintenance (mark as unscheduable and drain) 1 worker/master node
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Mark the node as scheduable
        - Check cluster and Ceph health

        """
        # Get 1 node of the type needed for the test iteration
        typed_nodes = get_nodes(node_type=node_type, num_of_nodes=1)
        assert typed_nodes, f"Failed to find a {node_type} node for the test"
        typed_node_name = typed_nodes[0].name

        # check csi-cephfsplugin-provisioner's and csi-rbdplugin-provisioner's
        # are ready, see BZ #2162504
        provis_pods = get_pods_having_label(
            constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
            defaults.ROOK_CLUSTER_NAMESPACE,
        )
        provis_pods += get_pods_having_label(
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
            defaults.ROOK_CLUSTER_NAMESPACE,
        )
        provis_pod_names = [p["metadata"]["name"] for p in provis_pods]

        # Maintenance the node (unschedule and drain)
        drain_nodes([typed_node_name])

        # avoid scenario when provisioners yet not been created (6 sec for creation)
        retry(ResourceNotFoundError, tries=2, delay=2, backoff=2)(
            wait_for_pods_to_be_running
        )(pod_names=provis_pod_names, raise_pod_not_found_error=True)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

        # Mark the node back to schedulable
        schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=90)

    @tier4a
    @skipif_bm
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*["worker"], marks=pytest.mark.polarion_id("OCS-1292")),
        ],
    )
    def test_node_maintenance_restart_activate(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        node_type,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        OCS-1292:
        - Maintenance (mark as unscheduable and drain) 1 worker node
        - Restart the node
        - Mark the node as schedulable
        - Check cluster and Ceph health
        - Check cluster functionality by creating and deleting resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)

        """
        # Get 1 node of the type needed for the test iteration
        typed_nodes = get_nodes(node_type=node_type, num_of_nodes=1)
        assert typed_nodes, f"Failed to find a {node_type} node for the test"
        typed_node_name = typed_nodes[0].name

        reboot_events_cmd = (
            f"get events -A --field-selector involvedObject.name="
            f"{typed_node_name},reason=Rebooted -o yaml"
        )

        # Find the number of reboot events in 'typed_node_name'
        num_events = len(typed_nodes[0].ocp.exec_oc_cmd(reboot_events_cmd)["items"])

        # Maintenance the node (unschedule and drain). The function contains logging
        drain_nodes([typed_node_name])

        # Restarting the node
        nodes.restart_nodes(nodes=typed_nodes, wait=False)

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

        wait_for_nodes_status(
            node_names=[typed_node_name],
            status=constants.NODE_READY_SCHEDULING_DISABLED,
        )

        # Mark the node back to schedulable
        schedule_nodes([typed_node_name])

        # Check cluster and Ceph health and checking basic cluster
        # functionality by creating resources (pools, storageclasses,
        # PVCs, pods - both CephFS and RBD), run IO and delete the resources
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

    @tier3
    @pytest.mark.parametrize(
        argnames=["nodes_type"],
        argvalues=[
            pytest.param(*["worker"], marks=pytest.mark.polarion_id("OCS-1273")),
            pytest.param(*["master"], marks=pytest.mark.polarion_id("OCS-1271")),
        ],
    )
    def test_2_nodes_maintenance_same_type(self, nodes_type):
        """
        OCS-1273/OCs-1271:
        - Try draining 2 nodes from the same type - should fail
        - Check cluster and Ceph health

        """
        # Get 2 nodes
        typed_nodes = get_nodes(node_type=nodes_type, num_of_nodes=2)
        assert typed_nodes, f"Failed to find a {nodes_type} node for the test"

        typed_node_names = [typed_node.name for typed_node in typed_nodes]

        # Try draining 2 nodes - should fail
        try:
            drain_nodes(typed_node_names)
        except TimeoutExpired:
            log.info(f"Draining of nodes {typed_node_names} failed as expected")

        schedule_nodes(typed_node_names)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @tier2
    @pytest.mark.polarion_id("OCS-1274")
    def test_2_nodes_different_types(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        OCS-1274:
        - Maintenance (mark as unscheduable and drain) 1 worker node and 1
          master node
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Mark the nodes as scheduable
        - Check cluster and Ceph health

        """
        # Get 1 node from each type
        nodes = [
            get_nodes(node_type=node_type, num_of_nodes=1)[0]
            for node_type in ["worker", "master"]
        ]
        assert nodes, "Failed to find a nodes for the test"

        node_names = [typed_node.name for typed_node in nodes]

        # Maintenance the nodes (unschedule and drain)
        drain_nodes(node_names)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

        # Mark the nodes back to schedulable
        schedule_nodes(node_names)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @tier4a
    @aws_based_platform_required
    @ipi_deployment_required
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(*["rbd"], marks=pytest.mark.polarion_id("OCS-2128")),
            pytest.param(*["cephfs"], marks=pytest.mark.polarion_id("OCS-2129")),
        ],
    )
    def test_simultaneous_drain_of_two_ocs_nodes(
        self,
        pvc_factory,
        pod_factory,
        dc_pod_factory,
        interface,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        OCS-2128/OCS-2129:
        - Create PVCs and start IO on DC based app pods
        - Add one extra node in two of the AZs and label the nodes
          with OCS storage label
        - Maintenance (mark as unscheduable and drain) 2 worker nodes
          simultaneously
        - Confirm that OCS and DC pods are in running state
        - Remove unscheduled nodes
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Check cluster and Ceph health

        """
        # Get OSD running nodes
        osd_running_worker_nodes = get_osd_running_nodes()
        log.info(f"OSDs are running on nodes {osd_running_worker_nodes}")

        # Label osd nodes with fedora app
        label_worker_node(
            osd_running_worker_nodes, label_key="dc", label_value="fedora"
        )
        log.info("Successfully labeled worker nodes with {dc:fedora}")

        # Create DC app pods
        log.info("Creating DC based app pods and starting IO in background")
        interface = (
            constants.CEPHBLOCKPOOL if interface == "rbd" else constants.CEPHFILESYSTEM
        )
        dc_pod_obj = []
        for i in range(2):
            dc_pod = dc_pod_factory(interface=interface, node_selector={"dc": "fedora"})
            pod.run_io_in_bg(dc_pod, fedora_dc=True)
            dc_pod_obj.append(dc_pod)

        # Get the machine name using the node name
        machine_names = [
            machine.get_machine_from_node_name(osd_running_worker_node)
            for osd_running_worker_node in osd_running_worker_nodes[:2]
        ]
        log.info(
            f"{osd_running_worker_nodes} associated " f"machine are {machine_names}"
        )

        # Get the machineset name using machine name
        machineset_names = [
            machine.get_machineset_from_machine_name(machine_name)
            for machine_name in machine_names
        ]
        log.info(
            f"{osd_running_worker_nodes} associated machineset "
            f"is {machineset_names}"
        )

        # Add a new node and label it
        add_new_node_and_label_it(machineset_names[0])
        add_new_node_and_label_it(machineset_names[1])

        # Drain 2 nodes
        drain_nodes(osd_running_worker_nodes[:2])

        # Check the pods should be in running state
        all_pod_obj = pod.get_all_pods(wait=True)
        for pod_obj in all_pod_obj:
            if ("-1-deploy" or "ocs-deviceset") not in pod_obj.name:
                try:
                    helpers.wait_for_resource_state(
                        resource=pod_obj, state=constants.STATUS_RUNNING, timeout=200
                    )
                except ResourceWrongStatusException:
                    # 'rook-ceph-crashcollector' on the failed node stucks at
                    # pending state. BZ 1810014 tracks it.
                    # Ignoring 'rook-ceph-crashcollector' pod health check as
                    # WA and deleting its deployment so that the pod
                    # disappears. Will revert this WA once the BZ is fixed
                    if "rook-ceph-crashcollector" in pod_obj.name:
                        ocp_obj = ocp.OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE)
                        pod_name = pod_obj.name
                        deployment_name = "-".join(pod_name.split("-")[:-2])
                        command = f"delete deployment {deployment_name}"
                        ocp_obj.exec_oc_cmd(command=command)
                        log.info(f"Deleted deployment for pod {pod_obj.name}")

        # DC app pods on the drained node will get automatically created on other
        # running node in same AZ. Waiting for all dc app pod to reach running state
        pod.wait_for_dc_app_pods_to_reach_running_state(dc_pod_obj, timeout=1200)
        log.info("All the dc pods reached running state")

        # Remove unscheduled nodes
        # In scenarios where the drain is attempted on >3 worker setup,
        # post completion of drain we are removing the unscheduled nodes so
        # that we maintain 3 worker nodes.
        log.info(f"Removing scheduled nodes {osd_running_worker_nodes[:2]}")
        remove_node_objs = get_node_objs(osd_running_worker_nodes[:2])
        remove_nodes(remove_node_objs)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @bugzilla("1861104")
    @bugzilla("1946573")
    @skipif_managed_service
    @skipif_more_than_three_workers
    @pytest.mark.polarion_id("OCS-2524")
    @tier4a
    def test_pdb_check_simultaneous_node_drains(
        self,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
        node_drain_teardown,
    ):
        """
        - Check for OSD PDBs before drain
        - Maintenance (mark as unschedulable and drain) 2 worker node with delay of 30 secs
        - Drain will be completed on worker node A
        - Drain will be pending on worker node B due to blocking PDBs
        - Check mon failover in first 10 mins, then 15 and 20 mins
        - Check the OSD PDBs
        - Mark the node A as schedulable
        - Let drain finish on Node B
        - Again check mon failover in first 10 mins and then in intervals
        - Mark the node B as schedulable
        - Check cluster and Ceph health

        """

        # Validate OSD PDBs before drain operation
        assert (
            not validate_existence_of_blocking_pdb()
        ), "Blocking PDBs exist, Can't perform drain"
        # Get 2 worker nodes to drain
        typed_nodes = get_nodes(num_of_nodes=2)
        assert len(typed_nodes) == 2, "Failed to find worker nodes for the test"
        node_A = typed_nodes[0].name
        node_B = typed_nodes[1].name

        # Drain Node A and validate blocking PDBs
        drain_nodes([node_A])
        pdb_sample = TimeoutSampler(
            timeout=100,
            sleep=10,
            func=validate_existence_of_blocking_pdb,
        )
        if not pdb_sample:
            log.error("Failed to create PDBs post node A drain")
        else:
            log.info("PDBs are created post node A drain")
        # Inducing delay between 2 drains
        # Node-B drain expected to be in pending due to blocking PDBs
        time.sleep(30)
        try:
            drain_nodes([node_B])
            # After the drain check Mon failover in 10th, 15th and 20th min
            timeout = [600, 300, 300]
            for failover in timeout:
                sample = TimeoutSampler(
                    timeout=failover,
                    sleep=10,
                    func=helpers.check_number_of_mon_pods,
                )
                if not sample.wait_for_func_status(result=True):
                    assert "Number of mon pods not equal to expected_mon_count=3"
        except TimeoutExpired:
            # Mark the node-A back to schedulable and let drain finish in Node-B
            schedule_nodes([node_A])

        time.sleep(40)

        # Validate OSD PDBs
        assert (
            validate_existence_of_blocking_pdb()
        ), "Blocking PDBs not created post second drain"

        # Mark the node-B back to schedulable and recover the cluster
        schedule_nodes([node_B])

        sample = TimeoutSampler(
            timeout=100,
            sleep=10,
            func=validate_existence_of_blocking_pdb,
        )
        if not sample.wait_for_func_status(result=False):
            log.error("Blocking PDBs still exist")

        # After the drain check mon failover in 10th, 15th and 20th Min
        timeout = [600, 300, 300]
        for failover in timeout:
            sample = TimeoutSampler(
                timeout=failover,
                sleep=10,
                func=helpers.check_number_of_mon_pods,
            )
            if not sample.wait_for_func_status(result=True):
                assert "Number of Mon pods not equal to expected_mon_count=3"

        sample = TimeoutSampler(
            timeout=100,
            sleep=10,
            func=verify_pdb_mon,
            disruptions_allowed=1,
            max_unavailable_mon=1,
        )
        if not sample.wait_for_func_status(result=True):
            assert "The expected mon-pdb is not equal to actual mon pdb"

        # wait for storage pods
        pod.wait_for_storage_pods()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=50)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()
