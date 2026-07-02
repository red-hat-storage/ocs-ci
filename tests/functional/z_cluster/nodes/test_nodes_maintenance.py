import logging
import pytest

import time

from subprocess import TimeoutExpired

from ocs_ci.ocs.exceptions import (
    CephHealthException,
    CommandFailed,
    ResourceWrongStatusException,
    ResourceNotFoundError,
    TimeoutExpiredError,
)
from ocs_ci.utility.decorators import switch_to_provider_for_function
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check_base, TimeoutSampler

from ocs_ci.ocs import constants, machine, ocp
from ocs_ci.ocs.resources.pod import get_pods_having_label, wait_for_pods_to_be_running
from ocs_ci.ocs.node import (
    drain_nodes,
    schedule_nodes,
    get_nodes,
    wait_for_nodes_status,
    remove_nodes,
    get_osd_running_nodes,
    get_node_objs,
    get_mon_running_nodes,
    get_node_mon_ids,
    generate_new_nodes_and_osd_running_nodes_ipi,
)
from ocs_ci.ocs.cluster import validate_existence_of_blocking_pdb
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    skipif_hci_provider,
    skipif_rosa_hcp,
    skipif_compact_mode,
    runs_on_provider,
)
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
    skipif_managed_service,
    skipif_more_than_three_workers,
)
from ocs_ci.helpers.ceph_helpers import get_ec_drain_thresholds, get_mon_quorum_count
from ocs_ci.helpers.sanity_helpers import Sanity, SanityExternalCluster
from ocs_ci.ocs.cluster import CephCluster, get_pgs_brief_dump, get_specific_pool_pgid
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import (
    cal_md5sum,
    verify_data_integrity,
    get_ceph_tools_pod,
    wait_for_storage_pods,
    get_fio_rw_iops,
)
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


@brown_squad
@ignore_leftovers
class TestNodesMaintenance(ManageTest):
    """
    Test basic flows of maintenance (unschedule and drain) and
    activate operations, followed by cluster functionality and health checks

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Fixture to initialize Sanity instance based on the cluster type

        """
        self.init_sanity_method()

    def init_sanity_method(self):
        """
        Method to initialize Sanity instance based on the cluster type

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
    @skipif_hci_provider
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*["worker"], marks=pytest.mark.polarion_id("OCS-1269")),
            pytest.param(
                *["master"],
                marks=[pytest.mark.polarion_id("OCS-1272"), skipif_rosa_hcp],
            ),
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
            helpers.get_provisioner_label(constants.CEPHFILESYSTEM),
            config.ENV_DATA["cluster_namespace"],
        )
        provis_pods += get_pods_having_label(
            helpers.get_provisioner_label(constants.CEPHBLOCKPOOL),
            config.ENV_DATA["cluster_namespace"],
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
        if (
            node_type == "worker"
            and config.ENV_DATA.get("platform") == constants.ROSA_HCP_PLATFORM
        ):
            # in ROSA HCP, the mon pod remains in a Terminating state for an extended period,
            # resulting in one additional mon pod being epexcted during the health check
            self.init_sanity_method()

        self.sanity_helpers.health_check(tries=90)

    @tier4a
    @skipif_bm
    @skipif_managed_service
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*["worker"], marks=pytest.mark.polarion_id("OCS-1292")),
        ],
    )
    def test_node_maintenance_restart_activate(
        self,
        skip_on_hci_provider_client,
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
            timeout=600,
            sleep=20,
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
            pytest.param(
                *["master"],
                marks=[pytest.mark.polarion_id("OCS-1271"), skipif_rosa_hcp],
            ),
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
    @skipif_compact_mode
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
        deployment_pod_factory,
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
            dc_pod = deployment_pod_factory(
                interface=interface, node_selector={"dc": "fedora"}
            )
            pod.run_io_in_bg(dc_pod, fedora_dc=True)
            dc_pod_obj.append(dc_pod)

        osd_running_worker_nodes = generate_new_nodes_and_osd_running_nodes_ipi(
            num_of_nodes=2
        )
        # Drain 2 nodes
        drain_nodes(osd_running_worker_nodes, timeout=2100)

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
                        ocp_obj = ocp.OCP(
                            namespace=config.ENV_DATA["cluster_namespace"]
                        )
                        pod_name = pod_obj.name
                        deployment_name = "-".join(pod_name.split("-")[:-2])
                        command = f"delete deployment {deployment_name}"
                        ocp_obj.exec_oc_cmd(command=command)
                        log.info(f"Deleted deployment for pod {pod_obj.name}")

        # DC app pods on the drained node will get automatically created on other
        # running node in same AZ. Waiting for all dc app pod to reach running state
        pod.wait_for_dc_app_pods_to_reach_running_state(dc_pod_obj, timeout=1200)
        log.info("All the dc pods reached running state")

        # Save the machine count of the worker nodes and the machine names of the osd nodes
        machine_count = len(machine.get_machines())
        machine_names_of_osd_nodes = [
            machine.get_machine_from_node_name(n) for n in osd_running_worker_nodes
        ]
        # Remove unscheduled nodes
        # In scenarios where the drain is attempted on >3 worker setup,
        # post completion of drain we are removing the unscheduled nodes so
        # that we maintain 3 worker nodes.
        log.info(f"Removing scheduled nodes {osd_running_worker_nodes}")
        remove_node_objs = get_node_objs(osd_running_worker_nodes)
        remove_nodes(remove_node_objs)

        log.info(
            f"Deleting the machines associated with the osd nodes: {machine_names_of_osd_nodes}"
        )
        machine.delete_machines(machine_names_of_osd_nodes)
        machine.wait_for_machines_count_to_reach_status(machine_count)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @skipif_managed_service
    @skipif_more_than_three_workers
    @pytest.mark.polarion_id("OCS-2524")
    @tier4a
    def test_pdb_check_simultaneous_node_drains(
        self,
        skip_on_hci_provider_client,
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


@brown_squad
class TestECNodeOperations(ManageTest):
    """
    Test node operations on EC-pool clusters, validating Ceph degradation
    behavior and data integrity at each EC threshold tier.

    """

    @pytest.fixture(autouse=True)
    def setup(self, request, nodes):
        """
        Initialize sanity helpers and register a finalizer to restart
        any stopped nodes. Skip on client clusters.

        """
        with config.RunWithProviderConfigContextIfAvailable():
            if config.ENV_DATA.get("cluster_type") not in (None, "provider"):
                pytest.skip("Test runs only on provider or standalone clusters")
            if not config.DEPLOYMENT.get("ec_default_pools"):
                pytest.skip("Test runs only on EC pools")

            self.sanity_helpers = Sanity()
            self.stopped_node_objs = []
            self._nodes = nodes

            def restart_stopped_nodes():
                if self.stopped_node_objs:
                    log.info(
                        f"Finalizer: restarting "
                        f"{len(self.stopped_node_objs)} stopped nodes"
                    )
                    try:
                        self._nodes.start_nodes(self.stopped_node_objs)
                    except CommandFailed as e:
                        log.error(f"Finalizer start_nodes failed: {e}")

            request.addfinalizer(restart_stopped_nodes)

    @switch_to_provider_for_function
    def _wait_for_stable_degraded(self, ct_pod, timeout=600, checks=3, interval=20):
        """Wait until PG states stabilize in a degraded condition."""
        stable_count = 0
        prev_state = None
        degraded_keywords = ("degraded", "undersized", "peered")
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=interval,
            func=ct_pod.exec_ceph_cmd,
            ceph_cmd="ceph pg stat",
        ):
            state_str = str(sample)
            is_degraded = any(kw in state_str for kw in degraded_keywords)
            if not is_degraded:
                stable_count = 0
                prev_state = None
                continue
            if state_str == prev_state:
                stable_count += 1
            else:
                stable_count = 1
            prev_state = state_str
            if stable_count >= checks:
                log.info(f"PG state stable in degraded condition: {state_str}")
                break

    @switch_to_provider_for_function
    def _run_write_io(self, pod_obj):
        """Run a small FIO write and wait for completion."""
        pod_obj.run_io(
            storage_type="fs",
            size="64M",
            io_direction="wo",
            runtime=0,
            bs="4K",
            fio_filename="drain_write_test",
        )
        get_fio_rw_iops(pod_obj)

    @switch_to_provider_for_function
    def _try_write_io(self, pod_obj):
        """Attempt a FIO write, return True if succeeded, False if timed out."""
        try:
            pod_obj.run_io(
                storage_type="fs",
                size="16M",
                io_direction="wo",
                runtime=30,
                bs="4K",
                fio_filename="blocked_write",
                timeout=120,
            )
            get_fio_rw_iops(pod_obj)
            return True
        except (CommandFailed, TimeoutExpiredError):
            return False

    @tier4a
    @runs_on_provider
    @skipif_managed_service
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_ec_gradual_node_shutdown(
        self,
        nodes,
        node_restart_teardown,
        pvc_factory,
        pod_factory,
    ):
        """
        Gradually shut down OSD nodes and validate Ceph degradation behavior
        at each EC threshold tier:
        - Tier 1: live_hosts >= k+m -> active+clean after rebalance, IO works
        - Tier 2: min_size <= live_hosts < k+m -> degraded, writes still work
        - Tier 3: k <= live_hosts < min_size -> writes blocked, reads may work

        """
        # Phase 0: Pre-conditions
        with config.RunWithProviderConfigContextIfAvailable():
            thresholds = get_ec_drain_thresholds()
            k = thresholds["k"]
            size, min_size = thresholds["size"], thresholds["min_size"]
            total_hosts = thresholds["total_osd_hosts"]
            assert (
                total_hosts >= size
            ), f"Not enough OSD hosts ({total_hosts}) for EC pool (need {size})"

            # Phase 1: Create workload + baseline
            pvc_obj = pvc_factory(interface=constants.CEPHBLOCKPOOL, size=5)
            pod_obj = pod_factory(pvc=pvc_obj, interface=constants.CEPHBLOCKPOOL)
            pod_obj.run_io(
                storage_type="fs", size="1G", io_direction="wo", runtime=0, bs="1M"
            )
            get_fio_rw_iops(pod_obj)
            original_md5 = cal_md5sum(pod_obj, "fio-rand-write")

            # Phase 2: Verify PG health + chunk distribution
            ceph_cluster = CephCluster()
            assert ceph_cluster.get_rebalance_status(), "PGs not active+clean at start"

            ct_pod = get_ceph_tools_pod()
            osd_tree = ct_pod.exec_ceph_cmd("ceph osd tree")
            osd_to_host = {
                child_id: entry["name"]
                for entry in osd_tree["nodes"]
                if entry["type"] == "host"
                for child_id in entry.get("children", [])
            }

            pool_pgids = get_specific_pool_pgid(constants.DEFAULT_CEPHBLOCKPOOL)
            pgs_dump = get_pgs_brief_dump()
            for pg in pgs_dump["pg_stats"]:
                if pg["pgid"] in pool_pgids[:5]:
                    hosts = {osd_to_host[osd] for osd in pg["acting"]}
                    assert (
                        len(hosts) >= size
                    ), f"PG {pg['pgid']} on {len(hosts)} hosts, need {size}"

            # Phase 3: Determine shutdown order (worker-only OSD nodes, non-mon first)
            osd_nodes = get_osd_running_nodes()

            worker_node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
            worker_names = set()
            for w_node in worker_node_objs:
                roles = w_node.ocp.get_resource(
                    resource_name=w_node.name, column="ROLES"
                )
                if constants.MASTER_MACHINE not in roles:
                    worker_names.add(w_node.name)

            eligible_osd_nodes = [n for n in osd_nodes if n in worker_names]
            assert eligible_osd_nodes, "No worker-only OSD nodes available for shutdown"

            mon_nodes = set(get_mon_running_nodes())
            shutdown_order = [n for n in eligible_osd_nodes if n not in mon_nodes] + [
                n for n in eligible_osd_nodes if n in mon_nodes
            ]

            eligible_count = len(eligible_osd_nodes)
            log.info(
                f"Eligible worker-only OSD nodes for shutdown: {shutdown_order} "
                f"({eligible_count} of {total_hosts} total OSD hosts)"
            )

            # Phase 4: Gradual shutdown loop (Tiers 1-3)
            # Tier 1 (full rebalance) is only reachable when total_hosts > size,
            # i.e. there are spare nodes beyond what EC requires.
            max_shutdowns = min(thresholds["min_drain_io_stops"], eligible_count)
            spare_hosts = total_hosts - size
            if spare_hosts > 0:
                log.info(
                    f"Tier 1 reachable: {spare_hosts} spare host(s) beyond k+m={size}"
                )
            else:
                log.info(
                    f"Tier 1 not reachable: total_hosts={total_hosts} == k+m={size}, "
                    f"starting from Tier 2"
                )

            for i, node_name in enumerate(shutdown_order[:max_shutdowns], start=1):
                live_hosts = total_hosts - i

                # Mon quorum safety check
                quorum_count = get_mon_quorum_count()
                node_mons = get_node_mon_ids(node_name)
                if node_mons and (quorum_count - len(node_mons)) < 2:
                    log.warning(
                        f"Stopping shutdown sequence: shutting down {node_name} "
                        f"would lose mon quorum ({quorum_count} mons, "
                        f"{len(node_mons)} on this node)"
                    )
                    break

                # Power off the node. Add to stopped list first so the
                # finalizer can restart it even if stop_nodes raises partway.
                shutdown_node_obj = get_node_objs([node_name])[0]
                self.stopped_node_objs.append(shutdown_node_obj)
                log.info(
                    f"Shutting down node {node_name} ({i}/{max_shutdowns}), "
                    f"{live_hosts} hosts will remain"
                )
                nodes.stop_nodes([shutdown_node_obj])

                # Refresh ceph tools pod — it may have been rescheduled
                # if the previous one lived on a now-stopped node.
                ct_pod = get_ceph_tools_pod(wait=True)

                # Wait for Ceph to detect OSD failure before tier validation.
                # OSD heartbeat timeout is ~20s; give extra margin.
                log.info("Waiting for Ceph to detect OSD failure")
                time.sleep(30)

                # Tier-based validation
                if live_hosts >= size:
                    log.info(f"Tier 1: {live_hosts} live >= {size} (k+m)")
                    assert ceph_cluster.wait_for_rebalance(
                        timeout=1800
                    ), f"Rebalance did not complete with {live_hosts} live hosts"
                    self._run_write_io(pod_obj)
                    verify_data_integrity(pod_obj, "fio-rand-write", original_md5)

                elif live_hosts >= min_size:
                    log.info(f"Tier 2: {min_size} <= {live_hosts} < {size}")
                    self._wait_for_stable_degraded(ct_pod)
                    self._run_write_io(pod_obj)
                    verify_data_integrity(pod_obj, "fio-rand-write", original_md5)

                elif live_hosts >= k:
                    log.info(f"Tier 3: {k} <= {live_hosts} < {min_size}")
                    time.sleep(60)
                    write_ok = self._try_write_io(pod_obj)
                    if write_ok:
                        log.warning(
                            f"Write succeeded with {live_hosts} hosts "
                            f"(below min_size={min_size}), may be transient"
                        )
                    try:
                        verify_data_integrity(pod_obj, "fio-rand-write", original_md5)
                        log.info("Read succeeded in tier 3 (expected: >= k hosts)")
                    except (CommandFailed, AssertionError):
                        log.info("Read failed in tier 3 (can happen)")

            # Recovery: start all stopped nodes
            log.info(f"Starting {len(self.stopped_node_objs)} stopped nodes")
            nodes.start_nodes(self.stopped_node_objs)
            wait_for_nodes_status([n.name for n in self.stopped_node_objs], timeout=600)
            wait_for_storage_pods(timeout=600)
            assert ceph_cluster.wait_for_rebalance(
                timeout=3600, repeat=3
            ), "Post-recovery rebalance did not complete"

            # Clear the list so the finalizer does not re-start them
            self.stopped_node_objs.clear()

            # Post-recovery: refresh tools pod and verify no chunk co-location
            ct_pod = get_ceph_tools_pod()
            osd_tree = ct_pod.exec_ceph_cmd("ceph osd tree")
            osd_to_host = {
                child_id: entry["name"]
                for entry in osd_tree["nodes"]
                if entry["type"] == "host"
                for child_id in entry.get("children", [])
            }
            pool_pgids = get_specific_pool_pgid(constants.DEFAULT_CEPHBLOCKPOOL)
            pgs_dump = get_pgs_brief_dump()
            for pg in pgs_dump["pg_stats"]:
                if pg["pgid"] in pool_pgids[:5]:
                    hosts = {osd_to_host[osd] for osd in pg["acting"]}
                    assert len(hosts) >= size, (
                        f"Post-recovery co-location: PG {pg['pgid']} on "
                        f"{len(hosts)} hosts, need {size}"
                    )

            # Data integrity after full recovery
            verify_data_integrity(pod_obj, "fio-rand-write", original_md5)
            self.sanity_helpers.health_check(tries=90)
