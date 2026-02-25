import json
import random
import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    rosa_hcp_required,
    hcp_required,
    tier4a,
    polarion_id,
    brown_squad,
    ignore_leftovers,
    runs_on_provider,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.machinepool import NodeConf, MachinePools
from ocs_ci.ocs.node import (
    unschedule_nodes,
    schedule_nodes,
    get_node_pods,
    get_worker_nodes,
    get_node_objs,
    get_osd_running_nodes,
    wait_for_nodes_status,
)
from ocs_ci.ocs import node
from ocs_ci.ocs import ocp
from ocs_ci.ocs.platform_nodes import (
    HypershiftAWSNode,
    PlatformNodesFactory,
)
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.cluster import check_ceph_health_after_add_capacity
from ocs_ci.helpers.disruption_helpers import FIOIntegrityChecker
from botocore.exceptions import ClientError
from ocs_ci.ocs.exceptions import CommandFailed, UnavailableResourceException
from ocs_ci.utility.utils import get_random_str, ceph_health_check, TimeoutSampler

log = logging.getLogger(__name__)

MACHINESET_KIND = "machinesets.machine.openshift.io"


def get_provider_machinesets():
    """
    Get machinesets from the provider cluster using the
    machine.openshift.io API group. On OCP 4.21+ with CAPI,
    the unqualified 'machinesets' kind resolves to
    cluster.x-k8s.io which may return empty results.

    Returns:
        list[tuple]: List of (machineset_name, replica_count) tuples.
    """
    ms_ocp = ocp.OCP(
        kind=MACHINESET_KIND,
        namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
    )
    result = []
    for item in ms_ocp.get().get("items", []):
        name = item["metadata"]["name"]
        replicas = item.get("spec", {}).get("replicas", 0)
        result.append((name, replicas))
    log.info(f"Provider machinesets: {result}")
    return result


def scale_provider_machineset(ms_name, replicas):
    """
    Scale a provider machineset to the desired replica count.

    Args:
        ms_name (str): Machineset name.
        replicas (int): Target replica count.
    """
    ms_ocp = ocp.OCP(
        kind=MACHINESET_KIND,
        namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
    )
    ms_ocp.exec_oc_cmd(
        f"scale {MACHINESET_KIND}/{ms_name} "
        f"--replicas={replicas} "
        f"-n {constants.OPENSHIFT_MACHINE_API_NAMESPACE}"
    )
    log.info(f"Scaled machineset '{ms_name}' to {replicas} replicas")


def select_osd_node_name():
    """
    select randomly one of the osd nodes

    Returns:
        str: the selected osd node name

    """
    osd_node_names = node.get_osd_running_nodes()
    osd_node_name = random.choice(osd_node_names)
    log.info(f"Selected OSD is {osd_node_name}")
    return osd_node_name


def get_osd_pod_name(osd_node_name):
    """
    get the osd pod name from the osd node name

    Args:
        osd_node_name (str): the osd node name

    Returns:
        Pod: the osd pod object

    """
    osd_pods = get_osd_pods()
    osd_pod_name = get_node_pods(osd_node_name, osd_pods)[0]
    log.info(f"OSD pod name is {osd_pod_name}")
    return osd_pod_name


class TestAddDifferentInstanceTypeNode(ManageTest):
    @pytest.fixture
    def setup(self, request):
        """
        Method to set test variables
        """
        self.osd_node_name = select_osd_node_name()
        self.osd_pod = get_osd_pod_name(self.osd_node_name)
        self.machine_pool_new = f"workers-{get_random_str(3)}"
        log.info(f"New machine pool name is {self.machine_pool_new}")
        log.info(f"OSD node name is {self.osd_node_name}")

        def finalizer():
            """
            Teardown function to schedule initial node back
            """
            schedule_nodes([self.osd_node_name])

        request.addfinalizer(finalizer)

    @tier4a
    @brown_squad
    @rosa_hcp_required
    @polarion_id("OCS-6270")
    def test_add_ocs_node_non_default_machinepool(self, setup, add_nodes):
        """
        Test to add 1 ocs node with a different instance type via ROSA machinepool
        and wait till rebalance is completed.

        Runs only on ROSA HCP clusters where MachinePools are managed via rosa CLI.

        Steps:
        1. Create a new machinepool with a different instance type and label it
        2. Select any node with OSD and cordon it
        3. Delete OSD pod on unscheduled node
        4. Verify all OSD pods are running
        5. Verify data rebalancing completes
        """

        instance_types = ["m5.xlarge", "m5.4xlarge", "m5.8xlarge", "m5.12xlarge"]
        cluster_name = config.ENV_DATA["cluster_name"]
        namespace = config.ENV_DATA["cluster_namespace"]
        ceph_health_tries = 40
        machine_pools = MachinePools(cluster_name=cluster_name)
        machine_pool = machine_pools.filter(
            machinepool_id=config.ENV_DATA["machine_pool"], pick_first=True
        )
        alt_inst_type = random.choice(
            (
                [
                    i_type
                    for i_type in instance_types
                    if i_type != machine_pool.instance_type
                ]
            )
        )

        node_conf = NodeConf(
            **{"machinepool_id": self.machine_pool_new, "instance_type": alt_inst_type}
        )
        add_nodes(ocs_nodes=True, node_count=1, node_conf=node_conf)

        unschedule_nodes([self.osd_node_name])
        self.osd_pod.delete(wait=True)

        ceph_health_check(namespace=namespace, tries=ceph_health_tries, delay=60)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"


@ignore_leftovers
class TestAddRemoveHubNodeWithClientIO(ManageTest):
    """
    Test adding and removing OCS nodes on the hub (provider) cluster
    while verifying that client (hosted) cluster IO continues
    uninterrupted and data integrity is maintained.

    The hub cluster hosts OSD pods and ceph. The client cluster only runs
    CSI node plugins and consumes storage via PVCs.
    """

    @pytest.fixture
    def restore_hub_nodes(self, request):
        """Record initial hub worker state and restore on teardown."""
        self._hub_initial_workers = None
        self._hub_node_added = False

        def finalizer():
            if not self._hub_node_added or not self._hub_initial_workers:
                return
            log.info("Teardown: scaling machinesets back")
            try:
                machinesets = get_provider_machinesets()
                for ms_name, replicas in machinesets:
                    if replicas > 1:
                        scale_provider_machineset(ms_name, replicas - 1)
                        break
            except CommandFailed as e:
                log.warning(f"Failed to restore hub nodes: {e}")

        request.addfinalizer(finalizer)

    @tier4a
    @brown_squad
    @hcp_required
    @runs_on_provider
    @polarion_id("OCS-7715")
    def deprecated_test_add_remove_hub_node_verify_client_io(
        self, restore_hub_nodes, pvc_factory, pod_factory
    ):
        """
        Add a worker node to the hub (provider) cluster by scaling
        its machineset, verify client IO integrity, then remove the
        node and verify again.

        Steps:
        1. Record initial hub workers and machineset state
        2. Start FIO integrity checker on client cluster
        3. Scale up machineset to add 1 hub worker, label for OCS
        4. Verify client IO integrity after addition
        5. Start new FIO integrity checker on client cluster
        6. Scale down machineset to remove the added node
        7. Verify client IO integrity after removal
        """
        initial_hub_workers = get_worker_nodes()
        self._hub_initial_workers = initial_hub_workers
        log.info(
            f"Initial hub workers ({len(initial_hub_workers)}): "
            f"{initial_hub_workers}"
        )

        machinesets = get_provider_machinesets()
        assert machinesets, "No machinesets found on provider cluster"
        ms_name, ms_replicas = machinesets[0]

        # ---- Phase 1: Add hub node ----
        log.info("======== Phase 1: Hub Node Addition ========")
        with config.RunWithFirstConsumerConfigContextIfAvailable():
            add_checker = FIOIntegrityChecker(pvc_factory, pod_factory)
            add_checker.start_io(bg_runtime=500)

        log.info(
            f"-------- Scaling machineset '{ms_name}' from "
            f"{ms_replicas} to {ms_replicas + 1} --------"
        )
        scale_provider_machineset(ms_name, ms_replicas + 1)
        self._hub_node_added = True

        log.info("Waiting for new hub worker node to appear")
        new_hub_nodes = []
        for sample in TimeoutSampler(
            timeout=600,
            sleep=30,
            func=get_worker_nodes,
        ):
            new_hub_nodes = list(set(sample) - set(initial_hub_workers))
            if new_hub_nodes:
                log.info(f"New hub node(s): {new_hub_nodes}")
                break
            log.info(f"No new hub nodes yet. Workers " f"({len(sample)}): {sample}")

        assert new_hub_nodes, (
            "No new hub worker node appeared after scaling " "machineset"
        )
        new_node = new_hub_nodes[0]
        wait_for_nodes_status(
            node_names=[new_node],
            status=constants.NODE_READY,
            timeout=300,
        )

        log.info(f"Labeling node '{new_node}' with OCS label")
        node_obj = ocp.OCP(kind="node")
        node_obj.add_label(
            resource_name=new_node,
            label=constants.OPERATOR_NODE_LABEL,
        )

        with config.RunWithFirstConsumerConfigContextIfAvailable():
            add_checker.wait_and_verify()
        log.info("======== Phase 1: Hub Node Addition PASSED ========")

        # ---- Phase 2: Remove hub node ----
        log.info("======== Phase 2: Hub Node Removal ========")
        with config.RunWithFirstConsumerConfigContextIfAvailable():
            remove_checker = FIOIntegrityChecker(pvc_factory, pod_factory)
            remove_checker.start_io(bg_runtime=500)

        log.info(
            f"-------- Scaling machineset '{ms_name}' from "
            f"{ms_replicas + 1} to {ms_replicas} --------"
        )
        scale_provider_machineset(ms_name, ms_replicas)

        log.info(f"Waiting for node '{new_node}' to disappear")
        for sample in TimeoutSampler(
            timeout=600,
            sleep=30,
            func=get_worker_nodes,
        ):
            if new_node not in sample:
                log.info(
                    f"Node '{new_node}' removed. " f"Workers ({len(sample)}): {sample}"
                )
                break
            log.info(
                f"Node '{new_node}' still present. "
                f"Workers ({len(sample)}): {sample}"
            )

        self._hub_node_added = False

        current_hub_workers = get_worker_nodes()
        assert len(current_hub_workers) == len(initial_hub_workers), (
            f"Hub worker count mismatch after removal: expected "
            f"{len(initial_hub_workers)}, got "
            f"{len(current_hub_workers)}"
        )

        with config.RunWithFirstConsumerConfigContextIfAvailable():
            remove_checker.wait_and_verify()
        log.info("======== Phase 2: Hub Node Removal PASSED ========")


@ignore_leftovers
class TestAddNodeToClientCluster(ManageTest):
    """
    Test adding a worker node to the client (hosted) cluster and verifying
    that ODF client components (CSI node plugins) are scheduled on the new node.
    """

    @pytest.fixture
    def scale_back_nodepool(self, request):
        """Record initial nodepool size and scale back on teardown."""
        self._initial_replicas = None
        self._np_name = None
        self._node_added = False

        def finalizer():
            if (
                self._np_name
                and self._initial_replicas is not None
                and self._node_added
            ):
                log.info(
                    f"Teardown: scaling NodePool '{self._np_name}' "
                    f"back to {self._initial_replicas}"
                )
                try:
                    with config.RunWithProviderConfigContextIfAvailable():
                        nodepool_ocp = ocp.OCP(
                            kind="NodePool",
                            namespace=constants.CLUSTERS_NAMESPACE,
                            resource_name=self._np_name,
                        )
                        patch = {"spec": {"replicas": self._initial_replicas}}
                        nodepool_ocp.patch(
                            params=json.dumps(patch),
                            format_type="merge",
                        )
                    log.info(
                        f"Scaled NodePool '{self._np_name}' back to "
                        f"{self._initial_replicas}"
                    )
                except CommandFailed as e:
                    log.warning(f"Failed to scale back nodepool: {e}")

        request.addfinalizer(finalizer)

    def _scale_nodepool(self, np_name, target_replicas):
        """
        Patch NodePool replicas and wait for the change to take effect.

        Args:
            np_name (str): NodePool resource name.
            target_replicas (int): Desired replica count.
        """
        with config.RunWithProviderConfigContextIfAvailable():
            nodepool_ocp = ocp.OCP(
                kind="NodePool",
                namespace=constants.CLUSTERS_NAMESPACE,
                resource_name=np_name,
            )
            patch = {"spec": {"replicas": target_replicas}}
            nodepool_ocp.patch(params=json.dumps(patch), format_type="merge")
        log.info(f"Patched NodePool '{np_name}' to {target_replicas} " f"replicas")
        node_util = HypershiftAWSNode()
        node_util._wait_nodepool_replicas_ready(np_name, target_replicas)

    @tier4a
    @brown_squad
    @hcp_required
    @runs_on_provider
    @polarion_id("OCS-7714")
    def test_add_remove_client_node_verify_odf(
        self, scale_back_nodepool, pvc_factory, pod_factory
    ):
        """
        Add a worker node to the client cluster, verify ODF client pods
        schedule on it, then remove the node and verify the cluster
        recovers.

        Steps:
        1. Record initial nodepool size and worker nodes
        2. Start FIO integrity checker (write files, md5sum, bg FIO)
        3. Scale up NodePool, wait for new node, label it for OCS
        4. Verify CSI node plugin pods on the new node
        5. Verify FIO integrity after node addition
        6. Scale down NodePool back to initial size
        7. Wait for worker count to return to initial
        8. Verify md5sum of integrity files survived removal
        """
        with config.RunWithFirstConsumerConfigContextIfAvailable():
            cluster_name = config.ENV_DATA.get("cluster_name")
            node_util = HypershiftAWSNode()
            nodepools = node_util._get_nodepools_for_cluster(cluster_name)
            if not nodepools:
                raise UnavailableResourceException(
                    f"No NodePool found for cluster '{cluster_name}'"
                )

            self._np_name = nodepools[0]["metadata"]["name"]
            self._initial_replicas = nodepools[0].get("spec", {}).get("replicas", 0)
            log.info(
                f"NodePool '{self._np_name}' initial replicas: "
                f"{self._initial_replicas}"
            )

            initial_workers = get_worker_nodes()
            log.info(
                f"Initial worker nodes ({len(initial_workers)}): " f"{initial_workers}"
            )

            # ---- Phase 1: Add node ----
            log.info("======== Phase 1: Node Addition ========")
            add_checker = FIOIntegrityChecker(pvc_factory, pod_factory)
            add_checker.start_io(bg_runtime=500)

            log.info(
                "-------- Scaling NodePool "
                f"'{self._np_name}' from "
                f"{self._initial_replicas} to "
                f"{self._initial_replicas + 1} --------"
            )
            node_op_start = time.time()

            node_util.create_and_attach_nodes_to_cluster(
                node_conf={},
                node_type=constants.RHCOS,
                num_nodes=1,
            )

            new_nodes = []
            for sample in TimeoutSampler(
                timeout=450,
                sleep=30,
                func=get_worker_nodes,
            ):
                current_workers = sample
                new_nodes = list(set(current_workers) - set(initial_workers))
                if new_nodes:
                    log.info(f"New node(s) detected: {new_nodes}")
                    break
                log.info(
                    f"No new nodes yet. Current workers "
                    f"({len(current_workers)}): "
                    f"{current_workers}"
                )

            assert new_nodes, (
                f"No new worker node appeared after scaling "
                f"NodePool. Initial workers: "
                f"{initial_workers}, "
                f"Current workers: {get_worker_nodes()}"
            )
            self._node_added = True

            new_node_name = new_nodes[0]
            log.info(f"Waiting for node '{new_node_name}' to become " f"Ready")
            wait_for_nodes_status(
                node_names=[new_node_name],
                status=constants.NODE_READY,
                timeout=300,
            )

            log.info(f"Labeling node '{new_node_name}' with OCS label")
            node_obj = ocp.OCP(kind="node")
            node_obj.add_label(
                resource_name=new_node_name,
                label=constants.OPERATOR_NODE_LABEL,
            )

            add_duration = time.time() - node_op_start
            log.info(f"-------- Node add completed in " f"{add_duration:.0f}s --------")

            pod_obj = ocp.OCP(
                kind="Pod",
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            all_pods = pod_obj.get(
                field_selector=f"spec.nodeName={new_node_name}",
            )
            nodeplugin_pods = [
                p["metadata"]["name"]
                for p in all_pods.get("items", [])
                if "nodeplugin" in p["metadata"]["name"]
            ]
            log.info(f"CSI nodeplugin pods on '{new_node_name}': " f"{nodeplugin_pods}")
            assert nodeplugin_pods, f"No CSI nodeplugin pods on '{new_node_name}'"

            add_checker.wait_and_verify()
            log.info("======== Phase 1: Node Addition PASSED ========")

            # ---- Phase 2: Remove node ----
            log.info("======== Phase 2: Node Removal ========")
            log.info(
                f"-------- Scaling NodePool "
                f"'{self._np_name}' from "
                f"{self._initial_replicas + 1} to "
                f"{self._initial_replicas} --------"
            )
            remove_op_start = time.time()

            self._scale_nodepool(self._np_name, self._initial_replicas)

            log.info("Waiting for worker count to return to " f"{len(initial_workers)}")
            for sample in TimeoutSampler(
                timeout=450,
                sleep=30,
                func=get_worker_nodes,
            ):
                if len(sample) <= len(initial_workers):
                    log.info(
                        f"Worker count restored. "
                        f"Current workers ({len(sample)}): "
                        f"{sample}"
                    )
                    break
                log.info(
                    f"Waiting for scale-down. "
                    f"Current workers ({len(sample)}): "
                    f"{sample}"
                )

            self._node_added = False
            remove_duration = time.time() - remove_op_start
            log.info(
                f"-------- Node removal completed in "
                f"{remove_duration:.0f}s --------"
            )

            current_workers = get_worker_nodes()
            assert len(current_workers) == len(initial_workers), (
                f"Worker count mismatch after removal: "
                f"expected {len(initial_workers)}, "
                f"got {len(current_workers)}"
            )

            log.info(
                "-------- Verifying Phase 1 integrity files "
                "survived node removal --------"
            )
            add_checker.verify_md5sum_only()
            log.info("======== Phase 2: Node Removal PASSED ========")


@ignore_leftovers
class TestExpandOSDOnHub(ManageTest):
    """
    Test adding a hub node and expanding OSD capacity onto it while
    verifying client IO continues uninterrupted. The added node and
    OSDs are intentionally left in place -- OSD removal is not a
    supported ODF operation.
    """

    @tier4a
    @brown_squad
    @hcp_required
    @runs_on_provider
    @polarion_id("OCS-7716")
    def test_expand_osd_on_hub_verify_client_io(
        self,
        pvc_factory,
        pod_factory,
    ):
        """
        Add a hub node, expand OSD capacity, verify Ceph rebalance
        while client IO runs.

        The added node and OSDs remain after the test. OSD removal
        is not a supported ODF operation.

        Steps:
        1. Validate initial OSD count matches deviceset config
        2. Start FIO integrity checker on client cluster
        3. Add 1 OCS worker node to hub via machineset scaling
        4. Expand OSD capacity (add_capacity)
        5. Wait for new OSD pods, verify Ceph health and rebalance
        6. Verify client IO integrity
        """
        initial_hub_workers = get_worker_nodes()
        deviceset_count = storage_cluster.get_deviceset_count()
        osd_size = storage_cluster.get_osd_size()
        expected_initial_osds = deviceset_count * 3

        with config.RunWithProviderConfigContextIfAvailable():
            osd_pods_before = get_osd_pods()
        osd_pod_names_before = {p.name for p in osd_pods_before}
        initial_osd_count = len(osd_pods_before)

        log.info(
            f"Initial state: {initial_osd_count} OSD pods, "
            f"deviceset count={deviceset_count}, "
            f"expected OSDs={expected_initial_osds}, "
            f"OSD size={osd_size}Gi, "
            f"hub workers ({len(initial_hub_workers)}): "
            f"{initial_hub_workers}"
        )

        if initial_osd_count != expected_initial_osds:
            pytest.skip(
                f"OSD count mismatch: {initial_osd_count} "
                f"running OSD pods but deviceset config "
                f"expects {expected_initial_osds} "
                f"(deviceset_count={deviceset_count} x 3). "
                f"Cluster may be in inconsistent state."
            )

        machinesets = get_provider_machinesets()
        assert machinesets, "No machinesets found on provider cluster"
        ms_name, ms_replicas = machinesets[0]

        with config.RunWithFirstConsumerConfigContextIfAvailable():
            integrity_checker = FIOIntegrityChecker(pvc_factory, pod_factory)
            integrity_checker.start_io(bg_runtime=3600)

        log.info(
            f"-------- Scaling machineset '{ms_name}' from "
            f"{ms_replicas} to {ms_replicas + 1} --------"
        )
        scale_provider_machineset(ms_name, ms_replicas + 1)

        log.info("Waiting for new hub worker node to appear")
        new_hub_nodes = []
        for sample in TimeoutSampler(
            timeout=600,
            sleep=30,
            func=get_worker_nodes,
        ):
            new_hub_nodes = list(set(sample) - set(initial_hub_workers))
            if new_hub_nodes:
                log.info(f"New hub node(s): {new_hub_nodes}")
                break
            log.info(f"No new hub nodes yet. Workers " f"({len(sample)}): {sample}")

        assert new_hub_nodes, "No new hub worker appeared after scaling " "machineset"
        new_hub_node = new_hub_nodes[0]
        wait_for_nodes_status(
            node_names=[new_hub_node],
            status=constants.NODE_READY,
            timeout=300,
        )
        log.info(f"Labeling node '{new_hub_node}' with OCS label")
        node_obj = ocp.OCP(kind="node")
        node_obj.add_label(
            resource_name=new_hub_node,
            label=constants.OPERATOR_NODE_LABEL,
        )

        log.info(f"-------- Expanding OSD capacity by " f"{osd_size}Gi --------")
        storage_cluster.add_capacity(osd_size)

        expected_osd_count = (deviceset_count + 1) * 3
        log.info(
            f"Waiting for {expected_osd_count} OSD pods to be "
            f"Running (on provider cluster)"
        )
        with config.RunWithProviderConfigContextIfAvailable():
            pod_obj = ocp.OCP(
                kind=constants.POD,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            pod_obj.wait_for_resource(
                timeout=600,
                sleep=10,
                condition=constants.STATUS_RUNNING,
                selector=constants.OSD_APP_LABEL,
                resource_count=expected_osd_count,
            )

        check_ceph_health_after_add_capacity(ceph_rebalance_timeout=3600)

        with config.RunWithProviderConfigContextIfAvailable():
            osd_pods_after = get_osd_pods()
        new_osd_pods = [p for p in osd_pods_after if p.name not in osd_pod_names_before]
        log.info(f"New OSD pods: {[p.name for p in new_osd_pods]}")
        assert new_osd_pods, (
            f"No new OSD pods appeared after add_capacity. "
            f"Before: {osd_pod_names_before}, "
            f"After: {[p.name for p in osd_pods_after]}"
        )

        with config.RunWithFirstConsumerConfigContextIfAvailable():
            integrity_checker.wait_and_verify()
        log.info("======== OSD Expansion + Client IO Verified " "========")


@ignore_leftovers
class TestRestartOSDNodeOnHub(ManageTest):
    """
    Test restarting an OSD node on the hub (provider) cluster while
    verifying client IO continues uninterrupted.
    """

    @pytest.fixture
    def ensure_nodes_up(self, request):
        """Ensure all hub nodes are running after the test."""

        def finalizer():
            log.info("Teardown: ensuring all hub nodes are running")
            try:
                with config.RunWithProviderConfigContextIfAvailable():
                    factory = PlatformNodesFactory()
                    nodes_platform = factory.get_nodes_platform()
                    nodes_platform.restart_nodes_by_stop_and_start_teardown()
            except (CommandFailed, ClientError) as e:
                log.warning(f"Failed to verify hub nodes in " f"teardown: {e}")

        request.addfinalizer(finalizer)

    @tier4a
    @brown_squad
    @hcp_required
    @runs_on_provider
    # @polarion_id("placeholder")
    def test_restart_osd_node_verify_client_io(
        self, ensure_nodes_up, pvc_factory, pod_factory
    ):
        """
        Restart one OSD node on the hub cluster and verify that
        client IO is not interrupted. The node is restarted via
        EC2 stop/start and comes back automatically.

        Steps:
        1. Get OSD running nodes on the hub
        2. Start FIO integrity checker on client cluster
        3. Restart a random OSD node (stop + start)
        4. Wait for the node to become Ready
        5. Wait for OSD pods to recover on the node
        6. Verify Ceph health
        7. Verify client IO integrity
        """
        with config.RunWithProviderConfigContextIfAvailable():
            osd_node_names = get_osd_running_nodes()
            assert osd_node_names, "No OSD nodes found on provider"

            osd_node_name = random.choice(osd_node_names)
            osd_node = get_node_objs([osd_node_name])[0]
            log.info(f"Selected OSD node for restart: " f"'{osd_node_name}'")

        with config.RunWithFirstConsumerConfigContextIfAvailable():
            integrity_checker = FIOIntegrityChecker(pvc_factory, pod_factory)
            integrity_checker.start_io(bg_runtime=500)

        with config.RunWithProviderConfigContextIfAvailable():
            log.info(
                f"-------- Restarting OSD node "
                f"'{osd_node_name}' (stop + start) --------"
            )
            restart_start = time.time()

            factory = PlatformNodesFactory()
            nodes_platform = factory.get_nodes_platform()
            nodes_platform.restart_nodes_by_stop_and_start(
                nodes=[osd_node], wait=True, force=True
            )

            log.info(f"Waiting for node '{osd_node_name}' to become " f"Ready")
            wait_for_nodes_status(
                node_names=[osd_node_name],
                status=constants.NODE_READY,
                timeout=420,
            )

            restart_duration = time.time() - restart_start
            log.info(
                f"-------- Node restart completed in "
                f"{restart_duration:.0f}s --------"
            )

            log.info("Waiting for all OSD pods to be Running")
            pod_obj = ocp.OCP(
                kind=constants.POD,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            osd_count = len(get_osd_pods())
            pod_obj.wait_for_resource(
                timeout=300,
                sleep=10,
                condition=constants.STATUS_RUNNING,
                selector=constants.OSD_APP_LABEL,
                resource_count=osd_count,
            )

            log.info("Verifying Ceph health after node restart")
            ceph_health_check(
                namespace=config.ENV_DATA["cluster_namespace"],
                tries=40,
                delay=30,
            )

        with config.RunWithFirstConsumerConfigContextIfAvailable():
            integrity_checker.wait_and_verify()
        log.info("======== OSD Node Restart + Client IO Verified " "========")
