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
    wait_for_nodes_status,
)
from ocs_ci.ocs import node
from ocs_ci.ocs import ocp
from ocs_ci.ocs.platform_nodes import HypershiftAWSNode
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.helpers.disruption_helpers import FIOIntegrityChecker
from ocs_ci.utility.utils import get_random_str, ceph_health_check, TimeoutSampler

log = logging.getLogger(__name__)


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
class TestAddNodeToHubWithClientIO(ManageTest):
    """
    Test adding OCS nodes to the hub (provider) cluster — same as
    test_add_ocs_node from test_node_expansion.py — while verifying that
    client (hosted) cluster IO continues uninterrupted.

    The hub cluster hosts OSD pods and ceph. The client cluster only runs
    CSI node plugins and consumes storage via PVCs.
    """

    @tier4a
    @brown_squad
    @hcp_required
    @polarion_id("OCS-7715")
    def test_add_hub_node_verify_client_io(self, add_nodes, pvc_factory, pod_factory):
        """
        Add OCS nodes to the hub cluster and verify rebalance completes
        while client cluster IO runs without errors. During the hub node
        add and rebalance, FIO runs on both RBD and CephFS PVCs on the
        client cluster. After the operation, data integrity is verified
        via md5sum and FIO error counters.

        Steps:
        1. Switch to client cluster, write integrity files on RBD and
           CephFS, compute md5sum, start background FIO
        2. Switch back to hub cluster, add OCS nodes
        3. Wait for ceph rebalance to complete
        4. Wait for background FIO, check IO errors and md5sum integrity
        """
        log.info("-------- Setting up IO on client cluster --------")
        with config.RunWithFirstConsumerConfigContextIfAvailable():
            integrity_checker = FIOIntegrityChecker(pvc_factory, pod_factory)
            integrity_checker.start_io(bg_runtime=500)

        log.info("-------- Adding OCS nodes to hub cluster --------")
        add_nodes(ocs_nodes=True)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"

        log.info("-------- Verifying client cluster IO integrity --------")
        with config.RunWithFirstConsumerConfigContextIfAvailable():
            integrity_checker.wait_and_verify()


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

        def finalizer():
            if self._np_name and self._initial_replicas is not None:
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
                            params=json.dumps(patch), format_type="merge"
                        )
                    log.info(
                        f"Scaled NodePool '{self._np_name}' back to "
                        f"{self._initial_replicas}"
                    )
                except Exception as e:
                    log.warning(f"Failed to scale back nodepool: {e}")

        request.addfinalizer(finalizer)

    @tier4a
    @brown_squad
    @hcp_required
    @polarion_id("OCS-7714")
    def test_add_client_node_verify_odf_scheduled(
        self, scale_back_nodepool, pvc_factory, pod_factory
    ):
        """
        Add a worker node to the client cluster, label it with the OCS label,
        and verify ODF client pods (cephfs/rbd node plugins) schedule on it.
        During the node scaling operation, FIO runs on both RBD and CephFS
        PVCs. After the operation, data integrity is verified via md5sum
        and FIO error counters.

        Steps:
        1. Record initial nodepool size and worker nodes
        2. Write integrity files on RBD and CephFS PVCs, compute md5sum,
           start background FIO
        3. Scale up the NodePool to add 1 worker node
        4. Wait for the new node to appear and become Ready
        5. Label the new node for OCS
        6. Verify CSI node plugin pods are scheduled on the new node
        7. Wait for background FIO, check IO errors and md5sum integrity
        """
        with config.RunWithFirstConsumerConfigContextIfAvailable():
            cluster_name = config.ENV_DATA.get("cluster_name")
            node_util = HypershiftAWSNode()
            nodepools = node_util._get_nodepools_for_cluster(cluster_name)
            if not nodepools:
                raise Exception(f"No NodePool found for cluster '{cluster_name}'")

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

            integrity_checker = FIOIntegrityChecker(pvc_factory, pod_factory)
            integrity_checker.start_io(bg_runtime=500)

            log.info(
                "-------- Starting node add operation: scaling "
                f"NodePool '{self._np_name}' from "
                f"{self._initial_replicas} to "
                f"{self._initial_replicas + 1} --------"
            )
            node_op_start = time.time()

            node_util.create_and_attach_nodes_to_cluster(
                node_conf={}, node_type=constants.RHCOS, num_nodes=1
            )

            new_nodes = []
            node_wait_timeout = 450
            log.info(
                f"Waiting up to {node_wait_timeout}s for a new worker "
                f"node to appear in the cluster"
            )
            for sample in TimeoutSampler(
                timeout=node_wait_timeout,
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
                    f"({len(current_workers)}): {current_workers}"
                )

            assert new_nodes, (
                f"No new worker node appeared after scaling NodePool. "
                f"Initial workers: {initial_workers}, "
                f"Current workers: {get_worker_nodes()}, "
                f"NodePool '{self._np_name}' expected replicas: "
                f"{self._initial_replicas + 1}"
            )

            new_node_name = new_nodes[0]
            log.info(f"Waiting for new node '{new_node_name}' to become Ready")
            wait_for_nodes_status(
                node_names=[new_node_name],
                status=constants.NODE_READY,
                timeout=300,
            )

            log.info(f"Labeling new node '{new_node_name}' with OCS label")
            node_obj = ocp.OCP(kind="node")
            node_obj.add_label(
                resource_name=new_node_name,
                label=constants.OPERATOR_NODE_LABEL,
            )
            log.info(
                f"Successfully labeled '{new_node_name}' with "
                f"{constants.OPERATOR_NODE_LABEL}"
            )

            node_op_duration = time.time() - node_op_start
            log.info(
                "-------- Node add operation completed in "
                f"{node_op_duration:.0f}s --------"
            )

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
            log.info(
                f"CSI nodeplugin pods on new node '{new_node_name}': "
                f"{nodeplugin_pods}"
            )
            assert nodeplugin_pods, (
                f"No CSI nodeplugin pods found on new node " f"'{new_node_name}'"
            )

            integrity_checker.wait_and_verify()
