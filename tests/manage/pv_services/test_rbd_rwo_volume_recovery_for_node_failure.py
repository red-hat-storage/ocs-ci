import logging
import pytest
import json
import time

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    skipif_bm,
    skipif_ibm_cloud,
    skipif_managed_service,
)
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.helpers.sanity_helpers import Sanity

from ocs_ci.ocs.resources import pod as res_pod
from ocs_ci.ocs.platform_nodes import PlatformNodesFactory
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
    wait_for_pods_to_be_running,
    get_pod_node,
    get_deployments_having_label,
)
from ocs_ci.framework.pytest_customization.marks import (
    tier4b,
    polarion_id,
    brown_squad,
)
from ocs_ci.ocs.node import (
    taint_nodes,
    wait_for_nodes_status,
    label_nodes,
)

log = logging.getLogger(__name__)


@brown_squad
@tier4b
@skipif_managed_service
@skipif_ocs_version("<4.14")
class TestRWOVolumeRecover(ManageTest):
    """
    RHSTOR-2500- Enable faster RWO recovery in case of node failures
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def setup(
        self,
        project_factory,
        pvc_factory,
        teardown_factory,
        service_account_factory,
        pod_factory,
    ):
        """
        Identify the nodes and start DeploymentConfig based app pods using
        PVC with ReadWriteOnce (RWO) access mode on selected nodes

        Args:
            service_account_factory : A fixture to create service account
            teardown_factory : A fixture for teardown
            project_factory: A fixture to create new project
            pvc_factory: A fixture to create new PVCs
            pod_factory: A fixture to create deploymentconfig pods

        Returns:
            deployment config, app pod object and pvc object
        """

        # create a project for simple-app deployment
        project = project_factory(project_name="test-project")

        # create a pvc
        pvc = pvc_factory(
            project=project,
            access_mode=constants.ACCESS_MODE_RWO,
            size=20,
        )
        log.info(f"Pvc created: {pvc.name}")

        # create service account
        service_account_obj = service_account_factory(project=project)

        # create simple-app deployment
        simple_app_data = templating.load_yaml(constants.SIMPLE_APP_POD_YAML)
        simple_app_data["metadata"]["namespace"] = project.namespace
        simple_app_data["spec"]["template"]["spec"][
            "serviceAccountName"
        ] = service_account_obj.name
        simple_app_data["spec"]["template"]["spec"]["volumes"][0][
            "persistentVolumeClaim"
        ]["claimName"] = pvc.name

        simple_app_dc = helpers.create_resource(**simple_app_data)

        teardown_factory(simple_app_dc)

        self.simple_app_dc_obj = Deployment(
            **get_deployments_having_label(
                label="app=simple-app", namespace=project.namespace
            )[0]
        )
        self.simple_app_pod = Pod(
            **get_pods_having_label(
                label="app=simple-app", namespace=project.namespace
            )[0]
        )
        helpers.wait_for_resource_state(
            resource=self.simple_app_pod, state=constants.STATUS_RUNNING, timeout=300
        )

        return self.simple_app_dc_obj, self.simple_app_pod, pvc.backed_pv_obj

    def get_fence_status(self):
        """

        Get networkfence status

        """
        log.info("Verifying network fence is created")
        out = run_cmd("oc get networkfences.csiaddons.openshift.io -o json")
        nf_json = json.loads(out)

        # Veify that items key exist and is not empty, if yes display the fence state
        if "items" in nf_json and nf_json["items"]:
            if (
                "spec" in nf_json["items"][0]
                and "fenceState" in nf_json["items"][0]["spec"]
            ):
                self.fence_state = nf_json["items"][0]["spec"]["fenceState"]
                log.info(f"The fenceState is: {self.fence_state}")
            else:
                log.error("Either 'spec' or 'fenceState' key is missing")
        else:
            log.error("The 'items' list is empty or missing")

    def get_cidr_ip_from_ceph_blocklist(self):
        """

        Get_cidr_ip_from_ceph_blocklist

        """
        log.info("Verifying CIDRs are created")
        ct_pod = res_pod.get_ceph_tools_pod()
        self.ceph_osd_blockslist_ls = ct_pod.exec_ceph_cmd(
            ceph_cmd="ceph osd blocklist ls",
            out_yaml_format=False,
        )
        log.info(f"Ceph blocklist info: {self.ceph_osd_blockslist_ls}")

    def set_configmap_watch_for_node_failure_rook_ceph_operator(self, configmap_value):
        """

        Set ROOK_WATCH_FOR_NODE_FAILURE on configmap of rook-ceph-operator

        """
        configmap_obj = OCP(
            kind=constants.CONFIGMAP,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
        )
        log.info(f"Setting ROOK_WATCH_FOR_NODE_FAILURE to: {configmap_value}")
        params = f'{{"data": {{"ROOK_WATCH_FOR_NODE_FAILURE": "{configmap_value}"}}}}'
        configmap_obj.patch(params=params, format_type="merge")

    @skipif_bm
    @skipif_ibm_cloud
    @polarion_id("OCS-5284")
    def test_rwo_volume_recovery_post_node_failure(self):
        """
        OCS-5284/OCS-5286/OCS-5288:
        - Start DeploymentConfig based app pods on 1 node
        - Taint the node (where app pods are running) with label nodeshutdown:NoExecute
        - Turn off the node
        - Verify that networkfence is created and cidr ip is added in osd blocklist command
        - Verify that app pod is running on new node
        - Remove the taint from the node and power on the node
        - verify that networkfence and cidr ip is removed post taint removal
        """
        nodes = PlatformNodesFactory().get_nodes_platform()
        node = get_pod_node(self.simple_app_pod)
        log.info(f"{self.simple_app_pod.name} pod is scheduled on node {node}")
        log.info(f"Selected node is '{node.name}'")

        log.info(f"Shutting down node '{node.name}'")
        nodes.stop_nodes([node])
        wait_for_nodes_status(node_names=[node.name], status=constants.NODE_NOT_READY)
        log.info(f"The node '{node.name}' reached '{constants.NODE_NOT_READY}' status")

        log.info("Taint node with nodeshutdown:NoExecute")
        taint_nodes(
            nodes=[node.name],
            taint_label="node.kubernetes.io/out-of-service=nodeshutdown:NoExecute",
        )

        time.sleep(600)

        # Get networkfence status
        self.get_fence_status()
        assert (
            self.fence_state == "Fenced"
        ), "Assertion failed: Fencing failed, fence_state is not 'Fenced'"

        # Check that cidr ip is added in ceph osd blocklist command
        self.get_cidr_ip_from_ceph_blocklist()
        assert (
            "range" in self.ceph_osd_blockslist_ls
        ), "Assertion failed: CIDR ip is not added in osd blocklist"

        time.sleep(540)

        # Verify that app pod is scheduled on new node
        node = get_pod_node(self.simple_app_pod).name
        log.info(f"{self.simple_app_pod.name} pod is scheduled on node {node}")
        nodes.start_nodes(nodes=[node])
        wait_for_nodes_status(node_names=[node.name], timeout=600)
        self.sanity_helpers.health_check(cluster_check=False, tries=60)

        log.info("Checking storage pods status")
        wait_for_pods_to_be_running(timeout=60)

        log.info("Removing the taint from the node")
        taint_nodes(
            nodes=[node.name],
            taint_label="node.kubernetes.io/out-of-service=nodeshutdown:NoExecute-",
        )

        log.info("Verifying networkfence status and CIDR ip removal status")

        # Verify that network fence and cidr ips are removed post taint removal
        self.get_fence_status()
        assert (
            self.fence_state != "Fenced"
        ), "Assertion failed: Failed to remove the Network Fence"

        self.get_cidr_ip_from_ceph_blocklist()
        assert (
            "range" not in self.ceph_osd_blockslist_ls
        ), "Assertion failed: CIDR ip is still present in osd blocklist"

    @skipif_bm
    @skipif_ibm_cloud
    @polarion_id("OCS-5285")
    def test_rwo_volume_recovery_post_node_failure_with_configmap_false(self):
        """
        OCS-5285:
        - Change the watch_for_node_failure value rook ceph operator config map to false
        - Start DeploymentConfig based app pods on 1 node
        - Taint the node (where app pods are running) with label nodeshutdown:NoExecute
        - Turn off the node
        - Verify that networkfence is not created and cidr ip is not added in osd blocklist command
        - Verify that nodeloass recovery fails
        - Remove the taint from the node and power on the node
        - Change the watch_for_node_failure value rook ceph operator config map to true
        """
        log.info("Setting rook configmap value to FALSE")
        self.set_configmap_watch_for_node_failure_rook_ceph_operator(
            configmap_value=False
        )

        nodes = PlatformNodesFactory().get_nodes_platform()

        # Get the node object where app pod is running
        node = get_pod_node(self.simple_app_pod)
        log.info(f"{self.simple_app_pod.name} pod is scheduled on node {node}")
        log.info(f"Selected node is '{node.name}'")
        log.info(f"Shutting down node '{node.name}'")
        nodes.stop_nodes([node])
        wait_for_nodes_status(node_names=[node.name], status=constants.NODE_NOT_READY)
        log.info(f"The node '{node.name}' reached '{constants.NODE_NOT_READY}' status")

        log.info("Taint node with nodeshutdown:NoExecute label")
        taint_nodes(
            nodes=[node.name],
            taint_label="node.kubernetes.io/out-of-service=nodeshutdown:NoExecute",
        )

        time.sleep(600)

        # Get networkfence status
        self.get_fence_status()
        assert self.fence_state != "Fenced", "Assertion failed: Network Fence exists"

        # Check that cidr ip is added in ceph osd blocklist command
        self.get_cidr_ip_from_ceph_blocklist()
        assert (
            "range" not in self.ceph_osd_blockslist_ls
        ), "Assertion failed: CIDR ip is added in osd blocklist"

        time.sleep(1200)

        # start the node
        nodes.start_nodes(nodes=[node])
        wait_for_nodes_status(node_names=[node.name], timeout=600)

        log.info("Removing the taint from the node")
        taint_nodes(
            nodes=[node.name],
            taint_label="node.kubernetes.io/out-of-service=nodeshutdown:NoExecute-",
        )

        self.sanity_helpers.health_check(cluster_check=False, tries=60)
        log.info("Checking storage pods status")
        # Validate storage pods are running
        wait_for_pods_to_be_running(timeout=60)

        log.info("Setting rook configmap value to default")
        self.set_configmap_watch_for_node_failure_rook_ceph_operator(
            configmap_value=True
        )

    @skipif_bm
    @skipif_ibm_cloud
    @polarion_id("OCS-5287")
    def test_rwo_volume_recovery_post_node_failure_with_rook_operator(self):
        """
        OCS-5287:
        - Start DeploymentConfig based app pods on a node where rook operator pod is running
        - Taint the node (where app pods are running) with label nodeshutdown:NoExecute
        - Turn off the node
        - Verify that networkfence is created and cidr ip is added in osd blocklist command
        - Verify that app pod is running on new node
        - Remove the taint from the node and power on the node
        - verify that networkfence and cidr ip is removed post taint removal
        """
        nodes = PlatformNodesFactory().get_nodes_platform()
        rook_operator_pods = res_pod.get_operator_pods()
        rook_operator_pod = rook_operator_pods[0]

        rook_operator_pod_name = rook_operator_pod.name
        log.info(f"rook operator pod found: {rook_operator_pod_name}")

        # Get the node name that has the rook operator pod running on
        operator_node = res_pod.get_pod_node(rook_operator_pod)
        operator_node_name = operator_node.get().get("metadata").get("name")
        log.info(
            f"{rook_operator_pod_name} pod is running on node {operator_node_name}"
        )
        for node1 in nodes:
            if node1.name != operator_node_name:
                label_nodes(nodes=node1, label="ssd")

        pod_deleted = self.simple_app_pod.delete(wait=False)
        assert pod_deleted, f"Failed to delete pod {self.simple_app_pod.name}"

        node = get_pod_node(self.simple_app_pod)
        log.info(f"{self.simple_app_pod.name} pod is scheduled on node {node}")
        log.info(f"Selected node is '{node.name}'")

        log.info(f"Shutting down node '{node.name}'")
        nodes.stop_nodes([node])
        wait_for_nodes_status(node_names=[node.name], status=constants.NODE_NOT_READY)
        log.info(f"The node '{node.name}' reached '{constants.NODE_NOT_READY}' status")

        log.info("Taint node with nodeshutdown:NoExecute")
        # ocs_nodes = get_worker_nodes()
        taint_nodes(
            nodes=[node.name],
            taint_label="node.kubernetes.io/out-of-service=nodeshutdown:NoExecute",
        )

        time.sleep(600)

        self.get_fence_status()

        assert (
            self.fence_state == "Fenced"
        ), "Assertion failed: Fencing failed, fence_state is not 'Fenced'"

        self.get_cidr_ip_from_ceph_blocklist()

        assert (
            "range" in self.ceph_osd_blockslist_ls
        ), "Assertion failed: CIDR ip is not added in osd blocklist"

        time.sleep(540)

        node = get_pod_node(self.simple_app_pod).name
        log.info(f"{self.simple_app_pod.name} pod is scheduled on node {node}")
        nodes.start_nodes(nodes=[node])
        wait_for_nodes_status(node_names=[node.name], timeout=600)
        self.sanity_helpers.health_check(cluster_check=False, tries=60)

        log.info("Checking storage pods status")
        wait_for_pods_to_be_running(timeout=60)

        log.info("Removing the taint from the node")
        taint_nodes(
            nodes=[node.name],
            taint_label="node.kubernetes.io/out-of-service=nodeshutdown:NoExecute-",
        )

        log.info("Verifying networkfence status and CIDR ip removal status")

        self.get_fence_status()

        assert (
            self.fence_state != "Fenced"
        ), "Assertion failed: Failed to remove the Network Fence"

        self.get_cidr_ip_from_ceph_blocklist()

        assert (
            "range" not in self.ceph_osd_blockslist_ls
        ), "Assertion failed: CIDR ip is still present in osd blocklist"

        for node1 in nodes:
            if node1.name != operator_node_name:
                label_nodes(nodes=node1, label="ssd-")
