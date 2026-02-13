import logging
import random
import time
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    acceptance,
    polarion_id,
    green_squad,
    skipif_ocs_version,
)
from ocs_ci.ocs import (
    ocp,
    constants,
)
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.resources.daemonset import DaemonSet
from ocs_ci.helpers.helpers import verify_socket_on_node

logger = logging.getLogger(__name__)


@skipif_ocs_version("<4.20")
class TestCSIADDonDaemonset(ManageTest):
    """
    Test class for CSI addon daemonset verification
    """

    @pytest.mark.parametrize(
        argnames=["daemonset_name", "pod_label"],
        argvalues=[
            pytest.param(
                constants.DAEMONSET_CSI_RBD_CSI_ADDONS,
                constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7386")],
            ),
            pytest.param(
                constants.DAEMONSET_CSI_CEPHFS_CSI_ADDONS,
                constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7501")],
            ),
        ],
    )
    def test_csi_addon_daemonset_exists(self, daemonset_name, pod_label):
        """
        Verify that the CSI addon daemonset exists and is properly configured

        Args:
            daemonset_name (str): Name of the CSI addon daemonset to verify
            pod_label (str): Expected pod label for the daemonset

        steps:
        1. Check if CSI addon Daemonset exists
        2. Verify daemonset configuration

        """
        daemonsets = DaemonSet(namespace=config.ENV_DATA["cluster_namespace"])
        logger.info("Validating existence of CSI Addon daemonset")

        # Verify Daemonset exists
        assert daemonsets.check_resource_existence(
            should_exist=True, resource_name=daemonset_name
        ), f"CSI addon daemonset '{daemonset_name}' does not exist"
        logger.info(f"CSIaddon daemonset '{daemonset_name}' exists")

        # Verify daemonset configuration
        logger.info("Validating configuration of CSI Addon daemonset")
        daemonset_info = daemonsets.get(resource_name=daemonset_name)

        actual_labels = (
            daemonset_info.get("spec", {})
            .get("template", {})
            .get("metadata", {})
            .get("labels", {})
        )
        assert (
            actual_labels
        ), f" The Daemonset {daemonset_name} has label {actual_labels} "
        expected_label = pod_label.split("=")
        for key, value in actual_labels.items():
            assert expected_label == [
                key,
                value,
            ], f"expected label {pod_label} not found in daemonset labels"
        logger.info("CSI addon daemonset has correct labels")

    @pytest.mark.parametrize(
        argnames=["pod_label"],
        argvalues=[
            pytest.param(
                constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
                marks=[
                    acceptance,
                    tier1,
                    green_squad,
                    pytest.mark.polarion_id("OCS-7374"),
                ],
            ),
            pytest.param(
                constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
                marks=[
                    acceptance,
                    tier1,
                    green_squad,
                    pytest.mark.polarion_id("OCS-7503"),
                ],
            ),
        ],
    )
    def test_csi_addon_pods_containers_ready(self, pod_label):
        """
        Verify that all containers in CSI-addon pods are in ready status
        Steps:
        1. Get all CSI Addons Pods
        2. Check each container in each pod
        3. Verify Container readiness status of each pod
        OCS-7503 is part verification of DFBUGS_5082 automation

        """
        logger.info("Validating containers in csi addon pods having ready status")
        namespace = config.ENV_DATA["cluster_namespace"]
        csi_addon_pods = get_pods_having_label(pod_label, namespace)
        for pod in csi_addon_pods:
            container_status_list = pod.get("status").get("containerStatuses")
            for container_status in container_status_list:
                assert container_status[
                    "ready"
                ], f"container {container_status['name']} in pod {pod.name} is not ready"
        logger.info("All containers in CSI-addon DaemonSet pods are ready")

    @pytest.mark.parametrize(
        argnames=["pod_label"],
        argvalues=[
            pytest.param(
                constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7373")],
            ),
            pytest.param(
                constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7502")],
            ),
        ],
    )
    def test_csi_addon_pods_uses_pod_network(self, pod_label):
        """
        Verify that CSI-addon used pod network instead of host network
        OCS-7502 is part verification of DFBUGS_5082 automation

        """

        logger.info(
            "Validating csi addon pod using pod-network instead of host network"
        )
        namespace = config.ENV_DATA["cluster_namespace"]
        csi_addon_pods = get_pods_having_label(pod_label, namespace)
        for pod in csi_addon_pods:
            host_network = pod.get("spec").get("hostNetwork", False)
            assert (
                not host_network
            ), f" CSI-addon pod {pod.name} is using host network instead of pod network"
        logger.info(
            "CSI-addon DaemonSet pods using pod network instead of host-network"
        )

    @pytest.mark.parametrize(
        argnames=["daemonset_name"],
        argvalues=[
            pytest.param(
                constants.DAEMONSET_CSI_RBD_CSI_ADDONS,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7375")],
            ),
            pytest.param(
                constants.DAEMONSET_CSI_CEPHFS_CSI_ADDONS,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7504")],
            ),
        ],
    )
    def test_csi_addon_daemonset_desired_vs_ready(self, daemonset_name):
        """
        Verify that CSI addon DaemonSet has desired number of ready and available pods
        Step:
        1. Get CSI-addon DaemonSet status
        2. Compare desired Vs ready pod counts
        3. Verify all pods are available
        OCS-7504 is part verification of DFBUGS_5082 automation

        """
        logger.info(
            "Validating CSI-addon DaemonSet has correct number of Desired, ready and available pods"
        )
        csi_addon_daemonset = DaemonSet(
            resource_name=daemonset_name,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        csi_addon_daemonset_status = csi_addon_daemonset.get_status()

        number_ready = csi_addon_daemonset_status["numberReady"]
        number_available = csi_addon_daemonset_status["numberAvailable"]
        desired_number_ready = csi_addon_daemonset_status["desiredNumberScheduled"]

        assert (
            number_ready == desired_number_ready
        ), f"Expected {desired_number_ready} pods to be ready, but found {number_ready} pods ready"
        assert (
            number_available == desired_number_ready
        ), f"Expected {desired_number_ready} pods to be available, but found {number_available} pods available"
        logger.info(
            f"Verified CSI-addon DaemonSet status- Desired: {desired_number_ready}, "
            f"Ready: {number_ready}, Available: {number_available}"
        )

    @tier1
    @green_squad
    @polarion_id("OCS-7305")
    @pytest.mark.parametrize(
        argnames=["pod_label"],
        argvalues=[
            pytest.param(
                constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7305")],
            ),
            pytest.param(
                constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7505")],
            ),
        ],
    )
    def test_csi_addon_pods_on_worker_nodes(self, pod_label):
        """
        Verify that the CSI addon pods are running on each worker node
        step:
        1. Get all worker nodes (including master nodes that also have worker role)
        2. Get CSI addon daemonset pods
        3. Verify each worker node has a CSI addon pod
        OCS-7505 is part verification of DFBUGS_5082 automation

        """
        logger.info("Validating csi addon pods on each worker node")
        namespace = config.ENV_DATA["cluster_namespace"]

        # Get all nodes with worker role, including master nodes that also have worker role
        # Skipping use of get_worker_nodes() which excludes master nodes in HCI provider clusters.
        label = "node-role.kubernetes.io/worker"
        ocp_node_obj = ocp.OCP(kind=constants.NODE)
        nodes = ocp_node_obj.get(selector=label).get("items")
        worker_nodes_names = [node.get("metadata").get("name") for node in nodes]
        logger.info(
            f"Current available worker nodes (including master nodes with worker role) are {worker_nodes_names}"
        )

        csi_addon_pods = get_pods_having_label(pod_label, namespace)
        assert len(csi_addon_pods) > 0, "csi addon pods not found"
        logger.info(f"Found {len(csi_addon_pods)} csi addon pods")

        assert len(csi_addon_pods) == len(worker_nodes_names), (
            f"Expected {len(worker_nodes_names)} csi addon pods, one per worker node, "
            f"found {len(csi_addon_pods)}"
        )
        # verify each node has csi addon pod
        csi_pod_running_nodes_name = []
        for pod_obj in csi_addon_pods:
            csi_pod_running_node_name = pod_obj.get("spec").get("nodeName")
            assert csi_pod_running_node_name in worker_nodes_names, (
                f"CSI addon pod {pod_obj['metadata']['name']} is running on "
                f"node {csi_pod_running_node_name} which is not a worker node"
            )
            csi_pod_running_nodes_name.append(csi_pod_running_node_name)

        pod_missed_node = set(worker_nodes_names) - set(csi_pod_running_nodes_name)
        assert (
            not pod_missed_node
        ), f"worker node {pod_missed_node} do not have CSI addon pods"
        logger.info("CSI addon pods running on each worker node")

    @pytest.mark.parametrize(
        argnames=["pod_label"],
        argvalues=[
            pytest.param(
                constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7387")],
            ),
            pytest.param(
                constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7506")],
            ),
        ],
    )
    def test_csi_addon_pod_restart(self, pod_label):
        """
        Restart a CSI-addons pod and validate it restored to running state.
        OCS-7506 is part verification of DFBUGS_5082 automation

        """
        namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        pod_obj = ocp.OCP(kind="Pod", namespace=namespace)

        csi_addons_pod_objs = get_pods_having_label(pod_label, namespace)
        pod_data = random.choice(csi_addons_pod_objs)
        pod_obj.delete(resource_name=pod_data["metadata"]["name"])
        time.sleep(5)

        csi_addon_pod_new = get_pods_having_label(pod_label, namespace)
        csi_addon_pod_names_list = [
            pod_data["metadata"]["name"] for pod_data in csi_addon_pod_new
        ]

        assert wait_for_pods_to_be_running(
            namespace=namespace, pod_names=csi_addon_pod_names_list
        ), "CSI-addons pod didn't came up is running status "

    @pytest.mark.parametrize(
        argnames=["pod_label", "plugin_dir", "socket_name"],
        argvalues=[
            pytest.param(
                constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
                constants.RBD_CSI_ADDONS_PLUGIN_DIR,
                constants.RBD_CSI_ADDONS_SOCKET_NAME,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7379")],
            ),
            pytest.param(
                constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
                constants.CEPHFS_CSI_ADDONS_PLUGIN_DIR,
                constants.CEPHFS_CSI_ADDONS_SOCKET_NAME,
                marks=[tier1, green_squad, pytest.mark.polarion_id("OCS-7507")],
            ),
        ],
    )
    def test_csi_addons_socket_creation_per_pods_node(
        self, pod_label, plugin_dir, socket_name
    ):
        """
        csi-addons.sock are used for communication for csi-addons.
        This test ensure the socket creation of csi-addons.sock socket
        on hostpath for each pods node.
        Steps:
        1. Get all csi-addons pods
        2. Get nodes of each csi-addons pod
        3. Verify socket creation on nodes
        OCS-7507 is part verification of DFBUGS_5082 automation

        """
        logger.info(
            "Validating csi-addons socket creation on nodes of each csi-addons pod"
        )
        namespace = config.ENV_DATA["cluster_namespace"]
        # 1. Get all csi-addons pods
        csi_addon_pods = get_pods_having_label(pod_label, namespace)
        # Verify socket creation on node of each csi-addons pod
        for pod_obj in csi_addon_pods:
            csi_pod_running_node_name = pod_obj.get("spec").get("nodeName")
            assert verify_socket_on_node(
                node_name=csi_pod_running_node_name,
                host_path=plugin_dir,
                socket_name=socket_name,
            ), f"csi-addons Socket not found on node {csi_pod_running_node_name}"

    @pytest.mark.parametrize(
        argnames=["pod_label"],
        argvalues=[
            pytest.param(
                constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
                marks=[
                    green_squad,
                    pytest.mark.tier4c,
                    pytest.mark.polarion_id("OCS-7376"),
                ],
            ),
            pytest.param(
                constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
                marks=[
                    green_squad,
                    pytest.mark.tier4c,
                    pytest.mark.polarion_id("OCS-7511"),
                ],
            ),
        ],
    )
    def test_csi_addons_pod_crash_recovery(self, pod_label):
        """
        Test csi-addons pod recovery after pod crash and ensure the restart count.
        1. Get all csi-addons pods
        2. Pick a random csi-addons pod
        3. Crash the csi-addons pod
        4. Wait for pod to be Running and check restart count
        OCS-7511 is part verification of DFBUGS_5082 automation

        """
        logger.info(
            "Validating csi-addons pod recovery after pod crash with increase in restart count."
        )
        namespace = config.ENV_DATA["cluster_namespace"]

        # 1. Get all csi-addons pods
        csi_addons_pod_objs = get_pods_having_label(pod_label)
        # 2. Pick a random csi-addons pod
        pod_data = random.choice(csi_addons_pod_objs)
        pod_name = pod_data["metadata"]["name"]
        # Note restart count of the selected pod
        restart_count_before = (
            pod_data.get("status").get("containerStatuses")[0].get("restartCount")
        )

        # 3. Crash the csi-addons pod using 'kill 1'
        pod_crash_cmd = f'exec {pod_name} -- /bin/sh -c "kill 1"'
        ocp_pod = ocp.OCP(kind="pod", namespace=namespace)
        ocp_pod.exec_oc_cmd(pod_crash_cmd)

        # Give time for pod to restart
        time.sleep(10)

        # 4. Wait for pod to be Running and check restart count
        assert wait_for_pods_to_be_running(
            namespace=namespace, pod_names=[pod_name]
        ), f"CSI-addons pod {pod_name} didn't come up to running status"

        pod_obj = ocp_pod.get(resource_name=pod_name)
        restart_count_after = (
            pod_obj.get("status").get("containerStatuses")[0].get("restartCount")
        )
        assert (
            restart_count_after > restart_count_before
        ), f"Restart count should increase, Pod restart count of pod- {pod_name} is {restart_count_after} "
