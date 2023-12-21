import logging
import pytest
import random

from ocs_ci.framework import config
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    acceptance,
    bugzilla,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources import pod as res_pod
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


@green_squad
class TestPvcAssignPodNode(ManageTest):
    """
    Automates the following test cases:
    OCS-717 - RBD: Assign nodeName to a POD using RWO PVC
    OCS-744 - CephFS: Assign nodeName to a POD using RWO PVC
    OCS-1258 - CephFS: Assign nodeName to a POD using RWX PVC
    OCS-1257 - RBD: Assign nodeName to a POD using RWX PVC
    """

    def verify_access_token_notin_odf_pod_logs(self):
        """
        This function will verify logs of kube-rbac-proxy container in odf-operator-controller-manager pod
        shouldn't contain api access token
        """
        odf_operator_pod_objs = res_pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector_label="app.kubernetes.io/name",
            selector=[constants.ODF_SUBSCRIPTION],
        )
        error_msg = "Authorization: Bearer"
        pod_log = res_pod.get_pod_logs(
            pod_name=odf_operator_pod_objs[0].name, container="kube-rbac-proxy"
        )
        assert not (
            error_msg in pod_log
        ), f"Logs should not contain the error message '{error_msg}'"

    @acceptance
    @bugzilla("2136852")
    @tier1
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-717")
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-744")
            ),
        ],
    )
    def test_rwo_pvc_assign_pod_node(self, interface, pvc_factory, teardown_factory):
        """
        Test assign nodeName to a pod using RWO pvc
        """
        worker_nodes_list = get_worker_nodes()

        # Create a RWO PVC
        pvc_obj = pvc_factory(
            interface=interface,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )

        # Create a pod on a particular node
        selected_node = random.choice(worker_nodes_list)
        logger.info(f"Creating a pod on node: {selected_node} with pvc {pvc_obj.name}")

        pod_obj = helpers.create_pod(
            interface_type=interface,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace,
            node_name=selected_node,
            pod_dict_path=constants.NGINX_POD_YAML,
        )
        teardown_factory(pod_obj)

        # Confirm that the pod is running on the selected_node
        timeout = 120
        if (
            config.ENV_DATA["platform"].lower()
            in constants.HCI_PROVIDER_CLIENT_PLATFORMS
        ):
            timeout = 180
        helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=timeout
        )
        pod_obj.reload()
        assert pod.verify_node_name(
            pod_obj, selected_node
        ), "Pod is running on a different node than the selected node"

        # Run IO
        logger.info(f"Running IO on pod {pod_obj.name}")
        pod_obj.run_io(storage_type="fs", size="512M", runtime=30, invalidate=0)
        pod.get_fio_rw_iops(pod_obj)

        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_12 and config.ENV_DATA.get(
            "platform"
        ) not in constants.HCI_PROVIDER_CLIENT_PLATFORMS + [
            constants.FUSIONAAS_PLATFORM
        ]:
            self.verify_access_token_notin_odf_pod_logs()

    @acceptance
    @tier1
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-1257")
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-1258")
            ),
        ],
    )
    def test_rwx_pvc_assign_pod_node(self, interface, pvc_factory, teardown_factory):
        """
        Test assign nodeName to a pod using RWX pvc
        """
        worker_nodes_list = get_worker_nodes()
        if interface == constants.CEPHBLOCKPOOL:
            volume_mode = "Block"
            storage_type = "block"
            block_pv = True
            pod_yaml = constants.CSI_RBD_RAW_BLOCK_POD_YAML
        else:
            volume_mode = ""
            storage_type = "fs"
            block_pv = False
            pod_yaml = ""

        # Create a RWX PVC
        pvc_obj = pvc_factory(
            interface=interface,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
            volume_mode=volume_mode,
        )

        # Create two pods on selected nodes
        pod_list = []
        selected_nodes = random.sample(worker_nodes_list, k=2)
        logger.info(f"Creating {len(selected_nodes)} pods with pvc {pvc_obj.name}")
        for node in selected_nodes:
            logger.info(f"Creating pod on node: {node}")
            pod_obj = helpers.create_pod(
                interface_type=interface,
                pvc_name=pvc_obj.name,
                namespace=pvc_obj.namespace,
                node_name=node,
                pod_dict_path=pod_yaml,
                raw_block_pv=block_pv,
            )
            pod_list.append(pod_obj)
            teardown_factory(pod_obj)

        # Confirm that both pods are running on the selected_nodes
        logger.info("Checking whether pods are running on the selected nodes")
        for index in range(0, len(selected_nodes)):
            pod_obj = pod_list[index]
            selected_node = selected_nodes[index]
            timeout = 120
            if (
                config.ENV_DATA["platform"].lower()
                in constants.HCI_PROVIDER_CLIENT_PLATFORMS
            ):
                timeout = 180
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=timeout
            )
            pod_obj.reload()
            assert pod.verify_node_name(pod_obj, selected_node), (
                f"Pod {pod_obj.name} is running on a different node "
                "than the selected node"
            )

        # Run IOs on all pods. FIO Filename is kept same as pod name
        with ThreadPoolExecutor() as p:
            for pod_obj in pod_list:
                logger.info(f"Running IO on pod {pod_obj.name}")
                p.submit(
                    pod_obj.run_io,
                    storage_type=storage_type,
                    size="512M",
                    runtime=30,
                    fio_filename=pod_obj.name,
                    invalidate=0,
                )

        # Check IO from all pods
        for pod_obj in pod_list:
            pod.get_fio_rw_iops(pod_obj)

        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_12 and config.ENV_DATA.get(
            "platform"
        ) not in constants.HCI_PROVIDER_CLIENT_PLATFORMS + [
            constants.FUSIONAAS_PLATFORM
        ]:
            self.verify_access_token_notin_odf_pod_logs()
