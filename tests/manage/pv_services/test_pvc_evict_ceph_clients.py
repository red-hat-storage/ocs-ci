import logging
import pytest
import random
import json

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.testlib import tier2
from ocs_ci.framework.pytest_customization.marks import skipif_ocs_version, green_squad
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

logger = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.11")
class TestPvcEvictCephClients:
    """
    Test the ceph-fs PVC (RWX) mount with the following scenarios involving evict ceph-fs clients:
    1. When two pods tries to mount the same PVC and scheduled on same node
    2. When two pods tries to mount the same PVC and scheduled on different node
    """

    @pytest.mark.parametrize(
        argnames=["node"],
        argvalues=[
            pytest.param(
                "same",
                marks=[
                    pytest.mark.bugzilla("1901499"),
                    pytest.mark.polarion_id("OCS-3985"),
                ],
            ),
            pytest.param(
                "different",
                marks=[pytest.mark.polarion_id("OCS-3984")],
            ),
        ],
    )
    def test_pvc_evict_ceph_clients(self, node, pvc_factory, pod_factory):
        worker_nodes = get_worker_nodes()

        # create a RWX PVC
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        # create a pod in particular node
        selected_node = random.choice(worker_nodes)
        logger.info(f"creating first pod on node {selected_node}")
        pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj,
            node_name=selected_node,
            pod_dict_path=constants.NGINX_POD_YAML,
            status=constants.STATUS_RUNNING,
        )

        # fetch the sub-volume path
        pvc_obj.reload()
        sub_volume_path = OCP(kind="pv").get(resource_name=pvc_obj.backed_pv)["spec"][
            "csi"
        ]["volumeAttributes"]["subvolumePath"]

        # evict ceph-fs clients
        logger.info("Evicting ceph-fs clients!")
        ceph_tools_pod_obj = get_ceph_tools_pod()
        cmd_ls = "ceph tell mds.0 client ls"
        output_ls_cmd = json.loads(
            ceph_tools_pod_obj.exec_sh_cmd_on_pod(command=cmd_ls)
        )
        ids = list()
        for instance in output_ls_cmd:
            if instance["client_metadata"]["root"] == sub_volume_path:
                ids.append(instance["id"])
        logger.info(f"Client Ids to evict: {ids}")
        try:
            for id in ids:
                cmd_evict = f"ceph tell mds.0 client evict id={id}"
                ceph_tools_pod_obj.exec_sh_cmd_on_pod(command=cmd_evict)
        except CommandFailed as e:
            raise ValueError(f"[Error] Client eviction failed: {e}")
        else:
            logger.info("Clients are evicted successfully!")

        # running second pod on same/different node
        if node == "different":
            worker_nodes.remove(selected_node)
            selected_node = random.choice(worker_nodes)

        logger.info(f"Running second pod on {selected_node} node")
        second_pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj,
            node_name=selected_node,
            pod_dict_path=constants.NGINX_POD_YAML,
            status=constants.STATUS_RUNNING,
        )

        # Run some IOs on second pod
        second_pod_obj.run_io(
            storage_type="fs", size="200M", io_direction="rw", runtime=10
        )
        try:
            second_pod_obj.get_fio_results()
        except CommandFailed as e:
            logger.exception(f"IO failed: {e}")
            raise
        else:
            logger.info("FIO is successful!!")
