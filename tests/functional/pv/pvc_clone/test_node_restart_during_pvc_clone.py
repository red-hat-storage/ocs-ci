import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier4,
    tier4b,
    ignore_leftovers,
    polarion_id,
    skipif_bm,
    skipif_ocp_version,
    skipif_vsphere_ipi,
)

logger = logging.getLogger(__name__)


@green_squad
@tier4
@tier4b
@ignore_leftovers
@skipif_bm
@skipif_vsphere_ipi
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
class TestNodeRestartDuringPvcClone(ManageTest):
    """
    Tests to verify PVC cloning will succeed if a node is restarted while
    cloning is in progress.

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, pvc_clone_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=3, num_of_rbd_pvc=15, num_of_cephfs_pvc=10
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure the nodes are up

        """

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()
            logger.assertion(
                "Ceph cluster health: expected='OK', actual checked via ceph_health_check()"
            )
            assert ceph_health_check(), "Ceph cluster health is not OK"
            logger.info("Ceph cluster health is OK")

        request.addfinalizer(finalizer)

    @polarion_id("OCS-2373")
    def test_worker_node_restart_during_pvc_clone(
        self, nodes, pvc_clone_factory, pod_factory
    ):
        """
        Verify PVC cloning will succeed if a worker node is restarted
        while cloning is in progress

        """
        file_name = "fio_test"
        executor = ThreadPoolExecutor(max_workers=len(self.pvcs) + 1)
        selected_node = node.get_nodes(
            node_type=constants.WORKER_MACHINE, num_of_nodes=1
        )

        logger.test_step("Run IO on all pods")
        logger.info(f"Starting IO on {len(self.pods)} pods")
        for pod_obj in self.pods:
            storage_type = (
                "block"
                if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK
                else "fs"
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=20,
                fio_filename=file_name,
                end_fsync=1,
                direct=int(pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK),
            )
            logger.debug(f"IO started on pod {pod_obj.name}")
        logger.info("Started IO on all pods")

        logger.test_step("Wait for IO to finish and calculate md5sum")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            logger.debug(f"IO finished on pod {pod_obj.name}")
            # Calculate md5sum
            file_name_pod = (
                file_name
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM)
                else pod_obj.get_storage_path(storage_type="block")
            )
            pod_obj.pvc.md5sum = pod.cal_md5sum(
                pod_obj,
                file_name_pod,
                pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
        logger.info("IO finished and md5sum calculated on all pods")

        logger.test_step(
            f"Restart worker node {selected_node[0].name} and create PVC clones concurrently"
        )
        # Restart node
        logger.info(f"Restarting node {selected_node[0].name}")
        restart_thread = executor.submit(nodes.restart_nodes, nodes=selected_node)

        logger.info(f"Creating clones of all {len(self.pvcs)} PVCs")
        for pvc_obj in self.pvcs:
            logger.debug(f"Creating clone of {pvc_obj.name}")
            pvc_obj.clone_proc = executor.submit(
                pvc_clone_factory, pvc_obj=pvc_obj, status=""
            )

        # Check result of 'restart_nodes'
        restart_thread.result()

        logger.test_step("Verify node is ready and cloned PVCs are Bound")
        node.wait_for_nodes_status(
            node_names=[node.get_node_name(selected_node[0])],
            status=constants.NODE_READY,
            timeout=300,
        )

        # Get cloned PVCs
        cloned_pvcs = [pvc_obj.clone_proc.result() for pvc_obj in self.pvcs]

        logger.info(f"Verifying {len(cloned_pvcs)} cloned PVCs are Bound")
        for pvc_obj in cloned_pvcs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=540
            )
            pvc_obj.reload()
        logger.info("Verified: Cloned PVCs are Bound")

        logger.test_step("Attach cloned PVCs to pods and verify they are running")
        clone_pod_objs = []
        for pvc_obj in cloned_pvcs:
            if pvc_obj.volume_mode == "Block":
                pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            else:
                pod_dict_path = ""
            clone_pod_obj = pod_factory(
                interface=pvc_obj.parent.interface,
                pvc=pvc_obj,
                status="",
                pod_dict_path=pod_dict_path,
                raw_block_pv=pvc_obj.volume_mode == "Block",
            )
            logger.debug(
                f"Attaching the PVC {pvc_obj.name} to pod {clone_pod_obj.name}"
            )
            clone_pod_objs.append(clone_pod_obj)

        # Verify the new pods are running
        for pod_obj in clone_pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=120)
        logger.info(f"All {len(clone_pod_objs)} new pods are running")

        logger.test_step("Verify data integrity using md5sum on clone pods")
        for pod_obj in clone_pod_objs:
            file_name_pod = (
                pod_obj.get_storage_path(storage_type="block")
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK)
                else file_name
            )
            pod.verify_data_integrity(
                pod_obj,
                file_name_pod,
                pod_obj.pvc.parent.md5sum,
                pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
            logger.debug(
                f"Verified: md5sum of {file_name_pod} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )
        logger.info("Data integrity check passed on all pods")

        logger.test_step("Run IO on clone pods and verify completion")
        logger.info(f"Starting IO on {len(clone_pod_objs)} new pods")
        for pod_obj in clone_pod_objs:
            storage_type = (
                "block"
                if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK
                else "fs"
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=20,
                fio_filename=f"{file_name}_1",
                end_fsync=1,
                direct=int(pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK),
            )
            logger.debug(f"IO started on pod {pod_obj.name}")
        logger.info("Started IO on all new pods")

        # Wait for IO to finish
        for pod_obj in clone_pod_objs:
            pod_obj.get_fio_results()
            logger.debug(f"IO finished on pod {pod_obj.name}")
        logger.info("IO finished on all new pods")
