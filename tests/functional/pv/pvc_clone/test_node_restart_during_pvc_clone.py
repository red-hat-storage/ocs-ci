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

log = logging.getLogger(__name__)


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
            assert ceph_health_check(), "Ceph cluster health is not OK"
            log.info("Ceph cluster health is OK")

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

        # Run IO
        log.info("Starting IO on all pods")
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
            )
            log.info(f"IO started on pod {pod_obj.name}")
        log.info("Started IO on all pods")

        # Wait for IO to finish
        log.info("Wait for IO to finish on pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            log.info(f"IO finished on pod {pod_obj.name}")
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

        # Restart node
        log.info(f"Restart node {selected_node[0].name}")
        restart_thread = executor.submit(nodes.restart_nodes, nodes=selected_node)

        log.info("Creating clone of all PVCs.")
        for pvc_obj in self.pvcs:
            log.info(f"Creating clone of {pvc_obj.name}")
            pvc_obj.clone_proc = executor.submit(
                pvc_clone_factory, pvc_obj=pvc_obj, status=""
            )

        # Check result of 'restart_nodes'
        restart_thread.result()

        log.info("Verify status of node.")
        node.wait_for_nodes_status(
            node_names=[node.get_node_name(selected_node[0])],
            status=constants.NODE_READY,
            timeout=300,
        )

        # Get cloned PVCs
        cloned_pvcs = [pvc_obj.clone_proc.result() for pvc_obj in self.pvcs]

        log.info("Verifying cloned PVCs are Bound")
        for pvc_obj in cloned_pvcs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=540
            )
            pvc_obj.reload()
        log.info("Verified: Cloned PVCs are Bound")

        # Attach the cloned PVCs to pods
        log.info("Attach the cloned PVCs to pods")
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
            log.info(f"Attaching the PVC {pvc_obj.name} to pod {clone_pod_obj.name}")
            clone_pod_objs.append(clone_pod_obj)

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in clone_pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        log.info("Verified: New pods are running")

        # Verify md5sum
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
            log.info(
                f"Verified: md5sum of {file_name_pod} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )
        log.info("Data integrity check passed on all pods")

        # Run IO
        log.info("Starting IO on the new pods")
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
            )
            log.info(f"IO started on pod {pod_obj.name}")
        log.info("Started IO on the new pods")

        # Wait for IO to finish
        log.info("Wait for IO to finish on the new pods")
        for pod_obj in clone_pod_objs:
            pod_obj.get_fio_results()
            log.info(f"IO finished on pod {pod_obj.name}")
        log.info("IO finished on the new pods")
