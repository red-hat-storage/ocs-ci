import logging
import pytest

from ocs_ci.ocs import constants, node
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2

log = logging.getLogger(__name__)


@green_squad
@tier2
@pytest.mark.polarion_id("OCS-2599")
class TestRbdBlockPvc(ManageTest):
    """
    Tests RBD block PVC
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, pvc_factory, pod_factory):
        """
        Create PVC and pods

        """
        self.pvc_size = 10

        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=self.pvc_size,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
            volume_mode=constants.VOLUME_MODE_BLOCK,
            size_unit="Mi",
        )

        worker_nodes_list = node.get_worker_nodes()

        self.pod_objs = []
        for node_name in worker_nodes_list:
            pod_obj = pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                pvc=self.pvc_obj,
                status=constants.STATUS_RUNNING,
                node_name=node_name,
                pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
                raw_block_pv=True,
            )
            self.pod_objs.append(pod_obj)

    def test_rbd_block_rwx_pvc(self, pod_factory):
        """
        Test RBD Block volume mode RWX PVC

        """
        # Find initial md5sum value
        log.info("Find initial md5sum value")
        for pod_obj in self.pod_objs:
            # Find initial md5sum
            pod_obj.md5sum_before_io = pod_obj.exec_sh_cmd_on_pod(
                command=f"dd iflag=direct if={pod_obj.get_storage_path(storage_type='block')} | md5sum"
            )
        md5sum_values_initial = [pod_obj.md5sum_before_io for pod_obj in self.pod_objs]
        assert (
            len(set(md5sum_values_initial)) == 1
        ), "Initial md5sum values from the pods are not same"
        md5sum_value_initial = md5sum_values_initial[0]

        # Run IO from each pod and verify md5sum on all pods
        for io_pod in self.pod_objs:
            # Run IO from one pod
            log.info("Run IO from one pod")
            io_pod.run_io(
                storage_type="block",
                size=f"{int(self.pvc_size/2)}M",
                io_direction="write",
                runtime=5,
                end_fsync=1,
            )
            log.info(f"IO started on pod {io_pod.name}")

            # Wait for IO completion
            io_pod.get_fio_results()
            log.info(f"IO completed on pod {io_pod.name}")

            # Verify md5sum has changed after IO
            log.info("Verify md5sum has changed after IO. Verify from all pods.")
            for pod_obj in self.pod_objs:
                # Find md5sum
                pod_obj.md5sum_after_io = pod_obj.exec_sh_cmd_on_pod(
                    command=f"dd iflag=direct if={pod_obj.get_storage_path(storage_type='block')} | md5sum"
                )
                assert pod_obj.md5sum_after_io != md5sum_value_initial, (
                    f"md5sum obtained from the pod {pod_obj.name} has not changed after IO. "
                    f"IO was run from pod {io_pod.name}"
                )
                log.info(
                    f"md5sum obtained from the pod {pod_obj.name} has changed after IO from pod {io_pod.name}"
                )

            # Verify the md5sum value obtained from all the pods are same
            md5sum_values_final = [pod_obj.md5sum_after_io for pod_obj in self.pod_objs]
            assert (
                len(set(md5sum_values_final)) == 1
            ), f"md5sum values from the pods after IO are not same-{md5sum_values_final}"
            log.info(
                f"md5sum value obtained from all pods after running IO"
                f" from {io_pod.name} are same - {md5sum_values_final}"
            )
            md5sum_value_initial = md5sum_values_final[0]

        # Delete pods
        log.info("Deleting the pods")
        for pod_obj in self.pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        log.info("Deleted all the pods")

        pod_obj_new = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
            raw_block_pv=True,
        )

        # Find md5sum value and compare
        log.info("Find md5sum value from new pod")
        md5sum_new = pod_obj_new.exec_sh_cmd_on_pod(
            command=f"dd iflag=direct if={pod_obj_new.get_storage_path(storage_type='block')} | md5sum"
        )
        assert (
            md5sum_new == md5sum_value_initial
        ), f"md5sum mismatch on new pod. Expected {md5sum_value_initial}. Obtained {md5sum_new}"

        # Run IO from new pod
        log.info("Run IO from new pod")
        pod_obj_new.run_io(
            storage_type="block",
            size=f"{int(self.pvc_size/2)}M",
            io_direction="write",
            runtime=30,
            end_fsync=1,
        )
        log.info(f"IO started on the new pod {pod_obj_new.name}")

        # Wait for IO completion
        pod_obj_new.get_fio_results()
        log.info(f"IO completed on the new pod {pod_obj_new.name}")
