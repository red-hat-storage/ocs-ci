import logging
import pytest

from ocs_ci.ocs import constants, node
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
)
from ocs_ci.ocs.resources.pod import cal_md5sum

log = logging.getLogger(__name__)


@tier2
@pytest.mark.polarion_id("")
class TestRbdBlockPvc(ManageTest):
    """
    Tests RBD block PVC
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, pvc_factory, pod_factory):
        """
        Create PVC and pods

        """
        self.pvc_size = 5

        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=self.pvc_size,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
            volume_mode=constants.VOLUME_MODE_BLOCK,
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
            pod_obj.md5sum_before_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )
        md5sum_values_initial = [pod_obj.md5sum_before_io for pod_obj in self.pod_objs]
        assert (
            len(set(md5sum_values_initial)) == 1
        ), "Initial md5sum values from the pods are not same"

        # Run IO from one pod
        log.info("Run IO from one pod")
        self.pod_objs[0].run_io(
            storage_type="block",
            size="1G",
            io_direction="write",
            runtime=30,
            end_fsync=1,
        )
        log.info(f"IO started on pod {self.pod_objs[0].name}")

        # Wait for IO completion
        self.pod_objs[0].get_fio_results()
        log.info(f"IO completed on pod {self.pod_objs[0].name}")

        # Verify md5sum has changed after IO
        log.info("Verify md5sum has changed after IO. Verify from all pods.")
        for pod_obj in self.pod_objs:
            pod_obj.md5sum_after_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )
            assert (
                pod_obj.md5sum_before_io != pod_obj.md5sum_after_io
            ), (
                f"md5sum obtained from the pod {pod_obj.name} has not changed after IO. "
                f"IO was run from pod {self.pod_objs[0].name}"
            )
            log.info(
                f"md5sum obtained from the pod {pod_obj.name} has changed after IO"
            )

        # Verify the md5sum value obtained from all the pods are same
        md5sum_values_final = [pod_obj.md5sum_after_io for pod_obj in self.pods]
        assert (
            len(set(md5sum_values_final)) == 1
        ), "md5sum values from the pods after IO are not same"

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
        md5sum_new = cal_md5sum(
            pod_obj=pod_obj_new,
            file_name=pod_obj_new.get_storage_path(storage_type="block"),
            block=True,
        )
        assert md5sum_new == md5sum_values_final[0], "md5sum mismatch on new pod"

        # Run IO
        log.info("Run IO from one pod")
        pod_obj_new.run_io(
            storage_type="block",
            size="1G",
            io_direction="write",
            runtime=30,
            end_fsync=1,
        )
        log.info(f"IO started on pod {pod_obj_new.name}")

        # Wait for IO completion
        pod_obj_new.get_fio_results()
        log.info(f"IO completed on pod {pod_obj_new.name}")
