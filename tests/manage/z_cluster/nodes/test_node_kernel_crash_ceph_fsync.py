import logging
import random
import pytest

from time import sleep
from ocs_ci.ocs import constants, node
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import E2ETest, tier1, bugzilla
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.node import get_worker_nodes
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@brown_squad
@tier1
@bugzilla("2075068")
class TestKernelCrash(E2ETest):
    """
    Tests to verify kernel crash
    """

    result_dir = constants.FLEXY_MNT_CONTAINER_DIR + "/mydir"
    END = 125

    def creates_files(self, pod_obj):
        while True:
            pod_obj.exec_sh_cmd_on_pod(
                "for ((i=1;i<=%d;i++)); do dd if=/dev/zero of=%s/emp_$i.txt bs=1M count=1; done"
                % (self.END, self.result_dir)
            )

    def remove_files(self, pod_obj):
        while True:
            pod_obj.exec_sh_cmd_on_pod(
                "for ((i=1;i<=%d;i++));do rm -f %s/emp_$i.txt ; done"
                % (self.END, self.result_dir)
            )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            if self.create_thread:
                self.create_thread.cancel()
            if self.delete_thread:
                self.delete_thread.cancel()
            if self.fsync_thread:
                self.fsync_thread.cancel()
            request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames="interface_type",
        argvalues=[
            pytest.param(
                *[constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id("OCS-3947"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2597"),
            ),
        ],
    )
    def test_node_kernel_crash_ceph_fsync(
        self, pvc_factory, teardown_factory, dc_pod_factory, interface_type
    ):
        """
        1. Create 1GiB PVC
        2. Attach PVC to an application pod
        3. Copy file fsync.py to pod
        4. Execute create delete file operation parallely with fsync.py
        5. Check Node gets Panic or not
        """

        worker_nodes_list = get_worker_nodes()

        # Create a Cephfs, rbd PVC
        pvc_obj = pvc_factory(
            interface=interface_type,
        )

        # Create a pod on a particular node
        selected_node = random.choice(worker_nodes_list)
        log.info(f"Creating a pod on node: {selected_node} with pvc {pvc_obj.name}")

        pod_obj = dc_pod_factory(
            interface=interface_type,
            pvc=pvc_obj,
        )

        file = constants.FSYNC
        cmd = f"oc cp {file} {pvc_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        log.info("Files copied successfully ")

        command = f"mkdir {self.result_dir}"
        pod_obj.exec_cmd_on_pod(command=command)
        log.info("Starting creation and deletion of files on volume")

        # Create and delete files on mount point
        create_executor = ThreadPoolExecutor(max_workers=1)
        self.create_thread = create_executor.submit(self.creates_files, pod_obj)
        sleep(3)

        log.info("Started deletion of files on volume")
        delete_executor = ThreadPoolExecutor(max_workers=1)
        self.delete_thread = delete_executor.submit(self.remove_files, pod_obj)

        fsync_executor = ThreadPoolExecutor(max_workers=1)
        self.fsync_thread = fsync_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 fsync.py"
        )

        # Check Node gets Panic or not
        try:
            node.wait_for_nodes_status(
                selected_node, status=constants.NODE_NOT_READY, timeout=60
            )

        except ResourceWrongStatusException:
            log.info(f"(No kernel panic observed on {selected_node})")
        else:
            assert f"({selected_node} is in Not Ready state)"
