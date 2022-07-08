import logging
import random
import pytest

from time import sleep
from ocs_ci.ocs import constants, node
from ocs_ci.framework.testlib import E2ETest, tier1, bugzilla, polarion_id
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.node import get_worker_nodes
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@tier1
@bugzilla("2075068")
@polarion_id("OCS-3947")
class TestKernelCrash(E2ETest):
    """
    Tests to verify kernel crash
    """

    result_dir = constants.MOUNT_POINT + "/mydir"
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
                "for ((i=1;i<=%d;i++));rm -f %s/emp_$i.txt ; done"
                % (self.END, self.result_dir)
            )

    @pytest.mark.parametrize(
        argnames="interface_type",
        argvalues=[
            pytest.param(
                *[constants.CEPHFILESYSTEM],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL],
            ),
        ],
    )
    def test_node_kernel_crash_ceph_fsync(
        self, pvc_factory, teardown_factory, pod_factory, interface_type
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

        # Set interface argument for reference
        pvc_obj.interface = interface_type

        # Create a pod on a particular node
        selected_node = random.choice(worker_nodes_list)
        log.info(f"Creating a pod on node: {selected_node} with pvc {pvc_obj.name}")

        pod_obj = pod_factory(
            interface=interface_type,
            pvc=pvc_obj,
            pod_dict_path=constants.NGINX_POD_YAML,
        )

        file = constants.FSYNC
        cmd = f"oc cp {file} {pvc_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        log.info("Files copied successfully ")

        commands = [
            f"mkdir {self.result_dir}",
            "apt-get update",
        ]
        for cmd in commands:
            pod_obj.exec_cmd_on_pod(command=cmd)
        pod_obj.exec_sh_cmd_on_pod(command="apt-get install python -y")
        log.info("Starting creation and deletion of files on volume")

        # Create and delete files on mount point
        create_executor = ThreadPoolExecutor(max_workers=1)
        create_executor.submit(self.creates_files, pod_obj)
        sleep(3)

        log.info("Started deletion of files on volume")
        delete_executor = ThreadPoolExecutor(max_workers=1)
        delete_executor.submit(self.remove_files, pod_obj)

        ThreadPoolExecutor(max_workers=1).submit(
            pod_obj.exec_sh_cmd_on_pod, command="python fsync.py"
        )

        # Check Node gets Panic or not
        try:
            node.wait_for_nodes_status(
                selected_node, status=constants.NODE_NOT_READY, timeout=60
            )

        except ResourceWrongStatusException:
            log.info("Node in Ready status found, hence TC is Passed.")
        else:
            assert "Node in NotReady status found due to it gets panic, hence TC is failed."
