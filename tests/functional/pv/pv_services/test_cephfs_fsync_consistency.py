import logging
import tempfile

from ocs_ci.framework.testlib import ManageTest, tier1, green_squad, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import exec_cmd
from ocs_ci.ocs.node import get_worker_nodes


logger = logging.getLogger(__name__)


@tier1
@green_squad
@polarion_id("OCS-6797")
class TestCephfsFsyncConsistency(ManageTest):
    """
    Ensuring File Completeness Post-fsync with Shared CephFS Volume

    """

    def write_and_read(self, client_pod_obj, server_pod_obj):
        """

        Function for line-based I/O(write and read) between client and server pods.

        Args:
            client_pod_obj (obj): Client pod object
            server_pod_obj (obj): Server pod object

        """

        script_content = (
            "import os, sys\n"
            "filename = '/mnt/shared_filed.html'\n"
            "i = int(sys.argv[1])\n"
            "with open(filename, 'a') as f:\n"
            "    line = f'Test fsync {i}'\n"
            "    f.write(line + '\\n')\n"
            "    f.flush()\n"
            "    os.fsync(f.fileno())\n"
        )

        # Write the script content to a temporary local file
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".py"
        ) as tmp_script:
            tmp_script.write(script_content.strip())
            local_script_path = tmp_script.name
        try:
            exec_cmd(
                cmd=f"oc cp {local_script_path} {client_pod_obj.name}:/tmp/sync_script.py"
            )
        except Exception as e:
            logger.error(f"Failed to copy sync script to pod: {e}")

        logger.info(
            "Starting 1000 write-and-read cycles between client and server pods"
        )
        for i in range(1, 1001):
            # Write lines to client pod
            execution_command = f"python3 /tmp/sync_script.py {i}"
            client_pod_obj.exec_cmd_on_pod(
                command=execution_command, out_yaml_format=False
            )
            logger.debug(f"Wrote line to client pod: Test fsync {i}")

            # Read line from server pod
            self.server_read_output.append(
                server_pod_obj.exec_cmd_on_pod(
                    command="tail -n 1 /mnt/shared_filed.html"
                )
            )
            logger.debug(f"Read line from server pod: Test fsync {i}")

    def test_cephfs_fsync_consistency(
        self,
        project_factory,
        pvc_factory,
        service_account_factory,
        deployment_pod_factory,
    ):
        """
        Procedure:
            1.Set Up Shared Storage: Create a PVC with sc=cephfs and mode=RWX.
            2.Deploy Client Pod on Worker Node A, attaching the shared PVC.
            3.Deploy Server Pod on Worker Node B, attaching the same PVC.
            4.Client Pod write a file to the shared PVC, then call fsync to ensure data is flushed to disk.
            5.Server Pod Read the file
        """

        logger.test_step("Create CephFS PVC with RWX access mode")
        self.pvc_size = 10
        self.server_read_output = []

        project_obj = project_factory()

        self.pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=self.pvc_size,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        logger.test_step("Deploy client and server pods on separate worker nodes")
        worker_node_names = get_worker_nodes()

        pod_obj_client = deployment_pod_factory(
            pvc=self.pvc_obj,
            node_name=worker_node_names[0],
        )

        pod_obj_server = deployment_pod_factory(
            pvc=self.pvc_obj,
            node_name=worker_node_names[1],
        )

        logger.test_step(
            "Perform 1000 write-and-read cycles with fsync between client and server pods"
        )
        self.write_and_read(pod_obj_client, pod_obj_server)

        logger.test_step("Verify all 1000 lines were read correctly by the server pod")
        logger.assertion(
            f"Server read output count: expected=1000, actual={len(self.server_read_output)}"
        )
        assert (
            len(self.server_read_output) == 1000
        ), f"Failed validation: expected 1000 got {len(self.server_read_output)}"
        last_string = self.server_read_output[-1]
        logger.assertion(
            f"Last line content: expected='Test fsync 1000', actual='{last_string}'"
        )
        assert (
            last_string == "Test fsync 1000"
        ), f"Last line doesn't match, last line {last_string}, expected: Test fsync 1000"
