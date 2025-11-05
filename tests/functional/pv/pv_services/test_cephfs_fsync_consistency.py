import logging
import tempfile
import textwrap

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
        """ """
        filename = "/mnt/shared_filed.html"
        data = "Test fsync "
        lines_to_write = [f"{data}{i}\n" for i in range(1, 2501)]
        logger.info("Writing lines and syncing...")
        try:
            for line in lines_to_write:
                self.client_write_lines(
                    client_pod_obj=client_pod_obj, filename=filename, data=line
                )
                logger.info(f"Wrote line: {line}")
                self.server_read_output.append(
                    self.read_lines_from_server(
                        server_pod_obj=server_pod_obj, filename=filename
                    )
                )
                logger.info(f"Line read correctly: {line}")
        except Exception as e:
            logger.error(f"Error during write and read of file: {str(e)}")

        logger.info("Finished writing to and reading from file")

    def client_write_lines(self, client_pod_obj, filename, data):
        """
        Function for writing lines to a specified file path within the pod.

        Args:
            client_pod_obj (obj): Client pod object
            filename (str): File path

        """

        script_content = (
            "import os\n"
            f"with open('{filename}', 'a') as f:\n"
            f"    f.write({repr(data)})\n"
            f"    f.flush()\n"
            f"    os.fsync(f.fileno())\n"
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
            execution_command = "python3 /tmp/sync_script.py"
            client_pod_obj.exec_cmd_on_pod(
                command=execution_command, out_yaml_format=False
            )
        except Exception as e:
            logger.error("ERROR: Failed to execute command on pod.")
            logger.error(f"Error details: {e}")

    def read_lines_from_server(self, server_pod_obj, filename):
        """
        Function for read lines from a specified file path within the pod.

        Args:
            server_pod_obj (obj): Client pod object
            filename (str): File path

        """

        script_content = textwrap.dedent(
            f"""\
        import os
        with open('{filename}', 'r') as f:
            print(f.readlines())
        """
        )
        # Write the script content to a temporary local file
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".py"
        ) as tmp_script:
            tmp_script.write(script_content.strip())
            local_script_path = tmp_script.name
        try:
            exec_cmd(
                cmd=f"oc cp {local_script_path} {server_pod_obj.name}:/tmp/sync_read.py"
            )
            execution_command = "python3 /tmp/sync_read.py"
            return server_pod_obj.exec_cmd_on_pod(
                command=execution_command, out_yaml_format=False
            )
        except Exception as e:
            logger.error("ERROR: Failed to execute command on pod.")
            logger.error(f"Error details: {e}")

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

        worker_node_names = get_worker_nodes()

        pod_obj_client = deployment_pod_factory(
            pvc=self.pvc_obj,
            node_name=worker_node_names[0],
        )

        pod_obj_server = deployment_pod_factory(
            pvc=self.pvc_obj,
            node_name=worker_node_names[1],
        )

        self.write_and_read(pod_obj_client, pod_obj_server)

        assert (
            len(self.server_read_output) == 2500
        ), f"Failed validation: expected 2500 got {len(self.server_read_output)}"
        last_string = (
            self.server_read_output[-1]
            .strip()
            .strip("[]")
            .replace("'", "")
            .split(", ")[-1]
            .strip()
        )
        assert (
            last_string == "Test fsync 2500\\n"
        ), f"Last line doesn't match, last line {last_string}, expected: Test fsync 2500\n"
