import logging

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

        # # Copy write_to_file inside the client pod
        # exec_cmd(
        #     cmd=f"oc cp {constants.WRITE_TO_FILE_USING_FSYNC} {pod_obj_client.name}:/tmp"
        # )
        #
        # command_client = f"python3 /tmp/write_to_file.py {project_obj.namespace} {pod_obj_server.name}"
        # pod_obj_client.exec_cmd_on_pod(
        #     command=command_client, out_yaml_format=False, timeout=1800
        # )

        command = (
            f"python3 {constants.WRITE_TO_FILE_USING_FSYNC} "
            f"{project_obj.namespace} {pod_obj_client.name} {pod_obj_server.name}"
        )
        exec_cmd(cmd=command)

        command = "cat /mnt/shared_filed.html"
        server_read_output = pod_obj_server.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )

        count = server_read_output.count("Test fsync")
        assert (
            count == 2500
        ), f"Expected 2500 occurrences of 'Test sync', but found {count}"

        client_read_output = pod_obj_server.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        count = client_read_output.count("Test fsync")
        assert (
            count == 2500
        ), f"Expected 2500 occurrences of 'Test sync', but found {count}"
