import logging

from ocs_ci.framework.testlib import ManageTest, tier1, green_squad, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_worker_nodes


logger = logging.getLogger(__name__)


@tier1
@green_squad
@polarion_id("OCS-6797")
class TestCephfsFsyncConsistency(ManageTest):
    """
    Ensuring File Completeness Post-fsync with Shared CephFS Volume

    """

    def test_cephfs_fsync_consistency(self, project_factory, pvc_factory, pod_factory):
        """
        Procedure:
            1.Set Up Shared Storage: Create a PVC with sc=cephfs and mode=RWX.
            2.Deploy Client Pod on Worker Node A, attaching the shared PVC.
            3.Deploy Server Pod on Worker Node B, attaching the same PVC.
            4.Client Pod write a file to the shared PVC, then call fsync to ensure data is flushed to disk.
            5.Server Pod Read the file
        """

        self.pvc_size = 10

        self.pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=self.pvc_size,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        worker_node_names = get_worker_nodes()

        pod_obj_client = pod_factory(
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_name="client",
            node_name=worker_node_names[0],
        )

        pod_obj_server = pod_factory(
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_name="server",
            node_name=worker_node_names[1],
        )

        command_client = (
            "bash -c "
            + '"for i in {1..2500}; do '
            + "echo "
            + "'Test sync '"
            + "  >> /var/lib/www/html/shared_file.html"
            + " && sync; "
            + 'done"'
        )
        pod_obj_client.exec_cmd_on_pod(
            command=command_client,
            out_yaml_format=False,
        )
        command_server = "bash -c " + '"cat ' + ' /var/lib/www/html/shared_file.html"'
        server_read_output = pod_obj_server.exec_cmd_on_pod(
            command=command_server,
            out_yaml_format=False,
        )
        count = server_read_output.count("Test sync")
        assert (
            count == 2500
        ), f"Expected 2500 occurrences of 'Test sync', but found {count}"
