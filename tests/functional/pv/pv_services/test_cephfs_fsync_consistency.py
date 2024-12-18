import logging

from ocs_ci.framework.testlib import ManageTest, tier2, bugzilla, green_squad
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_worker_nodes


logger = logging.getLogger(__name__)


@tier2
@bugzilla("2212310")
@green_squad
class TestCephfsFsyncConsistency(ManageTest):
    """
    Ensuring File Completeness Post-fsync with Shared CephFS Volume

    """

    def test_cephfs_fsync_consistency(self, teardown_project_factory):
        """
        Procedure:
            1.Set Up Shared Storage: Create a PVC with sc=cephfs and mode=RWX.
            2.Deploy Client Pod on Worker Node A, attaching the shared PVC.
            3.Deploy Server Pod on Worker Node B, attaching the same PVC.
            4.Client Pod write a file to the shared PVC, then call fsync to ensure data is flushed to disk.
            5.Server Pod Read the file
        """
        project_name = "cephfs-rwx-ns"
        pod_name = "cephfs-rwx-pod"
        project_obj = helpers.create_project(project_name=project_name)
        worker_node_names = get_worker_nodes()
        teardown_project_factory(project_obj)
        logger.info(
            f"Create new pvc sc_name={constants.CEPHFILESYSTEM_SC} namespace={project_name}, "
            f"size=6Gi, access_mode={constants.ACCESS_MODE_RWX}"
        )
        pvc_obj = helpers.create_pvc(
            sc_name=constants.CEPHFILESYSTEM_SC,
            namespace=project_name,
            size="6Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        logger.info(
            f"Create new pod. Pod name={pod_name},"
            f"interface_type={constants.CEPHBLOCKPOOL}"
        )
        pod_obj_client = helpers.create_pod(
            pvc_name=pvc_obj.name,
            namespace=project_obj.namespace,
            pod_name="client",
            node_name=worker_node_names[0],
        )
        pod_obj_server = helpers.create_pod(
            pvc_name=pvc_obj.name,
            namespace=project_obj.namespace,
            pod_name="server",
            node_name=worker_node_names[1],
        )
        logger.info("Wait for pods move to Running state")
        helpers.wait_for_resource_state(
            pod_obj_client, state=constants.STATUS_RUNNING, timeout=300
        )
        helpers.wait_for_resource_state(
            pod_obj_server, state=constants.STATUS_RUNNING, timeout=300
        )
        # storage_path = pod_obj_client.get_storage_path()
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
