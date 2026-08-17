"""
Tests for user namespace support with CephFS shared storage.
"""

import logging
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest, tier1, polarion_id
from ocs_ci.ocs import constants, node
from ocs_ci.helpers.helpers import (
    create_userns_project,
    create_userns_pod,
    verify_file_ownership,
    wait_for_resource_state,
)

logger = logging.getLogger(__name__)

CONTAINER_UID = 10900
CONTAINER_GID = 10900
SUPPLEMENTAL_GROUPS_BASE = 10000
USERNS_UID_RANGE = "10000/1000"


@tier1
@green_squad
@skipif_ocs_version("<4.22")
class TestUserNamespaceCephFS(ManageTest):
    """
    Verify CephFS I/O under user namespaces (hostUsers=false)
    with UID remapping and data persistence.
    """

    @polarion_id("OCS-8224")
    def test_user_namespace_shared_io_and_uid_remapping(
        self,
        teardown_project_factory,
        pvc_factory,
        teardown_factory,
    ):
        """
        Two pods on different workers share a CephFS RWX volume
        under user namespace isolation.

        Steps:
            1. Create project with restricted PSA and UID
               annotations.
            2. Create CephFS RWX PVC (1Gi).
            3-5. Deploy two pods on different workers with
                 hostUsers=false.
            6. Verify both pods are Running.
            7. Write file from pod-1, verify container uid.
            8. Read file from pod-2, write another file.
            9. Read second file from pod-1 (cross-pod I/O).
            10-13. Assert host UID is remapped on worker-1.
            14. Assert different remapped UID on worker-2.
        """
        logger.info("Step 1: Create project with restricted PSA")
        project_obj = create_userns_project(uid_range=USERNS_UID_RANGE)
        teardown_project_factory(project_obj)
        ns_name = project_obj.namespace
        logger.info("Step 2: Create CephFS RWX PVC (1Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=1,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")
        logger.info("Steps 3-6: Deploy pods with hostUsers=false")
        worker_nodes = node.get_worker_nodes()
        assert len(worker_nodes) >= 2, (
            f"Need >= 2 worker nodes, " f"found {len(worker_nodes)}"
        )
        pods = []
        for i in range(2):
            p = create_userns_pod(
                pvc_name=pvc_obj.name,
                namespace=ns_name,
                node_name=worker_nodes[i],
            )
            teardown_factory(p)
            pods.append(p)

        for p in pods:
            wait_for_resource_state(
                resource=p,
                state=constants.STATUS_RUNNING,
                timeout=300,
            )

        pod1, pod2 = pods
        node1 = pod1.get()["spec"]["nodeName"]
        node2 = pod2.get()["spec"]["nodeName"]
        logger.info(f"Pod {pod1.name} on {node1}, " f"Pod {pod2.name} on {node2}")
        assert node1 != node2, f"Both pods on same node: {node1}"
        logger.info("Step 7: Write from pod-1 and read back")
        out = pod1.exec_sh_cmd_on_pod(
            "id && echo testing > /mnt/test/a " "&& cat /mnt/test/a && sync"
        )
        logger.info(f"Pod-1 output: {out}")
        assert f"uid={CONTAINER_UID}" in out, (
            f"Expected uid={CONTAINER_UID} in id output, " f"got: {out}"
        )
        assert "testing" in out, f"Expected 'testing' in output: {out}"
        logger.info("Step 8: Read from pod-2 and write new file")
        out = pod2.exec_sh_cmd_on_pod(
            "cat /mnt/test/a " "&& echo testing_again > /mnt/test/b && sync"
        )
        logger.info(f"Pod-2 output: {out}")
        assert "testing" in out, f"Pod-2 could not read file from pod-1: {out}"
        logger.info("Step 9: Cross-read from pod-1")
        out = pod1.exec_sh_cmd_on_pod("cat /mnt/test/b")
        logger.info(f"Pod-1 cross-read: {out}")
        assert "testing_again" in out, f"Pod-1 could not read file from pod-2: {out}"
        logger.info("Shared I/O validation passed")
        logger.info(f"Steps 10-13: Verify UID remapping on {node1}")
        host_uid_1 = node.get_host_uid_for_pod(node1, pod1.name)
        assert host_uid_1 != CONTAINER_UID, (
            f"Host UID {host_uid_1} equals container UID "
            f"{CONTAINER_UID} — remapping not active "
            f"on {node1}"
        )
        logger.info(
            f"Remapping confirmed on {node1}: "
            f"host UID {host_uid_1} != {CONTAINER_UID}"
        )
        logger.info(f"Step 14: Verify UID remapping on {node2}")
        host_uid_2 = node.get_host_uid_for_pod(node2, pod2.name)
        assert host_uid_2 != CONTAINER_UID, (
            f"Host UID {host_uid_2} equals container UID "
            f"{CONTAINER_UID} — remapping not active "
            f"on {node2}"
        )
        assert host_uid_1 != host_uid_2, (
            f"Host UIDs should differ across nodes: "
            f"worker-1={host_uid_1}, "
            f"worker-2={host_uid_2}"
        )
        logger.info(
            f"Remapping confirmed on {node2}: "
            f"host UID {host_uid_2} != {CONTAINER_UID} "
            f"and != worker-1 UID {host_uid_1}"
        )
        logger.info("User namespace shared I/O and UID remapping " "test passed")

    @polarion_id("OCS-8225")
    def test_user_namespace_rwo_ownership_and_persistence(
        self,
        teardown_project_factory,
        pvc_factory,
        teardown_factory,
    ):
        """
        Single pod with CephFS RWO volume under user namespace.
        Verifies file ownership, UID remapping, and data
        persistence across pod restart.

        Steps:
            1. Create project with restricted PSA and UID
               annotations.
            2. Create CephFS RWO PVC (1Gi).
            3. Deploy pod with hostUsers=false.
            4. Verify container uid=10900.
            5. Write file and read back.
            6. Verify file content.
            7. Verify file ownership (uid=10900,
               gid=10000 supplemental-groups base).
            8. Verify host UID is remapped.
            9. Delete pod.
            10. Recreate pod with same PVC.
            11. Verify data persists.
            12. Verify ownership persists.
        """
        logger.info("Step 1: Create project with restricted PSA")
        project_obj = create_userns_project(uid_range=USERNS_UID_RANGE)
        teardown_project_factory(project_obj)
        ns_name = project_obj.namespace
        logger.info("Step 2: Create CephFS RWO PVC (1Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=1,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")
        logger.info("Step 3: Deploy pod with hostUsers=false")
        worker_nodes = node.get_worker_nodes()
        assert len(worker_nodes) >= 1, "No worker nodes"
        target_node = worker_nodes[0]

        pod_obj = create_userns_pod(
            pvc_name=pvc_obj.name,
            namespace=ns_name,
            node_name=target_node,
        )
        teardown_factory(pod_obj)
        wait_for_resource_state(
            resource=pod_obj,
            state=constants.STATUS_RUNNING,
            timeout=300,
        )
        pod_node = pod_obj.get()["spec"]["nodeName"]
        logger.info(f"Pod {pod_obj.name} running on {pod_node}")
        logger.info("Step 4: Verify container UID")
        out = pod_obj.exec_sh_cmd_on_pod("id")
        logger.info(f"Container id: {out}")
        assert (
            f"uid={CONTAINER_UID}" in out
        ), f"Expected uid={CONTAINER_UID}, got: {out}"
        logger.info("Steps 5-6: Write and read file")
        pod_obj.exec_sh_cmd_on_pod("echo rwo-test > /mnt/test/file1 && sync")
        out = pod_obj.exec_sh_cmd_on_pod("cat /mnt/test/file1")
        logger.info(f"File content: {out}")
        assert "rwo-test" in out, f"File content mismatch: {out}"
        logger.info("Step 7: Verify file ownership")
        verify_file_ownership(
            pod_obj,
            "/mnt/test/file1",
            CONTAINER_UID,
            SUPPLEMENTAL_GROUPS_BASE,
        )
        logger.info(
            f"Ownership correct: " f"{CONTAINER_UID}:{SUPPLEMENTAL_GROUPS_BASE}"
        )
        logger.info("Step 8: Verify host UID remapping")
        host_uid = node.get_host_uid_for_pod(pod_node, pod_obj.name)
        assert host_uid != CONTAINER_UID, (
            f"Host UID {host_uid} equals container UID "
            f"{CONTAINER_UID} — remapping not active"
        )
        logger.info(
            f"Remapping confirmed: host UID {host_uid} "
            f"!= container UID {CONTAINER_UID}"
        )
        logger.info(f"Step 9: Delete pod {pod_obj.name}")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        logger.info(f"Pod {pod_obj.name} deleted")
        logger.info("Step 10: Recreate pod with same PVC")
        pod_obj.create()
        wait_for_resource_state(
            resource=pod_obj,
            state=constants.STATUS_RUNNING,
            timeout=300,
        )
        logger.info(f"Pod {pod_obj.name} recreated and Running")
        logger.info("Step 11: Verify data persists")
        out = pod_obj.exec_sh_cmd_on_pod("cat /mnt/test/file1")
        logger.info(f"Persisted content: {out}")
        assert "rwo-test" in out, f"Data did not persist after pod restart: " f"{out}"
        logger.info("Step 12: Verify ownership persists")
        verify_file_ownership(
            pod_obj,
            "/mnt/test/file1",
            CONTAINER_UID,
            SUPPLEMENTAL_GROUPS_BASE,
        )
        logger.info(
            f"Ownership persisted: " f"{CONTAINER_UID}:{SUPPLEMENTAL_GROUPS_BASE}"
        )
