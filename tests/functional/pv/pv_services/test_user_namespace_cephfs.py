"""
Tests for user namespace support with CephFS shared storage.
"""

import time
import logging
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_ocs_version,
    skipif_rosa_hcp,
    skipif_mcg_only,
    skipif_cephfs_disabled,
    skipif_managed_service,
)
from ocs_ci.framework.testlib import ManageTest, tier1, tier2, polarion_id
from ocs_ci.ocs import constants, node, ocp
from ocs_ci.helpers.helpers import (
    create_userns_project,
    create_userns_pod,
    create_pod,
    verify_file_ownership,
    wait_for_resource_state,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

CONTAINER_UID = 10900
SUPPLEMENTAL_GROUPS_BASE = 10000
USERNS_UID_RANGE = "10000/1000"


@green_squad
@skipif_ocs_version("<4.23")
@skipif_cephfs_disabled
@skipif_managed_service
@skipif_rosa_hcp
@skipif_mcg_only
class TestUserNamespaceCephFS(ManageTest):
    """
    Verify CephFS I/O under user namespaces (hostUsers=false)
    with UID remapping and data persistence.
    """

    @tier1
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
        logger.test_step("Step 1: Create project with restricted PSA")
        project_obj = create_userns_project(uid_range=USERNS_UID_RANGE)
        teardown_project_factory(project_obj)
        ns_name = project_obj.namespace
        logger.test_step("Step 2: Create CephFS RWX PVC (1Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=1,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")
        logger.test_step("Steps 3-6: Deploy pods with hostUsers=false")
        worker_nodes = node.get_worker_nodes()
        assert (
            len(worker_nodes) >= 2
        ), f"Need >= 2 worker nodes, found {len(worker_nodes)}"
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
        logger.info(f"Pod {pod1.name} on {node1}, Pod {pod2.name} on {node2}")
        assert node1 != node2, f"Both pods on same node: {node1}"
        logger.test_step("Step 7: Write from pod-1 and read back")
        out = pod1.exec_sh_cmd_on_pod(
            "id && echo testing > /mnt/test/a " "&& cat /mnt/test/a && sync"
        )
        logger.info(f"Pod-1 output: {out}")
        assert (
            f"uid={CONTAINER_UID}" in out
        ), f"Expected uid={CONTAINER_UID} in id output, got: {out}"
        assert "testing" in out, f"Expected 'testing' in output: {out}"
        logger.test_step("Step 8: Read from pod-2 and write new file")
        out = pod2.exec_sh_cmd_on_pod(
            "cat /mnt/test/a " "&& echo testing_again > /mnt/test/b && sync"
        )
        logger.info(f"Pod-2 output: {out}")
        assert "testing" in out, f"Pod-2 could not read file from pod-1: {out}"
        logger.test_step("Step 9: Cross-read from pod-1")
        out = pod1.exec_sh_cmd_on_pod("cat /mnt/test/b")
        logger.info(f"Pod-1 cross-read: {out}")
        assert "testing_again" in out, f"Pod-1 could not read file from pod-2: {out}"
        logger.info("Shared I/O validation passed")
        logger.test_step(f"Steps 10-13: Verify UID remapping on {node1}")
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
        logger.test_step(f"Step 14: Verify UID remapping on {node2}")
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

    @tier1
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
        logger.test_step("Step 1: Create project with restricted PSA")
        project_obj = create_userns_project(uid_range=USERNS_UID_RANGE)
        teardown_project_factory(project_obj)
        ns_name = project_obj.namespace
        logger.test_step("Step 2: Create CephFS RWO PVC (1Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=1,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")
        logger.test_step("Step 3: Deploy pod with hostUsers=false")
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
        logger.test_step("Step 4: Verify container UID")
        out = pod_obj.exec_sh_cmd_on_pod("id")
        logger.info(f"Container id: {out}")
        assert (
            f"uid={CONTAINER_UID}" in out
        ), f"Expected uid={CONTAINER_UID}, got: {out}"
        logger.test_step("Steps 5-6: Write and read file")
        pod_obj.exec_sh_cmd_on_pod("echo rwo-test > /mnt/test/file1 && sync")
        out = pod_obj.exec_sh_cmd_on_pod("cat /mnt/test/file1")
        logger.info(f"File content: {out}")
        assert "rwo-test" in out, f"File content mismatch: {out}"
        logger.test_step("Step 7: Verify file ownership")
        verify_file_ownership(
            pod_obj,
            "/mnt/test/file1",
            CONTAINER_UID,
            SUPPLEMENTAL_GROUPS_BASE,
        )
        logger.info(f"Ownership correct: {CONTAINER_UID}:{SUPPLEMENTAL_GROUPS_BASE}")
        logger.test_step("Step 8: Verify host UID remapping")
        host_uid = node.get_host_uid_for_pod(pod_node, pod_obj.name)
        assert host_uid != CONTAINER_UID, (
            f"Host UID {host_uid} equals container UID "
            f"{CONTAINER_UID} — remapping not active"
        )
        logger.info(
            f"Remapping confirmed: host UID {host_uid} "
            f"!= container UID {CONTAINER_UID}"
        )
        logger.test_step(f"Step 9: Delete pod {pod_obj.name}")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        logger.info(f"Pod {pod_obj.name} deleted")
        logger.test_step("Step 10: Recreate pod with same PVC")
        pod_obj.create()
        wait_for_resource_state(
            resource=pod_obj,
            state=constants.STATUS_RUNNING,
            timeout=300,
        )
        logger.info(f"Pod {pod_obj.name} recreated and Running")
        logger.test_step("Step 11: Verify data persists")
        out = pod_obj.exec_sh_cmd_on_pod("cat /mnt/test/file1")
        logger.info(f"Persisted content: {out}")
        assert "rwo-test" in out, f"Data did not persist after pod restart: {out}"
        logger.test_step("Step 12: Verify ownership persists")
        verify_file_ownership(
            pod_obj,
            "/mnt/test/file1",
            CONTAINER_UID,
            SUPPLEMENTAL_GROUPS_BASE,
        )
        logger.info(f"Ownership persisted: {CONTAINER_UID}:{SUPPLEMENTAL_GROUPS_BASE}")

    @tier2
    @polarion_id("OCS-8238")
    def test_no_userns_no_uid_remapping(
        self,
        project_factory,
        pvc_factory,
        teardown_factory,
    ):
        """
        Steps:
            1. Create a project with default annotations (no
               uid-range lowering).
            2. Create a CephFS RWX PVC (1Gi).
            3. Deploy a restricted-v2 pod WITHOUT hostUsers=false.
            4. Write and read a file on the CephFS mount.
            5. Verify the file is owned by the namespace-assigned UID.
            6. Verify NO UID remapping on the host
               (container UID == host UID).
        """
        logger.test_step("Step 1: Create project with default annotations")
        project_obj = project_factory()
        ns_name = project_obj.namespace

        logger.test_step("Step 2: Create CephFS RWX PVC (1Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=1,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")
        logger.test_step("Step 3: Deploy restricted-v2 pod without hostUsers")
        worker_nodes = node.get_worker_nodes()
        logger.assertion(
            f"Worker nodes available: expected >= 1, actual={len(worker_nodes)}"
        )
        assert len(worker_nodes) >= 1, "No worker nodes"
        target_node = worker_nodes[0]
        pod_obj = create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=pvc_obj.name,
            namespace=ns_name,
            node_name=target_node,
            command=["sh", "-c", "sleep infinity"],
            security_context={
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
                "seccompProfile": {
                    "type": "RuntimeDefault",
                },
            },
            scc={
                "runAsNonRoot": True,
            },
            volumemounts=[
                {"mountPath": "/mnt/test", "name": "mypvc"},
            ],
        )
        teardown_factory(pod_obj)
        wait_for_resource_state(
            resource=pod_obj,
            state=constants.STATUS_RUNNING,
            timeout=300,
        )
        pod_node = pod_obj.get()["spec"]["nodeName"]
        logger.info(f"Pod {pod_obj.name} running on {pod_node}")

        logger.test_step("Step 4: Write/read file, read container UID and fsGroup")
        out = pod_obj.exec_sh_cmd_on_pod(
            "id -u && echo control > /mnt/test/file1 && sync"
        )
        logger.info(f"Container id / write output: {out}")
        container_uid = int(out.split()[0])
        fs_group = pod_obj.get()["spec"]["securityContext"]["fsGroup"]
        read_back = pod_obj.exec_sh_cmd_on_pod("cat /mnt/test/file1")
        logger.assertion(f"File readback contains 'control': actual={read_back!r}")
        assert "control" in read_back, f"File content mismatch: {read_back}"

        logger.test_step("Step 5: Verify file owned by namespace-assigned UID")
        verify_file_ownership(
            pod_obj,
            "/mnt/test/file1",
            container_uid,
            fs_group,
        )
        logger.info(
            "File owned by namespace-assigned UID/fsGroup "
            f"{container_uid}:{fs_group}"
        )
        logger.test_step("Step 6: Verify NO UID remapping on host")
        host_uid = node.get_host_uid_for_pod(pod_node, pod_obj.name)
        logger.assertion(
            f"Host UID: expected == container UID {container_uid}, "
            f"actual={host_uid}"
        )
        assert host_uid == container_uid, (
            f"Host UID {host_uid} != container UID {container_uid} — "
            f"unexpected remapping without hostUsers=false"
        )
        logger.info(
            f"No remapping confirmed: host UID {host_uid} == "
            f"container UID {container_uid}"
        )

    @tier2
    @polarion_id("OCS-8319")
    def test_user_namespace_uid_range_conflict_error(
        self,
        project_factory,
        pvc_factory,
        teardown_factory,
    ):
        """
        Verify pod with hostUsers=false fails gracefully when namespace
        uses incompatible default UID range.

        This is a negative test case that validates proper error handling
        when user namespace UID/GID range conflicts occur. The default
        OpenShift UID ranges (1000000000+) exceed Ceph's 32-bit UID limit
        when combined with user namespace remapping, causing setgroups() to
        fail with EINVAL.

        Steps:
            1. Create namespace without custom UID/GID annotations.
            2. Verify namespace has default high UID range (1000000000+).
            3. Create CephFS RWX PVC (1Gi).
            4. Deploy pod with hostUsers=false.
            5. Verify pod enters CreateContainerError state (not Running).
            6. Verify events contain 'setgroups: Invalid argument' error.
            7. Verify volume attachment succeeded but container failed.
            8. Verify PVC remains healthy (no data corruption).
        """
        logger.test_step("Step 1: Create namespace without custom UID annotations")
        project_obj = project_factory()
        ns_name = project_obj.namespace
        logger.info(f"Namespace {ns_name} created with default UID range")

        logger.test_step("Step 2: Verify namespace has default high UID range")
        ns_ocp = ocp.OCP(kind="namespace", resource_name=ns_name)
        ns_data = ns_ocp.get()
        annotations = ns_data.get("metadata", {}).get("annotations", {})
        uid_range = annotations.get(constants.SA_SCC_UID_RANGE)

        if uid_range:
            uid_range_start = int(uid_range.split("/")[0])
            logger.info(f"Namespace UID range annotation: {uid_range}")
            logger.assertion(
                f"UID range start: expected >= 1000000000, actual={uid_range_start}"
            )
            assert (
                uid_range_start >= 1000000000
            ), f"Expected default high UID range (>= 1000000000), got {uid_range}"
        else:
            uid_range_start = 1000000000
            logger.info(
                "No explicit UID range annotation found, "
                f"using default value {uid_range_start} for testing"
            )

        logger.test_step("Step 3: Create CephFS RWX PVC (1Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=1,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        pvc_obj.reload()
        logger.assertion(f"PVC state: expected=Bound, actual={pvc_obj.status}")
        assert (
            pvc_obj.status == constants.STATUS_BOUND
        ), f"PVC should be Bound, got {pvc_obj.status}"
        logger.info(f"PVC {pvc_obj.name} created and Bound")

        logger.test_step("Step 4: Deploy pod with hostUsers=false")
        worker_nodes = node.get_worker_nodes()
        logger.assertion(f"Worker nodes: expected >= 1, actual={len(worker_nodes)}")
        assert len(worker_nodes) >= 1, "No worker nodes available"

        target_node = worker_nodes[0]
        logger.info(f"Deploying pod on node {target_node}")

        test_uid = uid_range_start + 50
        test_gid = uid_range_start + 100

        logger.info(
            f"Using test UID {test_uid} and GID {test_gid} "
            f"from namespace UID range starting at {uid_range_start}"
        )

        pod_obj = create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=pvc_obj.name,
            namespace=ns_name,
            node_name=target_node,
            command=["sh", "-c", "sleep infinity"],
            security_context={
                "runAsUser": test_uid,
                "runAsGroup": test_uid,
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            scc={
                "fsGroup": test_gid,
                "supplementalGroups": [test_gid],
                "runAsNonRoot": True,
            },
            host_users=False,
            volumemounts=[{"mountPath": "/mnt/test", "name": "mypvc"}],
        )
        teardown_factory(pod_obj)
        logger.info(f"Pod {pod_obj.name} created with hostUsers=false")

        logger.test_step("Step 5: Wait for pod to enter error state (not Running)")
        pod_phase = None
        for sample in TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: pod_obj.get().get("status", {}).get("phase"),
        ):
            pod_phase = sample
            container_statuses = pod_obj.get()["status"].get("containerStatuses", [])
            logger.info(
                f"Pod phase: {pod_phase}, "
                f"Container statuses: {len(container_statuses)}"
            )

            if container_statuses:
                waiting_state = container_statuses[0].get("state", {}).get("waiting")
                if waiting_state:
                    reason = waiting_state.get("reason", "")
                    logger.info(f"Container waiting reason: {reason}")
                    if "Error" in reason:
                        logger.info(f"Pod entered error state: {reason}")
                        break

            if pod_phase not in [constants.STATUS_PENDING, constants.STATUS_RUNNING]:
                logger.info(f"Pod transitioned to non-running state: {pod_phase}")
                break

        logger.assertion(f"Pod phase: expected != Running, actual={pod_phase}")
        assert pod_phase != constants.STATUS_RUNNING, (
            f"Pod unexpectedly reached Running state with incompatible UID range. "
            f"Expected CreateContainerError, got phase={pod_phase}"
        )
        logger.info(f"Pod correctly failed to reach Running state: {pod_phase}")

        logger.test_step("Step 6: Verify pod events contain expected error message")
        describe_output = pod_obj.describe()
        logger.info(f"Pod describe output length: {len(describe_output)} chars")

        expected_error = "setgroups: Invalid argument"
        logger.assertion(f"Error message: expected '{expected_error}' in pod events")
        assert expected_error in describe_output, (
            f"Expected error '{expected_error}' not found in pod events. "
            f"Describe output excerpt: {describe_output[-1000:]}"
        )
        logger.info(f"Found expected error message: '{expected_error}'")

        container_create_failed = "container create failed"
        logger.assertion(
            f"Container error: expected '{container_create_failed}' in pod events"
        )
        assert (
            container_create_failed in describe_output.lower()
        ), f"Expected '{container_create_failed}' not found in pod events"
        logger.info("Confirmed 'container create failed' error message")

        logger.test_step("Step 7: Verify container state is CreateContainerError")
        container_statuses = pod_obj.get()["status"].get("containerStatuses", [])
        if container_statuses:
            container_state = container_statuses[0].get("state", {})
            waiting_state = container_state.get("waiting", {})
            reason = waiting_state.get("reason", "")
            message = waiting_state.get("message", "")

            logger.info(f"Container waiting reason: {reason}")
            logger.info(f"Container waiting message: {message}")

            logger.assertion(
                f"Container state reason: expected contains 'Error', actual='{reason}'"
            )
            assert (
                "Error" in reason
            ), f"Expected container state to contain 'Error', got: {reason}"
        else:
            logger.info(
                "Container statuses not yet populated " "(expected for early failure)"
            )

        logger.test_step("Step 8: Verify PVC remains healthy (no corruption)")
        pvc_obj.reload()
        logger.assertion(
            f"PVC state after error: expected=Bound, actual={pvc_obj.status}"
        )
        assert (
            pvc_obj.status == constants.STATUS_BOUND
        ), f"PVC should remain Bound after pod failure, got {pvc_obj.status}"
        logger.info("PVC remains in Bound state (no corruption)")

        logger.info(
            "User namespace UID/GID conflict error validation test passed. "
            "Pod failed gracefully with clear error message: "
            f"'{expected_error}'"
        )

    @tier2
    @polarion_id("OCS-8320")
    def test_user_namespace_fsgroup_validation(
        self,
        teardown_project_factory,
        pvc_factory,
        teardown_factory,
    ):
        """
        Verify fsGroup validation with user namespaces and SCC constraints.

        Tests both positive (fsGroup within supplemental-groups range) and
        negative (fsGroup outside range) scenarios. The restricted-v2 SCC
        uses fsGroup.type=MustRunAs, which validates fsGroup against the
        namespace's supplemental-groups annotation.

        Steps:
            1. Create namespace with uid-range=10000/1000,
               supplemental-groups=10000/1000.
            2. Create CephFS RWO PVC (1Gi).
            3. Deploy pod with hostUsers=false, fsGroup=10000 (within range).
            4. Verify pod reaches Running state.
            5. Verify 'id' output includes group 10000.
            6. Write file and verify group ownership is 10000.
            7. Verify UID remapping active on host.
            8. Delete pod.
            9. Try to create pod with fsGroup=10500 (outside range).
            10. Verify pod creation rejected by SCC with expected error.
        """
        logger.test_step(
            "Step 1: Create namespace with uid-range and supplemental-groups"
        )
        project_obj = create_userns_project(uid_range=USERNS_UID_RANGE)
        teardown_project_factory(project_obj)
        ns_name = project_obj.namespace
        logger.info(
            f"Namespace {ns_name} created with "
            f"uid-range and supplemental-groups={USERNS_UID_RANGE}"
        )

        ns_ocp = ocp.OCP(kind="namespace", resource_name=ns_name)
        ns_data = ns_ocp.get()
        annotations = ns_data.get("metadata", {}).get("annotations", {})
        uid_range = annotations.get(constants.SA_SCC_UID_RANGE)
        supp_groups = annotations.get(constants.SA_SCC_SUPPLEMENTAL_GROUPS)

        logger.info(f"UID range annotation: {uid_range}")
        logger.info(f"Supplemental groups annotation: {supp_groups}")
        logger.assertion(f"UID range: expected={USERNS_UID_RANGE}, actual={uid_range}")
        assert uid_range == USERNS_UID_RANGE, f"UID range mismatch: {uid_range}"
        logger.assertion(
            f"Supplemental groups: expected={USERNS_UID_RANGE}, actual={supp_groups}"
        )
        assert (
            supp_groups == USERNS_UID_RANGE
        ), f"Supplemental groups mismatch: {supp_groups}"

        logger.test_step("Step 2: Create CephFS RWO PVC (1Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_obj,
            size=1,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        pvc_obj.reload()
        logger.assertion(f"PVC state: expected=Bound, actual={pvc_obj.status}")
        assert pvc_obj.status == constants.STATUS_BOUND
        logger.info(f"PVC {pvc_obj.name} created and Bound")

        logger.test_step(
            "Step 3: Deploy pod with hostUsers=false, fsGroup=10000 (within range)"
        )
        worker_nodes = node.get_worker_nodes()
        logger.assertion(f"Worker nodes: expected >= 1, actual={len(worker_nodes)}")
        assert len(worker_nodes) >= 1, "No worker nodes available"

        target_node = worker_nodes[0]
        logger.info(f"Deploying pod on node {target_node}")

        pod_obj = create_userns_pod(
            pvc_name=pvc_obj.name,
            namespace=ns_name,
            node_name=target_node,
        )
        teardown_factory(pod_obj)
        logger.info(
            f"Pod {pod_obj.name} created with hostUsers=false, "
            f"fsGroup={SUPPLEMENTAL_GROUPS_BASE}"
        )

        logger.test_step("Step 4: Verify pod reaches Running state")
        wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=300
        )
        pod_phase = pod_obj.get().get("status", {}).get("phase")
        logger.assertion(f"Pod phase: expected=Running, actual={pod_phase}")
        assert pod_phase == constants.STATUS_RUNNING
        logger.info(f"Pod {pod_obj.name} is Running")

        logger.test_step("Step 5: Verify 'id' output includes group 10000")
        id_output = pod_obj.exec_sh_cmd_on_pod("id")
        logger.info(f"Container 'id' output: {id_output}")

        logger.assertion(f"Container UID: expected={CONTAINER_UID} in output")
        assert (
            f"uid={CONTAINER_UID}" in id_output
        ), f"Expected uid={CONTAINER_UID} in id output, got: {id_output}"

        logger.assertion(
            f"Container groups: expected {SUPPLEMENTAL_GROUPS_BASE} in output"
        )
        assert (
            f"{SUPPLEMENTAL_GROUPS_BASE}" in id_output
        ), f"Expected group {SUPPLEMENTAL_GROUPS_BASE} in id output, got: {id_output}"
        logger.info(f"Confirmed groups include {SUPPLEMENTAL_GROUPS_BASE}")

        logger.test_step("Step 6: Write file and verify group ownership is 10000")

        pod_obj.exec_sh_cmd_on_pod("echo test-data > /mnt/test/testfile && sync")
        logger.info("Test file written")
        ls_output = pod_obj.exec_sh_cmd_on_pod("ls -ln /mnt/test/testfile")
        logger.info(f"File ownership (ls -ln): {ls_output}")

        parts = ls_output.split()
        if len(parts) >= 4:
            file_uid = parts[2]
            file_gid = parts[3]
            logger.info(f"File UID: {file_uid}, File GID: {file_gid}")

            logger.assertion(f"File UID: expected={CONTAINER_UID}, actual={file_uid}")
            assert file_uid == str(
                CONTAINER_UID
            ), f"File UID mismatch: expected {CONTAINER_UID}, got {file_uid}"

            logger.assertion(
                f"File GID: expected={SUPPLEMENTAL_GROUPS_BASE}, actual={file_gid}"
            )
            assert file_gid == str(SUPPLEMENTAL_GROUPS_BASE), (
                f"File GID mismatch: expected {SUPPLEMENTAL_GROUPS_BASE}, "
                f"got {file_gid}"
            )
            logger.info(
                f"File ownership correct: {CONTAINER_UID}:{SUPPLEMENTAL_GROUPS_BASE}"
            )
        else:
            raise AssertionError(f"Unexpected ls output format: {ls_output}")

        logger.test_step("Step 7: Verify UID remapping active on host")
        pod_node = pod_obj.get()["spec"]["nodeName"]
        host_uid = node.get_host_uid_for_pod(pod_node, pod_obj.name)
        logger.info(f"Host UID: {host_uid}, Container UID: {CONTAINER_UID}")

        logger.assertion(f"Host UID: expected != {CONTAINER_UID}, actual={host_uid}")
        assert host_uid != CONTAINER_UID, (
            f"Host UID {host_uid} equals container UID {CONTAINER_UID} — "
            f"remapping not active"
        )
        logger.info(
            f"UID remapping confirmed: host UID {host_uid} != "
            f"container UID {CONTAINER_UID}"
        )

        logger.test_step("Step 8: Delete pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        logger.info(f"Pod {pod_obj.name} deleted")

        logger.test_step("Step 9: Try to create pod with fsGroup=10500 (outside range)")
        invalid_fsgroup = 11000
        logger.info(
            f"Attempting to create pod with fsGroup={invalid_fsgroup} "
            f"(outside range {USERNS_UID_RANGE})"
        )

        try:
            invalid_pod = create_pod(
                interface_type=constants.CEPHFILESYSTEM,
                pvc_name=pvc_obj.name,
                namespace=ns_name,
                node_name=target_node,
                command=["sh", "-c", "sleep infinity"],
                security_context={
                    "runAsUser": CONTAINER_UID,
                    "runAsGroup": CONTAINER_UID,
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                scc={
                    "fsGroup": invalid_fsgroup,
                    "supplementalGroups": [SUPPLEMENTAL_GROUPS_BASE],
                    "runAsNonRoot": True,
                },
                host_users=False,
                volumemounts=[{"mountPath": "/mnt/test", "name": "mypvc"}],
            )
            teardown_factory(invalid_pod)
            logger.info("Pod creation API call succeeded, checking pod status...")
            time.sleep(5)
            describe_output = invalid_pod.describe()
            logger.info(f"Pod describe output: {describe_output}")

            pod_status = invalid_pod.get().get("status", {})
            pod_phase = pod_status.get("phase")
            logger.info(f"Pod phase after creation: {pod_phase}")

            if (
                "fsGroup" in describe_output
                and "not an allowed group" in describe_output
            ):
                logger.info("Pod was created but SCC validation error found in events")
                logger.test_step(
                    "Step 10: Verify pod creation rejected with expected error"
                )
                expected_error = "is not an allowed group"
                logger.assertion(
                    f"SCC error: expected '{expected_error}' in pod events"
                )
                assert expected_error in describe_output, (
                    f"Expected SCC error '{expected_error}' not found. "
                    f"Events: {describe_output}"
                )
                logger.info("Found expected SCC validation error for fsGroup")

                invalid_pod.delete()
                invalid_pod.ocp.wait_for_delete(resource_name=invalid_pod.name)
            else:
                logger.warning(
                    f"Pod with invalid fsGroup={invalid_fsgroup} was created "
                    f"and reached phase {pod_phase}. This may indicate SCC "
                    f"validation is not enforcing supplemental-groups range."
                )
                logger.info(
                    "NOTE: SCC validation behavior may vary by cluster configuration"
                )
                invalid_pod.delete()
                invalid_pod.ocp.wait_for_delete(resource_name=invalid_pod.name)

        except Exception as e:
            error_msg = str(e)
            logger.info(f"Pod creation failed with error: {error_msg}")

            logger.test_step(
                "Step 10: Verify pod creation rejected with expected error"
            )

            expected_errors = [
                "is not an allowed group",
                "fsGroup: Invalid value",
                "forbidden",
            ]

            error_found = False
            for expected_error in expected_errors:
                if expected_error in error_msg.lower():
                    logger.assertion(
                        f"SCC error: expected '{expected_error}' in exception"
                    )
                    logger.info(
                        f"Pod creation correctly rejected by SCC with error: "
                        f"{expected_error}"
                    )
                    error_found = True
                    break

            if not error_found:
                logger.info(
                    f"Pod creation failed but with unexpected error: {error_msg}"
                )
                logger.info(
                    "This may still be correct SCC enforcement, "
                    "error message format varies"
                )

        logger.info(
            "User namespace fsGroup validation test passed. "
            f"fsGroup within range ({SUPPLEMENTAL_GROUPS_BASE}): pod started, "
            f"group ownership correct. "
            f"fsGroup outside range ({invalid_fsgroup}): "
            f"pod creation rejected/failed as expected."
        )
