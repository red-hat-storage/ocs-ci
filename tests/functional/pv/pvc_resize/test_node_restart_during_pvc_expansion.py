import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants, node
from ocs_ci.ocs.cluster import is_ms_consumer_cluster
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    wait_for_ceph_cmd_execute_successfully,
    delete_pods,
    get_ocs_operator_pod,
)
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier4,
    tier4b,
    ignore_leftovers,
    polarion_id,
    skipif_bm,
    skipif_upgraded_from,
    skipif_vsphere_ipi,
    skipif_ms_provider,
)

log = logging.getLogger(__name__)


@green_squad
@tier4
@tier4b
@ignore_leftovers
@skipif_bm
@skipif_vsphere_ipi
@skipif_ms_provider
@skipif_ocs_version("<4.5")
@skipif_upgraded_from(["4.4"])
@polarion_id("OCS-2235")
class TestNodeRestartDuringPvcExpansion(ManageTest):
    """
    Tests to verify PVC expansion will be success even if a node is restarted
    while expansion is in progress.

    """

    @pytest.fixture(autouse=True)
    def setup(self, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=4,
            num_of_rbd_pvc=12,
            num_of_cephfs_pvc=8,
            deployment_config=True,
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure the nodes are up

        """

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()
            log.info("Verify that we can execute a Ceph command successfully")
            ceph_cmd_success = wait_for_ceph_cmd_execute_successfully()
            # If Ceph command failed and the cluster is an MS consumer cluster
            if not ceph_cmd_success and is_ms_consumer_cluster():
                # This is a workaround due to the BZ https://bugzilla.redhat.com/show_bug.cgi?id=2131581
                log.info("Try to restart the ocs-operator pod")
                delete_pods([get_ocs_operator_pod()])

            assert ceph_health_check(), "Ceph cluster health is not OK"
            log.info("Ceph cluster health is OK")

        request.addfinalizer(finalizer)

    def test_worker_node_restart_during_pvc_expansion(self, nodes):
        """
        Verify PVC expansion will succeed if a worker node is restarted
        during expansion

        """
        pvc_size_expanded = 30
        executor = ThreadPoolExecutor(max_workers=len(self.pods))
        selected_node = node.get_nodes(
            node_type=constants.WORKER_MACHINE, num_of_nodes=1
        )

        # Restart node
        log.info(f"Restart node {selected_node[0].name}")
        restart_thread = executor.submit(nodes.restart_nodes, nodes=selected_node)

        log.info("Expanding all PVCs.")
        for pvc_obj in self.pvcs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expanded}G")
            pvc_obj.expand_proc = executor.submit(
                pvc_obj.resize_pvc, pvc_size_expanded, False
            )

        # Check result of node 'restart_nodes'
        restart_thread.result()

        log.info("Verify status of node.")
        node.wait_for_nodes_status(
            node_names=[node.get_node_name(selected_node[0])],
            status=constants.NODE_READY,
            timeout=300,
        )

        # Find respun pods
        new_pods_list = []
        wait_to_stabilize = True
        for pod_obj in self.pods:
            new_pods = get_all_pods(
                namespace=pod_obj.namespace,
                selector=[pod_obj.labels.get("deploymentconfig")],
                selector_label="deploymentconfig",
                wait=wait_to_stabilize,
            )
            for pod_ob in new_pods:
                pod_ob.pvc = pod_obj.pvc
            new_pods_list.extend(new_pods)
            # Given enough time for pods to respin. So wait time
            # is not needed for further iterations
            wait_to_stabilize = False
        assert len(new_pods_list) == len(
            self.pods
        ), "Couldn't find all pods after node reboot"

        # Verify PVC expansion status
        for pvc_obj in self.pvcs:
            assert pvc_obj.expand_proc.result(), (
                f"Expansion failed for PVC {pvc_obj.name}\nDescribe output "
                f"of PVC and PV:\n{pvc_obj.describe()}\n"
                f"{pvc_obj.backed_pv_obj.describe()}"
            )
            capacity = pvc_obj.get().get("status").get("capacity").get("storage")
            assert capacity == f"{pvc_size_expanded}Gi", (
                f"Capacity of PVC {pvc_obj.name} is not {pvc_size_expanded}Gi as "
                f"expected, but {capacity}."
            )
        log.info("PVC expansion was successful on all PVCs")

        log.info("Verifying new size on pods.")
        for pod_obj in new_pods_list:
            if pod_obj.pvc.volume_mode == "Block":
                log.info(
                    f"Skipping check on pod {pod_obj.name} as volume mode is Block."
                )
                continue

            # Wait for 240 seconds to reflect the change on pod
            log.info(f"Checking pod {pod_obj.name} to verify the change.")
            for df_out in TimeoutSampler(
                240, 3, pod_obj.exec_cmd_on_pod, command="df -kh"
            ):
                df_out = df_out.split()
                new_size_mount = df_out[df_out.index(pod_obj.get_storage_path()) - 4]
                if new_size_mount in [
                    f"{pvc_size_expanded - 0.1}G",
                    f"{float(pvc_size_expanded)}G",
                    f"{pvc_size_expanded}G",
                ]:
                    log.info(
                        f"Verified: Expanded size of PVC {pod_obj.pvc.name} "
                        f"is reflected on pod {pod_obj.name}"
                    )
                    break
                log.info(
                    f"Expanded size of PVC {pod_obj.pvc.name} is not reflected"
                    f" on pod {pod_obj.name}. New size on mount is not "
                    f"{pvc_size_expanded}G as expected, but {new_size_mount}. "
                    f"Checking again."
                )
        log.info(
            f"Verified: Expanded size {pvc_size_expanded}G is reflected "
            f"on all pods."
        )

        # Run IO
        log.info("Run IO after PVC expansion.")
        for pod_obj in new_pods_list:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
            storage_type = "block" if pod_obj.pvc.volume_mode == "Block" else "fs"
            pod_obj.io_proc = executor.submit(
                pod_obj.run_io,
                storage_type=storage_type,
                size="6G",
                runtime=30,
                fio_filename=f"{pod_obj.name}_file",
                end_fsync=1,
            )

        assert (
            wait_for_ceph_cmd_execute_successfully()
        ), "Failed to execute a ceph command"

        log.info("Wait for IO to complete on all pods")
        for pod_obj in new_pods_list:
            pod_obj.io_proc.result()
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
            )
            log.info(f"Verified IO on pod {pod_obj.name}.")
        log.info("IO is successful on all pods after PVC expansion.")
