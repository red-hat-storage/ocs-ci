import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    workloads,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import run_dd_io, cal_md5sum_vm
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-6396")
class TestVmSingleWorkerNodeFailure(E2ETest):
    """
    Test case for ensuring that both OpenShift Virtualization
    and ODF can recover from a worker node failure that hosts critical pods
    (such as OpenShift Virtualization VMs, OSD pods, or mon pods)
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again
        """

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()

        request.addfinalizer(finalizer)

    def test_vm_single_worker_node_failure(
        self, setup_cnv, nodes, project_factory, multi_cnv_workload
    ):
        """
        This test performs a worker node failure and verifies that
        VMs are rescheduled and data integrity is maintained.

        Test Steps:
            1. Deploy multiple VMs.
            2. Identify the worker node hosting the most VMs.
            3. Simulate node failure by restarting it.
            4. Verify node and pod recovery.
            5. Verify ODF and CNV health.
            6. Verify VM state and successful rescheduling.
            7. Verify data integrity using MD5 checksums.
            8. Run I/O test on recovered VMs.
        """
        odf_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        cnv_namespace = constants.CNV_NAMESPACE
        file_paths = ["/source_file.txt", "/new_file.txt"]

        source_csum = {}
        new_csum = {}
        node_vm_count = {}

        proj_obj = project_factory()
        vm_objs_def, vm_objs_aggr, sc_objs_def, sc_objs_aggr = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr

        log.info(f"Total VMs to process: {len(vm_list)}")

        for vm_obj in vm_list:
            source_csum[vm_obj.name] = run_dd_io(
                vm_obj=vm_obj, file_path=file_paths[0], verify=True
            )

        initial_vm_states = {
            vm_obj.name: [vm_obj.printableStatus(), vm_obj.get_vmi_instance().node()]
            for vm_obj in vm_objs_def + vm_objs_aggr
        }
        worker_nodes = node.get_worker_nodes()

        for vm_name, (vm_status, worker_node) in initial_vm_states.items():
            if worker_node in node_vm_count:
                node_vm_count[worker_node] += 1
            else:
                node_vm_count[worker_node] = 1

        valid_node_vm_count = {
            k: v for k, v in node_vm_count.items() if k in worker_nodes
        }

        if valid_node_vm_count:
            max_vm_node = max(valid_node_vm_count, key=valid_node_vm_count.get)
            log.info(
                f"Node with the maximum number of VMs: {max_vm_node} with {valid_node_vm_count[max_vm_node]} VMs"
            )
            node_name = max_vm_node
        else:
            log.error("No valid worker nodes found in node_vm_count.")
            node_name = None

        log.info(f"Attempting to restart node: {node_name}")
        node_obj = node.get_node_objs([node_name])
        nodes.restart_nodes_by_stop_and_start(node_obj)

        log.info("Performing post-failure health checks for ODF and CNV namespaces")

        ceph_health_check(tries=80)

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=odf_namespace,
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Not all pods are running in {odf_namespace} after node failure and recovery"

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=cnv_namespace,
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Not all pods are running in {cnv_namespace} after node failure and recovery"

        final_vm_states = {
            vm_obj.name: [vm_obj.printableStatus(), vm_obj.get_vmi_instance().node()]
            for vm_obj in vm_objs_def + vm_objs_aggr
        }
        log.info(f"Final VM states: {final_vm_states}")

        for vm_name in initial_vm_states:
            assert initial_vm_states[vm_name][0] == final_vm_states[vm_name][0], (
                f"VM {vm_name}: State mismatch. Initial: {initial_vm_states[vm_name][0]}, "
                f"Final: {final_vm_states[vm_name][0]}"
            )
            if initial_vm_states[vm_name][1] == node_name:
                assert initial_vm_states[vm_name][1] != final_vm_states[vm_name][1], (
                    f"VM {vm_name}: Rescheduling failed. Initially, VM is scheduled"
                    f" on node {node_name}, still on the same node"
                )

        for vm_obj in vm_list:
            vm_obj.wait_for_ssh_connectivity()
            new_csum[vm_obj.name] = cal_md5sum_vm(
                vm_obj=vm_obj, file_path=file_paths[0]
            )

            assert source_csum[vm_obj.name] == new_csum[vm_obj.name], (
                f"Failed: MD5 comparison failed in VM {vm_obj.name} before "
                f"{source_csum[vm_obj.name]}"
                f"and after {new_csum[vm_obj.name]} worker node failure"
            )
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
        log.info("Successfully completed I/O on all VMs after worker node failure")
