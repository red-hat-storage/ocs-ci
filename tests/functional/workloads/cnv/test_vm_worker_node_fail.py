import logging
import random

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    workloads,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import run_dd_io, cal_md5sum_vm
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.ocs.exceptions import ResourceWrongStatusException

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

    def test_vm_single_worker_node_failure(
        self, setup_cnv, nodes, project_factory, multi_cnv_workload
    ):
        """
        Test Steps:

        """

        odf_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        cnv_namespace = constants.CNV_NAMESPACE
        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csum = {}
        new_csum = {}

        proj_obj = project_factory()
        vm_objs_def, vm_objs_aggr, sc_objs_def, sc_objs_aggr = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr

        log.info(f"Total VMs to process: {len(vm_list)}")

        initial_vm_states = {
            vm_obj.name: [vm_obj.printableStatus(), vm_obj.get_vmi_instance().node()]
            for vm_obj in vm_objs_def + vm_objs_aggr
        }
        log.info(f"Initial VM states: {initial_vm_states}")

        for vm_obj in vm_list:
            source_csum[vm_obj.name] = run_dd_io(
                vm_obj=vm_obj, file_path=file_paths[0], verify=True
            )

        worker_nodes = node.get_osd_running_nodes()
        node_name = random.sample(worker_nodes, 1)
        node_name = node_name[0]

        log.info(f"Attempting to restart node: {node_name}")
        node_obj = node.get_node_objs([node_name])
        nodes.restart_nodes_by_stop_and_start(node_obj)

        log.info(f"Waiting for node {node_name} to return to Ready state")
        try:
            node.wait_for_nodes_status(
                node_names=[node_name],
                status=constants.NODE_READY,
            )
            log.info("Verifying all pods are running after node recovery")
            if not pod.wait_for_pods_to_be_running(timeout=720):
                raise ResourceWrongStatusException(
                    "Not all pods returned to running state after node recovery"
                )
        except ResourceWrongStatusException as e:
            log.error(
                f"Pods did not return to running state, attempting node restart: {e}"
            )

        ceph_health_check(tries=80)

        log.info("Performing post-failure health checks for ODF and CNV namespaces")
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
            new_csum[vm_obj.name] = cal_md5sum_vm(
                vm_obj=vm_obj, file_path=file_paths[0]
            )
            assert source_csum[vm_obj.name] == new_csum[vm_obj.name], (
                f"Failed: MD5 comparison failed in VM {vm_obj.name} before "
                "and after worker node failure"
            )
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
