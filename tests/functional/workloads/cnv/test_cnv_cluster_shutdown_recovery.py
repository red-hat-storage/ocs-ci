import pytest
import time
import random
import logging

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import unschedule_nodes, drain_nodes, schedule_nodes
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.node import get_nodes, wait_for_nodes_status
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.helpers.performance_lib import run_oc_command


logger = logging.getLogger(__name__)


@magenta_squad
class TestVmShutdownStart(E2ETest):
    """
    Tests related VMs shutdown and start
    """

    @workloads
    @pytest.mark.parametrize(
        argnames=["force"],
        argvalues=[
            pytest.param(True, marks=pytest.mark.polarion_id("OCS-6304")),
            pytest.param(
                False,
                marks=[
                    pytest.mark.polarion_id("OCS-6316"),
                    pytest.mark.jira("OCPBUGS-58027", run=False),
                ],
            ),
        ],
    )
    def test_vm_abrupt_graceful_shutdown_cluster(
        self,
        force,
        setup_cnv,
        project_factory,
        multi_cnv_workload,
        admin_client,
        nodes,
        vm_clone_fixture,
        vm_snapshot_restore_fixture,
    ):
        """
        This test performs the behaviour of VMs and data integrity after abrupt or Graceful shutdown of cluster

        Test steps:
        1. Create VMs using fixture multi_cnv_workload
        2. Create a clone of a VM.
        3. Create a snapshot of a VM ,Create new vm using Restore snapshot,
        4. Keep vms in different states (power on, paused, stoped)
        5. Initiate shutdown the cluster nodes as per OCP official documentation
            5.1 If force = True - abrupt shutdown
            5.2 If force = False - Graceful shutdown
        6. Initate ordered start of cluster after 10 min by following OCP official documentation.
        7. Verify cluster health Post-start
        8. Verify that VMs status post start
        9. Perform post restart data integrity check
        10. Perform some I/O operations on the VMs to ensure it is functioning as expected.
        11. Stop all the VMs created.
        """

        file_paths = ["/source_file.txt", "/new_file.txt"]

        logger.test_step("Create project and deploy CNV workload VMs")
        proj_obj = project_factory()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_def_compr,
            self.sc_obj_aggressive,
        ) = multi_cnv_workload(namespace=proj_obj.namespace)

        all_vms = self.vm_objs_def + self.vm_objs_aggr
        logger.info(f"Created {len(all_vms)} VMs successfully")

        logger.test_step("Write initial test data and prepare clone/snapshot VMs")
        source_csums = {}
        for vm_obj in all_vms:
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            source_csums[vm_obj.name] = source_csum

        vm_for_clone = next(
            (vm for vm in all_vms if vm.volume_interface == "DVT"), None
        )
        vm_for_snap = next((vm for vm in all_vms if vm.volume_interface == "PVC"), None)
        vm_for_stop = next((vm for vm in all_vms if vm.volume_interface == "DV"), None)

        if vm_for_clone is None:
            vm_for_clone = random.choice(all_vms)
        if vm_for_snap is None:
            vm_for_snap = random.choice(all_vms)
        if vm_for_stop is None:
            vm_for_stop = random.choice(all_vms)

        logger.info(f"Cloning VM '{vm_for_clone.name}'")
        cloned_vm = vm_clone_fixture(vm_for_clone, admin_client)
        csum = cal_md5sum_vm(vm_obj=cloned_vm, file_path=file_paths[0])
        source_csums[cloned_vm.name] = csum
        all_vms.append(cloned_vm)

        logger.info(f"Creating snapshot and restoring VM '{vm_for_snap.name}'")
        restored_vm = vm_snapshot_restore_fixture(vm_for_snap, admin_client)
        csum = cal_md5sum_vm(vm_obj=restored_vm, file_path=file_paths[0])
        source_csums[vm_for_snap.name] = csum

        logger.test_step("Shut down cluster nodes")
        worker_nodes = get_nodes(node_type="worker")
        master_nodes = get_nodes(node_type="master")

        worker_node_names = [node.name for node in worker_nodes]
        master_nodes_names = [node.name for node in master_nodes]

        if not force:
            logger.info("Stopping all VMs before graceful shutdown")
            run_oc_command(
                cmd="annotate cluster noobaa-db-pg-cluster --overwrite cnpg.io/hibernation=on"
            )
            for vm_obj in all_vms:
                if vm_obj.printableStatus() != constants.CNV_VM_STOPPED:
                    vm_obj.stop(wait=True)

            # unschedule Worker nodes
            unschedule_nodes(worker_node_names)
            # drain nodes
            drain_nodes(worker_node_names, disable_eviction=True)
            # shutting down worker nodes
            nodes.stop_nodes(nodes=worker_nodes, force=force)

            # unschedule master nodes
            unschedule_nodes(master_nodes_names)
            # drain master nodes
            drain_nodes(master_nodes_names, disable_eviction=True)
            # shutting down master nodes
            nodes.stop_nodes(nodes=master_nodes, force=force)

        else:
            logger.info(
                f"Stopping VM '{vm_for_stop.name}' and pausing VM '{vm_for_snap.name}'"
            )
            vm_for_stop.stop()
            vm_for_snap.pause()

            logger.info("Abruptly shutting down worker and master nodes")
            nodes.stop_nodes(nodes=worker_nodes, force=force)
            nodes.stop_nodes(nodes=master_nodes, force=force)

        logger.info("Waiting 5 minutes before starting nodes")
        time.sleep(300)

        logger.test_step("Start cluster nodes and wait for recovery")
        logger.info("Starting master and worker nodes")
        nodes.start_nodes(nodes=master_nodes)
        nodes.start_nodes(nodes=worker_nodes)
        all_nodes = master_nodes + worker_nodes
        all_node_names = [node.name for node in all_nodes]
        retry(
            (
                CommandFailed,
                TimeoutError,
                AssertionError,
                ResourceWrongStatusException,
            ),
            tries=15,
            delay=15,
        )(wait_for_nodes_status(node_names=all_node_names, timeout=1800))
        logger.info("All nodes are now in READY state")
        if not force:
            run_oc_command(
                "annotate cluster noobaa-db-pg-cluster --overwrite cnpg.io/hibernation=off"
            )

        # Schedule node
        schedule_nodes(worker_node_names)
        schedule_nodes(master_nodes_names)

        logger.info("Waiting for pods to reach running state")
        wait_for_pods_to_be_running(timeout=500)

        logger.test_step("Verify cluster and CNV health")
        try:
            logger.info("Running Ceph health check")
            Sanity().health_check(tries=50, cluster_check=False)
        except Exception as ex:
            logger.exception(f"Cluster health check failed: {ex}")
            raise ex

        logger.info("Running CNV post-install verification")
        cnv_obj = CNVInstaller()
        cnv_obj.post_install_verification()

        logger.test_step("Restore VM states after recovery")
        if not force:
            logger.info("Starting all VMs after graceful shutdown")
            for vm_obj in all_vms:
                if vm_obj.printableStatus() != constants.VM_RUNNING:
                    vm_obj.start()
        else:
            logger.info(f"Starting stopped VM '{vm_for_stop.name}'")
            vm_for_stop.start()
            for vm in all_vms:
                vm_status = vm.printableStatus()
                logger.assertion(
                    f"VM running state: vm='{vm.name}', "
                    f"expected='{constants.VM_RUNNING}', actual='{vm_status}', "
                    f"match={vm_status == constants.VM_RUNNING}"
                )
                assert (
                    vm_status == constants.VM_RUNNING
                ), f"VM '{vm.name}' did not reach running state, current status: '{vm_status}'"

        logger.test_step("Verify data integrity and run I/O on all VMs")
        for vm_obj in all_vms:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            logger.assertion(
                f"Data integrity: vm='{vm_obj.name}', "
                f"expected='{source_csums[vm_obj.name]}', actual='{new_csum}', "
                f"match={source_csums[vm_obj.name] == new_csum}"
            )
            assert (
                source_csums[vm_obj.name] == new_csum
            ), f"MD5 mismatch for VM '{vm_obj.name}' after cluster shutdown and recovery"

            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
        logger.info("Data integrity verified and I/O completed on all VMs")
