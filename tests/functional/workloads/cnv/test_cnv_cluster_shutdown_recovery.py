import logging
import pytest
import time
import random

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.node import get_nodes, wait_for_nodes_status
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocp_resources.virtual_machine_clone import VirtualMachineClone
from ocs_ci.ocs.cnv.virtual_machine import VirtualMachine
from ocp_resources.virtual_machine_restore import VirtualMachineRestore
from ocp_resources.virtual_machine_snapshot import VirtualMachineSnapshot
from ocs_ci.helpers.helpers import create_unique_resource_name

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
            pytest.param(False, marks=pytest.mark.polarion_id("OCS-6316")),
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

        # Create a project
        proj_obj = project_factory()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_def_compr,
            self.sc_obj_aggressive,
        ) = multi_cnv_workload(namespace=proj_obj.namespace)
        logger.info("All vms created successfully")

        all_vms = self.vm_objs_def + self.vm_objs_aggr
        source_csums = {}
        for vm_obj in all_vms:
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            source_csums[vm_obj.name] = source_csum

        # Choose VMs randomaly
        vm_for_clone, vm_for_stop, vm_for_snap = random.sample(all_vms, 3)

        # Create Clone of VM
        target_name = f"clone-{vm_for_clone.name}"
        with VirtualMachineClone(
            name="clone-vm-test",
            namespace=vm_for_clone.namespace,
            source_name=vm_for_clone.name,
            target_name=target_name,
        ) as vmc:
            vmc.wait_for_status(status=VirtualMachineClone.Status.SUCCEEDED)
        cloned_vm = VirtualMachine(
            vm_name=target_name, namespace=vm_for_clone.namespace
        )
        cloned_vm.start(wait=True)
        cloned_vm.wait_for_ssh_connectivity()
        all_vms.append(cloned_vm)
        csum = cal_md5sum_vm(vm_obj=cloned_vm, file_path=file_paths[0])
        source_csums[cloned_vm.name] = csum

        # Create a snapshot
        snapshot_name = f"snapshot-{vm_for_snap.name}"
        # Explicitly create the VirtualMachineSnapshot instance
        with VirtualMachineSnapshot(
            name=snapshot_name,
            namespace=vm_for_snap.namespace,
            vm_name=vm_for_snap.name,
            client=admin_client,
            teardown=False,
        ) as vm_snapshot:
            vm_snapshot.wait_snapshot_done()

        # Stopping VM before restoring
        vm_for_snap.stop()

        # Explicitly create the VirtualMachineRestore instance
        restore_snapshot_name = create_unique_resource_name(vm_snapshot.name, "restore")
        try:
            with VirtualMachineRestore(
                name=restore_snapshot_name,
                namespace=vm_for_snap.namespace,
                vm_name=vm_for_snap.name,
                snapshot_name=vm_snapshot.name,
                client=admin_client,
                teardown=False,
            ) as vm_restore:
                vm_restore.wait_restore_done()  # Wait for restore completion
                vm_for_snap.start()
                vm_for_snap.wait_for_ssh_connectivity(timeout=1200)
        finally:
            vm_snapshot.delete()

        csum = cal_md5sum_vm(vm_obj=vm_for_snap, file_path=file_paths[0])
        source_csums[vm_for_snap.name] = csum

        # Initiate abrupt shutdown the cluster nodes as per OCP official documentation
        worker_nodes = get_nodes(node_type="worker")
        master_nodes = get_nodes(node_type="master")

        if not force:
            logger.info("Stopping all the vms before graceful shutdown")
            for vm_obj in all_vms:
                if vm_obj.printableStatus() != constants.CNV_VM_STOPPED:
                    vm_obj.stop(wait=True)
        else:
            # Keep vms in different states (power on, paused, stoped)
            vm_for_stop.stop()
            vm_for_snap.pause()

        shutdown_type = "abruptly" if force else "gracefully"
        logger.info(f"{shutdown_type.capitalize()} shutting down worker & master nodes")

        nodes.stop_nodes(nodes=worker_nodes, force=force)
        nodes.stop_nodes(nodes=master_nodes, force=force)

        logger.info("waiting for 5 min before starting nodes")
        time.sleep(300)

        # Initate ordered start of cluster after 10 min by following OCP official documentation.
        logger.info("Starting worker & master nodes")
        nodes.start_nodes(nodes=master_nodes)
        nodes.start_nodes(nodes=worker_nodes)
        retry(
            (
                CommandFailed,
                TimeoutError,
                AssertionError,
                ResourceWrongStatusException,
            ),
            tries=15,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))
        logger.info("All nodes are now in READY state")

        logger.info("Waiting for pods to come in running state.")
        wait_for_pods_to_be_running(timeout=500)

        # Check cluster health
        try:
            logger.info("Making sure ceph health is OK")
            Sanity().health_check(tries=50, cluster_check=False)
        except Exception as ex:
            logger.error("Failed at cluster health check!!")
            raise ex

        # CNV health check
        cnv_obj = CNVInstaller()
        cnv_obj.post_install_verification()

        if not force:
            logger.info("Start all the vms after graceful shutdown")
            for vm_obj in all_vms:
                if vm_obj.printableStatus() != constants.VM_RUNNING:
                    vm_obj.start()
        else:
            # Verify that VMs status post start
            vm_for_clone.start()
            vm_for_stop.start()
            for vm in (vm_for_clone, vm_for_stop):
                assert (
                    vm.printableStatus() == constants.VM_RUNNING
                ), f"{vm.name} did not reach the running state."

        # Verifies vm status after start and ssh connectivity
        vm_for_clone.verify_vm(verify_ssh=True)
        vm_for_stop.verify_vm(verify_ssh=True)
        vm_for_snap.verify_vm(verify_ssh=True)

        # Perform post restart data integrity check
        for vm_obj in all_vms:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert source_csums[vm_obj.name] == new_csum, (
                f"ERROR: Failed data integrity before stopping the cluster and after starting the cluster "
                f"for VM '{vm_obj.name}'."
            )

            # Perform some I/O operations on the VMs to ensure it is functioning as expected.
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])

        # Stop all the VMs created.
        for vm_obj in all_vms:
            vm_obj.stop()
