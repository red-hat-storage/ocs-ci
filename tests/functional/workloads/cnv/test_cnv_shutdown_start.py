import logging
import pytest
import time

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

logger = logging.getLogger(__name__)


@magenta_squad
class TestVmShutdownStart(E2ETest):
    """
    Tests related VMs shutdown and start
    """

    @workloads
    @pytest.mark.polarion_id("OCS-6304")
    def test_vm_abrupt_shutdown_cluster(
        self,
        multi_cnv_workload,
        nodes,
        project_factory,
        cnv_workload,
        clone_vm_workload,
        setup_cnv,
        snapshot_factory,
        snapshot_restore_factory,
    ):
        """
        This test performs the behaviour of VMs and data integrity after abrupt shutdown of cluster

        Test steps:
        1. Create VMs using fixture multi_cnv_workload
        2. Create a clone of a VM PVC and new vm using cloned pvc.
        3. Create a snapshot for a VM backed pvc,Restore snapshot,Create new vm using restored pvc.
        4. Keep vms in different states (power on, paused, stoped)
        5. Initiate abrupt shutdown the cluster nodes as per OCP official documentation
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

        # Create VM using cloned pvc of source VM PVC
        all_vms[1].stop()
        clone_obj = clone_vm_workload(
            vm_obj=all_vms[1],
            volume_interface=all_vms[1].volume_interface,
            namespace=(
                all_vms[1].namespace
                if all_vms[1].volume_interface == constants.VM_VOLUME_PVC
                else None
            ),
        )[0]
        all_vms.append(clone_obj)
        csum = cal_md5sum_vm(vm_obj=clone_obj, file_path=file_paths[0])
        source_csums[clone_obj.name] = csum

        # Create a snapshot
        # Taking Snapshot of PVC
        pvc_obj = all_vms[3].get_vm_pvc_obj()
        snap_obj = snapshot_factory(pvc_obj)

        # Restore the snapshot
        res_snap_obj = snapshot_restore_factory(
            snapshot_obj=snap_obj,
            storageclass=vm_obj.sc_name,
            volume_mode=snap_obj.parent_volume_mode,
            access_mode=vm_obj.pvc_access_mode,
            status=constants.STATUS_BOUND,
            timeout=300,
        )

        # Create new VM using the restored PVC
        res_vm_obj = cnv_workload(
            source_url=constants.CNV_FEDORA_SOURCE,
            storageclass=all_vms[3].sc_name,
            existing_pvc_obj=res_snap_obj,
            namespace=vm_obj.namespace,
        )[-1]
        all_vms.append(res_vm_obj)
        csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
        source_csums[res_vm_obj.name] = csum

        # Keep vms in different states (power on, paused, stoped)
        all_vms[2].stop()
        all_vms[3].pause()

        # Initiate abrupt shutdown the cluster nodes as per OCP official documentation
        worker_nodes = get_nodes(node_type="worker")
        master_nodes = get_nodes(node_type="master")

        logger.info("Abruptly Shutting down worker & master nodes")
        nodes.stop_nodes(nodes=worker_nodes)
        nodes.stop_nodes(nodes=master_nodes)

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
            tries=30,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))
        logger.info("All nodes are now in READY state")

        logger.info("Waiting for 10 min for all pods to come in running state.")
        time.sleep(600)

        # check cluster health
        try:
            logger.info("Making sure ceph health is OK")
            Sanity().health_check(tries=50, cluster_check=False)
        except Exception as ex:
            logger.error("Failed at cluster health check!!")
            raise ex

        # Verify that VMs status post start
        all_vms[1].start()
        all_vms[2].start()

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
