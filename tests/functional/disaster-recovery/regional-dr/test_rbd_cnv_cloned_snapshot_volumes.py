import logging
import time
from time import sleep

import pytest

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.deployment import (
    RDRMultiClusterDROperatorsDeploy,
)
from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import turquoise_squad, rdr
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import (
    validate_data_integrity_vm,
    CnvWorkloadDiscoveredApps,
    modify_pod_pvc_name,
)
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.utility.utils import ceph_health_check
from tests.conftest import (
    multi_pvc_clone_factory,
    multi_snapshot_factory,
    snapshot_restore_factory,
)

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@pytest.mark.polarion_id("OCS-6111")
class TestCNVClonedSnapshotVolumesWithDiscoveredApps:
    """
    Test CNV Failover and Relocate with Discovered Apps on Cloned and Snapshot-restored RBD Volumes

    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        dr_conf = dict()
        # dr_conf["rbd_dr_scenario"] = config.ENV_DATA.get("rbd_dr_scenario", False)
        mco_obj = RDRMultiClusterDROperatorsDeploy(dr_conf)
        mco_obj.deploy_dr_policy(flatten=True)

        def finalizer():
            mco_obj.delete_drpolicy()

        request.addfinalizer(finalizer)

    def clone_snapshot_workload_pvcs(
        self,
        multi_pvc_clone_factory,
        multi_snapshot_factory,
        snapshot_restore_factory,
        volume_type=["clone", "snapshot"],
    ):
        """
        Create clones or snapshots of workload PVCs and return their names as a flat list.

        Args:
            multi_pvc_clone_factory (function): Factory to create multiple PVC clones.
            multi_snapshot_factory (function): Factory to create multiple PVC snapshots.
            snapshot_restore_factory (function): Factory to restore snapshots to PVCs.
            volume_type (list): List of volume types to process ("clone" and/or "snapshot").

        Returns:
            list: Flat list containing names of cloned or restored PVCs.
        """
        pvc_names = []  # Flat list for PVC names
        for volume in volume_type:
            cnv_obj = CnvWorkloadDiscoveredApps()
            pvcs = get_all_pvc_objs(namespace=cnv_obj.workload_namespace)

            if volume == "clone":
                logger.info("Creating clones of the workload PVCs")
                cloned_pvcs = multi_pvc_clone_factory(
                    pvc_obj=pvcs,
                    access_mode=constants.ACCESS_MODE_RWX,
                    storageclass=constants.DEFAULT_CNV_CEPH_RBD_SC,
                )
                # Extract names from the cloned PVC objects and append directly to the flat list
                cloned_pvc_names = [pvc.name for pvc in cloned_pvcs]
                pvc_names.extend(cloned_pvc_names)
                logger.info(f"Cloned PVCs: {cloned_pvc_names}")

            elif volume == "snapshot":
                logger.info("Creating snapshots of the workload PVCs")
                snapshots = multi_snapshot_factory(pvc_obj=pvcs, wait=True)
                # Extract names from the snapshot objects
                snapshot_names = [snapshot.name for snapshot in snapshots]

                logger.info(f"Creating PVCs from snapshots: {snapshot_names}")
                restore_snapshot_objs = [
                    snapshot_restore_factory(
                        snapshot_obj=snapshot,
                        volume_mode=snapshot.parent_volume_mode,
                        access_mode=constants.ACCESS_MODE_RWX,
                        status=constants.STATUS_BOUND,
                        timeout=300,
                    )
                    for snapshot in snapshots
                ]

                # Extract names from the restored PVC objects and append directly to the flat list
                restored_pvc_names = [restore.name for restore in restore_snapshot_objs]
                pvc_names.extend(restored_pvc_names)
                logger.info(f"Restored PVCs from snapshots: {restored_pvc_names}")

        return pvc_names

    @pytest.mark.parametrize(
        argnames=["volume_type"],
        argvalues=[
            pytest.param(
                "clone",
            ),
            pytest.param(
                "snapshot-restored",
            ),
        ],
    )
    def test_cnv_failover_and_relocate_discovered_apps_cloned_volumes(
        self,
        volume_type,
        discovered_apps_dr_workload_cnv,
        pod_factory,
        snapshot_factory,
        pvc_clone_factory,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify cnv application Failover and Relocate with Discovered Apps
        on Cloned and Snapshot-restored RBD volumes
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is Down
            2) Relocate back to primary

        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        cnv_workloads = discovered_apps_dr_workload_cnv(pvc_vm=1)

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace, discovered_apps=True
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace, discovered_apps=True
        )

        # Creating a file (file1) on VM and calculating its MD5sum
        for cnv_wl in cnv_workloads:
            md5sum_original.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[0],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        for cnv_wl, md5sum in zip(cnv_workloads, md5sum_original):
            logger.info(
                f"Original checksum of file {vm_filepaths[0]} on VM {cnv_wl.workload_name}: {md5sum}"
            )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace, discovered_apps=True
        )

        logger.info(f"Waiting for {scheduling_interval} minutes")
        sleep(scheduling_interval * 60)
        cnv_obj = CnvWorkloadDiscoveredApps()

        cnv_obj.delete_workload(delete_project=False)

        # Create clones or snapshot-restored PVCs
        cloned_or_restored_volumes = self.clone_snapshot_workload_pvcs(
            multi_pvc_clone_factory=multi_pvc_clone_factory,
            multi_snapshot_factory=multi_snapshot_factory,
            snapshot_restore_factory=snapshot_restore_factory,
        )

        logger.info(
            f"Volume type: {volume_type}, Available volumes: {cloned_or_restored_volumes}"
        )

        # Select the correct volumes based on the volume_type
        if volume_type == "clone":
            # Use the first nested list of cloned PVCs
            selected_volumes = cloned_or_restored_volumes[0]
            logger.info(f"Testing with cloned volumes: {selected_volumes}")

        modify_pod_pvc_name()
        cnv_obj.deploy_workload_flattening()

        # Stop primary cluster nodes
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        logger.info(
            f"Stopping nodes of primary cluster: {primary_cluster_name_before_failover}"
        )
        nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)
        wait_time = 5
        logger.info(f"Wait for {wait_time} mins after stopping nodes")
        time.sleep(wait_time * 60)
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
        )
        logger.info("Start the nodes of down cluster")
        nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
        wait_for_nodes_status([node.name for node in primary_cluster_nodes])
        another_wait_time = 3
        logger.info(f"Wait for {another_wait_time} mins for pods to stabilize")
        sleep(another_wait_time * 60)
        logger.info("Wait for all the pods in openshift-storage to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()
        logger.info("Doing cleanup operations post failover")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
            workload_namespace=cnv_workloads[0].workload_namespace,
            workload_dir=cnv_workloads[0].workload_dir,
        )

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        # Validating data integrity (file1) after failing-over VMs to secondary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )

        # Creating a file (file2) post failover
        for cnv_wl in cnv_workloads:
            md5sum_failover.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[1],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        for cnv_wl, md5sum in zip(cnv_workloads, md5sum_failover):
            logger.info(
                f"Checksum of files written after Failover: {vm_filepaths[1]} on VM {cnv_wl.workload_name}: {md5sum}"
            )

        # Doing Relocate
        primary_cluster_name_after_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace, discovered_apps=True
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace, discovered_apps=True
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace, discovered_apps=True
        )
        logger.info("Running Relocate Steps")
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        dr_helpers.relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=cnv_workloads[0],
        )
        # Verify resources creation on primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        # Validating data integrity (file1) after relocating VMs back to primary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )

        # Validating data integrity (file2) after relocating VMs back to primary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[1], md5sum_failover, "Relocate"
        )

        # Creating a file (file3) post relocate
        for cnv_wl in cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )
