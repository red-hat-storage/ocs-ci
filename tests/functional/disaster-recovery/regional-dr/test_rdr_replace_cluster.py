import logging

import pytest

from time import sleep

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad, tier4a
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.helpers.dr_helpers import (
    replace_cluster,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import (
    validate_data_integrity,
    validate_data_integrity_vm,
)
from ocs_ci.ocs.node import (
    get_node_objs,
    wait_for_nodes_status,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@tier4a
@turquoise_squad
class TestRDRReplaceCluster:
    """
    Test recovery to a replacement cluster with Regional-DR.

    Automates the procedure described in the ODF documentation section
    "Recovering to a replacement cluster with Regional-DR":
    the original primary managed cluster is permanently lost, applications are
    failed over to the surviving (secondary) cluster, DR configuration is removed
    and rebuilt against a freshly imported replacement cluster, and finally the
    applications are relocated back onto the replacement cluster.

    Unlike Metro-DR, Regional-DR does not use cluster fencing, so the primary
    cluster is only simulated as lost by stopping its nodes before failover.

    """

    @pytest.mark.polarion_id("OCS-XXXX")
    def test_rdr_replace_cluster(
        self,
        dr_workload,
        discovered_apps_dr_workload,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Verify recovery of a Regional-DR setup to a replacement cluster with a
        mixed set of protected applications running simultaneously:

            - Two ApplicationSet based apps (one RBD, one CephFS)
            - One CNV (VM) workload deployed as a discovered app
            - Two Busybox discovered apps (one RBD, one CephFS)

        Test steps:
            1. Deploy all workloads and write initial data
            2. Simulate permanent loss of the primary cluster (stop its nodes)
            3. Failover every application to the surviving cluster
            4. Verify resources and data integrity on the surviving cluster
            5. Recover the (now stale) primary cluster and clean up the discovered
               apps resources on it
            6. Replace the lost cluster: rebuild DR against a freshly imported
               replacement cluster and re-protect every application
            7. Relocate every application back onto the replacement cluster
            8. Verify resources and data integrity on the replacement cluster

        """
        md5sum_original = []
        vm_filepaths = ["/dd_file1.txt"]

        # Deploy two ApplicationSet based apps, one RBD backed and one CephFS backed
        logger.info("Deploy ApplicationSet workloads (RBD and CephFS)")
        dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        appset_workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )
        appset_rbd, appset_cephfs = appset_workloads[0], appset_workloads[1]

        # Deploy two Busybox discovered apps, one RBD backed and one CephFS backed
        logger.info("Deploy Busybox discovered app workloads (RBD and CephFS)")
        discovered_apps_dr_workload(kubeobject=1, pvc_interface=constants.CEPHBLOCKPOOL)
        discovered_busybox = discovered_apps_dr_workload(
            kubeobject=1, pvc_interface=constants.CEPHFILESYSTEM
        )
        discovered_rbd, discovered_cephfs = (
            discovered_busybox[0],
            discovered_busybox[1],
        )

        # Deploy a single CNV (VM) discovered app
        logger.info("Deploy CNV discovered app workload")
        cnv_workloads = discovered_apps_dr_workload_cnv(pvc_vm=1)
        cnv_wl = cnv_workloads[0]

        appset_workloads = [appset_rbd, appset_cephfs]
        busybox_discovered_workloads = [discovered_rbd, discovered_cephfs]
        discovered_workloads = [cnv_wl] + busybox_discovered_workloads

        # All workloads are deployed onto the same primary managed cluster. Use one
        # of the ApplicationSet apps to identify the primary/secondary clusters.
        appset_rbd_drpc = f"{appset_rbd.appset_placement_name}-drpc"
        primary_cluster_name = get_current_primary_cluster_name(
            appset_rbd.workload_namespace,
            constants.APPLICATION_SET,
            resource_name=appset_rbd_drpc,
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            appset_rbd.workload_namespace,
            constants.APPLICATION_SET,
            resource_name=appset_rbd_drpc,
        )

        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        CNVInstaller().download_and_extract_virtctl_binary()

        # Write initial data to the VM and record its checksum
        logger.info("Write initial data to the VM and record checksum")
        md5sum_original.append(
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[0],
                username=cnv_wl.vm_username,
                verify=True,
            )
        )

        # Let the IOs sync across clusters before simulating the cluster loss
        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_wl.workload_namespace,
            discovered_apps=True,
            resource_name=cnv_wl.discovered_apps_placement_name,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for IOs to sync")
        sleep(wait_time * 60)

        # Simulate permanent loss of the primary managed cluster by stopping its
        # nodes.
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
        nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)
        logger.info("Waiting 300s for the primary cluster to become unreachable")
        sleep(300)

        # Failover every application to the surviving (secondary) cluster
        logger.info(f"Failover all workloads to secondary: {secondary_cluster_name}")
        for wl in appset_workloads:
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=wl.workload_namespace,
                workload_type=constants.APPLICATION_SET,
                workload_placement_name=wl.appset_placement_name,
            )
        for wl in discovered_workloads:
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=wl.workload_namespace,
                discovered_apps=True,
                workload_placement_name=wl.discovered_apps_placement_name,
                old_primary=primary_cluster_name,
            )

        # Verify resources and data integrity on the surviving cluster
        logger.info("Verify resources on surviving cluster after failover")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in appset_workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )
            validate_data_integrity(wl.workload_namespace)
        for wl in busybox_discovered_workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
            )
            validate_data_integrity(wl.workload_namespace)
        wait_for_all_resources_creation(
            cnv_wl.workload_pvc_count,
            cnv_wl.workload_pod_count,
            cnv_wl.workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_wl.discovered_apps_placement_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_wl.vm_name,
            namespace=cnv_wl.workload_namespace,
            phase=constants.STATUS_RUNNING,
        )
        validate_data_integrity_vm(
            [cnv_wl], vm_filepaths[0], md5sum_original, "Failover"
        )

        # Recover the primary cluster so its stale discovered apps resources can be
        # cleaned up before it is detached and replaced.
        logger.info(f"Recovering the primary cluster: {primary_cluster_name}")
        config.switch_to_cluster_by_name(primary_cluster_name)
        nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
        wait_for_nodes_status(
            [node.name for node in primary_cluster_nodes], timeout=600, sleep=10
        )
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)

        logger.info("Cleanup discovered apps resources on the old primary")
        for wl in discovered_workloads:
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=wl.discovered_apps_placement_name,
                old_primary=primary_cluster_name,
                workload_namespace=wl.workload_namespace,
                workload_dir=wl.workload_dir,
                vrg_name=wl.discovered_apps_placement_name,
            )

        # Remove DR configuration and rebuild it against the replacement cluster,
        # re-protecting both the ApplicationSet apps and the discovered apps on the
        # surviving cluster.
        replace_cluster(
            appset_workloads,
            primary_cluster_name,
            secondary_cluster_name,
            discovered_apps_workload=discovered_workloads,
        )

        logger.info("Verify discovered apps are DR protected after replacement")
        for wl in discovered_workloads:
            dr_helpers.validate_application_odf_cli(
                drpc_name=wl.discovered_apps_placement_name,
                namespace=constants.DR_OPS_NAMESPACE,
            )

        # After replacement the surviving cluster is the current primary and the
        # freshly imported replacement cluster is the new secondary.
        replacement_cluster_name = get_current_secondary_cluster_name(
            appset_rbd.workload_namespace,
            constants.APPLICATION_SET,
            resource_name=appset_rbd_drpc,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_wl.workload_namespace,
            discovered_apps=True,
            resource_name=cnv_wl.discovered_apps_placement_name,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for IOs to sync before relocate")
        sleep(wait_time * 60)

        # Relocate every application back onto the replacement cluster
        logger.info(
            f"Relocate all workloads to replacement: {replacement_cluster_name}"
        )
        for wl in appset_workloads:
            dr_helpers.relocate(
                preferred_cluster=replacement_cluster_name,
                namespace=wl.workload_namespace,
                workload_type=constants.APPLICATION_SET,
                workload_placement_name=wl.appset_placement_name,
            )
        for wl in discovered_workloads:
            dr_helpers.relocate(
                preferred_cluster=replacement_cluster_name,
                namespace=wl.workload_namespace,
                workload_placement_name=wl.discovered_apps_placement_name,
                discovered_apps=True,
                old_primary=secondary_cluster_name,
                workload_instance=wl,
            )

        # Verify the ApplicationSet resources are removed from the old primary
        # (surviving cluster). Discovered apps cleanup is handled within relocate.
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in appset_workloads:
            wait_for_all_resources_deletion(wl.workload_namespace)

        # Verify resources and data integrity on the replacement cluster
        logger.info("Verify resources on replacement cluster after relocate")
        config.switch_to_cluster_by_name(replacement_cluster_name)
        for wl in appset_workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )
            validate_data_integrity(wl.workload_namespace)
        for wl in busybox_discovered_workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
            )
            validate_data_integrity(wl.workload_namespace)
        wait_for_all_resources_creation(
            cnv_wl.workload_pvc_count,
            cnv_wl.workload_pod_count,
            cnv_wl.workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_wl.discovered_apps_placement_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_wl.vm_name,
            namespace=cnv_wl.workload_namespace,
            phase=constants.STATUS_RUNNING,
        )
        validate_data_integrity_vm(
            [cnv_wl], vm_filepaths[0], md5sum_original, "Relocate"
        )
