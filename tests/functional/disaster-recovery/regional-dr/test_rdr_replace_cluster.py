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

    @pytest.mark.parametrize(
        argnames=["workload_type"],
        argvalues=[
            pytest.param(
                constants.SUBSCRIPTION,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="subscription",
            ),
            pytest.param(
                constants.APPLICATION_SET,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="appset",
            ),
        ],
    )
    def test_rdr_replace_cluster(
        self,
        workload_type,
        nodes_multicluster,
        dr_workload,
    ):
        """
        Verify recovery of a Regional-DR setup to a replacement cluster for
        Subscription and ApplicationSet based applications.

        """

        # Deploy a Subscription or ApplicationSet based application
        if workload_type == constants.SUBSCRIPTION:
            wl = dr_workload(num_of_subscription=1)[0]
        else:
            wl = dr_workload(num_of_subscription=0, num_of_appset=1)[0]
        self.namespace = wl.workload_namespace
        self.workload_type = wl.workload_type
        workload = [wl]

        # DRPC lookup differs for ApplicationSet (DRPC lives in the GitOps
        # namespace and must be referenced by name).
        resource_name = (
            f"{wl.appset_placement_name}-drpc"
            if self.workload_type == constants.APPLICATION_SET
            else None
        )

        # Identify the primary managed cluster hosting the application
        primary_cluster_name = get_current_primary_cluster_name(
            self.namespace, self.workload_type, resource_name=resource_name
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        node_objs = get_node_objs()
        secondary_cluster_name = get_current_secondary_cluster_name(
            self.namespace, self.workload_type, resource_name=resource_name
        )

        # Simulate permanent loss of the primary managed cluster by stopping
        # its nodes.
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
        nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        # Failover the application to the surviving (secondary) cluster
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=wl.workload_namespace,
            workload_type=self.workload_type,
            workload_placement_name=(
                wl.appset_placement_name
                if self.workload_type != constants.SUBSCRIPTION
                else None
            ),
        )

        # Verify the application is running on the surviving cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            wl.workload_namespace,
        )

        # Validate data integrity on the surviving cluster
        validate_data_integrity(wl.workload_namespace)

        # Remove DR configuration and rebuild it against the replacement cluster.
        # This detaches the lost primary, imports the recovery cluster, reinstalls
        # the Multicluster Orchestrator, and re-creates the mirror peer and DR
        # policy before re-applying it to the surviving workloads.
        replace_cluster(workload, primary_cluster_name, secondary_cluster_name)

        # After replacement the surviving cluster is the current primary and the
        # freshly imported replacement cluster is the new secondary.
        replacement_cluster_name = get_current_secondary_cluster_name(
            self.namespace, self.workload_type, resource_name=resource_name
        )

        # Relocate the application back onto the replacement cluster
        dr_helpers.relocate(
            preferred_cluster=replacement_cluster_name,
            namespace=wl.workload_namespace,
            workload_type=self.workload_type,
            workload_placement_name=(
                wl.appset_placement_name
                if self.workload_type != constants.SUBSCRIPTION
                else None
            ),
        )

        # Verify resources are deleted from the old primary (surviving cluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        wait_for_all_resources_deletion(wl.workload_namespace)

        # Verify resources are created on the replacement cluster
        config.switch_to_cluster_by_name(replacement_cluster_name)
        wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            wl.workload_namespace,
        )

        # Validate data integrity on the replacement cluster
        validate_data_integrity(wl.workload_namespace)

    def test_rdr_replace_cluster_discovered_apps_cnv(
        self,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Verify recovery of a Regional-DR setup to a replacement cluster for a
        CNV (VM) workload deployed as a discovered app.

        Test steps:
            1. Deploy a CNV discovered app workload and write initial data
            2. Simulate permanent loss of the primary cluster (stop its nodes)
            3. Failover the discovered app to the surviving cluster
            4. Verify VM and data integrity on the surviving cluster
            5. Recover the (now stale) primary cluster and clean up its resources
            6. Replace the lost cluster: rebuild DR against a freshly imported
               replacement cluster and re-protect the discovered app
            7. Relocate the discovered app back onto the replacement cluster
            8. Verify VM and data integrity on the replacement cluster

        """
        md5sum_original = []
        vm_filepaths = ["/dd_file1.txt"]

        logger.info("Deploy CNV discovered app workload")
        cnv_workloads = discovered_apps_dr_workload_cnv(pvc_vm=1)
        workload = cnv_workloads
        placement_name = cnv_workloads[0].discovered_apps_placement_name
        workload_namespace = cnv_workloads[0].workload_namespace

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workload_namespace,
            discovered_apps=True,
            resource_name=placement_name,
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        CNVInstaller().download_and_extract_virtctl_binary()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload_namespace,
            discovered_apps=True,
            resource_name=placement_name,
        )

        logger.info("Write initial data to VMs and record checksums")
        for cnv_wl in cnv_workloads:
            md5sum_original.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[0],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload_namespace,
            discovered_apps=True,
            resource_name=placement_name,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for IOs to sync")
        sleep(wait_time * 60)

        # Simulate permanent loss of the primary managed cluster by stopping
        # its nodes.
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
        nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)
        logger.info("Waiting 300s for the primary cluster to become unreachable")
        sleep(300)

        logger.info(f"Failover discovered app to secondary: {secondary_cluster_name}")
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=workload_namespace,
            discovered_apps=True,
            workload_placement_name=placement_name,
            old_primary=primary_cluster_name,
        )

        logger.info("Verify resources on surviving cluster after failover")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            workload_namespace,
            discovered_apps=True,
            vrg_name=placement_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        logger.info("Validate data integrity on surviving cluster after failover")
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )

        # Recover the primary cluster so its stale resources can be cleaned up
        # before it is detached and replaced.
        logger.info(f"Recovering the primary cluster: {primary_cluster_name}")
        config.switch_to_cluster_by_name(primary_cluster_name)
        nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
        wait_for_nodes_status(
            [node.name for node in primary_cluster_nodes], timeout=600, sleep=10
        )
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)

        logger.info("Cleanup discovered apps resources on the old primary")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=placement_name,
            old_primary=primary_cluster_name,
            workload_namespace=workload_namespace,
            workload_dir=cnv_workloads[0].workload_dir,
            vrg_name=placement_name,
        )

        # Remove DR configuration and rebuild it against the replacement cluster,
        # re-protecting the discovered app on the surviving cluster.
        replace_cluster(
            workload,
            primary_cluster_name,
            secondary_cluster_name,
            discovered_apps=True,
        )

        logger.info("Verify discovered app is DR protected after replacement")
        dr_helpers.validate_application_odf_cli(
            drpc_name=placement_name,
            namespace=constants.DR_OPS_NAMESPACE,
        )

        # After replacement the surviving cluster is the current primary and the
        # freshly imported replacement cluster is the new secondary.
        replacement_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload_namespace,
            discovered_apps=True,
            resource_name=placement_name,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload_namespace,
            discovered_apps=True,
            resource_name=placement_name,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for IOs to sync before relocate")
        sleep(wait_time * 60)

        logger.info(
            f"Relocate discovered app to replacement: {replacement_cluster_name}"
        )
        dr_helpers.relocate(
            preferred_cluster=replacement_cluster_name,
            namespace=workload_namespace,
            workload_placement_name=placement_name,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=cnv_workloads[0],
        )

        logger.info("Verify resources on replacement cluster after relocate")
        config.switch_to_cluster_by_name(replacement_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            workload_namespace,
            discovered_apps=True,
            vrg_name=placement_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        logger.info("Validate data integrity on replacement cluster after relocate")
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )
