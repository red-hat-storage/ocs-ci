import logging

import pytest
import time
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.pytest_customization.marks import (
    tier4a,
    turquoise_squad,
    vsphere_platform_required,
)
from ocs_ci.framework import config
from ocs_ci.ocs.acm.acm import validate_cluster_import
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour, CommandFailed
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.helpers.dr_helpers import (
    add_label_to_appsub,
    create_klusterlet_config,
    enable_fence,
    enable_unfence,
    get_fence_state,
    get_nodes_from_active_zone,
    failover,
    relocate,
    restore_backup,
    create_backup_schedule,
    set_current_primary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_passive_acm_index,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    gracefully_reboot_ocp_nodes,
    verify_drpolicy_cli,
    verify_restore_is_completed,
    verify_fence_state,
    verify_backup_is_taken,
    remove_parameter_klusterlet_config,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.utility import vsphere

logger = logging.getLogger(__name__)


@tier4a
@turquoise_squad
@vsphere_platform_required
class TestApplicationFailoverAndRelocateWhenZoneDown:
    """
    Failover and Relocate with one entire Zone down (Co-situated Hub Recovery)

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes_multicluster, dr_workload):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        self.managed_cluster_node_objs = []
        self.ceph_vms = []
        self.primary_cluster_name = ""

        def finalizer():

            if self.managed_cluster_node_objs is not None:
                config.switch_to_cluster_by_name(self.primary_cluster_name)
                try:
                    nodes_multicluster[
                        self.managed_cluster_index
                    ].restart_nodes_by_stop_and_start_teardown()
                except CommandFailed:
                    nodes_multicluster[self.managed_cluster_index].start_nodes(
                        self.managed_cluster_node_objs
                    )
                    wait_for_nodes_status(
                        [node.name for node in self.managed_cluster_node_objs]
                    )

            if self.ceph_vms is not None:
                for vm in self.ceph_vms:
                    status = self.vm_objs.get_vm_power_status(vm=vm)
                    if status == "poweredOff":
                        self.vm_objs.start_vms(vms=[vm])

            if (
                self.primary_cluster_name is not None
                and get_fence_state(
                    drcluster_name=self.primary_cluster_name,
                    switch_ctx=get_passive_acm_index(),
                )
                == "Fenced"
            ):
                enable_unfence(
                    drcluster_name=self.primary_cluster_name,
                    switch_ctx=get_passive_acm_index(),
                )
                gracefully_reboot_ocp_nodes(self.primary_cluster_name)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-4787")
    def test_application_failover_and_relocate(
        self,
        nodes_multicluster,
        dr_workload,
    ):

        """
        Tests to verify failover and relocate all apps in a single zone after a zone disruption

        1. Deploy applications on managed clusters
        2. Enable Backup on Hub clusters
        3. Bring the entire active zone down
        4. Restore to new hub or passive hub, and validate the drpolicy is validated
        5. Failure the application from down managed cluster to surviving managed cluster
        6. Bring the managed cluster and ceph nodes up which were down in step 3
        7. Relocate the application back to managed cluster

        """

        # Deploy applications on managed clusters
        # ToDO: deploy application on both managed clusters
        workloads = dr_workload(
            num_of_subscription=1, num_of_appset=1, switch_ctx=get_passive_acm_index()
        )
        self.namespace = workloads[0].workload_namespace

        # Create application on Primary managed cluster
        set_current_primary_cluster_context(self.namespace)
        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=self.namespace
        )
        secondary_cluster_name = get_current_secondary_cluster_name(self.namespace)

        # Create backup-schedule on active hub
        create_backup_schedule()
        wait_time = 300
        logger.info(f"Wait {wait_time} until backup is taken ")
        time.sleep(wait_time)

        # Validate backup is scheduled or not
        verify_backup_is_taken()

        # Get nodes from zone where active hub running
        (
            active_hub_index,
            active_hub_cluster_node_objs,
            self.managed_cluster_index,
            self.managed_cluster_node_objs,
            ceph_node_ips,
        ) = get_nodes_from_active_zone(self.namespace)

        # Shutdown one zone
        logger.info("Shutting down all the nodes from active hub zone")
        nodes_multicluster[self.managed_cluster_index].stop_nodes(
            self.managed_cluster_node_objs
        )
        nodes_multicluster[active_hub_index].stop_nodes(active_hub_cluster_node_objs)
        host = config.ENV_DATA["vsphere_server"]
        user = config.ENV_DATA["vsphere_user"]
        password = config.ENV_DATA["vsphere_password"]
        self.vm_objs = vsphere.VSPHERE(host, user, password)
        self.ceph_vms = [
            self.vm_objs.get_vm_by_ip(ip=each_ip, dc="None")
            for each_ip in ceph_node_ips
        ]
        self.vm_objs.stop_vms(vms=self.ceph_vms)
        logger.info(
            "All nodes from active hub zone are powered off, "
            f"wait {wait_time} seconds before restoring in passive hub"
        )
        time.sleep(wait_time)

        # Restore new hub
        restore_backup()
        logger.info(f"Wait {wait_time} until restores are taken ")
        time.sleep(wait_time)

        # Verify the restore is completed
        verify_restore_is_completed()

        # Add KlusterletConfig
        create_klusterlet_config()

        # Validate the surviving managed cluster is successfully imported on the new hub
        for sample in TimeoutSampler(
            timeout=1800,
            sleep=60,
            func=validate_cluster_import,
            cluster_name=secondary_cluster_name,
            switch_ctx=get_passive_acm_index(),
        ):
            if sample:
                logger.info(
                    f"Cluster: {secondary_cluster_name} successfully imported post hub recovery"
                )
                # Validate klusterlet addons are running on managed cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                wait_for_pods_to_be_running(
                    namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
                )
                break
            else:
                logger.error(
                    f"import of cluster: {secondary_cluster_name} failed post hub recovery"
                )
                raise UnexpectedBehaviour(
                    f"import of cluster: {secondary_cluster_name} failed post hub recovery"
                )

        # Wait or verify the drpolicy is in validated state
        verify_drpolicy_cli(switch_ctx=get_passive_acm_index())

        # Edit the global KlusterletConfig on the new hub and remove
        # the parameter appliedManifestWorkEvictionGracePeriod and its value.
        remove_parameter_klusterlet_config()

        # For sub app pods to show up after failover in ACM 2.11
        # Workaround: Add a new label with any value to the AppSub on the hub
        add_label_to_appsub(workloads)

        # Fenced the primary managed cluster
        enable_fence(
            drcluster_name=self.primary_cluster_name,
            switch_ctx=get_passive_acm_index(),
        )
        # Verify the primary managed cluster is in Fenced state
        verify_fence_state(
            drcluster_name=self.primary_cluster_name,
            state=constants.ACTION_FENCE,
            switch_ctx=get_passive_acm_index(),
        )

        # Failover action via CLI
        failover_results = []
        config.switch_ctx(get_passive_acm_index())
        with ThreadPoolExecutor() as executor:
            for wl in workloads:
                failover_results.append(
                    executor.submit(
                        failover,
                        failover_cluster=secondary_cluster_name,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        switch_ctx=get_passive_acm_index(),
                        workload_placement_name=wl.appset_placement_name
                        if wl.workload_type != constants.SUBSCRIPTION
                        else None,
                    )
                )
                time.sleep(60)

        # Wait for failover results
        for fl in failover_results:
            fl.result()

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Start nodes of the managed cluster and ceph nodes which is down
        wait_time = 120
        logger.info(f"Wait time {wait_time} before recovering the cluster")
        time.sleep(wait_time)
        # Recover ceph nodes
        self.vm_objs.start_vms(vms=self.ceph_vms)
        # Recover active managed cluster
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        logger.info(
            "Recover active managed cluster which went down during site-failure"
        )
        nodes_multicluster[self.managed_cluster_index].start_nodes(
            self.managed_cluster_node_objs
        )
        wait_for_nodes_status([node.name for node in self.managed_cluster_node_objs])
        logger.info(
            "Check if recovered managed cluster is successfully imported on the new hub"
        )
        for sample in TimeoutSampler(
            timeout=900,
            sleep=60,
            func=validate_cluster_import,
            cluster_name=self.primary_cluster_name,
            switch_ctx=get_passive_acm_index(),
        ):
            if sample:
                logger.info(
                    f"Cluster: {self.primary_cluster_name} successfully imported post hub recovery"
                )
                # Validate klusterlet addons are running on managed cluster
                config.switch_to_cluster_by_name(self.primary_cluster_name)
                wait_for_pods_to_be_running(
                    namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
                )
                break
            else:
                logger.error(
                    f"Import of cluster: {self.primary_cluster_name} failed post hub recovery"
                )
                raise UnexpectedBehaviour(
                    f"Import of cluster: {self.primary_cluster_name} failed post hub recovery"
                )

        logger.info("Wait for approx. an hour to surpass 1hr eviction period timeout")
        time.sleep(3600)
        # Validate ceph health OK
        logger.info("Checking for Ceph Health OK")
        ceph_health_check(tries=40, delay=30)

        # Verify application are deleted from old cluster
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        for wl in workloads:
            wait_for_all_resources_deletion(wl.workload_namespace)

        # Validate data integrity
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            validate_data_integrity(wl.workload_namespace)

        # Unfenced the managed cluster which was Fenced earlier
        enable_unfence(
            drcluster_name=self.primary_cluster_name,
            switch_ctx=get_passive_acm_index(),
        )
        # Verify the primary managed cluster is in Unfenced state
        verify_fence_state(
            drcluster_name=self.primary_cluster_name,
            state=constants.ACTION_UNFENCE,
            switch_ctx=get_passive_acm_index(),
        )

        # Reboot the nodes which unfenced
        gracefully_reboot_ocp_nodes(self.primary_cluster_name)

        # Application Relocate to Primary managed cluster
        logger.info("Start the process of Relocate from CLI")
        relocate_results = []
        config.switch_ctx(get_passive_acm_index())
        with ThreadPoolExecutor() as executor:
            for wl in workloads:
                relocate_results.append(
                    executor.submit(
                        relocate,
                        preferred_cluster=self.primary_cluster_name,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        switch_ctx=get_passive_acm_index(),
                        workload_placement_name=wl.appset_placement_name
                        if wl.workload_type != constants.SUBSCRIPTION
                        else None,
                    )
                )
                time.sleep(60)

        # Wait for relocate results
        for rl in relocate_results:
            rl.result()

        # Verify resources deletion from previous primary or current secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            wait_for_all_resources_deletion(wl.workload_namespace)

        # Verify resources creation on preferredCluster
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        for wl in workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Validate data integrity
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        for wl in workloads:
            validate_data_integrity(wl.workload_namespace)
