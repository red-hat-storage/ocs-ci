import logging

import pytest
import time
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.pytest_customization.marks import tier4a, turquoise_squad
from ocs_ci.framework import config
from ocs_ci.ocs.acm.acm import AcmAddClusters, validate_cluster_import
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.helpers.dr_helpers import (
    enable_fence,
    enable_unfence,
    get_fence_state,
    failover,
    relocate,
    restore_backup,
    create_backup_schedule,
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_passive_acm_index,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    gracefully_reboot_ocp_nodes,
    verify_drpolicy_cli,
    verify_restore_is_completed,
    verify_fence_state,
)
from ocs_ci.helpers.dr_helpers_ui import (
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_active_acm_index
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.utility import vsphere

logger = logging.getLogger(__name__)


@tier4a
@turquoise_squad
class TestApplicationFailoverAndRelocateWhenZoneDown:
    """
    Failover and Relocate with one entire Zone down (Co-situated Hub Recovery)

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, dr_workload):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name
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
        setup_acm_ui,
        nodes_multicluster,
        dr_workload,
        node_restart_teardown,
    ):

        """
        Tests to verify failover and relocate all apps in a single zone after a zone disruption

        """

        if config.RUN.get("mdr_failover_via_ui"):
            acm_obj = AcmAddClusters()

        workloads = dr_workload(num_of_subscription=1, num_of_appset=1)
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

        # Get nodes from zone where active hub running
        config.switch_ctx(get_active_acm_index())
        active_hub_index = config.cur_index
        zone = config.ENV_DATA.get("zone")
        active_hub_cluster_node_objs = get_node_objs()
        set_current_primary_cluster_context(self.namespace)
        if config.ENV_DATA.get("zone") == zone:
            managed_cluster_index = config.cur_index
            managed_cluster_node_objs = get_node_objs()
        else:
            set_current_secondary_cluster_context(self.namespace)
            managed_cluster_index = config.cur_index
            managed_cluster_node_objs = get_node_objs()
        external_cluster_node_roles = config.EXTERNAL_MODE.get(
            "external_cluster_node_roles"
        )
        ceph_node_ips = []
        for ceph_node in external_cluster_node_roles:
            if (
                external_cluster_node_roles[ceph_node].get("location").get("datacenter")
                != "zone-b"
            ):
                continue
            else:
                ceph_node_ips.append(
                    external_cluster_node_roles[ceph_node].get("ip_address")
                )

        # Shutdown one zones
        logger.info("Shutting down all the nodes from active hub zone")
        nodes_multicluster[managed_cluster_index].stop_nodes(managed_cluster_node_objs)
        nodes_multicluster[active_hub_index].stop_nodes(active_hub_cluster_node_objs)
        host = config.ENV_DATA["vsphere_server"]
        user = config.ENV_DATA["vsphere_user"]
        password = config.ENV_DATA["vsphere_password"]
        vm_objs = vsphere.VSPHERE(host, user, password)
        ceph_vms = [
            vm_objs.get_vm_by_ip(ip=each_ip, dc="None") for each_ip in ceph_node_ips
        ]
        vm_objs.stop_vms(vms=ceph_vms)
        logger.info(
            "All nodes from active hub zone are powered off, "
            f"wait {wait_time} seconds before restoring in passive hub"
        )

        # Restore new hub
        restore_backup()
        wait_time = 300
        logger.info(f"Wait {wait_time} until restores are taken ")
        time.sleep(wait_time)

        # Verify the restore is completed
        verify_restore_is_completed()

        # Validate the clusters are imported
        clusters = [self.primary_cluster_name, secondary_cluster_name]
        for cluster in clusters:
            for sample in TimeoutSampler(
                timeout=1800,
                sleep=60,
                func=validate_cluster_import,
                cluster_name=cluster,
                switch_ctx=get_passive_acm_index(),
            ):
                if sample:
                    logger.info(
                        f"Cluster: {cluster} successfully imported post hub recovery"
                    )
                    # Validate klusterlet addons are running on managed cluster
                    config.switch_to_cluster_by_name(cluster)
                    wait_for_pods_to_be_running(
                        namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
                    )
                    break
                else:
                    logger.error(
                        f"import of cluster: {cluster} failed post hub recovery"
                    )
                    raise UnexpectedBehaviour(
                        f"import of cluster: {cluster} failed post hub recovery"
                    )

        # Wait or verify the drpolicy is in validated state
        verify_drpolicy_cli(switch_ctx=get_passive_acm_index())

        # ToDo: Deploy application in both managed cluster and
        #  to verify the applications are present in secondary cluster

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

        # Application Failover to Secondary managed cluster
        if (
            config.RUN.get("mdr_failover_via_ui")
            and self.workload_type == constants.SUBSCRIPTION
        ):
            logger.info("Start the process of Failover from ACM UI")
            config.switch_ctx(get_passive_acm_index())
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{workloads[0].workload_name}-1",
                policy_name=workloads[0].dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
            )
        else:
            failover_results = []
            with ThreadPoolExecutor() as executor:
                for wl in workloads:
                    failover_results.append(
                        executor.submit(
                            failover,
                            failover_cluster=secondary_cluster_name,
                            namespace=wl.workload_namespace,
                            switch_ctx=get_passive_acm_index(),
                        )
                    )
                    time.sleep(5)

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
        vm_objs.start_vms(vms=ceph_vms)
        # Recover active managed cluster
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        logger.info(
            "Recover active managed cluster which went down during site-failure"
        )
        nodes_multicluster[managed_cluster_index].start_nodes(managed_cluster_node_objs)
        wait_for_nodes_status([node.name for node in managed_cluster_node_objs])
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
        secondary_cluster_name = get_current_secondary_cluster_name(self.namespace)
        if (
            config.RUN.get("mdr_relocate_via_ui")
            and self.workload_type == constants.SUBSCRIPTION
        ):
            logger.info("Start the process of Relocate from ACM UI")
            # Relocate via ACM UI
            config.switch_ctx(get_passive_acm_index())
            check_cluster_status_on_acm_console(acm_obj)
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{workloads[0].workload_name}-1",
                policy_name=workloads[0].dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
                action=constants.ACTION_RELOCATE,
            )
        else:
            relocate_results = []
            with ThreadPoolExecutor() as executor:
                for wl in workloads:
                    relocate_results.append(
                        executor.submit(
                            relocate,
                            preferred_cluster=secondary_cluster_name,
                            namespace=wl.workload_namespace,
                            switch_ctx=get_passive_acm_index(),
                        )
                    )
                    time.sleep(5)

            # Wait for relocate results
            for rl in relocate_results:
                rl.result()

        # Verify resources deletion from previous primary or current secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

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
