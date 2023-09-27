import logging
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import tier4a, turquoise_squad
from ocs_ci.framework import config
from ocs_ci.ocs.acm.acm import AcmAddClusters, validate_cluster_import
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
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
)
from ocs_ci.helpers.dr_helpers_ui import (
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
    verify_drpolicy_ui,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_active_acm_index
from ocs_ci.utility import version, vsphere

logger = logging.getLogger(__name__)


@tier4a
@turquoise_squad
class TestApplicationFailoverAndRelocateWhenZoneDown:
    """
    Test failover and relocate all apps in a single zone after a zone disruption
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, dr_workload):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name
                and get_fence_state(self.primary_cluster_name) == "Fenced"
            ):
                enable_unfence(self.primary_cluster_name)
                gracefully_reboot_ocp_nodes(self.namespace, self.primary_cluster_name)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-XXXX")
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
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error(
                    "ODF/ACM version isn't supported for Failover/Relocate operation"
                )
                raise NotImplementedError

        acm_obj = AcmAddClusters()
        workload = dr_workload(num_of_subscription=1)[0]
        self.namespace = workload.workload_namespace

        # Create application on Primary managed cluster
        set_current_primary_cluster_context(workload.workload_namespace)
        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=workload.workload_namespace
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            workload.workload_namespace
        )

        # Create backup-schedule on active hub
        create_backup_schedule()
        # ToDo: To verfiy all the backups are taken
        wait_time = 300
        logger.info(f"Wait {wait_time} until backup is taken ")
        time.sleep(wait_time)

        # Get nodes from zone where active hub running
        config.switch_ctx(get_active_acm_index())
        active_hub_index = config.cur_index
        zone = config.ENV_DATA.get("zone")
        active_hub_cluster_node_objs = get_node_objs()
        set_current_primary_cluster_context(workload.workload_namespace)
        if config.ENV_DATA.get("zone") == zone:
            managed_cluster_index = config.cur_index
            managed_cluster_node_objs = get_node_objs()
        else:
            set_current_secondary_cluster_context(workload.workload_namespace)
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

        # Validate the secondary managed cluster are imported
        validate_cluster_import(cluster_name=secondary_cluster_name)

        # Validate klusterlet addons are running on managed cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        wait_for_pods_to_be_running(
            namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
        )

        # Wait or verify the drpolicy is in validated state
        if config.RUN.get("mdr_failover_via_ui"):
            verify_drpolicy_ui(acm_obj, 0)
        else:
            verify_drpolicy_cli()

        # ToDo: Deploy application in both managed cluster and
        #  to verify the applications are present in secondary cluster

        # Fenced the primary managed cluster
        enable_fence(
            drcluster_name=self.primary_cluster_name,
            switch_ctx=config.switch_ctx(get_passive_acm_index()),
        )

        # Application Failover to Secondary managed cluster
        if config.RUN.get("mdr_failover_via_ui"):
            logger.info("Start the process of Failover from ACM UI")
            config.switch_ctx(get_passive_acm_index())
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{workload.workload_name}-1",
                policy_name=workload.dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
            )
        else:
            failover(
                failover_cluster=secondary_cluster_name,
                namespace=workload.workload_namespace,
                switch_ctx=config.switch_ctx(get_passive_acm_index()),
            )

        # Verify application are running in other managedcluster
        # And not in previous cluster
        set_current_primary_cluster_context(workload.workload_namespace)
        wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )

        # Verify the failover status from UI
        if config.RUN.get("mdr_failover_via_ui"):
            config.switch_ctx(get_passive_acm_index())
            verify_failover_relocate_status_ui(acm_obj)

        # Start nodes of the managed cluster and ceph nodes which is down
        wait_time = 120
        vm_objs.start_vms(vms=ceph_vms)
        logger.info(
            f"Wait for {wait_time} seconds before starting the nodes of managed cluster which is down"
        )
        time.sleep(wait_time)
        nodes_multicluster[managed_cluster_index].start_nodes(managed_cluster_node_objs)
        logger.info(
            f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
        )
        time.sleep(wait_time)
        wait_for_nodes_status([node.name for node in managed_cluster_node_objs])

        wait_for_cluster_connectivity()
        logger.info(f"Wait for {wait_time} seconds after cluster is bought up")
        time.sleep(wait_time)

        # Validate the primary managed cluster is imported which was down
        validate_cluster_import(cluster_name=self.primary_cluster_name)

        # Validate klusterlet addons are running on managed cluster
        config.switch_to_cluster_by_name(self.primary_cluster_name)
        wait_for_pods_to_be_running(
            namespace=constants.ACM_ADDONS_NAMESPACE, timeout=300, sleep=15
        )

        # Verify application are deleted from old cluster
        set_current_secondary_cluster_context(workload.workload_namespace)
        wait_for_all_resources_deletion(workload.workload_namespace)

        # Validate data integrity
        set_current_primary_cluster_context(workload.workload_namespace)
        validate_data_integrity(workload.workload_namespace)

        # Unfenced the managed cluster which was Fenced earlier
        enable_unfence(
            drcluster_name=self.primary_cluster_name,
            switch_ctx=config.switch_ctx(get_passive_acm_index()),
        )

        # Reboot the nodes which unfenced
        gracefully_reboot_ocp_nodes(
            workload.workload_namespace, self.primary_cluster_name
        )

        # Application Relocate to Primary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(
            workload.workload_namespace
        )
        if config.RUN.get("mdr_relocate_via_ui"):
            logger.info("Start the process of Relocate from ACM UI")
            # Relocate via ACM UI
            config.switch_ctx(get_passive_acm_index())
            check_cluster_status_on_acm_console(acm_obj)
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{workload.workload_name}-1",
                policy_name=workload.dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
                action=constants.ACTION_RELOCATE,
            )
        else:
            relocate(
                secondary_cluster_name,
                workload.workload_namespace,
                switch_ctx=config.switch_ctx(get_passive_acm_index()),
            )

        # Verify resources deletion from previous primary or current secondary cluster
        set_current_secondary_cluster_context(workload.workload_namespace)
        wait_for_all_resources_deletion(workload.workload_namespace)

        # Verify resources creation on preferredCluster
        set_current_primary_cluster_context(workload.workload_namespace)
        wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )

        # Verify Relocate status from UI
        if config.RUN.get("mdr_relocate_via_ui"):
            config.switch_ctx(get_passive_acm_index())
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )

        # Validate data integrity
        set_current_primary_cluster_context(workload.workload_namespace)
        validate_data_integrity(workload.workload_namespace)
