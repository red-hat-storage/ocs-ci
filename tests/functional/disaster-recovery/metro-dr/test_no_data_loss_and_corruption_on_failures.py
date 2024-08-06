import logging
import pytest
import time


from ocs_ci.framework.pytest_customization.marks import (
    tier4a,
    turquoise_squad,
    vsphere_platform_required,
)
from ocs_ci.framework import config
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.pod import restart_pods_having_label
from ocs_ci.helpers.dr_helpers import (
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    get_current_primary_cluster_name,
    get_active_acm_index,
)
from ocs_ci.utility import vsphere
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@tier4a
@turquoise_squad
@vsphere_platform_required
class TestNoDataLossAndDataCorruptionOnFailures:
    """
    The  Objective of this test cases is to make sure that the MDR cluster remains accessible
    and NO DU/DL/DC is observed when following Failures are induced with supported applications are running

    1) Noobaa pods failures - repeat at least 5-7 times
    2) Rolling reboot of the nodes in all zones one at a time
    3) RHCS nodes failures
        a. 1 RHCS node in one zone
        b. All the RHCS nodes in one zone
        c. All the RHCS nodes in one zone - Repeated to mimic Santa lucia issue

    """

    @pytest.mark.polarion_id("OCS-4793")
    def test_no_data_loss_and_data_corruption_on_failures(
        self, nodes_multicluster, dr_workload
    ):

        # Deploy Subscription based application
        workloads = dr_workload(num_of_subscription=1, num_of_appset=1)
        self.namespace = workloads[0].workload_namespace

        # Create application on Primary managed cluster
        set_current_primary_cluster_context(self.namespace)
        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=self.namespace
        )

        # Validate data integrity
        for wl in workloads:
            config.switch_to_cluster_by_name(self.primary_cluster_name)
            validate_data_integrity(wl.workload_namespace)

        # Noobaa pod restarts atleast 5 times and verify the data integrity
        for i in range(5):
            restart_pods_having_label(label=constants.NOOBAA_APP_LABEL)
        logger.info(
            "Verify the data integrity of application after repeated failures of Noobaa pods"
        )
        for wl in workloads:
            config.switch_to_cluster_by_name(self.primary_cluster_name)
            validate_data_integrity(wl.workload_namespace)

        # Get the nodes from one active zone and reboot of the nodes in all zones
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
        # Rolling reboot of the nodes in all zones one at a time
        wait_time = 300
        logger.info("Shutting down all the nodes from active hub zone")
        nodes_multicluster[managed_cluster_index].restart_nodes_by_stop_and_start(
            managed_cluster_node_objs
        )
        nodes_multicluster[active_hub_index].restart_nodes_by_stop_and_start(
            active_hub_cluster_node_objs
        )
        host = config.ENV_DATA["vsphere_server"]
        user = config.ENV_DATA["vsphere_user"]
        password = config.ENV_DATA["vsphere_password"]
        vm_objs = vsphere.VSPHERE(host, user, password)
        ceph_vms = [
            vm_objs.get_vm_by_ip(ip=each_ip, dc="None") for each_ip in ceph_node_ips
        ]
        vm_objs.restart_vms(vms=ceph_vms)
        logger.info(
            "All nodes from active hub zone are rebooted/restarted."
            f"Wait for {wait_time} for the nodes up"
        )
        time.sleep(wait_time)
        wait_for_nodes_status([node.name for node in managed_cluster_node_objs])
        wait_for_nodes_status([node.name for node in active_hub_cluster_node_objs])
        # Validate ceph health OK
        ceph_health_check(tries=40, delay=30)

        # Again verify the data integrity of application
        logger.info(
            "Verify the data integrity of application after all nodes from active hub zone are rebooted"
        )
        for wl in workloads:
            config.switch_to_cluster_by_name(self.primary_cluster_name)
            validate_data_integrity(wl.workload_namespace)

        # RHCS nodes failures
        # 1 RHCS node in one zone
        vm_objs.restart_vms(vms=[ceph_vms[0]])
        time.sleep(wait_time)
        # Validate ceph health OK
        ceph_health_check(tries=40, delay=30)

        # All the RHCS nodes in one zone
        vm_objs.restart_vms(vms=ceph_vms)
        time.sleep(wait_time)
        # Validate ceph health OK
        ceph_health_check(tries=40, delay=30)

        # All the RHCS nodes in one zone - Repeated to mimic Santa lucia issue
        for i in range(10):
            vm_objs.restart_vms(vms=ceph_vms)
            logger.info(
                f"Wait {wait_time} before another restart of ceph nodes from zones"
            )
            time.sleep(wait_time)
            # Validate ceph health OK
        ceph_health_check(tries=120, delay=30)

        # Again verify the data integrity of application
        logger.info(
            "Verify the data integrity of application after repeated restart of ceph nodes from zones"
        )
        for wl in workloads:
            config.switch_to_cluster_by_name(self.primary_cluster_name)
            validate_data_integrity(wl.workload_namespace)
