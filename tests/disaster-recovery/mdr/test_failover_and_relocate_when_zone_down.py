import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import mdr_test
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.constants import MDR_ZONE_YAML
from ocs_ci.helpers.dr_helpers import (
    enable_fence,
    enable_unfence,
    get_fence_state,
    failover,
    relocate,
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    gracefully_reboot_ocp_nodes,
)
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs.platform_nodes import VMWareNodes
from ocs_ci.utility.utils import convert_hostnames_to_ips
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


@mdr_test
class TestFailoverAndRelocateWhenZoneDown(ManageTest):
    """
    Test Failover and Relocate actions for a busybox application

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, rdr_workload):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():
            if self.drcluster_name and get_fence_state(self.drcluster_name) == "Fenced":
                enable_unfence(self.drcluster_name)
                gracefully_reboot_ocp_nodes(self.namespace, self.drcluster_name)

        request.addfinalizer(finalizer)

    #
    @pytest.mark.parametrize(
        argnames=["zone_down"],
        argvalues=[
            pytest.param("b", marks=pytest.mark.polarion_id("OCS-xxxx"), id="zone_b"),
            pytest.param("c", marks=pytest.mark.polarion_id("OCS-xxxx"), id="zone_c"),
            pytest.param("a", marks=pytest.mark.polarion_id("OCS-xxxx"), id="zone_a"),
        ],
    )
    def test_failover_and_relocate_when_one_zone_down(self, zone_down, rdr_workload):
        """
        Tests to verify application failover and relocate
        between managed clusters when one zone down

        """
        #
        # Create application on Primary managed cluster
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        primary_cluster_name = get_current_primary_cluster_name(
            namespace=rdr_workload.workload_namespace
        )
        self.drcluster_name = primary_cluster_name
        self.namespace = rdr_workload.workload_namespace
        #
        # Make zone down
        odf_nodes = ceph_nodes = arbiter_node = []
        zone_yaml_data = templating.load_yaml(MDR_ZONE_YAML)
        if "b" == zone_down:
            odf_nodes = zone_yaml_data["b"]["odf_nodes"]
            ceph_nodes = zone_yaml_data["b"]["ceph_nodes"]
        elif "c" == zone_down:
            odf_nodes = zone_yaml_data["c"]["odf_nodes"]
            ceph_nodes = zone_yaml_data["c"]["ceph_nodes"]
        else:
            arbiter_node = zone_yaml_data["a"]

        vm_objs = VMWareNodes()
        odf_vms = ceph_vms = arbiter_vms = []

        # Make one zone down
        config.switch_acm_ctx()
        if odf_nodes:
            odf_node_ips = convert_hostnames_to_ips(hostnames=odf_nodes["nodes"])
            odf_vms = vm_objs.get_vm_from_ips(odf_node_ips, odf_nodes["dc"])
            vm_objs.vsphere.stop_vms(vms=odf_vms)
        if ceph_nodes:
            ceph_node_ips = convert_hostnames_to_ips(hostnames=ceph_nodes["nodes"])
            ceph_vms = vm_objs.get_vm_from_ips(ceph_node_ips, ceph_nodes["dc"])
            vm_objs.vsphere.stop_vms(vms=ceph_vms)
        if arbiter_node:
            arbiter_node_ips = convert_hostnames_to_ips(
                hostnames=arbiter_node["arbiter_node"]
            )
            arbiter_vms = vm_objs.get_vm_from_ips(arbiter_node_ips, arbiter_node["dc"])
            vm_objs.vsphere.stop_vms(vms=arbiter_vms)

        # Fenced the primary managed cluster
        enable_fence(drcluster_name=self.drcluster_name)

        # Application Failover to Secondary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )
        failover(
            failover_cluster=secondary_cluster_name,
            namespace=rdr_workload.workload_namespace,
        )

        # Verify application are running in other managedcluster
        # And not in previous cluster
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        # Verify application are deleted from old cluster
        set_current_secondary_cluster_context(rdr_workload.workload_namespace)
        wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        # ToDo: Validate same PV being used

        # Validate data integrity
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        validate_data_integrity(rdr_workload.workload_namespace)

        # Bring up zone which was down
        config.switch_acm_ctx()
        if odf_vms:
            vm_objs.vsphere.start_vms(odf_vms)
        if ceph_vms:
            vm_objs.vsphere.start_vms(ceph_vms)
        if arbiter_vms:
            vm_objs.vsphere.start_vms(arbiter_vms)

        # ToDo: Validate all nodes ar up and ceph health OK

        # Unfenced the managed cluster which was Fenced earlier
        set_current_secondary_cluster_context(rdr_workload.workload_namespace)
        enable_unfence(drcluster_name=self.drcluster_name)

        # Reboot the nodes which unfenced
        gracefully_reboot_ocp_nodes(
            rdr_workload.workload_namespace, self.drcluster_name
        )

        # ToDo: Validate PV is in Released state

        # Application Relocate to Primary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )
        relocate(secondary_cluster_name, rdr_workload.workload_namespace)

        # Verify resources deletion from previous primary or current secondary cluster
        set_current_secondary_cluster_context(rdr_workload.workload_namespace)
        wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        # Verify resources creation on preferredCluster
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        # Validate data integrity
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        validate_data_integrity(rdr_workload.workload_namespace)
