import logging
import pytest

from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier4a,
    E2ETest,
    vsphere_platform_required,
    skipif_vsphere_ipi,
    bugzilla,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.utility import vsphere
from ocs_ci.ocs.node import verify_all_nodes_created, get_osd_running_nodes


logger = logging.getLogger(__name__)


@tier4a
@skipif_vsphere_ipi
@vsphere_platform_required
@bugzilla("1904302")
@pytest.mark.polarion_id("OCS-2658")
class TestVmDeletionAndReplacement(E2ETest):
    """
    Test to delete VM with data disk and replacing with another node
    """

    @pytest.fixture(autouse=True)
    def init(self):

        self.cluster_name = config.ENV_DATA.get("cluster_name")
        self.cluster = config.ENV_DATA["vsphere_cluster"]
        self.datacenter = config.ENV_DATA["vsphere_datacenter"]
        self.datastore = config.ENV_DATA["vsphere_datastore"]
        self.server = config.ENV_DATA["vsphere_server"]
        self.user = config.ENV_DATA["vsphere_user"]
        self.password = config.ENV_DATA["vsphere_password"]
        self.sanity_helpers = Sanity()

    def test_vm_deletion_and_replacement(
        self, pvc_factory, pod_factory, add_nodes, bucket_factory, rgw_bucket_factory
    ):
        """
        1. Get OSDs and OSD running nodes
        2. Get compute VMs
        3. Destroy VM
        4. Replace VM
        5. Label node as OCS
        6. Check all pods are running and cluster in Healthy state
        7. Create Resources
        """

        # OSDs before running VM replacement
        old_osds = []
        for osd in pod.get_osd_pods():
            old_osds.append(osd.name)
        logger.info(f"OSDs before VM replacement {old_osds}")

        # Get osd running nodes
        osd_nodes = get_osd_running_nodes()
        logger.info(f"osd_nodes {osd_nodes}")

        # Get compute VMs from the cluster
        vm_obj = vsphere.VSPHERE(self.server, self.user, self.password)
        old_vms = vm_obj.get_compute_vms_in_pool(
            name=self.cluster_name, dc=self.datacenter, cluster=self.cluster
        )
        logger.info(f"Type of vm {type(old_vms)}")
        logger.info(f"old_vms {old_vms}")

        # Destroy VM along with data disk
        vm_obj.destroy_vms(vms=old_vms[0])
        vms = vm_obj.get_compute_vms_in_pool(
            name=self.cluster_name, dc=self.datacenter, cluster=self.cluster
        )
        assert len(old_vms) > len(vms), "Failed to destroy one compute VM"
        logger.info("Destroyed VM")

        # Add new VM and label it with OCS
        logger.info("Adding new VM to the OCP cluster")
        add_nodes()
        verify_all_nodes_created()

        # Check cluster health and create resources
        self.sanity_helpers.health_check(tries=120)

        # OSDs after running VM replacement
        current_osds = []
        for osd in pod.get_osd_pods():
            current_osds.append(osd.name)
        logger.info(f"OSDs after VM replacement {current_osds}")

        # Get new osd
        new_osd = [osd_name for osd_name in current_osds if osd_name not in old_osds]
        logger.info(f"New osd {new_osd}")

        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
