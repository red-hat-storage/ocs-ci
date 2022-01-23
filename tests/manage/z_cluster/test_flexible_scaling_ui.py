import logging


from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_no_lso,
    tier1,
    vsphere_platform_required,
    skipif_ui_not_support,
    bugzilla,
    ui,
)
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.add_replace_device_ui import AddReplaceDeviceUI
from ocs_ci.ocs.node import get_node_names
from ocs_ci.deployment.vmware import VSPHEREBASE
from ocs_ci.ocs import constants, defaults
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pv import check_available_pvs
from ocs_ci.ocs.cluster import check_ceph_health_after_add_capacity


log = logging.getLogger(__name__)


@ui
@tier1
@skipif_no_lso
@bugzilla("1943280")
@vsphere_platform_required
@skipif_ui_not_support("add_capacity")
class TestFlexibleScalingUI(ManageTest):
    """
    Test Flexible Scaling via UI

    """

    def test_flexible_scaling_ui(self, setup_ui):
        """
        Test Procedure:
        1.Add new disk to worker node
        2.Add capacity via UI
        3.Verify all OSD pods move to Running state
        4.check ceph health after add capacity

        """
        log.info("Check the number of osd pods")
        num_osd_pods = len(get_osd_pods())

        log.info("Choose one worker node")
        nodes = get_node_names()
        nodes = [nodes[0]]

        log.info(f"Add new disk to node {nodes}")
        vsphere_base = VSPHEREBASE()
        vsphere_base.add_disks_per_node(
            size=config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE),
            disk_type=config.DEPLOYMENT.get("provision_type", constants.VM_DISK_TYPE),
            node_names=nodes,
            extra_disks=1,
        )

        log.info("Verify the number of PVs in Available state.")
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=check_available_pvs,
            expected_available_pvs=1,
        )
        if not sample.wait_for_func_status(True):
            raise Exception("The number of PVs on Available state is not as expected")

        logging.info("Add capacity via UI")
        infra_ui_obj = AddReplaceDeviceUI(setup_ui)
        infra_ui_obj.add_capacity_ui()

        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=num_osd_pods + 1,
        )
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        check_ceph_health_after_add_capacity(ceph_rebalance_timeout=3600)
