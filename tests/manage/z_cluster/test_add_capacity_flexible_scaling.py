import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_bm,
)
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    tier1,
    acceptance,
    flexible_scaling_required,
)
from ocs_ci.utility.localstorage import check_pvs_created
from ocs_ci.ocs.node import add_disk_to_node
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.ocs.cluster import check_ceph_health_after_add_capacity
from ocs_ci.ocs.resources.storage_cluster import (
    osd_encryption_verification,
    get_storage_cluster,
)
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service
from ocs_ci.ocs.ui.helpers_ui import ui_add_capacity_conditions
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
from ocs_ci.ocs.ui.add_replace_device_ui import AddReplaceDeviceUI


logger = logging.getLogger(__name__)


@ignore_leftovers
@tier1
@acceptance
@skipif_bm
@flexible_scaling_required
@skipif_managed_service
class TestAddCapacityFlexibleScaling(ManageTest):
    """
    Automates adding variable capacity to the flexible scaling cluster
    """

    def test_add_capacity_flexible_scaling(self):
        """
        Test to add variable capacity to the OSD cluster

        """
        existing_osd_pods = get_osd_pods()
        node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
        add_disk_to_node(node_objs[0])
        check_pvs_created(num_pvs_required=1)
        if ui_add_capacity_conditions():
            try:
                setup_ui = login_ui()
                add_ui_obj = AddReplaceDeviceUI(setup_ui)
                add_ui_obj.add_capacity_ui()
                close_browser(setup_ui)
            except Exception as e:
                logger.error(
                    f"Add capacity via UI is not applicable and "
                    f"CLI method will be done. The error is {e}"
                )
                self.change_count_disks_on_storage_cluster_yaml()
        else:
            self.change_count_disks_on_storage_cluster_yaml()

        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=len(existing_osd_pods) + 1,
        )

        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        check_ceph_health_after_add_capacity(ceph_rebalance_timeout=3600)

    def change_count_disks_on_storage_cluster_yaml(self):
        existing_osd_pods = get_osd_pods()
        sc = get_storage_cluster()
        # adding the storage capacity to the cluster
        params = f"""[{{ "op": "replace", "path": "/spec/storageDeviceSets/0/count",
                    "value": {str(len(existing_osd_pods) + 1)}}}]"""
        sc.patch(
            resource_name=sc.get()["items"][0]["metadata"]["name"],
            params=params.strip("\n"),
            format_type="json",
        )
