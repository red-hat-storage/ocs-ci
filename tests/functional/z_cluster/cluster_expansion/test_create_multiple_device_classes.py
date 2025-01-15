import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    tier1,
    brown_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.helpers.multiple_device_classes import (
    create_new_lvs_for_new_deviceclass,
    verification_steps_after_adding_new_deviceclass,
)
from ocs_ci.ocs.resources.pv import wait_for_pvs_in_lvs_to_reach_status
from ocs_ci.ocs.resources.storage_cluster import (
    add_new_deviceset_in_storagecluster,
    get_storage_cluster,
)
from ocs_ci.helpers.helpers import (
    create_ceph_block_pool_for_deviceclass,
    create_deviceclass_storageclass,
)
from ocs_ci.utility.utils import ceph_health_check


log = logging.getLogger(__name__)


@brown_squad
@tier1
@ignore_leftovers
class TestMultipleDeviceClasses(ManageTest):
    """
    Automate the multiple device classes tests

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Check that the new osd size has increased and increase the resize osd count

        """
        log.info("Wait for the ceph health to be OK")
        ceph_health_check(tries=20)

    def test_add_new_ssd_device_class(self):
        osd_node_names = get_osd_running_nodes()
        log.info(f"osd node names = {osd_node_names}")
        lvs_obj = create_new_lvs_for_new_deviceclass(osd_node_names)
        log.info(
            f"Wait for the PVs in the LocalVolumeSet {lvs_obj.name} to be available"
        )
        wait_for_pvs_in_lvs_to_reach_status(
            lvs_obj, len(osd_node_names), constants.STATUS_AVAILABLE
        )

        log.info(
            f"Add a new deviceset in the storagecluster for the new LocalVolumeSet {lvs_obj.name} "
            f"which will also create a new deviceclass"
        )
        res = add_new_deviceset_in_storagecluster(lvs_obj.name, lvs_obj.name)
        assert res, "Failed to patch the storagecluster with the new deviceset"
        sc_obj = get_storage_cluster()
        sc_obj.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            column="PHASE",
            timeout=180,
            sleep=10,
        )

        log.info(f"Add a new CephBlockPool for the device class {lvs_obj.name}")
        cbp_obj = create_ceph_block_pool_for_deviceclass(lvs_obj.name)
        assert (
            cbp_obj
        ), f"Failed to create the CephBlockPool for the device class {lvs_obj.name}"
        cbp_obj.ocp.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=cbp_obj.name,
            column="PHASE",
            timeout=120,
            sleep=10,
        )

        log.info(f"Add a new StorageClass for the pool {cbp_obj.name}")
        sc_obj = create_deviceclass_storageclass(pool_name=cbp_obj.name)
        assert sc_obj, f"Failed to create the StorageClass for the pool {cbp_obj.name}"

        log.info("Verification steps after adding a new deviceclass...")
        verification_steps_after_adding_new_deviceclass()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
