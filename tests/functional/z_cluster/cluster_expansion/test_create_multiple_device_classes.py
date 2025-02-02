import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    tier1,
    brown_squad,
    skipif_no_lso,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.ocs.device_classes import (
    create_new_lvs_for_new_deviceclass,
    verification_steps_after_adding_new_deviceclass,
)
from ocs_ci.ocs.resources.pv import wait_for_pvs_in_lvs_to_reach_status
from ocs_ci.ocs.resources.storage_cluster import (
    add_new_deviceset_in_storagecluster,
    get_storage_cluster,
)
from ocs_ci.helpers.helpers import (
    create_ceph_block_pool,
    create_rbd_deviceclass_storageclass,
)
from ocs_ci.utility.utils import ceph_health_check


log = logging.getLogger(__name__)


@brown_squad
@tier1
@ignore_leftovers
@skipif_no_lso
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
    def teardown(self):
        """
        Check that the ceph health is OK

        """
        log.info("Wait for the ceph health to be OK")
        ceph_health_check(tries=20)

    def test_add_new_ssd_device_class(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        The test will perform the following steps:
        1. Get the osd nodes.
        2. Create a new LocalVolumeSet for the new deviceclass as defined in the function
        'create_new_lvs_for_new_deviceclass'.
        3. Wait for the PVS in the LocalVolumeSet above to be available.
        4. Add a new device set in the storagecluster for the new LocalVolumeSet above,
        which will also create a new deviceclass.
        5. Wait for the storagecluster to be ready.
        6. Create a new CephBlockPool for the device class above.
        7. Wait for the CephBlockPool to be ready.
        8. Create a new StorageClass for the pool.
        9. Run the Verification steps as defined in the function
        'verification_steps_after_adding_new_deviceclass'.
        10. Check the cluster and Ceph health.
        11. Check basic cluster functionality by creating some resources.

        """
        osd_node_names = get_osd_running_nodes()
        log.info(f"osd node names = {osd_node_names}")
        lvs_obj = create_new_lvs_for_new_deviceclass(osd_node_names)
        log.info(
            f"Wait for the PVs in the LocalVolumeSet {lvs_obj.name} to be available"
        )
        wait_for_pvs_in_lvs_to_reach_status(
            lvs_obj.name, len(osd_node_names), constants.STATUS_AVAILABLE
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

        log.info(f"Create a new CephBlockPool for the device class {lvs_obj.name}")
        cbp_obj = create_ceph_block_pool(device_class=lvs_obj.name)
        assert (
            cbp_obj
        ), f"Failed to create the CephBlockPool for the device class {lvs_obj.name}"
        cbp_obj.ocp.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=cbp_obj.name,
            column="PHASE",
            timeout=180,
            sleep=10,
        )

        log.info(f"Create a new StorageClass for the pool {cbp_obj.name}")
        sc_obj = create_rbd_deviceclass_storageclass(pool_name=cbp_obj.name)
        assert sc_obj, f"Failed to create the StorageClass for the pool {cbp_obj.name}"

        log.info("Verification steps after adding a new deviceclass...")
        verification_steps_after_adding_new_deviceclass()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
