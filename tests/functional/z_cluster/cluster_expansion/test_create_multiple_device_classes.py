import logging
import random

import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    tier2,
    brown_squad,
    skipif_no_lso,
    skipif_bm,
    skipif_hci_provider_or_client,
    polarion_id,
    ui,
    black_squad,
    skipif_external_mode,
    skipif_mcg_only,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.ocs.device_classes import (
    create_new_lvs_for_new_deviceclass,
    verify_deviceclasses_steps,
    add_disks_matching_lvs_size,
    get_default_lvs_obj,
    verify_available_pvs_for_deviceclass,
)
from ocs_ci.ocs.resources.pv import (
    wait_for_pvs_in_lvs_to_reach_status,
    get_pv_objs_in_sc,
    wait_for_new_pvs_status,
)
from ocs_ci.ocs.resources.pvc import wait_for_pvcs_in_deviceset_to_reach_status
from ocs_ci.ocs.resources.storage_cluster import (
    add_new_deviceset_in_storagecluster,
    get_storage_cluster,
    get_first_sc_name_from_storagecluster,
)
from ocs_ci.helpers.helpers import (
    create_ceph_block_pool,
    create_rbd_deviceclass_storageclass,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator


log = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@skipif_no_lso
@skipif_bm
@skipif_hci_provider_or_client
@skipif_external_mode
@skipif_mcg_only
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

    @tier2
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
        num_of_new_pvs = len(osd_node_names)
        lvs_obj = create_new_lvs_for_new_deviceclass(osd_node_names)
        log.info(
            f"Wait for the PVs in the LocalVolumeSet {lvs_obj.name} to be available"
        )
        wait_for_pvs_in_lvs_to_reach_status(
            lvs_obj.name, num_of_new_pvs, constants.STATUS_AVAILABLE
        )

        log.info(
            f"Add a new deviceset in the storagecluster for the new LocalVolumeSet {lvs_obj.name} "
            f"which will also create a new deviceclass"
        )
        res = add_new_deviceset_in_storagecluster(lvs_obj.name, lvs_obj.name)
        assert res, "Failed to patch the storagecluster with the new deviceset"
        log.info("Wait for the new deviceset PVCs to reach Bound status")
        wait_for_pvcs_in_deviceset_to_reach_status(
            lvs_obj.name, num_of_new_pvs, constants.STATUS_BOUND
        )

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
        verify_deviceclasses_steps()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

    @tier2
    @polarion_id("OCS-7431")
    def test_add_new_ssd_device_class_same_size(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        The test will perform the following steps:
        1. Get the osd nodes.
        2. Add new disks for the new deviceclass using the same existing LocalVolumeSet
        with the same storage size.
        3. Wait for the PVS in the LocalVolumeSet above to be available.
        4. Add a new device set in the storagecluster for the existing LocalVolumeSet above
        with the same storage class name and size, which will also create a new deviceclass.
        5. Wait for the storagecluster to be ready.
        6. Create a new CephBlockPool for the device class above.
        7. Wait for the CephBlockPool to be ready.
        8. Create a new StorageClass for the pool.
        9. Run the Verification steps as defined in the function
        'verification_steps_after_adding_new_deviceclass'.
        10. Check the cluster and Ceph health.
        11. Check basic cluster functionality by creating some resources.

        """
        sc_name = get_first_sc_name_from_storagecluster()
        log.info(f"StorageCluster first storageclass name = {sc_name}")
        current_pv_objs = get_pv_objs_in_sc(sc_name)
        lvs_obj = get_default_lvs_obj()

        osd_nodes = get_osd_running_nodes()
        log.info(f"osd node names = {osd_nodes}")
        log.info("Add new disks for the new deviceclass")
        add_disks_matching_lvs_size(osd_nodes)
        num_of_new_pvs = len(osd_nodes)

        log.info("Wait for the new PVs in the existing LocalVolumeSet to be available")
        wait_for_new_pvs_status(
            current_pv_objs=current_pv_objs,
            sc_name=lvs_obj.name,
            expected_status=constants.STATUS_AVAILABLE,
            num_of_new_pvs=num_of_new_pvs,
        )

        suffix = "".join(random.choices("0123456789", k=5))
        device_class_name = f"{lvs_obj.name}-{suffix}"
        deviceset_name = device_class_name
        # The storage size defined as "1" in the deviceset as per documentation,
        # but will default to the existing OSD size
        storage_size = "1"
        log.info(
            f"Add a new deviceset in the storagecluster for the new LocalVolumeSet: {device_class_name} "
            f"which will also create a new deviceclass"
        )
        res = add_new_deviceset_in_storagecluster(
            device_class=device_class_name,
            name=deviceset_name,
            sc_name=sc_name,
            storage_size=storage_size,
        )
        assert (
            res
        ), f"Failed to patch the storagecluster with the new deviceset {device_class_name}"
        log.info("Wait for the new deviceset PVCs to reach Bound status")
        wait_for_pvcs_in_deviceset_to_reach_status(
            deviceset_name, num_of_new_pvs, constants.STATUS_BOUND
        )

        sc_obj = get_storage_cluster()
        sc_obj.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            column="PHASE",
            timeout=180,
            sleep=10,
        )

        log.info(f"Create a new CephBlockPool for the device class {device_class_name}")
        cbp_obj = create_ceph_block_pool(device_class=device_class_name)
        assert (
            cbp_obj
        ), f"Failed to create the CephBlockPool for the device class {device_class_name}"
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
        verify_deviceclasses_steps()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )


@brown_squad
@ui
@black_squad
@ignore_leftovers
@skipif_no_lso
@skipif_bm
@skipif_hci_provider_or_client
@skipif_external_mode
@skipif_mcg_only
class TestMultipleDeviceClassesUI(ManageTest):
    """
    Automate the multiple device classes tests via UI

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory):
        """
        Initialize Sanity instance

        """
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.bucket_factory = bucket_factory
        self.rgw_bucket_factory = rgw_bucket_factory
        self.sanity_helpers = Sanity()

    def post_deviceclass_checks(self):
        """
        Run the verification steps after adding a new device class:
        1. Check the cluster and Ceph health.
        2. Check basic cluster functionality by creating some resources.

        """
        log.info("Verification steps after adding a new deviceclass...")
        verify_deviceclasses_steps()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            self.pvc_factory,
            self.pod_factory,
            self.bucket_factory,
            self.rgw_bucket_factory,
        )

    @pytest.fixture(autouse=True)
    def teardown(self):
        """
        Check that the ceph health is OK

        """
        log.info("Wait for the ceph health to be OK")
        ceph_health_check(tries=20)

    @tier2
    def test_add_new_device_class_ui(self, setup_ui_session):
        """
        The test will perform the following steps:
        1. Verify that there are available PVs for attaching a new device class.
        2. Navigate to the 'Attach Storage' form in the UI.
        3. Fill the form with the default values and submit it, which will add a new device class.
        4. Run the verification steps as defined in the method 'post_deviceclass_checks'.

        """
        verify_available_pvs_for_deviceclass()
        attach_storage = PageNavigator().nav_to_attach_storage_page()
        attach_storage.send_form_with_default_values()
        self.post_deviceclass_checks()
