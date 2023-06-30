import logging
import pytest
from ocs_ci.framework.testlib import ManageTest, tier1, ui
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    skipif_ocs_version,
)
from ocs_ci.ocs.constants import CEPHBLOCKPOOL
from ocs_ci.ocs.ui.block_pool import BlockPoolUI
from ocs_ci.ocs.ui.storageclass import StorageClassUI

log = logging.getLogger(__name__)


@ui
@tier1
@skipif_external_mode
@skipif_ocs_version("<4.9")
@pytest.mark.polarion_id("OCS-3890")
class TestMultipleScOnePool(ManageTest):
    """
    Create new rbd pool with replica 2 and compression.
    Attach it to 2 new storageclasses.
    Verify RBD, Storageclass in UI.

    """

    def test_multiple_sc_one_pool(
        self,
        setup_ui_class,
        ceph_pool_factory,
        storageclass_factory,
    ):
        """
        This test function does below,
        *. Creates 2 Storage Class with creating one rbd pool for both
        *. Verify the UI for storage class and rbd
        """

        log.info("Creating new pool with replica2 and compression")
        pool_obj = ceph_pool_factory(
            interface=CEPHBLOCKPOOL,
            compression="aggressive",
        )

        log.info(f"Creating first storageclass with pool {pool_obj.name}")
        sc_obj1 = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            new_rbd_pool=False,
            pool_name=pool_obj.name,
        )

        log.info(f"Creating second storageclass with pool {pool_obj.name}")
        sc_obj2 = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            new_rbd_pool=False,
            pool_name=pool_obj.name,
        )

        sc_obj_list = [sc_obj1, sc_obj2]

        # Check if 2 storage class exists in the pool page
        blockpool_ui_obj = BlockPoolUI()
        assert blockpool_ui_obj.check_pool_existence(pool_obj.name)
        assert (
            blockpool_ui_obj.check_storage_class_attached(pool_obj.name) == 2
        ), "The Storage class didnot matched."

        # Check storage class existence in UI
        storageclass_ui_obj = StorageClassUI()
        for storageclass in sc_obj_list:
            storageclass_name = storageclass.name
            assert storageclass_ui_obj.verify_storageclass_existence(storageclass_name)
