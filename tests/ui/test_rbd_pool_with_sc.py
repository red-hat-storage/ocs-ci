import logging
import pytest
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
)
from ocs_ci.framework.testlib import skipif_ocs_version, ui

from ocs_ci.ocs.ui.storageclass import StorageClassUI
from ocs_ci.ocs.ui.block_pool import BlockPoolUI
from ocs_ci.ocs.ui.base_ui import StorageSystemNavigator

logger = logging.getLogger(__name__)


@pytest.mark.polarion_id("OCS-3887")
class TestBlockPoolOperations:
    """
    Verify storageclass is correctly attached/deattach from pool.
    """

    def teardown(self):
        def finalizer():
            self.delete_block_pool()

    def delete_block_pool(self):
        """Cleanup block pool."""
        if self.pool_name is not None:
            logger.info(f"Deleting Block Pool {self.pool_name}.")
            try:
                self.bkp_obj.delete_pool(self.pool_name)
            except Exception as e:
                logger.error(f"Error Removing pool {self.pool_name} : {e}")
        else:
            logger.info("Pool is 'None' Nothing to delete.")

    @ui
    @tier1
    @skipif_ocs_version("<4.8")
    def test_block_pool_operation(
        self,
        setup_ui,
        storageclass_factory_class,
        ceph_pool_factory_class,
        secret_factory_class,
        request,
    ):
        """
        Verify storageclass is correctly attached/deattach from pool.
        1. Create rbd pool.
        2. Create storageclass with the pool.
        3. Check that in pool list and page the storageclass is there.
        4. Delete the storageclass and check that it disapear from pool page and pool
        """
        self.sc_name = self.pool_name = None
        self.bkp_obj = BlockPoolUI(setup_ui)
        self.sc_obj = StorageClassUI(setup_ui)
        self.ssn_obj = StorageSystemNavigator(setup_ui)

        # create Storage Class with new Pool
        self.sc_name, self.pool_name = self.sc_obj.create_storageclass(
            create_new_pool=True
        )

        assert self.sc_name
        assert self.pool_name

        request.addfinalizer(self.teardown)
        # Verify Pool list contains correct storage class associated to pool.
        sc_exist = self.ssn_obj.cephblockpool_validate_storageclass(
            self.pool_name, self.sc_name
        )

        assert sc_exist

        # Delete the storageClass assiciated with the pool
        self.sc_obj.delete_rbd_storage_class(self.sc_name)

        # Verify the storageclass is not vesible on pool page.
        sc_exist = self.ssn_obj.cephblockpool_validate_storageclass(
            self.pool_name, self.sc_name
        )

        assert not sc_exist
