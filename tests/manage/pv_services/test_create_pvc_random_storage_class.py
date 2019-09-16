"""
A test for creating pvc with random sc
"""
import logging
import random

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier2
from tests import helpers
from tests.fixtures import (
    create_rbd_secret, create_cephfs_secret
)

log = logging.getLogger(__name__)


@tier2
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_cephfs_secret.__name__,
)
@pytest.mark.polarion_id("OCS-288")
class TestCreatePVCRandomStorageClass(ManageTest):
    """
    Creating PVC with random SC
    """

    def test_create_pvc_with_random_sc(self, teardown_factory):
        sc_list = []
        for i in range(5):
            log.info(f"Creating cephblockpool")
            cbp_obj = helpers.create_ceph_block_pool()
            log.info(
                f"{cbp_obj.name} created successfully"
            )
            log.info(
                f"Creating a RBD storage class using {cbp_obj.name}"
            )
            rbd_sc_obj = helpers.create_storage_class(
                interface_type=constants.CEPHBLOCKPOOL,
                interface_name=cbp_obj.name,
                secret_name=self.rbd_secret_obj.name
            )
            log.info(
                f"StorageClass: {rbd_sc_obj.name} "
                f"created successfully using {cbp_obj.name}"
            )
            sc_list.append(rbd_sc_obj)
            teardown_factory(cbp_obj)
            teardown_factory(rbd_sc_obj)
        log.info("Creating CephFs Storageclass")
        cephfs_sc_obj = helpers.create_storage_class(
            interface_type=constants.CEPHFILESYSTEM,
            interface_name=helpers.get_cephfs_data_pool_name(),
            secret_name=self.cephfs_secret_obj.name
        )
        sc_list.append(cephfs_sc_obj)
        teardown_factory(cephfs_sc_obj)

        # Create PVCs randomly with sc
        pvc_list = []
        for i in range(20):
            sc_name = random.choice(sc_list)
            log.info(f"Creating a PVC using {sc_name.name}")
            pvc_obj = helpers.create_pvc(sc_name.name)
            log.info(
                f"PVC: {pvc_obj.name} created successfully using "
                f"{sc_name.name}"
            )
            pvc_list.append(pvc_obj)
            teardown_factory(pvc_obj)
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
