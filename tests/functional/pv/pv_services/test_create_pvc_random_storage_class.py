"""
A test for creating pvc with random sc
"""
import random

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest


@green_squad
@pytest.mark.polarion_id("OCS-288")
class TestCreatePVCRandomStorageClass(ManageTest):
    """
    Creating PVC with random SC
    """

    def test_create_pvc_with_random_sc(
        self, secret_factory, storageclass_factory, pvc_factory
    ):
        sc_list = []
        rbd_secret_obj = secret_factory(interface=constants.CEPHBLOCKPOOL)
        cephfs_secret_obj = secret_factory(interface=constants.CEPHFILESYSTEM)

        # Create Rbd Sc
        for i in range(5):
            rbd_sc_obj = storageclass_factory(
                interface=constants.CEPHBLOCKPOOL, secret=rbd_secret_obj
            )
            sc_list.append(rbd_sc_obj)

        # Create CephFs Sc
        cephfs_sc_obj = storageclass_factory(
            interface=constants.CEPHFILESYSTEM, secret=cephfs_secret_obj
        )
        sc_list.append(cephfs_sc_obj)

        # Create PVCs randomly with sc
        for i in range(20):
            pvc_factory(storageclass=random.choice(sc_list))
