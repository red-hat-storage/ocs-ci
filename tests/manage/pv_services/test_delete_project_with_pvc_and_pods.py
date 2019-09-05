"""
The purpose of this test case is to delete at least 2 projects:
One project with just pvcs and the other with pvcs attached to a pod
"""
import logging
import random
import pytest

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.ocs import OCS
from tests import helpers

log = logging.getLogger(__name__)


@tier1
class TestDeleteProjectWithPVCAndPods(ManageTest):
    @pytest.fixture()
    def setup(self):
        """
        Set up the environment for the test
        :return: setup_success (True/False)
        """
        log.info("Creating OCP+OCS Setup")
        self.ocp_setup = ocp.OCP()
        self.ocs_setup = OCS() # TODO: Scale to at-least 800 pvcs/pods

        log.info("Creating CephFS Secret")
        self.cephfs_secret_obj = helpers.create_secret(constants.CEPHFILESYSTEM)
        log.info("Creating RBD Secret")
        self.rbd_secret_obj = helpers.create_secret(constants.CEPHBLOCKPOOL)
        log.info("Creating CephBlockPool")
        self.rbd_pool = helpers.create_ceph_block_pool()

        log.info("Creating Project 1")
        self.project = helpers.create_project()

        log.info("Creating RBD Storage class")
        self.rbd_sc_obj = helpers.create_storage_class(
            constants.CEPHBLOCKPOOL,
            self.rbd_pool.name,
            self.rbd_secret_obj.name
        )

        log.info("Creating CephFS Storage class")
        self.cephfs_sc_obj = helpers.create_storage_class(
            constants.CEPHFILESYSTEM,
            helpers.get_cephfs_data_pool_name(),
            self.cephfs_secret_obj.name
        )

        # Generate a given number of CephFS PVCs, randomly assigned RWO or RWX
        cephfs_pvcs_num = 100
        self.cephfs_pvcs = [helpers.create_pvc(sc_name=self.cephfs_sc_obj.name,
                                          access_mode=random.choice(
                                              [constants.ACCESS_MODE_RWO,
                                               constants.ACCESS_MODE_RWX]),
                                          namespace=self.project.namespace)
                       for i in range(0, cephfs_pvcs_num)]

        # Generate a given number of RBD PVCs, all RWO
        rbd_pvcs_num = 100
        self.rbd_pvcs = [helpers.create_pvc(sc_name=self.rbd_sc_obj.name,
                                       access_mode=constants.ACCESS_MODE_RWO,
                                       namespace=self.project.namespace)
                    for i in range(0, rbd_pvcs_num)]

    def test_delete_project_with_pvc_and_pods(self):
        pvcs = pvc.get_all_pvcs(self.project.namespace)
        # Delete the entire Project #TODO: Measure time!
        proj_del_stdout = self.project.delete(resource_name=self.namespace)
        # PVs delete might still be running, check until all PVs are deleted
        #helpers.validate_pv_delete #TODO: Why need count?



