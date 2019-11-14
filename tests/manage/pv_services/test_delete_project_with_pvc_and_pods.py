"""
The purpose of this test case is to delete at least 2 projects:
One project with just pvcs and the other with pvcs attached to a pod
"""
import logging
import time
import pytest
from itertools import cycle

from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources.ocs import OCS
from tests import helpers

log = logging.getLogger(__name__)


@pytest.mark.polarion_id("OCS-278")
@pytest.mark.parametrize(
    argnames=["pvcs_num"],
    argvalues=[
        pytest.param(
            100, marks=pytest.mark.tier1
        ),
        pytest.param(
            1000, marks=pytest.mark.scale
        )
    ]
)
class TestDeleteProjectWithPVCAndPods(ManageTest):
    """
    Create at least 2 projects, one with just pvcs and the other with pvcs
    attached to pod, and delete them.
    """

    @pytest.fixture()
    def setup(self, storageclass_factory):
        """
        Create an OCP+OCS Setup, as well as two Storage classes:
        one for CephFS and one for RBD.
        Scale the setup to at-least 800 pvcs/pods.
        """
        self.ocp_setup = ocp.OCP()
        self.ocs_setup = OCS()  # TODO: Scale the setup to at-least 800 pvcs/pods
        self.ceph_cluster = CephCluster()

        log.info("Creating CephFS Storage class")
        self.cephfs_sc = storageclass_factory(interface=constants.CEPHFILESYSTEM)
        log.info("Creating RBD Storage class")
        self.rbd_sc = storageclass_factory(interface=constants.CEPHBLOCKPOOL)

    def test_delete_project_with_pvc_and_pods(
        self, setup, project_factory, multi_pvc_factory,
        pvc_factory, pod_factory, pvcs_num
    ):
        # Start with Project 1
        log.info("Creating Project")
        project_1 = project_factory()

        log.info(f"Creating {pvcs_num} CephFS PVCs")
        # Generate a given number of CephFS PVCs, some RWO and some RWX
        rwo_rwx = cycle([constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX])
        for i in range(0, pvcs_num):
            pvc_factory(interface=constants.CEPHFILESYSTEM, project=project_1,
                        storageclass=self.cephfs_sc, access_mode=next(rwo_rwx))
        log.info(f"Creating {pvcs_num} RBD PVCs")
        multi_pvc_factory(interface=constants.CEPHBLOCKPOOL,
                          project=project_1,
                          storageclass=self.rbd_sc,
                          access_mode=constants.ACCESS_MODE_RWO,
                          num_of_pvc=pvcs_num)

        # Delete the entire Project 1 (along with all of its PVCs)
        pvs = helpers.get_all_pvs()
        used_before_deletion = self.ceph_cluster.check_ceph_pool_used_space(
            self.ceph_cluster.cluster_name
        )
        project_deletion_start_time = time.time()
        ocp.switch_to_default_rook_cluster_project()
        log.info("Deleting Project 1")
        project_1.delete(resource_name=project_1.namespace)
        log.info("Waiting for Project 1 deletion success...")
        project_1.wait_for_delete(resource_name=project_1.namespace)
        log.info("Project 1 deleted successfully.")
        project_deletion_time = time.time() - project_deletion_start_time
        log.info(f"Project 1 deletion time: {project_deletion_time} seconds")
        # PVs delete might still be running, check until all PVs are deleted
        # Check time between first PV delete and last PV delete
        pv_deletion_start_time = time.time()
        for pv in pvs:
            pv_start_time = time.time()
            pv_name = pv.backed_pv
            pv.ocp.wait_for_delete(pv_name)
            helpers.validate_pv_delete(pv_name)
            # Also check delete time per each PV
            pv_delete_time = time.time() - pv_start_time
            log.info(f"{pv_name} took {pv_delete_time} seconds to delete")
        pv_deletion_total_time = time.time() - pv_deletion_start_time
        log.info(
            f"{pv_deletion_total_time} "
            f"seconds between first and last PVC deletion"
        )
        # Verify space has been reclaimed
        used_after_deletion = self.ceph_cluster.check_ceph_pool_used_space(
            self.ceph_cluster.cluster_name
        )
        log.info("Verifying space has been reclaimed...")
        assert used_after_deletion < used_before_deletion, (
            f"Space hasn't been reclaimed after PVs deletion."
            f"/nUsed space before deletion: " f"{used_before_deletion}"
            f"/nUsed space after deletion: {used_after_deletion}"
        )
        log.info("Space reclaimed successfully.")

        # Project 1 done, moving on to Project 2
        log.info("Creating Project 2")
        project_2 = project_factory()

        log.info(f"Creating a mix of {2 * pvcs_num} CephFS & RBD PVCs "
                 "(each bound to an app pod)")
        # Create CephFS PVCs bound to an app pod
        for pvc_obj in multi_pvc_factory(interface=constants.CEPHFILESYSTEM,
                                         project=project_2,
                                         num_of_pvc=pvcs_num):
            pod_factory(pvc=pvc_obj, interface=constants.CEPHFILESYSTEM)
        # Create RBD PVCs bound to an app pod
        for pvc_obj in multi_pvc_factory(interface=constants.CEPHBLOCKPOOL,
                                         project=project_2,
                                         num_of_pvc=pvcs_num):
            pod_factory(pvc=pvc_obj, interface=constants.CEPHBLOCKPOOL)

        # Delete the entire Project 2 (along with all of its PVCs)
        pvs = helpers.get_all_pvs()
        used_before_deletion = self.ceph_cluster.check_ceph_pool_used_space(
            self.ceph_cluster.cluster_name
        )
        start_time = time.time()
        # Switch back to default project
        ocp.switch_to_default_rook_cluster_project()
        log.info("Deleting Project 2")
        project_2.delete(resource_name=project_2.namespace)
        log.info("Waiting for Project 2 deletion success...")
        project_2.wait_for_delete(resource_name=project_2.namespace)
        log.info("Project 2 deleted successfully.")
        project_deletion_time = time.time() - start_time
        log.info(f"Project 2 deletion time: {project_deletion_time} seconds")
        # PVs delete might still be running, check until all PVs are deleted
        # Check time between first PV delete and last PV delete
        pv_deletion_start_time = time.time()
        for pv in pvs:
            pv_start_time = time.time()
            pv_name = pv.backed_pv
            pv.ocp.wait_for_delete(pv_name)
            helpers.validate_pv_delete(pv_name)
            pv_delete_time = time.time() - pv_start_time
            # Also check delete time per each PV
            log.info(f"{pv_name} took {pv_delete_time} seconds to delete")
        pv_deletion_total_time = time.time() - pv_deletion_start_time
        log.info(
            f"{pv_deletion_total_time} "
            f"seconds between first and last PVC deletion"
        )
        # Verify space has been reclaimed
        used_after_deletion = self.ceph_cluster.check_ceph_pool_used_space(
            self.ceph_cluster.cluster_name
        )
        log.info("Verifying space has been reclaimed...")
        assert used_after_deletion < used_before_deletion, (
            f"Space hasn't been reclaimed after PVs deletion."
            f"/nUsed space before deletion: " f"{used_before_deletion}"
            f"/nUsed space after deletion: {used_after_deletion}"
        )
        log.info("Space reclaimed successfully.")
