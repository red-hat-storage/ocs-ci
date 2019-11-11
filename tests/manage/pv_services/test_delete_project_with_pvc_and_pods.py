"""
The purpose of this test case is to delete at least 2 projects:
One project with just pvcs and the other with pvcs attached to a pod
"""
import logging
import time
import pytest
from itertools import cycle

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources.ocs import OCS
from tests import helpers

log = logging.getLogger(__name__)


@tier1
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

        log.info("Creating {} CephFS PVCs".format(pvcs_num))
        # Generate a given number of CephFS PVCs, some RWO and some RWX
        rwo_rwx = cycle([constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX])
        for i in range(0, pvcs_num):
            pvc_factory(interface=constants.CEPHFILESYSTEM, project=project_1,
                        storageclass=self.cephfs_sc, access_mode=next(rwo_rwx))
        log.info("Creating {} RBD PVCs".format(pvcs_num))
        multi_pvc_factory(interface=constants.CEPHBLOCKPOOL,
                          project=project_1,
                          storageclass=self.rbd_sc,
                          access_mode=constants.ACCESS_MODE_RWO,
                          num_of_pvc=pvcs_num)

        # Delete the entire Project 1 (along with all of its PVCs)
        pvs = helpers.get_all_pvs()
        space_used_before_deletion = self.ceph_cluster.check_ceph_used_space()
        project_deletion_start_time = time.time()
        ocp.switch_to_default_rook_cluster_project()
        log.info("Deleting Project 1")
        project_1.delete(resource_name=project_1.namespace)
        log.info("Waiting for Project 1 deletion success...")
        project_1.wait_for_delete(resource_name=project_1.namespace)
        log.info("Project 1 deleted successfully.")
        project_deletion_time = time.time() - project_deletion_start_time
        log.info(
            "Project 1 deletion time: {} seconds".format(project_deletion_time))
        # PVs delete might still be running, check until all PVs are deleted
        # Check time between first PV delete and last PV delete
        time_between_pvcs_deletion_success = 0
        # Also check delete time per each PV
        individual_pv_delete_times = []
        for pv in pvs:
            pv_start_time = time.time()
            pv_name = pv.backed_pv
            pv.ocp.wait_for_delete(pv_name)
            helpers.validate_pv_delete(pv_name)
            pv_delete_time = time.time() - pv_start_time
            individual_pv_delete_times.append({pv_name: pv_delete_time})
            time_between_pvcs_deletion_success += pv_delete_time
        log.info("{} seconds between first and last PVC deletion".format(
            time_between_pvcs_deletion_success))
        # Verify space has been reclaimed
        space_used_after_deletion = self.ceph_cluster.check_ceph_used_space()
        log.info("Verifying space has been reclaimed...")
        assert space_used_after_deletion < space_used_before_deletion
        log.info("Space reclaimed successfully.")

        # Project 1 done, moving on to Project 2
        log.info("Creating Project 2")
        project_2 = project_factory()

        log.info("Creating a mix of {} CephFS & RBD PVCs "
                 "(each bound to an app pod)".format(2 * pvcs_num))
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
        space_used_before_deletion = self.ceph_cluster.check_ceph_used_space()
        start_time = time.time()
        # Switch back to default project
        ocp.switch_to_default_rook_cluster_project()
        log.info("Deleting Project 2")
        project_2.delete(resource_name=project_2.namespace)
        log.info("Waiting for Project 2 deletion success...")
        project_2.wait_for_delete(resource_name=project_2.namespace)
        log.info("Project 2 deleted successfully.")
        project_deletion_time = time.time() - start_time
        log.info(
            "Project 2 deletion time: {} seconds".format(project_deletion_time))
        # PVs delete might still be running, check until all PVs are deleted
        # Check time between first PV delete and last PV delete
        time_between_pvcs_deletion_success = 0
        # Also check delete time per each PV
        individual_pv_delete_times = []
        for pv in pvs:
            pv_start_time = time.time()
            pv_name = pv.backed_pv
            pv.ocp.wait_for_delete(pv_name)
            helpers.validate_pv_delete(pv_name)
            pv_delete_time = time.time() - pv_start_time
            individual_pv_delete_times.append({pv_name: pv_delete_time})
            time_between_pvcs_deletion_success += pv_delete_time
        log.info("{} seconds between first and last PVC deletion".format(
            time_between_pvcs_deletion_success))
        # Verify space has been reclaimed
        space_used_after_deletion = self.ceph_cluster.check_ceph_used_space()
        log.info("Verifying space has been reclaimed...")
        assert space_used_after_deletion < space_used_before_deletion, (
            f"Space hasn't been reclaimed after PVs deletion."
            f"/nUsed space before deletion: " f"{space_used_before_deletion}"
            f"/nUsed space after deletion: {space_used_after_deletion}"
        )
        log.info("Space reclaimed successfully.")
