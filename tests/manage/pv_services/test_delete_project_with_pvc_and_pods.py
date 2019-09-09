"""
The purpose of this test case is to delete at least 2 projects:
One project with just pvcs and the other with pvcs attached to a pod
"""
import logging
import random
import time
import pytest

from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.retry import retry
from tests import helpers

log = logging.getLogger(__name__)


@retry(UnexpectedBehaviour, tries=10, delay=3, backoff=1)
def check_ceph_used_space():
    """
    Check for the used space in cluster
    """
    ct_pod = pod.get_ceph_tools_pod()
    ceph_status = ct_pod.exec_ceph_cmd(ceph_cmd="ceph status")
    assert ceph_status is not None
    used = ceph_status['pgmap']['bytes_used']
    used_in_gb = used / constants.GB
    global used_space
    if used_space and used_space == used_in_gb:
        return used_in_gb
    used_space = used_in_gb
    raise UnexpectedBehaviour(
        f"In Ceph status, used size is keeping varying"
    )

@tier1
@pytest.mark.polarion_id("OCS-278")
class TestDeleteProjectWithPVCAndPods(ManageTest):
    """
    Create at least 2 projects, one with just pvcs and the other with pvcs
    attached to pod, and delete them.
    """
    cephfs_pvcs_num = 100
    rbd_pvcs_num = 100

    def test_delete_project_with_pvc_and_pods(self, project_factory,
                                              storageclass_factory, pvc_factory,
                                              multi_pvc_factory, pod_factory):
        log.info("Creating OCP+OCS Setup")
        ocp_setup = ocp.OCP()
        ocs_setup = OCS()  # TODO: Scale to at-least 800 pvcs/pods

        log.info("Creating CephFS Storage class")
        cephfs_sc = storageclass_factory(interface=constants.CEPHFILESYSTEM)
        log.info("Creating RBD Storage class")
        rbd_sc = storageclass_factory(interface=constants.CEPHBLOCKPOOL)

        log.info("Creating Project 1")
        project_1 = project_factory()

        log.info("Creating {} CephFS PVCs".format(self.cephfs_pvcs_num))
        # Generate a given number of CephFS PVCs, randomly assigned RWO or RWX
        cephfs_pvcs = [pvc_factory(interface=constants.CEPHFILESYSTEM,
                                   project=project_1,
                                   storageclass=cephfs_sc,
                                   access_mode=random.choice(
                                       [constants.ACCESS_MODE_RWO,
                                        constants.ACCESS_MODE_RWX]))
                       for i in range(0, self.cephfs_pvcs_num)]
        log.info("Creating {} RBD PVCs".format(self.rbd_pvcs_num))
        rbd_pvcs = multi_pvc_factory(interface=constants.CEPHBLOCKPOOL,
                                     project=project_1,
                                     storageclass=rbd_sc,
                                     access_mode=constants.ACCESS_MODE_RWO,
                                     num_of_pvc=self.rbd_pvcs_num)

        # CUTOFF POINT
        pvs = helpers.get_all_pvs()
        space_used_before_deletion = check_ceph_used_space()
        start_time = time.time()
        # Delete the entire Project (along with all of its PVCs)
        #ocp.switch_to_default_rook_cluster_project() #TODO: Is this needed?
        log.info("Deleting the entire first Project")
        self.project.delete(resource_name=self.project.namespace)
        log.info("Waiting for Project deletion success...")
        self.project.wait_for_delete(resource_name=self.project.namespace)
        log.info("Project deleted successfully.")
        project_deletion_time = time.time() - start_time
        log.info(
            "Project deletion time: {} seconds".format(project_deletion_time))
        # PVs delete might still be running, check until all PVs are deleted
        time_between_pvcs_deletion_success = 0
        for pv in pvs:
            pv_name = pv.backed_pv
            pv.ocp.wait_for_delete(pv_name)
            helpers.validate_pv_delete(pv_name)
        # Verify space has been reclaimed
        space_used_after_deletion = check_ceph_used_space()
        log.info("Verifying space has been reclaimed...")
        assert space_used_after_deletion < space_used_before_deletion
        log.info("Space reclaimed successfully.")
