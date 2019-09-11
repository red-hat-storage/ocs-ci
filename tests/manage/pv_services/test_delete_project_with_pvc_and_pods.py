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
        # Start with Project 1
        log.info("Creating OCP+OCS Setup")
        ocp_setup = ocp.OCP()
        ocs_setup = OCS()  # TODO: Scale the setup to at-least 800 pvcs/pods

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

        # Delete the entire Project 1 (along with all of its PVCs)
        pvs = helpers.get_all_pvs()
        space_used_before_deletion = check_ceph_used_space()
        project_deletion_start_time = time.time()
        #ocp.switch_to_default_rook_cluster_project() #TODO: Is this needed?
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
        space_used_after_deletion = check_ceph_used_space()
        log.info("Verifying space has been reclaimed...")
        assert space_used_after_deletion < space_used_before_deletion
        log.info("Space reclaimed successfully.")

        # Project 1 done, moving on to Project 2
        log.info("Creating Project 2")
        project_2 = project_factory()

        log.info("Creating {} CephFS PVCs "
                 "(each bound to an app pod)".format(self.cephfs_pvcs_num))
        cephfs_pvcs_pods = [
            pod_factory(pvc=pvc_obj, interface=constants.CEPHFILESYSTEM)
            for pvc_obj in multi_pvc_factory(interface=constants.CEPHFILESYSTEM,
                                             project=project_2,
                                             num_of_pvc=self.cephfs_pvcs_num)
        ]
        rbd_pvcs_pods = [
            pod_factory(pvc=pvc_obj, interface=constants.CEPHBLOCKPOOL)
            for pvc_obj in multi_pvc_factory(interface=constants.CEPHBLOCKPOOL,
                                             project=project_2,
                                             num_of_pvc=self.rbd_pvcs_num)
        ]
        # Delete the entire Project 2 (along with all of its PVCs)
        pvs = helpers.get_all_pvs()
        space_used_before_deletion = check_ceph_used_space()
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
        space_used_after_deletion = check_ceph_used_space()
        log.info("Verifying space has been reclaimed...")
        assert space_used_after_deletion < space_used_before_deletion
        log.info("Space reclaimed successfully.")
