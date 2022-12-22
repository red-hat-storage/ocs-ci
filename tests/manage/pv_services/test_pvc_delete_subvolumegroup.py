import pytest
import logging
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_ocs_version,
    polarion_id,
    bugzilla,
    tier2,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import get_cephfs_name

logger = logging.getLogger(__name__)


@skipif_ocs_version("<4.12")
@polarion_id("OCS-4664")
@bugzilla("2124469")
@tier2
class TestPvcDeleteSubVolumeGroup(ManageTest):
    """
    Delete PVC subvolumegroup and make sure PVC reaches bound state
    """

    cephfs_name = None
    subvolumegroup_ls_cmd = None

    @pytest.fixture(autouse=True)
    def setup(self, project_factory):
        """
        create resources for the test

        Args:
            project_factory: A fixture to create new project
        """
        self.cephfs_name = config.ENV_DATA.get("cephfs_name") or get_cephfs_name()
        self.subvolumegroup_ls_cmd = f"ceph fs subvolumegroup ls {self.cephfs_name}"

        logger.info("Creating project")
        self.proj_obj = project_factory()
        # Get ceph tool pod
        self.tool_pod = get_ceph_tools_pod()

    def is_volumegroup_empty(self):
        """
        Check if volumegroup is empty

        Returns:
            bool: True if empty, False otherwise
        """
        out = self.tool_pod.exec_ceph_cmd(
            ceph_cmd=f"ceph fs subvolume ls {self.cephfs_name} csi",
            format=None,
        )
        return not bool(out)

    def csi_exist(self):
        # List subvolumegroup (should be empty)
        subvolume = self.tool_pod.exec_ceph_cmd(
            ceph_cmd=self.subvolumegroup_ls_cmd, format=None
        )

        found_csi = False
        for sv in subvolume:
            if sv["name"] == "csi":
                found_csi = True
                break
        return found_csi

    def test_pvc_delete_subvolumegroup(self, pvc_factory):
        """
        Delete PVC subvolumegroup and make sure PVC reaches bound state
        """

        logger.info("Creating pvc")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=5,
            project=self.proj_obj,
        )
        logger.info(f"Deleting pvc {pvc_obj.name}")
        delete_pvcs(pvc_objs=[pvc_obj])

        # Make sure volumegroup is empty before deletion
        for sampler in TimeoutSampler(
            timeout=60, sleep=5, func=self.is_volumegroup_empty
        ):
            if sampler:
                logger.info("volumegroup is empty!")
                break

        # Remove subvolumegroup
        self.tool_pod.exec_ceph_cmd(
            ceph_cmd=f"ceph fs subvolumegroup rm {self.cephfs_name} csi",
            format=None,
        )
        # 'csi' should be removed
        assert not self.csi_exist(), "Subvolumegroup should contain 'csi'"
        logger.info("Subvolumegroup contain 'csi' as expected")

        logger.info("Creating pvc")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=5,
            project=self.proj_obj,
        )

        # List subvolumegroup (should NOT be empty and contain 'csi')
        assert self.csi_exist(), "Subvolumegroup should contain 'csi'"
        logger.info("Subvolumegroup contains 'csi' as expected")
