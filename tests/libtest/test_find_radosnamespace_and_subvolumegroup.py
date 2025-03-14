import logging
from ocs_ci.framework.testlib import ManageTest, libtest
from ocs_ci.helpers.helpers import (
    find_cephblockpoolradosnamespace,
    find_cephfilesystemsubvolumegroup,
)

logger = logging.getLogger(__name__)


@libtest
class TestFindCephblockpoolnsCephfssubvolumegroup(ManageTest):
    """
    Tests to find the default cephblockpoolradosnamespace and cephfilesystemsubvolumegroup
    linked to the native storageclient

    """

    def test_find_cephblockpoolradosnamespace(ManageTest):
        """
        Test to find the default cephblockpoolradosnamespace linked to the native storageclient

        """
        rados_ns = find_cephblockpoolradosnamespace()
        assert rados_ns, "The name of cephblockpoolradosnamespace was not found."
        logger.info(f"Cephblockpoolradosnamespace is {rados_ns}")

    def test_find_cephfilesystemsubvolumegroup(ManageTest):
        """
        Test to find the default cephfilesystemsubvolumegroup linked to the native storageclient

        """
        cephfs_subvolumegroup = find_cephfilesystemsubvolumegroup()
        assert (
            cephfs_subvolumegroup
        ), "The name of cephfilesystemsubvolumegroup was not found."
        logger.info(f"Cephfilesystemsubvolumegroup is {cephfs_subvolumegroup}")
