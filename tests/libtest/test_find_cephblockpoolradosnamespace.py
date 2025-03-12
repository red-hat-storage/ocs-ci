import logging
from ocs_ci.framework.testlib import ManageTest, libtest
from ocs_ci.helpers.helpers import find_cephblockpoolradosnamespace

logger = logging.getLogger(__name__)


@libtest
class TestFindCephblockpoolradosnamespace(ManageTest):
    """
    Tests to find the default cephblockpoolradosnamespace linked to the native storageclient

    """

    def test_find_cephblockpoolradosnamespace(ManageTest):
        """
        Test to find the default cephblockpoolradosnamespace linked to the native storageclient

        """
        rados_ns = find_cephblockpoolradosnamespace()
        assert rados_ns, "The name of cephblockpoolradosnamespace was not found."
        logger.info(f"Cephblockpoolradosnamespace is {rados_ns}")
