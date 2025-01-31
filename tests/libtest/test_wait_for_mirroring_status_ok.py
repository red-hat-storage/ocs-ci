from ocs_ci.framework.testlib import libtest, ManageTest
from ocs_ci.helpers.dr_helpers import wait_for_mirroring_status_ok


@libtest
class TestMirroringStatus(ManageTest):
    """
    Test to check if the function to check mirroring status works with and without hci configuration
    """

    wait_for_mirroring_status_ok()
