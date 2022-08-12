import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_lvm_not_installed,
)
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.ocs.cluster import LVM
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["volume_mode", "volume_binding_mode", "status"],
    argvalues=[
        pytest.param(
            *[
                constants.VOLUME_MODE_FILESYSTEM,
                constants.WFFC_VOLUMEBINDINGMODE,
                constants.STATUS_PENDING,
            ],
        ),
    ],
)
class TestLVMPVCResize(ManageTest):
    access_mode = constants.ACCESS_MODE_RWO
    block = False

    init_lvm = LVM()
    sample = TimeoutSampler()

    @tier1
    @skipif_ocs_version("<4.11")
    @skipif_lvm_not_installed
    def test_pvc_resive(self):
        pass
