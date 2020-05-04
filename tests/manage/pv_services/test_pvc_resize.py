import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, tier1

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.5')
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-325")
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-325")
        )
    ]
)
class TestPvcResize(ManageTest):
    """
    Tests to verify PVC resize

    """
    @pytest.fixture(autouse=True)
    def test_setup(self, interface, storageclass_factory, pvc_factory, pod_factory):
        """
        Create resources for the test

        """
        self.sc_obj = storageclass_factory(interface=interface)
        self.pvc_obj = pvc_factory(
            interface=interface, storageclass=self.sc_obj, size=3
        )
        self.pod_obj = pod_factory(interface=interface, pvc=self.pvc_obj)

    def test_pvc_resize(self):
        """
        Verify PVC resize feature

        """
        pvc_size_new = 8

        # Modify size of PVC and verify the change
        self.pvc_obj.resize_pvc(pvc_size_new, True)

        # Wait for 240 seconds to reflect the change on pod
        for df_out in TimeoutSampler(
            240, 3, self.pod_obj.exec_cmd_on_pod, command='df -kh'
        ):
            df_out = df_out.split()
            new_size_mount = df_out[df_out.index(self.pod_obj.get_storage_path()) - 4]
            if new_size_mount in [f'{pvc_size_new-0.1}G', f'{float(pvc_size_new)}G']:
                break
            log.info(
                f"New size on mount is not {pvc_size_new}G as expected, "
                f"but {new_size_mount}. Retrying."
            )

        log.info("Verified: Modified size is reflected on the pod.")
