import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier1, acceptance
)
from tests import helpers

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.5')
class TestPvcExpand(ManageTest):
    """
    Tests to verify PVC expansion

    """
    @pytest.fixture(autouse=True)
    def setup(self, multi_pvc_factory, pod_factory):
        """
        Create resources for the test

        """
        access_modes_cephfs = [
            constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX
        ]
        access_modes_rbd = [
            constants.ACCESS_MODE_RWO, f'{constants.ACCESS_MODE_RWO}-Block',
            f'{constants.ACCESS_MODE_RWX}-Block'
        ]

        self.pvcs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM, size=10,
            access_modes=access_modes_cephfs, status=constants.STATUS_BOUND,
            num_of_pvc=2, timeout=90
        )

        self.pvcs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.pvcs_cephfs[0].project, size=10,
            access_modes=access_modes_rbd,
            status=constants.STATUS_BOUND, num_of_pvc=3, timeout=90
        )

        pods_cephfs = helpers.create_pods(
            self.pvcs_cephfs, pod_factory, constants.CEPHFILESYSTEM, 2
        )
        pods_rbd = helpers.create_pods(
            self.pvcs_rbd, pod_factory, constants.CEPHBLOCKPOOL, 2
        )

        self.pods = pods_cephfs + pods_rbd

        # Set volume mode on PVC objects
        for pvc_obj in self.pvcs_cephfs+self.pvcs_rbd:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, 'volume_mode', pvc_info['spec']['volumeMode'])

    def expand_and_verify(self, pvc_size_new):
        """
        Modify size of PVC and verify the change

        Args:
            pvc_size_new (int): Size of PVC(in Gb) to expand

        """
        for pvc_obj in self.pvcs_cephfs + self.pvcs_rbd:
            log.info(
                f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G"
            )
            pvc_obj.resize_pvc(pvc_size_new, True)

        log.info(f"Verified: Size of all PVCs are expanded to {pvc_size_new}G")

        log.info(f"Verifying new size on pods.")
        for pod_obj in self.pods:
            if pod_obj.pvc.volume_mode == 'Block':
                log.info(
                    f"Skipping check on pod {pod_obj.name} as volume "
                    f"mode is Block."
                )
                continue

            # Wait for 240 seconds to reflect the change on pod
            log.info(f"Checking pod {pod_obj.name} to verify the change.")
            for df_out in TimeoutSampler(
                240, 3, pod_obj.exec_cmd_on_pod, command='df -kh'
            ):
                df_out = df_out.split()
                new_size_mount = df_out[
                    df_out.index(pod_obj.get_storage_path()) - 4
                ]
                if new_size_mount in [
                    f'{pvc_size_new - 0.1}G', f'{float(pvc_size_new)}G',
                    f'{pvc_size_new}G'
                ]:
                    log.info(
                        f"Verified: Expanded size of PVC {pod_obj.pvc.name} "
                        f"is reflected on pod {pod_obj.name}"
                    )
                    break
                log.info(
                    f"Expanded size of PVC {pod_obj.pvc.name} is not reflected"
                    f" on pod {pod_obj.name}. New size on mount is not "
                    f"{pvc_size_new}G as expected, but {new_size_mount}. "
                    f"Checking again."
                )
        log.info(
            f"Verified: Modified size {pvc_size_new}G is reflected "
            f"on all pods."
        )

    @acceptance
    @pytest.mark.polarion_id('OCS-2219')
    def test_pvc_expansion(self):
        """
        Verify PVC expand feature

        """
        pvc_size_new = 25

        # Modify size of PVCs and verify the change
        log.info(f"Expanding PVCs to {pvc_size_new}G")
        self.expand_and_verify(pvc_size_new)
