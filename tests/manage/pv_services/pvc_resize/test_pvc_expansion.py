import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier1, acceptance, skipif_upgraded_from
)
from tests import helpers

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.5')
@skipif_upgraded_from(['4.4'])
@pytest.mark.skipif(
    config.ENV_DATA['platform'].lower() == 'ibm_cloud',
    reason=(
        "Skipping tests on IBM Cloud due to bug 1871314 "
        "https://bugzilla.redhat.com/show_bug.cgi?id=1871314"
    )
)
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
            self.pvcs_cephfs, pod_factory, constants.CEPHFILESYSTEM, 2,
            constants.STATUS_RUNNING
        )
        pods_rbd = helpers.create_pods(
            self.pvcs_rbd, pod_factory, constants.CEPHBLOCKPOOL, 2,
            constants.STATUS_RUNNING
        )

        self.pods = pods_cephfs + pods_rbd

        # Set volume mode on PVC objects
        for pvc_obj in self.pvcs_cephfs + self.pvcs_rbd:
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

        log.info("Verifying new size on pods.")
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

    def run_io_and_verify(self, file_size, io_phase, verify=True):
        """
        Run fio on all pods and verify results

        Args:
            file_size (int): Size of fio file
            io_phase (str): pre_expand or post_expand
            verify (bool): True to verify fio, False otherwise

        """
        for pod_obj in self.pods:
            storage_type = (
                'block' if pod_obj.pvc.volume_mode == 'Block' else 'fs'
            )

            # Split file size and write from two pods if access mode is RWX
            size = (
                f'{int(file_size/2)}G' if (
                    pod_obj.pvc.access_mode == constants.ACCESS_MODE_RWX
                ) else f'{file_size}G'
            )
            log.info(f"Starting {io_phase} IO on pod {pod_obj.name}.")
            pod_obj.run_io(
                storage_type=storage_type, size=size, io_direction='write',
                runtime=60, fio_filename=f'{pod_obj.name}_{io_phase}'
            )
            log.info(f"{io_phase} IO started on pod {pod_obj.name}.")
        log.info(f"{io_phase} IO started on pods.")

        if not verify:
            return

        log.info(f"Verifying {io_phase} IO on pods.")
        for pod_obj in self.pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get('jobs')[0].get('error')
            assert err_count == 0, (
                f"{io_phase} IO error on pod {pod_obj.name}. "
                f"FIO result: {fio_result}"
            )
            log.info(f"Verified {io_phase} IO on pod {pod_obj.name}.")

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

    @pytest.mark.polarion_id('OCS-302')
    def test_pvc_expand_expanded_pvc(self):
        """
        Verify PVC expand of already expanded PVC

        """
        pvc_size_expanded_1 = 20
        pvc_size_expanded_2 = 25
        executor = ThreadPoolExecutor(max_workers=len(self.pods))

        # Do setup on pods for running IO
        log.info("Setting up pods for running IO.")
        for pod_obj in self.pods:
            log.info(f"Setting up pod {pod_obj.name} for running IO")
            if pod_obj.pvc.volume_mode == 'Block':
                storage_type = 'block'
            else:
                storage_type = 'fs'
            executor.submit(pod_obj.workload_setup, storage_type=storage_type)

        # Wait for setup on pods to complete
        for pod_obj in self.pods:
            log.info(
                f"Waiting for IO setup to complete on pod {pod_obj.name}"
            )
            for sample in TimeoutSampler(
                360, 2, getattr, pod_obj, 'wl_setup_done'
            ):
                if sample:
                    log.info(
                        f"Setup for running IO is completed on pod "
                        f"{pod_obj.name}."
                    )
                    break
        log.info("Setup for running IO is completed on all pods.")

        # Run IO and verify
        log.info("Starting pre-expand IO on all pods.")
        self.run_io_and_verify(9, 'pre_expand')
        log.info("Verified pre-expand IO result on pods.")

        log.info("Expanding all PVCs.")
        self.expand_and_verify(pvc_size_expanded_1)

        # Run IO and verify
        log.info("Starting post-expand IO on all pods.")
        self.run_io_and_verify(8, 'post_expand')
        log.info("Verified post-expand IO result on pods.")

        log.info("Expanding all PVCs for the second time.")
        self.expand_and_verify(pvc_size_expanded_2)

        # Run IO and verify
        log.info("Starting post-second-expand IO on all pods.")
        self.run_io_and_verify(6, 'post_expand')
        log.info("Verified post-second-expand IO result on pods.")
