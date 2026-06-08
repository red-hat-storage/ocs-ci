import logging
from time import sleep

import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients_push_missing_configs,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier2,
    acceptance,
    skipif_upgraded_from,
)
from ocs_ci.helpers import helpers
from ocs_ci.framework import config, config_safe_thread_pool_task

logger = logging.getLogger(__name__)


@green_squad
@skipif_ocs_version("<4.5")
@skipif_upgraded_from(["4.4"])
class TestPvcExpand(ManageTest):
    """
    Tests to verify PVC expansion

    """

    @pytest.fixture(autouse=True)
    def setup(self, multi_pvc_factory, pod_factory):
        """
        Create resources for the test

        """
        access_modes_cephfs = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        access_modes_rbd = [
            constants.ACCESS_MODE_RWO,
            f"{constants.ACCESS_MODE_RWO}-Block",
            f"{constants.ACCESS_MODE_RWX}-Block",
        ]

        self.pvcs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=10,
            access_modes=access_modes_cephfs,
            status=constants.STATUS_BOUND,
            num_of_pvc=2,
            timeout=300,
        )

        self.pvcs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.pvcs_cephfs[0].project,
            size=10,
            access_modes=access_modes_rbd,
            status=constants.STATUS_BOUND,
            num_of_pvc=3,
            timeout=300,
        )

        pods_cephfs = helpers.create_pods(
            self.pvcs_cephfs,
            pod_factory,
            constants.CEPHFILESYSTEM,
            2,
            constants.STATUS_RUNNING,
        )
        pods_rbd = helpers.create_pods(
            self.pvcs_rbd,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            2,
            constants.STATUS_RUNNING,
        )

        self.pods = pods_cephfs + pods_rbd

        # Set volume mode on PVC objects
        for pvc_obj in self.pvcs_cephfs + self.pvcs_rbd:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, "volume_mode", pvc_info["spec"]["volumeMode"])

    def expand_and_verify(self, pvc_size_new, start_delay=0):
        """
        Modify size of PVC and verify the change

        Args:
            pvc_size_new (int): Size of PVC(in Gb) to expand
            start_delay (int): Time in seconds to wait before starting the expansion process

        """
        # Wait some time before starting PVC expansion if needed
        sleep(start_delay)

        logger.info(f"Expanding all PVCs to {pvc_size_new}G")
        for pvc_obj in self.pvcs_cephfs + self.pvcs_rbd:
            logger.debug(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
            pvc_obj.resize_pvc(pvc_size_new, True)

        logger.info(f"Verified: Size of all PVCs are expanded to {pvc_size_new}G")

        logger.info("Verifying new size on pods")
        for pod_obj in self.pods:
            if pod_obj.pvc.volume_mode == "Block":
                logger.debug(
                    f"Skipping check on pod {pod_obj.name} as volume mode is Block."
                )
                continue

            # Wait for 240 seconds to reflect the change on pod
            logger.debug(f"Checking pod {pod_obj.name} to verify the change.")
            for df_out in TimeoutSampler(
                240, 3, pod_obj.exec_cmd_on_pod, command="df -kh"
            ):
                if not df_out:
                    continue
                df_out = df_out.split()
                new_size_mount = df_out[df_out.index(pod_obj.get_storage_path()) - 4]
                if (
                    pvc_size_new - 0.5 <= float(new_size_mount[:-1]) <= pvc_size_new
                    and new_size_mount[-1] == "G"
                ):
                    logger.debug(
                        f"Verified: Expanded size of PVC {pod_obj.pvc.name} "
                        f"is reflected on pod {pod_obj.name}"
                    )
                    break
                logger.debug(
                    f"Expanded size of PVC {pod_obj.pvc.name} is not reflected"
                    f" on pod {pod_obj.name}. New size on mount is not "
                    f"{pvc_size_new}G as expected, but {new_size_mount}. "
                    f"Checking again."
                )
        logger.info(
            f"Verified: Modified size {pvc_size_new}G is reflected on all pods."
        )

    def run_io_and_verify(self, file_size, io_phase, verify=True):
        """
        Run fio on all pods and verify results

        Args:
            file_size (int): Size of fio file
            io_phase (str): pre_expand or post_expand
            verify (bool): True to verify fio, False otherwise

        """
        logger.info(f"Starting {io_phase} IO on {len(self.pods)} pods")
        for pod_obj in self.pods:
            storage_type = "block" if pod_obj.pvc.volume_mode == "Block" else "fs"

            # Split file size and write from two pods if access mode is RWX
            size = (
                f"{int(file_size / 2)}G"
                if (pod_obj.pvc.access_mode == constants.ACCESS_MODE_RWX)
                else f"{file_size}G"
            )
            logger.debug(f"Starting {io_phase} IO on pod {pod_obj.name}")
            pod_obj.run_io(
                storage_type=storage_type,
                size=size,
                io_direction="write",
                runtime=60,
                fio_filename=f"{pod_obj.name}_{io_phase}",
                direct=int(storage_type == "block"),
            )
            logger.debug(f"{io_phase} IO started on pod {pod_obj.name}")
        logger.info(f"{io_phase} IO started on all pods")

        if not verify:
            return

        logger.info(f"Verifying {io_phase} IO on pods")
        for pod_obj in self.pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"{io_phase} IO error on pod {pod_obj.name}. "
                f"FIO result: {fio_result}"
            )
            logger.debug(f"Verified {io_phase} IO on pod {pod_obj.name}")
        logger.info(f"Verified {io_phase} IO on all pods")

    @provider_mode
    @acceptance
    @run_on_all_clients_push_missing_configs
    @tier1
    @pytest.mark.polarion_id("OCS-2219")
    def test_pvc_expansion(self, cluster_index):
        """
        Verify PVC expand feature

        """
        # Expand PVC with a small amount to fall behind default quota (100 Gi) for
        # openshift dedicated
        if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
            pvc_size_new = 15
        else:
            pvc_size_new = 25

        logger.test_step(f"Expand all PVCs to {pvc_size_new}G and verify")
        self.expand_and_verify(pvc_size_new)

    @tier2
    @pytest.mark.polarion_id("OCS-302")
    def test_pvc_expand_expanded_pvc(self):
        """
        Verify PVC expand of already expanded PVC

        """
        if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
            pvc_size_expanded_1 = 19
            pvc_size_expanded_2 = 20
        else:
            pvc_size_expanded_1 = 20
            pvc_size_expanded_2 = 25
        executor = ThreadPoolExecutor(max_workers=len(self.pods))

        logger.test_step("Set up pods for running IO")
        for pod_obj in self.pods:
            logger.debug(f"Setting up pod {pod_obj.name} for running IO")
            if pod_obj.pvc.volume_mode == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            executor.submit(pod_obj.workload_setup, storage_type=storage_type)

        # Wait for setup on pods to complete
        for pod_obj in self.pods:
            logger.debug(f"Waiting for IO setup to complete on pod {pod_obj.name}")
            for sample in TimeoutSampler(360, 2, getattr, pod_obj, "wl_setup_done"):
                if sample:
                    logger.debug(
                        f"Setup for running IO is completed on pod {pod_obj.name}"
                    )
                    break
        logger.info("Setup for running IO is completed on all pods")

        logger.test_step("Run pre-expand IO on all pods")
        self.run_io_and_verify(7, "pre_expand")

        logger.test_step(
            f"Expand PVCs to {pvc_size_expanded_1}G while running IO concurrently"
        )
        logger.info(
            f"Expanding all PVCs to {pvc_size_expanded_1}G after 3 seconds delay"
        )
        pvc_expand_process = executor.submit(
            config_safe_thread_pool_task,
            config.cur_index,
            self.expand_and_verify,
            pvc_size_new=pvc_size_expanded_1,
            start_delay=3,
        )

        logger.info("Running IO on all pods during PVC expansion")
        for process_running in TimeoutSampler(500, 3, pvc_expand_process.running):
            if process_running:
                self.run_io_and_verify(2, "during_expand")
            else:
                break
        logger.info("Verified IO on all pods during the expansion process")

        # Get PVC expansion result
        pvc_expand_process.result()

        logger.test_step("Run post-expand IO on all pods")
        self.run_io_and_verify(6, "post_expand")

        logger.test_step(
            f"Expand PVCs to {pvc_size_expanded_2}G (second expansion) while running IO concurrently"
        )
        logger.info(
            f"Expanding all PVCs to {pvc_size_expanded_2}G after 3 seconds delay"
        )
        pvc_expand_process = executor.submit(
            config_safe_thread_pool_task,
            config.cur_index,
            self.expand_and_verify,
            pvc_size_new=pvc_size_expanded_2,
            start_delay=3,
        )
        self.expand_and_verify(pvc_size_expanded_2)

        logger.info("Running IO on all pods during second PVC expansion")
        for process_running in TimeoutSampler(500, 3, pvc_expand_process.running):
            if process_running:
                self.run_io_and_verify(2, "during_second_expand")
            else:
                break
        logger.info("Verified IO on all pods during the second expansion process")

        # Get PVC expansion result
        pvc_expand_process.result()

        logger.test_step("Run post-second-expand IO on all pods")
        self.run_io_and_verify(6, "post_expand")
