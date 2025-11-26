import logging
import threading
import time

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cephfs_stress_helpers import (
    create_cephfs_stress_pod,
    continuous_checks_runner,
    verification_failures,
    stop_event,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pod


logger = logging.getLogger(__name__)


@magenta_squad
class TestCephfsStress(E2ETest):
    """
    CephFS break point test - without failures
    """

    def test_cephfs_breakpoint(
        self, threading_lock, project_factory, pvc_factory, teardown_factory
    ):
        """
        The primary objective of this test is to find the system's breaking point which is the critical
        threshold at which ODF operations cease to complete successfully within the defined resource limits

        The testing starts with a base configuration and gradually increases load on CephFS in incremental stages until
        the breaking point is reached

        Iteration 1: Using smallfiles, double the existing base directory count on the CephFS mount by creating new
        directories and files.
        Iteration 2: If stable, increase to three times the original base directory count
        Iteration 3: If stable, increase to four times the original base directory count
        Iteration 4: If stable, increase to five times the original base directory count
        Subsequent Iterations (Gradual Increase): If stable, continue increasing the file and directory count by
        factors of 3, then 2, then 1, from the previous iteration's total

        File Operations (for each iteration):
        Perform a variety of file operations (e.g., create,append,rename,stat,chmod,ls-l) on the iter-(1) data

        """
        CHECKS_RUNNER_INTERVAL_MINUTES = 30
        POD_INTERVAL_SECONDS = 300
        stress_checks_thread = threading.Thread(
            target=continuous_checks_runner,
            args=(CHECKS_RUNNER_INTERVAL_MINUTES, threading_lock),
            name="StressCheckRunnerThread",
            daemon=True,
        )
        stress_checks_thread.start()
        proj_name = "cephfs-stress-testing"
        proj_obj = project_factory(project_name=proj_name)
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=proj_obj,
            size=400,
            access_mode=constants.ACCESS_MODE_RWX,
            pvc_name="stress-cephfs-1",
        )
        standby_pod = create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=pvc_obj.name,
            namespace=proj_name,
            pod_name="standby-stress-pod",
        )
        teardown_factory(standby_pod)
        try:
            cephfs_stress_pod_obj = create_cephfs_stress_pod(
                namespace=proj_name, pvc_name=pvc_obj.name
            )
            logger.info(
                f"The CephFS-stress pod {cephfs_stress_pod_obj.name} is created "
            )

            while True:
                # Check for failure signal from the check-thread
                if verification_failures:
                    raise Exception(
                        f"Test failed due to validation failure: {verification_failures[0]}"
                    )

                status = cephfs_stress_pod_obj.status()

                if status == constants.STATUS_COMPLETED:
                    logger.info(
                        f"Pod '{cephfs_stress_pod_obj.name}' reached 'Completed' state"
                    )
                    break
                elif status == constants.STATUS_RUNNING:
                    logger.info(
                        f"******* {cephfs_stress_pod_obj.name} is still in {status} state. "
                        "Waiting for {POD_INTERVAL_SECONDS}s...*******"
                    )
                    logger.info(
                        f"******* Check {cephfs_stress_pod_obj.name} logs to get more details on the ongoing "
                        "file and directory opearations.....*******"
                    )
                    time.sleep(POD_INTERVAL_SECONDS)
                else:
                    raise Exception(
                        f"Pod '{cephfs_stress_pod_obj.name}' entered unexpected state '{status}' state"
                    )

        finally:
            teardown_factory(cephfs_stress_pod_obj)
            logger.info("Signaling check thread to stop...")
            stop_event.set()
            stress_checks_thread.join()
            logger.info("StressCheckRunnerThread has stopped")
