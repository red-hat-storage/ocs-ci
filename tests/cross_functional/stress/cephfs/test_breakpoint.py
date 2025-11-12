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

logger = logging.getLogger(__name__)


@magenta_squad
class TestCephfsStress(E2ETest):
    """
    CephFS break point test - without failures
    """

    def test_cephfs_breakpoint(self):
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
            args=(CHECKS_RUNNER_INTERVAL_MINUTES,),
            name="StressCheckRunnerThread",
            daemon=True,
        )
        stress_checks_thread.start()

        try:
            cephfs_stress_pod_obj = create_cephfs_stress_pod()
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
                    logger.debug(
                        f"Pod is 'Running'. Waiting for {POD_INTERVAL_SECONDS}s..."
                    )
                    time.sleep(POD_INTERVAL_SECONDS)
                else:
                    raise Exception(
                        f"Pod '{cephfs_stress_pod_obj.name}' entered unexpected state '{status}' state"
                    )

        finally:
            logger.info("Signaling check thread to stop...")
            stop_event.set()
            stress_checks_thread.join()
            logger.info("StressCheckRunnerThread has stopped")
