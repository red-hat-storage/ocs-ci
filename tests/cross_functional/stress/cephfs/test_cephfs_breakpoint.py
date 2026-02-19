import logging
import time

from ocs_ci.framework.pytest_customization.marks import magenta_squad, stress
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cephfs_stress_helpers import CephFSStressTestManager
from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


@stress
@magenta_squad
class TestCephfsStress(E2ETest):
    """
    CephFS break point test - without failures
    """

    def test_cephfs_breakpoint(
        self,
        project_factory,
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
        JOB_POD_INTERVAL_SECONDS = 300

        proj_name = "cephfs-stress-testing"
        project_factory(project_name=proj_name)
        stress_mgr = CephFSStressTestManager(namespace=proj_name)

        try:
            pvc_obj, _ = stress_mgr.setup_stress_test_environment(pvc_size="500Gi")

            stress_mgr.start_background_checks(
                interval_minutes=CHECKS_RUNNER_INTERVAL_MINUTES
            )

            cephfs_stress_job_obj = stress_mgr.create_cephfs_stress_job(
                pvc_name=pvc_obj.name,
                multiplication_factors="1,2",
                parallelism=2,
                completions=2,
                base_file_count=100,
            )
            logger.info(
                f"The CephFS-stress Job {cephfs_stress_job_obj.name} has been submitted"
            )

            while True:
                if stress_mgr.verification_failures:
                    raise Exception(
                        f"Test failed due to validation failure: {stress_mgr.verification_failures[0]}"
                    )

                status = cephfs_stress_job_obj.status()

                if status == "Complete":
                    logger.info(
                        f"Job '{cephfs_stress_job_obj.name}' reached 'Complete' state"
                    )
                    break
                elif status == constants.STATUS_RUNNING:
                    logger.info(
                        f"******* {cephfs_stress_job_obj.name} is still in {status} state. "
                        f"Waiting for {JOB_POD_INTERVAL_SECONDS}s...*******"
                    )
                    logger.info(
                        f"******* Check {cephfs_stress_job_obj.name} job logs to get more details on the ongoing "
                        "file and directory operations.....*******"
                    )
                    time.sleep(JOB_POD_INTERVAL_SECONDS)
                else:
                    raise Exception(
                        f"Job '{cephfs_stress_job_obj.name}' entered unexpected state '{status}' state"
                    )

        finally:
            stress_mgr.teardown()
