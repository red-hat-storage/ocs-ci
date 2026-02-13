import logging
import time

from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.pytest_customization.marks import magenta_squad, stress
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cephfs_stress_helpers import (
    run_stress_cleanup,
    get_mount_subdirs,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pod
from ocs_ci.helpers.cephfs_stress_helpers import CephFSStressTestManager


logger = logging.getLogger(__name__)


@stress
@magenta_squad
class TestCephfsStressCleanUp(E2ETest):
    """
    Test Cephfs incremental bulk operations cleanup
    """

    def test_cephfs_stress_cleanup(self, project_factory, teardown_factory):
        """
        Stress the cluster by performing bulk Cephfs data operations - Data creation, deletion and File operations
        in incremental stages and perform a stress clean up at the end by executing a parallelized deletion.

        The testing starts with a base configuration and gradually increases load on CephFS in incremental stages

        Iteration 1: Using smallfiles, double the existing base directory count on the CephFS mount by creating new
        directories and files.
        Iteration 2: If stable, increase to three times the original base directory count
        Iteration 3: If stable, increase to four times the original base directory count
        Iteration 4: If stable, increase to five times the original base directory count


        File Operations (for each iteration):
        Perform a variety of file operations (e.g., create,append,rename,stat,chmod,ls-l) on the iter-(1) data

        After completing all the iterations, perform a stress clean up by executing a parallelized deletion.
        """
        CHECKS_RUNNER_INTERVAL_MINUTES = 30
        JOB_POD_INTERVAL_SECONDS = 300
        m_factor = "1,2"
        parallelism = 3
        completions = 3

        proj_name = "cephfs-stress-testing"
        project_factory(project_name=proj_name)
        stress_mgr = CephFSStressTestManager(namespace=proj_name)

        pvc_obj, _ = stress_mgr.setup_stress_test_environment(pvc_size="500Gi")

        pods_list = []
        pod_count = int(parallelism)
        logger.info("Creating pods for stress cleanup operations")
        for count in range(pod_count):
            rm_pod = create_pod(
                interface_type=constants.CEPHFILESYSTEM,
                pvc_name=pvc_obj.name,
                namespace=proj_name,
                pod_name=f"cleanup-cephfs-stress-pod-{count}",
            )
            pods_list.append(rm_pod)

        teardown_factory(pods_list)
        try:
            stress_mgr.start_background_checks(
                interval_minutes=CHECKS_RUNNER_INTERVAL_MINUTES
            )

            cephfs_stress_job_obj = stress_mgr.create_cephfs_stress_job(
                pvc_name=pvc_obj.name,
                multiplication_factors=m_factor,
                parallelism=parallelism,
                completions=completions,
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

            logger.info("Starting stress cleanup....")
            subdirs = get_mount_subdirs(pods_list[0])

            with ThreadPoolExecutor(max_workers=len(pods_list)) as executor:
                futures = [
                    executor.submit(
                        run_stress_cleanup,
                        pods_list[i],
                        subdirs[i],
                        timeout=43200,
                        parallelism_count=50,
                    )
                    for i in range(len(pods_list))
                ]
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"One of the cleanup threads failed: {e}")
            logger.info("Stress cleanup is successful")

        finally:
            stress_mgr.teardown()
