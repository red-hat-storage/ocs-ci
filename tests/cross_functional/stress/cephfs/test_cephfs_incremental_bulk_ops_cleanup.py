import logging
import threading
import time

from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cephfs_stress_helpers import (
    create_cephfs_stress_job,
    continuous_checks_runner,
    verification_failures,
    stop_event,
    run_stress_cleanup,
    get_mount_subdirs,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pod


logger = logging.getLogger(__name__)


@magenta_squad
class TestCephfsStressCleanUp(E2ETest):
    """
    Test Cephfs incremental bulk operations cleanup
    """

    def test_cephfs_stress_cleanup(
        self, threading_lock, project_factory, pvc_factory, teardown_factory
    ):
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
        m_factor = 1, 2, 3, 4, 5
        parallelism = 10
        completions = 10

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
            pvc_name="cephfs-stress-pvc",
        )
        standby_pod = create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=pvc_obj.name,
            namespace=proj_name,
            pod_name="standby-cephfs-stress-pod",
        )
        teardown_factory(standby_pod)

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
            cephfs_stress_job_obj = create_cephfs_stress_job(
                namespace=proj_name,
                pvc_name=pvc_obj.name,
                multiplication_factor=m_factor,
                parallelism=parallelism,
                completions=completions,
            )
            logger.info(
                f"The CephFS-stress Job {cephfs_stress_job_obj.name} has been submitted"
            )
            while True:
                # Check for failure signal from the check-thread
                if verification_failures:
                    raise Exception(
                        f"Test failed due to validation failure: {verification_failures[0]}"
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
            teardown_factory(cephfs_stress_job_obj)
            logger.info("Signaling check thread to stop...")
            stop_event.set()
            stress_checks_thread.join()
            logger.info("StressCheckRunnerThread has stopped")
