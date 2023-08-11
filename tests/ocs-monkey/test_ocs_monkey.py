import logging
import shlex
import subprocess
import os
import time

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import ignore_leftovers
from ocs_ci.utility.utils import clone_repo, run_cmd, ceph_health_check
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@ignore_leftovers
def test_ocs_monkey():
    ocs_monkety_dir = "/tmp/ocs-monkey"
    # ocs-monkey run time in seconds
    run_time = 3600
    clone_repo(constants.OCS_MONKEY_REPOSITORY, ocs_monkety_dir)
    run_cmd(f"pip install -r {os.path.join(ocs_monkety_dir, 'requirements.txt')}")
    workload_run_cmd = f"python workload_runner.py -t {run_time}"
    chaos_runner_cmd = "python chaos_runner.py"

    start_time = time.time()
    log.info("Starting workload runner")
    popen_workload = subprocess.Popen(
        shlex.split(workload_run_cmd),
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        encoding="utf-8",
        cwd=ocs_monkety_dir,
    )

    log.info("Starting chaos runner")
    popen_chaos = subprocess.Popen(
        shlex.split(chaos_runner_cmd),
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        encoding="utf-8",
        cwd=ocs_monkety_dir,
    )

    while True:
        output_workload = popen_workload.stdout.readline()

        # Get the status of workload runner process
        ret_workload = popen_workload.poll()

        # Stream the workload runner output in console
        if output_workload:
            log.info(output_workload.strip())

        if ret_workload is not None:
            log.info("Workload runner completed.")
            log.debug(popen_workload.stdout.read())
            # Terminate chaos_runner if workload_runner is completed
            log.info("Terminating chaos runner")
            popen_chaos.terminate()
            # Check return value of workload runner process
            assert ret_workload == 0, (
                f"Workload runner exited with return value {ret_workload}. "
                f"Check logs for details."
            )
            log.info("ocs-monkey run completed")
            break

        output_chaos = popen_chaos.stdout.readline()
        # Get the status of chaos runner process
        ret_chaos = popen_chaos.poll()

        # Stream the chaos runner output in console
        if output_chaos:
            log.info(output_chaos.strip())

        if ret_chaos is not None:
            log.info("Chaos runner completed.")
            log.debug(popen_chaos.stdout.read())
            # Terminate workload_runner if chaos_runner is completed
            log.info("Terminating workload runner")
            popen_workload.terminate()
            assert ret_chaos == 0, (
                f"Chaos runner exited with return value {ret_chaos}. "
                f"Check logs for details."
            )
            log.info("ocs-monkey run completed")
            break

        # Terminate the process if it is not completed within the specified
        # time. Give grace period of 900 seconds considering the time
        # taken for setup
        if time.time() - start_time > run_time + 900:
            log.error(
                f"ocs-monkey did not complete with in the specified run time"
                f" {run_time} seconds. Killing the process now."
            )
            popen_workload.terminate()
            popen_chaos.terminate()
            raise TimeoutError(
                f"ocs-monkey did not complete with in the specified run time "
                f"{run_time} seconds. Killed the process after providing "
                f"grace period of 900 seconds."
            )

    assert ceph_health_check(tries=40, delay=30), "Ceph cluster health is not OK"
