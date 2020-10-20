import logging
import shlex
import subprocess
import os
import time

from ocs_ci.framework.testlib import ignore_leftovers
from ocs_ci.utility.utils import clone_repo, run_cmd
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@ignore_leftovers
def test_ocs_monkey():
    ocs_monkety_dir = '/tmp/ocs-monkey'
    # ocs-monkey run time in seconds
    run_time = 3200
    clone_repo(constants.OCS_MONKEY_REPOSITORY, ocs_monkety_dir)
    run_cmd(
        f"pip install -r {os.path.join(ocs_monkety_dir, 'requirements.txt')}"
    )
    workload_run_cmd = f"python workload_runner.py -t {run_time}"

    start_time = time.time()
    log.info("Starting ocs-monkey")
    popen_obj = subprocess.Popen(
        shlex.split(workload_run_cmd),
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        encoding='utf-8', cwd=ocs_monkety_dir
    )

    while True:
        output = popen_obj.stdout.readline()
        # Check whether the process is completed
        ret = popen_obj.poll()
        if len(output) == 0 and ret is not None:
            log.info("ocs-monkey run completed.")
            assert ret == 0, (
                f"ocs-monkey exited with return value {ret}. "
                f"Check logs for details."
            )
            break
        # Stream the output in console
        if output:
            log.info(output.strip())
        # Terminate the process if it is not completed within the specified
        # time. Give grace period of 600 seconds considering the time
        # taken for setup
        if time.time() - start_time > run_time + 600:
            log.error(
                f"ocs-monkey did not complete with in the specified run time"
                f" {run_time} seconds. Killing the process now."
            )
            popen_obj.terminate()
            raise TimeoutError(
                f"ocs-monkey did not complete with in the specified run time "
                f"{run_time} seconds. Killed the process after providing "
                f"grace period of 600 seconds."
            )
