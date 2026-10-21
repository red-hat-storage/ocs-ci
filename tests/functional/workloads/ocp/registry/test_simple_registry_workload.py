import pytest

import shlex
from subprocess import Popen, PIPE
import logging
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.utility.svt import (
    svt_project_clone,
    svt_create_venv_setup,
    svt_cluster_loader,
    svt_cleanup,
)

logger = logging.getLogger(__name__)


@magenta_squad
@pytest.mark.skip(
    reason="Skipped due to issue https://github.com/openshift/svt/issues/697"
)
class TestRegistryWorkload:
    def deprecated_test_registry_workload(self, iterations=5):
        """
        https://github.com/openshift/svt/blob/master/openshift_performance/ose3_perf/scripts/build_test-README.md
        The build_test.py scripts is a flexible tool for driving builds in OpenShift.
        It can execute builds concurrently, sequentially, randomly or in arbitrary combinations.
        Arguments for build_test.py:
             -z : No-login(oc login)
             -a : Run all builds in all projects
             -n : Number of iterations (Do not use more than 10)

        Args:
            iterations (int): Number of iterations

        """

        logger.test_step("Setup SVT environment")
        try:
            svt_project_clone()
            svt_create_venv_setup()
            svt_cluster_loader()

            logger.test_step(f"Execute build test with {iterations} iterations")
            cmd = (
                "/bin/sh -c 'source /tmp/venv/bin/activate && "
                f"python /tmp/svt/openshift_performance/ose3_perf/scripts/build_test.py -z -a -n {iterations}'"
            )
            logger.info(f"Running command: {cmd}")
            build = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
            stdout, stderr = build.communicate()
            out = stderr.split("\n".encode())[-5].split()[-1]
            logger.debug(f"Build output: {stderr}")

            logger.test_step("Verify build success count")
            build_count = int(out)
            logger.assertion(
                f"Successful builds: actual={build_count}, minimum=100, "
                f"passed={build_count >= 100}"
            )
            assert (
                build_count >= 100
            ), f"Insufficient successful builds: {build_count} < 100"

        finally:
            logger.test_step("Cleanup SVT resources")
            cleanup_result = svt_cleanup()
            logger.assertion(f"SVT cleanup: successful={cleanup_result}")
            assert cleanup_result, "SVT cleanup failed"
