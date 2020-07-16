import shlex
from subprocess import Popen, PIPE
from ocs_ci.framework.testlib import workloads
import logging
from ocs_ci.utility.svt import (
    svt_project_clone, svt_create_venv_setup,
    svt_cluster_loader, svt_cleanup
)
log = logging.getLogger(__name__)


@workloads
class TestRegistryWorkload:
    def test_registry_workload(self, iterations=5):
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

        try:
            svt_project_clone()
            svt_create_venv_setup()
            svt_cluster_loader()
            cmd = (
                "/bin/sh -c 'source /tmp/venv/bin/activate && "
                f"python /tmp/svt/openshift_performance/ose3_perf/scripts/build_test.py -z -a -n {iterations}'"
            )
            log.info(f"Running command {cmd}")
            build = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
            stdout, stderr = build.communicate()
            out = stderr.split('\n'.encode())[-10].split()[-1]
            log.info(stderr)
            assert (int(out) == 0), 'Failed builds found'

        finally:
            print("Calling svt cleanup")
            assert svt_cleanup()
