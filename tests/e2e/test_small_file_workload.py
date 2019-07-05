"""
Test to exercise Small File Workload
"""
import logging
import pytest
import time

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, workloads

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def ripsaw(request, storageclass_factory):

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)

    # Create Ceph Block Pool backed PVC
    storageclass_factory(sc_name='ceph-backed')
    # Create RipSaw Operator
    ripsaw = RipSaw()

    return ripsaw


@workloads
class TestSmallFileWorkload(E2ETest):
    """
    Deploy Ripsaw operator and run SmallFile workload
    SmallFile workload using https://github.com/distributed-system-analysis/smallfile
    smallfile is a python-based distributed POSIX workload generator which can be
    used to quickly measure performance for a variety of metadata-intensive
    workloads
    """
    @pytest.mark.polarion_id("OCS-1295")
    def test_smallfile_workload(self, ripsaw):
        """
        Run SmallFile Workload
        """
        log.info("Apply Operator CRD")
        ripsaw.apply_crd('resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml')

        log.info("Running SmallFile bench")
        sf_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)
        sf_obj = OCS(**sf_data)
        sf_obj.create()
        # wait for benchmark pods to get created - takes a while
        for bench_pod in TimeoutSampler(
            40, 3, get_pod_name_by_pattern, 'smallfile-client', 'my-ripsaw'
        ):
            try:
                if bench_pod[0] is not None:
                    small_file_client_pod = bench_pod[0]
                    break
            except IndexError:
                log.info("Bench pod not ready yet")

        bench_pod = OCP(kind='pod', namespace='my-ripsaw')
        log.info("Waiting for SmallFile benchmark to Run")
        assert bench_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=small_file_client_pod,
            sleep=30,
            timeout=600
        )
        start_time = time.time()
        timeout = 900
        while True:
            logs = bench_pod.exec_oc_cmd(
                f'logs {small_file_client_pod}',
                out_yaml_format=False
            )
            if "RUN STATUS DONE" in logs:
                log.info("SmallFile Benchmark Completed Successfully")
                break

            if timeout < (time.time() - start_time):
                raise TimeoutError(f"Timed out waiting for benchmark to complete")
            time.sleep(30)
