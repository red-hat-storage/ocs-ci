"""
Module to perform FIO workload
"""
import logging
import pytest
from elasticsearch import Elasticsearch
from ocs_ci.framework import config
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP, get_ocs_version, get_build
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    E2ETest, performance, rh_internal_lab_required
)
from ocs_ci.utility.performance_dashboard import push_perf_dashboard

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def ripsaw(request):

    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)
    return ripsaw


def analyze_regression(io_pattern, storage_class, es_username):
    """
    Analyzes the FIO result for variance and regression
    The test fails ff the test run has more than 5% regression

    Args:
        io_pattern (str): 'sequential' or 'random' workload
        es_username (str): ocs_build used in the CR object

    """
    es = Elasticsearch([{'host': constants.ES_SERVER_IP, 'port': constants.ES_SERVER_PORT}])
    # Todo: Fetch benchmark values for FIO, which
    #  Will be implemented after finalizing on h/w
    # fetch results for the current run with unique es_username
    fio_analyzed_result = es.search(index='ripsaw-fio-analyzed-result',
                                    body={"query": {"match": {'user': es_username}}})
    assert fio_analyzed_result['hits']['hits'], 'Results not found in Elasticsearch'
    # Initialize variables for codespeed results
    reads = 0
    writes = 0
    r_bw = 0
    w_bw = 0
    for result in fio_analyzed_result['hits']['hits']:
        test_data = result['_source']['ceph_benchmark_test']['test_data']
        object_size = test_data['object_size']
        operation = test_data['operation']
        total_iops = test_data['total-iops']
        log.info(
            f"io_pattern: {io_pattern}\n"
            f"block_size: {object_size}\n"
            f"operation: {operation}\n"
            f"total_iops: {total_iops}\n"
        )
        # Fail test if std deviation is above 5%
        # Todo: Remove the below skip for random workload once
        #  https://github.com/cloud-bulldozer/snafu/issues/180 is fixed
        if io_pattern == 'sequential':
            std_dev = 'std-dev-' + object_size
            variance = test_data[std_dev]
            log.info(f'variance - {variance}')
        # Todo: Fail test if 5% deviation from benchmark value

        # Extracting results for code speed
        if operation == "randread":
            if object_size == "4KiB":
                reads = total_iops
            if object_size == "1024KiB":  # if BS is 1M, then IOPS == Bandwidth
                r_bw = total_iops
        if operation == "randwrite":
            if object_size == "4KiB":
                writes = total_iops
            if object_size == "1024KiB":  # if BS is 1M, then IOPS == Bandwidth
                w_bw = total_iops
    # Pushing the results into codespeed
    push_perf_dashboard(storage_class, reads, writes, r_bw, w_bw)


@rh_internal_lab_required
@performance
@pytest.mark.parametrize(
    argnames=["interface", "io_pattern"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'sequential'], marks=pytest.mark.polarion_id("OCS-844")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'sequential'], marks=pytest.mark.polarion_id("OCS-845")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'random'], marks=pytest.mark.polarion_id("OCS-846")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'random'], marks=pytest.mark.polarion_id("OCS-847")
        )
    ]
)
class TestFIOBenchmark(E2ETest):
    """
    Run FIO perf test using ripsaw benchmark
    """
    def test_fio_workload_simple(self, ripsaw, interface, io_pattern):
        """
        This is a basic fio perf test
        """
        # Deployment ripsaw
        log.info("Deploying ripsaw operator")
        ripsaw.apply_crd(
            'resources/crds/'
            'ripsaw_v1alpha1_ripsaw_crd.yaml'
        )
        sc = 'ocs-storagecluster-ceph-rbd' if interface == 'CephBlockPool' else 'ocs-storagecluster-cephfs'

        # Create fio benchmark
        log.info("Create resource file for fio workload")
        fio_cr = templating.load_yaml(constants.FIO_CR_YAML)
        # Todo: have pvc_size set to 'get_osd_pods_memory_sum * 5'
        #  once pr-2037 is merged
        fio_cr['spec']['clustername'] = config.ENV_DATA['platform'] + get_build() + get_ocs_version()
        fio_cr['spec']['test_user'] = get_ocs_version() + interface + io_pattern
        fio_cr['spec']['workload']['args']['storageclass'] = sc
        if io_pattern == 'sequential':
            fio_cr['spec']['workload']['args']['jobs'] = ['write', 'read']
        log.info(f'fio_cr: {fio_cr}')
        fio_cr_obj = OCS(**fio_cr)
        fio_cr_obj.create()

        # Wait for fio client pod to be created
        for fio_pod in TimeoutSampler(
            300, 20, get_pod_name_by_pattern, 'fio-client', 'my-ripsaw'
        ):
            try:
                if fio_pod[0] is not None:
                    fio_client_pod = fio_pod[0]
                    break
            except IndexError:
                log.info("Bench pod not ready yet")

        # Wait for fio pod to initialized and complete
        log.info("Waiting for fio_client to complete")
        pod_obj = OCP(kind='pod')
        pod_obj.wait_for_resource(
            condition='Completed',
            resource_name=fio_client_pod,
            timeout=18000,
            sleep=300,
        )

        output = run_cmd(f'oc logs {fio_client_pod}')

        try:
            if 'Fio failed to execute' not in output:
                log.info("FIO has completed successfully")
        except IOError:
            log.info("FIO failed to complete")

        # Clean up fio benchmark
        log.info("Deleting FIO benchmark")
        fio_cr_obj.delete()
        analyze_regression(io_pattern, sc, es_username=fio_cr['spec']['test_user'])

        # todo: push results to codespeed
