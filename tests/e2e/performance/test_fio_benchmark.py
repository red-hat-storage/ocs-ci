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
from ocs_ci.framework.testlib import E2ETest, performance
from ocs_ci.ocs.resources import storage_cluster
from tests.helpers import get_worker_nodes

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def ripsaw(request):

    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)
    return ripsaw


def analyze_regression(io_pattern, es_username):
    """
    Args:
        es_username (str): ocs_build used in the CR object
    """
    es = Elasticsearch([{'host': constants.ES_SERVER_IP, 'port': constants.ES_SERVER_PORT}])
    # Fetch benchmark values for FIO
    bm = es.search(index='fio-benchmark')
    benchmark = bm['hits']['hits'][0]['_source']
    # fetch results for the current run with unique es_username
    fio_analyzed_result = es.search(index='ripsaw-fio-analyzed-result',
                                    body={"query": {"match": {'user': es_username}}})
    assert fio_analyzed_result['hits']['hits'], 'Results not found in Elasticsearch'
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
        if io_pattern == 'sequential':
            std_dev = 'std-dev-' + object_size
            variance = test_data[std_dev]
            assert variance <= 5, f'variance - {variance} is greater than 5%'
        bm_value = benchmark[object_size][operation]
        difference = (float(total_iops) - float(bm_value)) / float(bm_value) * 100
        log.info(f'deviation from bm {difference}')
        # Fail test if 5% deviation from benchmark value
        assert difference >= -5, f'{difference} from the benchmark value for {operation} with {object_size}'


@performance
@pytest.mark.parametrize(
    argnames=["interface", "io_pattern"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'sequential'], marks=pytest.mark.polarion_id("OCS-844")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 'random'], marks=pytest.mark.polarion_id("OCS-846")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 'sequential'], marks=pytest.mark.polarion_id("OCS-845")
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
        fill_storage = (storage_cluster.get_osd_size() * storage_cluster.get_deviceset_count()) / 2
        pvc_size = fill_storage / (3 * fio_cr['spec']['workload']['args']['servers'])
        # Todo: have pvc_size set to 'get_osd_pods_memory_sum * 5'
        #  once pr-2037 is merged
        fio_cr['spec']['clustername'] = config.ENV_DATA['platform'] + get_build() + get_ocs_version()
        fio_cr['spec']['test_user'] = interface + io_pattern
        fio_cr['spec']['workload']['args']['storagesize'] = str(pvc_size)+'Gi'
        fio_cr['spec']['workload']['args']['filesize'] = str(int(pvc_size * 0.8))+'GiB'
        fio_cr['spec']['workload']['args']['storageclass'] = sc
        if io_pattern == 'random':
            fio_cr['spec']['workload']['args']['prefill'] = 'true'
            fio_cr['spec']['workload']['args']['jobs'] = ['randwrite', 'randread']
        fio_cr['spec']['workload']['args']['rook_ceph_drop_cache_pod_ip'] = get_worker_nodes()
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
            sleep=60,
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
        analyze_regression(io_pattern, es_username=fio_cr['spec']['test_user'])

        # todo: push results to codespeed
