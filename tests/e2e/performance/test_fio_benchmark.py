"""
Module to perform FIO workload
"""
import logging
import pytest
from elasticsearch import Elasticsearch
from ocs_ci.framework import config
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, performance
from ocs_ci.ocs.resources import storage_cluster

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def ripsaw(request):

    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)
    return ripsaw


def get_ocs_version():
    """
    Return the OCS Version
    Returns:
         str: The version of the OCS
    """

    ocp_cluster = OCP(
        namespace=config.ENV_DATA['cluster_namespace'],
        kind='', resource_name='csv')
    return ocp_cluster.get()['items'][0]['spec']['version']


def get_ocp_build():
    """
    Return the OCP Build Version
    Returns:
         str: The build version of the OCP
    """
    ocp_cluster = OCP(
        namespace=config.ENV_DATA['cluster_namespace'],
        kind='', resource_name='clusterversion')
    return ocp_cluster.get()['items'][0]['status']['desired']['version']


def analyze_regression(es_username):
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
        std_dev = 'std-dev-' + object_size
        variance = test_data[std_dev]
        log.info(f"block_size: '{object_size}'\n"
                    f"operation: {operation}\n"
                    f"total_iops: {total_iops}\n"
                    f"variance: {variance}"
                    )
        # Fail test if std deviation is above 5%
        assert variance <= 5, f'variance - {variance} is greater than 5%'
        if not test_data[std_dev] > 5:
            bm_value = benchmark[object_size][operation]
            difference = (float(total_iops) - float(bm_value)) / float(bm_value) * 100
            # Fail test if 5% deviation from benchmark value
            assert difference >= -5, f'{difference} from the benchmark value for {operation} with {object_size}'


@performance
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-844")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-845")
        )
    ]
)
class TestFIOBenchmark(E2ETest):
    """
    Run FIO perf test using ripsaw benchmark
    """
    def test_fio_workload_simple(self, ripsaw, interface):
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
        pvc_size = (storage_cluster.get_osd_size() * storage_cluster.get_deviceset_count()) / 2
        # Todo: have pvc_size set to 'get_osd_pods_memory_sum * 5'
        #  once pr-2037 is merged
        fio_cr['spec']['clustername'] = config.ENV_DATA['platform'] + get_ocp_build() + get_ocs_version()
        fio_cr['spec']['test_user'] = get_ocs_version() + '_' + interface
        fio_cr['spec']['workload']['args']['storagesize'] = str(pvc_size)+'Gi'
        fio_cr['spec']['workload']['args']['filesize'] = str(int(pvc_size * 0.8))+'GiB'
        fio_cr['spec']['workload']['args']['storageclass'] = sc
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
            timeout=3600,
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
        analyze_regression(es_username=fio_cr['spec']['test_user'])
        # todo: push results to codespeed
