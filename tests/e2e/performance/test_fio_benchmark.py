"""
Module to perform FIO benchmark
"""
import logging
import pytest
import time

from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.utility.performance_dashboard import push_perf_dashboard
from ocs_ci.framework.testlib import E2ETest, performance
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.elasticsearch import ElasticSearch
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.version import get_environment_info

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def es(request):
    def teardown():
        es.cleanup()
    request.addfinalizer(teardown)
    es = ElasticSearch()
    return es


@pytest.fixture(scope='function')
def ripsaw(request):

    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)
    return ripsaw


class FIOResultsAnalyse(PerfResult):
    """
    This class is reading all test results from elasticsearch server (which the
    ripsaw running of the benchmark is generate), aggregate them by :
        test operation (e.g. create / delete etc.)
        sample (for test to be valid it need to run with more the one sample)
        host (test can be run on more then one pod {called host})

    It generates results for all tests as one unit which will be valid only
    if the deviation between samples is less the 5%

    """

    def __init__(self, uuid, crd):
        """
        Initialize the object by reading some of the data from the CRD file and
        by connecting to the ES server and read all results from it.

        Args:
            uuid (str): the unique uid of the test
            crd (dict): dictionary with test parameters - the test yaml file
                        that modify it in the test itself.

        """

        super(FIOResultsAnalyse, self).__init__(uuid, crd)
        self.index = 'ripsaw-fio-analyzed-result'
        self.new_index = 'ripsaw-fio-fullres'
        # make sure we have connection to the elastic search server
        self.es_connect()

    def analyze_results(self):
        """
        Analyzing the results of the test and creating one record with all test
        information

        """

        for result in self.es_read():
            test_data = result['_source']['ceph_benchmark_test']['test_data']
            object_size = test_data['object_size']
            operation = test_data['operation']
            total_iops = '{:.2f}'.format(test_data['total-iops'])
            std_dev = 'std-dev-' + object_size
            variance = '{:.2f}'.format(test_data[std_dev])
            if object_size in self.all_results.keys():
                self.all_results[object_size][operation] = {
                    'IOPS': total_iops, 'std_dev': variance}
            else:
                self.all_results[object_size] = {
                    operation: {'IOPS': total_iops, 'std_dev': variance}}

            log.info(
                f"\nio_pattern: {self.results['io_pattern']} : "
                f"block_size: {object_size} ; operation: {operation} ; "
                f"total_iops: {total_iops} ; variance - {variance}\n"
            )
        # Todo: Fail test if 5% deviation from benchmark value

    def codespeed_push(self):
        """
        Pushing the results into codespeed, for random test only!

        """

        # in case of io pattern is sequential - do nothing
        if self.results['io_pattern'] == 'sequential':
            return

        # in case of random test - push the results
        reads = self.all_results['4KiB']['randread']['IOPS']
        writes = self.all_results['4KiB']['randwrite']['IOPS']
        r_bw = self.all_results['1024KiB']['randread']['IOPS']
        w_bw = self.all_results['1024KiB']['randwrite']['IOPS']

        # Pushing the results into codespeed
        log.info(f'Pushing to codespeed : Read={reads} ; Write={writes} ; '
                 f'R-BW={r_bw} ; W-BW={w_bw}')
        push_perf_dashboard(self.results['storageclass'],
                            reads, writes, r_bw, w_bw)


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

    def test_fio_workload_simple(self, ripsaw, es, interface, io_pattern):
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

        # Saving the Original elastic-search IP and PORT - if defined in yaml
        es_server = ""
        es_port = ""
        if 'elasticsearch' in fio_cr['spec']:
            if 'server' in fio_cr['spec']['elasticsearch']:
                es_server = fio_cr['spec']['elasticsearch']['server']
            if 'port' in fio_cr['spec']['elasticsearch']:
                es_port = fio_cr['spec']['elasticsearch']['port']
        else:
            fio_cr['spec']['elasticsearch'] = {}

        # Use the internal define elastic-search server in the test
        fio_cr['spec']['elasticsearch'] = {'server': es.get_ip(),
                                           'port': es.get_port()}

        # Setting the data set to 40% of the total storage capacity
        ceph_cluster = CephCluster()
        ceph_capacity = ceph_cluster.get_ceph_capacity()
        total_data_set = int(ceph_capacity * 0.4)
        filesize = int(fio_cr['spec']['workload']['args']['filesize'].replace('GiB', ''))
        # To make sure the number of App pods will not be more then 50, in case
        # of large data set, changing the size of the file each pod will work on
        if total_data_set > 500:
            filesize = int(ceph_capacity * 0.008)
            fio_cr['spec']['workload']['args']['filesize'] = f'{filesize}GiB'
            # make sure that the storage size is larger then the file size
            fio_cr['spec']['workload']['args']['storagesize'] = f'{int(filesize * 1.2)}Gi'
        fio_cr['spec']['workload']['args']['servers'] = int(total_data_set / filesize)
        log.info(f'Total Data set to work on is : {total_data_set} GiB')

        environment = get_environment_info()
        if not environment['user'] == '':
            fio_cr['spec']['test_user'] = environment['user']
        fio_cr['spec']['clustername'] = environment['clustername']

        log.debug(f'Environment information is : {environment}')

        fio_cr['spec']['workload']['args']['storageclass'] = sc
        if io_pattern == 'sequential':
            fio_cr['spec']['workload']['args']['jobs'] = ['write', 'read']
            fio_cr['spec']['workload']['args']['iodepth'] = 1
        log.info(f'The FIO CR file is {fio_cr}')
        fio_cr_obj = OCS(**fio_cr)
        fio_cr_obj.create()

        # Wait for fio client pod to be created
        for fio_pod in TimeoutSampler(
            300, 20, get_pod_name_by_pattern, 'fio-client',
            constants.RIPSAW_NAMESPACE
        ):
            try:
                if fio_pod[0] is not None:
                    fio_client_pod = fio_pod[0]
                    break
            except IndexError:
                log.info("Bench pod not ready yet")

        # Getting the start time of the test
        start_time = time.strftime('%Y-%m-%dT%H:%M:%SGMT', time.gmtime())

        # Getting the UUID from inside the benchmark pod
        uuid = ripsaw.get_uuid(fio_client_pod)
        # Setting back the original elastic-search information
        fio_cr['spec']['elasticsearch'] = {'server': es_server, 'port': es_port}

        full_results = FIOResultsAnalyse(uuid, fio_cr)

        # Initialize the results doc file.
        for key in environment:
            full_results.add_key(key, environment[key])

        # Setting the global parameters of the test
        full_results.add_key('io_pattern', io_pattern)
        full_results.add_key('dataset', f'{total_data_set}GiB')
        full_results.add_key(
            'file_size', fio_cr['spec']['workload']['args']['filesize'])
        full_results.add_key(
            'servers', fio_cr['spec']['workload']['args']['servers'])
        full_results.add_key(
            'samples', fio_cr['spec']['workload']['args']['samples'])
        full_results.add_key(
            'operations', fio_cr['spec']['workload']['args']['jobs'])
        full_results.add_key(
            'block_sizes', fio_cr['spec']['workload']['args']['bs'])
        full_results.add_key(
            'io_depth', fio_cr['spec']['workload']['args']['iodepth'])
        full_results.add_key(
            'jobs', fio_cr['spec']['workload']['args']['numjobs'])
        full_results.add_key(
            'runtime', {
                'read': fio_cr['spec']['workload']['args']['read_runtime'],
                'write': fio_cr['spec']['workload']['args']['write_runtime']
            }
        )
        full_results.add_key(
            'storageclass', fio_cr['spec']['workload']['args']['storageclass'])
        full_results.add_key(
            'vol_size', fio_cr['spec']['workload']['args']['storagesize'])

        # Wait for fio pod to initialized and complete
        log.info("Waiting for fio_client to complete")
        pod_obj = OCP(kind='pod')
        pod_obj.wait_for_resource(
            condition='Completed',
            resource_name=fio_client_pod,
            timeout=18000,
            sleep=300,
        )

        # Getting the end time of the test
        end_time = time.strftime('%Y-%m-%dT%H:%M:%SGMT', time.gmtime())
        full_results.add_key('test_time', {'start': start_time,
                                           'end': end_time})

        output = run_cmd(f'oc logs {fio_client_pod}')
        log.info(f'The Test log is : {output}')

        try:
            if 'Fio failed to execute' not in output:
                log.info("FIO has completed successfully")
        except IOError:
            log.info("FIO failed to complete")

        # Clean up fio benchmark
        log.info("Deleting FIO benchmark")
        fio_cr_obj.delete()

        log.debug(f'Full results is : {full_results.results}')

        es._copy(full_results.es)
        # Adding this sleep between the copy and the analyzing of the results
        # since sometimes the results of the read (just after write) are empty
        time.sleep(30)
        full_results.analyze_results()  # Analyze the results
        # Writing the analyzed test results to the Elastic-Search server
        full_results.es_write()
        full_results.codespeed_push()  # Push results to codespeed
        # Creating full link to the results on the ES server
        log.info(f'The Result can be found at ; {full_results.results_link()}')
