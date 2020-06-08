"""
Test to exercise Small File Workload
"""
import logging
import pytest
import time

from ocs_ci.ocs.ocp import (OCP, get_clustername, get_ocs_version,
                            get_build, get_ocp_channel)
from ocs_ci.ocs.node import get_provider
from ocs_ci.utility.utils import TimeoutSampler, get_ocp_version, run_cmd
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, performance
from tests.helpers import get_logs_with_errors
from ocs_ci.ocs.exceptions import CommandFailed
from elasticsearch import (Elasticsearch, exceptions as esexp)
import numpy as np

log = logging.getLogger(__name__)

# Defining dictionary of keywords to look in the logs, foreach keyword we have
# the key-name in the final results output, the separate character to split the
# line in the log file, the position of the result and the numpy operation to
# use when we have more then one pod in the test.
keys_to_pars = {
    'files/thread': {
        'name': 'Files_per_Treads', 'sep': ' ', 'pos': -1, 'op': None
    },
    'file size (KB)': {
        'name': 'File_size_KB', 'sep': ' ', 'pos': -1, 'op': None
    },
    'total threads': {
        'name': 'Total_Treads', 'sep': ' ', 'pos': -1, 'op': np.sum
    },
    'total files': {
        'name': 'Total_Files', 'sep': '=', 'pos': -1, 'op': np.sum
    },
    'elapsed time': {
        'name': 'Elapsed_Time', 'sep': '=', 'pos': -1, 'op': np.average
    },
    'files/sec': {
        'name': 'Files_per_Sec', 'sep': '=', 'pos': -1, 'op': np.sum
    },
    'IOPS': {
        'name': 'IOPS', 'sep': ' ', 'pos': -1, 'op': np.sum
    },
    'MiB/sec': {
        'name': 'Bandwidth', 'sep': '=', 'pos': -1, 'op': np.sum
    },
    'total data': {
        'name': 'Total_Data_GB', 'sep': ' ', 'pos': -2, 'op': np.sum
    },
}


class SmallFileLogParser(object):

    """
    This class is reading the benchmark pod logs, and parse it

    """
    def __init__(self, pod):
        """
        Initialize the parser object

        Args:
            pod (str): the pod name

        """
        self.podname = pod
        self.bench_pod = OCP(kind='pod', namespace='my-ripsaw')
        self.results = {}

    def __str__(self):
        """
        Create the output of the print() command of this object

        Returns:
            str : string that contain the pod-name of the object and the results
                  from this pod

        """
        output = f'podname = {self.podname}\n'
        output += f'results = {self.results}\n'
        return output

    def read(self):
        """
        Read the logs from the pod and pars it into the results dictionary

        """
        log.info(f'Getting {self.podname} logs.')
        logs = self.bench_pod.exec_oc_cmd(f'logs {self.podname}',
                                          out_yaml_format=False
                                          )
        sec_res = {}  # used for one results section (operation)
        for line in logs.split('\n'):
            line = line.strip()
            if 'completed sample' in line:
                sample = line.split()[9]
                if sample not in self.results.keys():
                    self.results[sample] = {}

                operation = line.split()[12]
                if operation not in self.results[sample].keys():
                    self.results[sample][operation] = sec_res
                    sec_res = {}

            for key in keys_to_pars.keys():
                if key in line:
                    value = line.split(keys_to_pars[key]['sep'])[
                        keys_to_pars[key]['pos']].strip()
                    sec_res[keys_to_pars[key]['name']] = float(value)
        log.debug(f'The Results from {self.podname} are {self.results}')

    def aggregate(self):
        """
        Aggregating all samples results from one pod to one set of results for
        each operation

        Returns:
            dict: dictionary of aggregated results

        """
        log.info(f'Aggregating samples results for {self.podname}')
        fr = {}
        for smp in self.results.keys():
            for op in self.results[smp].keys():
                if op not in fr.keys():
                    fr[op] = {}
                for key in keys_to_pars.keys():
                    key_name = keys_to_pars[key]['name']
                    if key_name in self.results[smp][op]:
                        if key_name not in fr[op]:
                            fr[op][key_name] = [self.results[smp][op][key_name]]
                        else:
                            fr[op][key_name].append(
                                self.results[smp][op][key_name])

        log.debug(f'The results for {self.podname} after aggregation are : {self.results}')
        self.results.update(fr)
        return fr


class SmallFileResultsAnalyse(object):
    """
    This class is reading all test results from elasticsearch server (which the
    ripsaw running of the benchmark is generate), aggregate them by :
        test operation (e.g. create / delete etc.)
        sample (for test to be valid it need to run with more the one sample)
        host (test can be run on more then one pod {called host})

    it generates results for all tests as one unit which will be valid only
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
        self.uuid = uuid
        self.server = crd['spec']['elasticsearch']['server']
        self.port = crd['spec']['elasticsearch']['port']
        self.index = crd['spec']['es_index'] + '-results'
        self.new_index = crd['spec']['es_index'] + '-fullres'
        self.all_results = {}

        # make sure we have connection to the elastic search server
        log.info(f'Connecting to ES {self.server} on port {self.port}')
        try:
            self.es = Elasticsearch([{'host': self.server, 'port': self.port}])
        except esexp.ConnectionError:
            log.error(f'can not connect to ES server {self.server}:{self.port}')

        # Creating full results dictionary
        self.results = {
            'clustername': crd['spec']['clustername'],
            'clients': crd['spec']['workload']['args']['clients'],
            'samples': crd['spec']['workload']['args']['samples'],
            'threads': crd['spec']['workload']['args']['threads'],
            'operations': crd['spec']['workload']['args']['operation'],
            'uuid': uuid,
            'full-res': {}
        }

        # Calculate the number of records for the test
        self.records = self.results['clients'] * self.results['threads']
        self.records *= self.results['samples']
        self.records *= len(self.results['operations'])

    def __str__(self):
        """
        Create the output of the print() command of this object

        Returns:
            str : string that contain all test information and results

        """
        output = 'ElasticSearch object information\n'
        output += f'UUID = {self.uuid}\n'
        output += f'server = {self.server}\n'
        output += f'port = {self.port}\n'
        output += f'index = {self.index}\n'
        output += f'new_index = {self.new_index}\n'
        output += f'all results = {self.all_results}\n'
        output += f'results = {self.results}\n'
        output += f'records = {self.records}\n'

        return output

    def add_key(self, key, value):
        """
        Adding (key and value) to this object results dictionary as a new
        dictionary.

        Args:
            key (str): String which will be the key for the value
            value (*): value to add, can be any kind of data type

        """
        self.results.update({key: value})

    def write(self):
        """
        Writing the results to the elasticsearch server

        """
        log.info('Writing all data to ES server')
        try:
            self.es.index(index=self.new_index,
                          doc_type='_doc',
                          body=self.results,
                          id=self.uuid)
        except esexp.ConnectionError:
            log.error('can not write data to ES server')

    def aggregate_host_results(self, host_res):
        """
        Aggregation results from all hosts in single sample
        (each host is actually a pod)

        Args:
            host_res (dict): dictionary of host results - user for multi hosts
                             test

        Returns:
            dict: dictionary of all the host results aggregated with the host
                  results that provide as argument to one set of results

        """
        results = {}
        log.info('Aggregating hosts results to one test-results')
        if self.results['full-res'] == {}:
            self.results['full-res'] = host_res
            log.info('This is the first pod - return results as is')
        else:

            """
            Each test can do some operations, so i am running loop on all
            operations that we have in this particular test.

            """
            for op in self.results['operations']:

                """
                For each operation, I am looking for all interesting lines
                that need to be parse.

                """
                for key in keys_to_pars.keys():
                    key_name = keys_to_pars[key]['name']
                    oper = keys_to_pars[key]['op']
                    if oper:  # if operation to do on samples op is not None

                        """
                        Not all operations must have all information
                        (e.g. create operation does not have IOPS)

                        """
                        if key_name in self.results['full-res'][op]:

                            """
                            For each samples combine the host information (that
                            passed as parameter) to the total results by using
                            the particular operation (e.g. sum or average)

                            """
                            for index in range(self.results['samples']):
                                cur_data = self.results[
                                    'full-res'][op][key_name][index]
                                new_data = host_res[op][key_name][index]
                                self.results['full-res'][op][key_name][
                                    index] = oper([cur_data, new_data])

        return results

    def aggregate_samples_results(self):
        """
        Aggregation results from all hosts in single sample, and compare
        between samples.

        Returns:
            bool: True if results deviation (between samples) is les or equal
                       to 5%, otherwise False

        """
        test_pass = True
        for op in self.results["operations"]:
            log.debug(f'Aggregating {op} - {self.results["full-res"][op]}')
            results = self.results["full-res"][op]

            for key in keys_to_pars.keys():
                if keys_to_pars[key]["name"] in results.keys():
                    average_res = np.average(results[keys_to_pars[key]["name"]])
                    if key == "IOPS":
                        st_deviation = np.std(results[keys_to_pars[key]["name"]])
                        mean = np.mean(results[keys_to_pars[key]["name"]])

                        pct_dev = (st_deviation / mean) * 100
                        # TODO: replace the 20% to 5% when we will have Perf HW
                        if pct_dev > 20:
                            log.error(f'IOPS Deviation for {op} is more the 20% ({pct_dev})')
                            test_pass = False

                    results[keys_to_pars[key]["name"]] = average_res

                self.results["full-res"][op] = results

        return test_pass


@pytest.fixture(scope='function')
def ripsaw(request, storageclass_factory):
    def teardown():
        ripsaw.cleanup()

    request.addfinalizer(teardown)

    ripsaw = RipSaw()

    return ripsaw


@performance
class TestSmallFileWorkload(E2ETest):
    """
    Deploy Ripsaw operator and run SmallFile workload
    SmallFile workload using https://github.com/distributed-system-analysis/smallfile
    smallfile is a python-based distributed POSIX workload generator which can be
    used to quickly measure performance for a variety of metadata-intensive
    workloads
    """

    @pytest.mark.parametrize(
        argnames=["file_size", "files", "threads", "samples", "interface"],
        argvalues=[
            pytest.param(
                *[4, 50000, 4, 3, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-1295"),
            ),

            pytest.param(
                *[16, 50000, 4, 3, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2020"),
            ),
            pytest.param(
                *[16, 200000, 4, 3, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2021"),
            ),
            pytest.param(
                *[4, 50000, 4, 3, constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id("OCS-2022"),
            ),
            pytest.param(
                *[16, 50000, 4, 3, constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id("OCS-2023"),
            ),

        ]
    )
    @pytest.mark.polarion_id("OCS-1295")
    def test_smallfile_workload(self, ripsaw, file_size, files, threads,
                                samples, interface):
        """
        Run SmallFile Workload
        """

        sf_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)

        # getting the name and email  of the user that running the test.
        log.info('Getting the Username and email of the running user')
        try:
            user = run_cmd('git config --get user.name').strip()
            email = run_cmd('git config --get user.email').strip()
        except CommandFailed:
            # if no git user define, use the default user from the CR file
            user = sf_data['spec']['test_user']
            email = ''
        log.info(f'Test was triggered by {user} : <{email}>')

        log.info('Apply Operator CRD')
        ripsaw.apply_crd('resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml')
        if interface == constants.CEPHBLOCKPOOL:
            storageclass = constants.DEFAULT_STORAGECLASS_RBD
        else:
            storageclass = constants.DEFAULT_STORAGECLASS_CEPHFS
        log.info(f'Using {storageclass} Storage class')
        sf_data['spec']['workload']['args']['storageclass'] = storageclass
        log.info('Running SmallFile benchmark')

        """
            Setting up the parameters for this test
        """
        sf_data['spec']['workload']['args']['file_size'] = file_size
        sf_data['spec']['workload']['args']['files'] = files
        sf_data['spec']['workload']['args']['threads'] = threads
        sf_data['spec']['workload']['args']['samples'] = samples
        sf_data['spec']['clustername'] = get_clustername()
        sf_data['spec']['test_user'] = f'{user}<{email}>'
        """
        Calculating the size of the volume that need to be test, it should
        be at least twice in the size then the size of the files, and at
        least 100Gi.

        Since the file_size is in Kb and the vol_size need to be in Gb, more
        calculation is needed.

        """
        clients = sf_data['spec']['workload']['args']['clients']
        test_files = files * threads * clients
        dataset = test_files * file_size / constants.GB2KB
        log.info(f'Total data set to use in the test is {dataset}')
        vol_size = int(files * threads * file_size * 3)
        vol_size = int(vol_size / constants.GB2KB)
        if vol_size < 100:
            vol_size = 100
        sf_data['spec']['workload']['args']['storagesize'] = f'{vol_size}Gi'

        sf_obj = OCS(**sf_data)
        sf_obj.create()

        # wait for benchmark pods to get created - takes a while
        clients = sf_data['spec']['workload']['args']['clients']
        log.info(f'Going to run on {clients} pods, waiting for pods to start')
        small_file_client_pods = {}
        for bench_pod in TimeoutSampler(
            1200, 5, get_pod_name_by_pattern, 'smallfile-client', 'my-ripsaw'
        ):
            try:
                if len(bench_pod) == clients:
                    log.info(f'The list of benchmark pods is {bench_pod}')
                    for bpod in bench_pod:
                        small_file_client_pods[bpod] = SmallFileLogParser(
                            bpod)
                    small_file_client_pod = bench_pod
                    break
            except IndexError:
                log.info('Bench pod not ready yet')

        # make sure all pods started
        assert len(small_file_client_pod) == clients, \
            f'Not All pods started {small_file_client_pod} out of {clients}'

        bench_pod = OCP(kind='pod', namespace='my-ripsaw')
        log.info('All PODs started, Waiting for SmallFile benchmark to Run')
        for smf_pod in small_file_client_pods.keys():
            assert bench_pod.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=smf_pod,
                sleep=10,
                timeout=600
            )

        start_time = time.time()

        # After testing manually, changing the timeout
        timeout = 3600
        log.info('The SmallFile benchmark is Running')

        """
        Getting the UUID from inside the benchmark pod (first one - since all
        have the same UUID

        """
        output = bench_pod.exec_oc_cmd(f'exec {small_file_client_pod[0]} env')
        uuid = ''
        for line in output.split():
            if 'uuid=' in line:
                uuid = line.split('=')[1]
                log.info(f'the UUID of the test is : {uuid}')
        full_results = SmallFileResultsAnalyse(uuid, sf_data)

        # Initialize the results doc file.
        full_results.add_key('user', sf_data['spec']['test_user'])
        full_results.add_key('ocp_version', get_ocp_version())
        full_results.add_key('ocp_build', get_build())
        full_results.add_key('ocp_channel', get_ocp_channel())
        full_results.add_key('ocs_version', get_ocs_version())
        full_results.add_key('vendor', get_provider())
        full_results.add_key('hosts', small_file_client_pod)
        full_results.add_key('start_time',
                             time.strftime('%Y-%m-%dT%H:%M:%SGMT',
                                           time.gmtime()))

        # Calculating the total size of the working data set - in GB
        full_results.add_key(
            'dataset',
            file_size * files * threads * full_results.results[
                'clients'] / constants.GB2KB
        )

        full_results.add_key('global_options', {
            'files': files,
            'file_size': file_size,
            'storageclass': sf_data['spec']['workload']['args']['storageclass'],
            'vol_size': sf_data['spec']['workload']['args']['storagesize']
        })
        log.debug(f'The Initial results object is : {full_results}')

        while True:

            finished = 0
            for smf_pod in small_file_client_pod:
                logs = bench_pod.exec_oc_cmd(
                    f'logs {smf_pod}',
                    out_yaml_format=False
                )
                if 'RUN STATUS DONE' in logs:
                    log.info(f'The pod {smf_pod} finished !')
                    finished += 1

            if finished == clients:
                full_results.add_key('end_time',
                                     time.strftime('%Y-%m-%dT%H:%M:%SGMT',
                                                   time.gmtime())
                                     )
                log.info('All Benchmark pods finished.')
                for smf_pod in small_file_client_pod:
                    small_file_client_pods[smf_pod].read()
                    log.debug(f'Results after reading {smf_pod} are :')
                    log.debug(small_file_client_pods[smf_pod].results)

                    full_results.aggregate_host_results(
                        small_file_client_pods[smf_pod].aggregate()
                    )

                    log.debug(f'The Results for {smf_pod} are : ')
                    log.debug(small_file_client_pods[smf_pod].results)

                test_status = full_results.aggregate_samples_results()
                log.info(f'The Test results are : {full_results}')
                log.info(f'The test status is {test_status}')
                full_results.write()  # Writing the results to the ElasticSearch
                break

            if timeout < (time.time() - start_time):
                raise TimeoutError(
                    'Timed out waiting for benchmark to complete')
            time.sleep(30)
        assert (not get_logs_with_errors() and test_status), 'Test Failed'
