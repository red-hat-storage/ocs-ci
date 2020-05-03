"""
Test to exercise Small File Workload
"""
import logging
import pytest
import time

from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler, get_ocp_version, run_cmd
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, performance
from tests.helpers import get_logs_with_errors
from elasticsearch import Elasticsearch

log = logging.getLogger(__name__)


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

    def analize_results(self, results, logs):

        def add_value(res, key, value):
            if key not in res.keys():
                res[key] = [value]
            else:
                res[key].append(value)
            return res
        op = ''
        for line in logs.split('\n'):
            log.debug(line)
            if "operation :" in line:
                op = line.split()[-1].strip()
            if "total threads" in line:
                results['smallfile-res'][op] = add_value(
                    results['smallfile-res'][op],
                    'total_threads', line.split()[-1].strip())
            if "total files" in line:
                results['smallfile-res'][op] = add_value(
                    results['smallfile-res'][op],
                    'total_files', line.split()[-1].strip())
            if "elapsed time" in line:
                results['smallfile-res'][op] = add_value(
                    results['smallfile-res'][op],
                    'elapsed_time', line.split()[-1].strip())
            if "files/sec" in line:
                results['smallfile-res'][op] = add_value(
                    results['smallfile-res'][op],
                    'filesPsec', line.split()[-1].strip())
            if "requested files" in line:
                results['smallfile-res'][op] = add_value(
                    results['smallfile-res'][op],
                    'percenrage', line.split()[0].strip())
        return results

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
            pytest.param(
                *[16, 200000, 4, 3, constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id("OCS-2024"),
            ),
        ]
    )
    @pytest.mark.polarion_id("OCS-1295")
    def test_smallfile_workload(self, ripsaw, file_size, files, threads, samples, interface):
        """
        Run SmallFile Workload
        """
        ceph_cluster = CephCluster()
        ocp_cluster = OCP()
        # getting the name and email  of the user that running the test.
        user = run_cmd('git config --get user.name').strip()
        email = run_cmd('git config --get user.email').strip()

        log.info("Apply Operator CRD")
        ripsaw.apply_crd('resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml')
        sf_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)
        if interface == constants.CEPHBLOCKPOOL:
            storageclass = constants.DEFAULT_STORAGECLASS_RBD
        else:
            storageclass = constants.DEFAULT_STORAGECLASS_CEPHFS
        log.info(f"Using {storageclass} Storageclass")
        sf_data['spec']['workload']['args']['storageclass'] = storageclass
        log.info("Running SmallFile bench")

        """
            Setting up the parameters for this test
        """
        sf_data['spec']['workload']['args']['file_size'] = file_size
        sf_data['spec']['workload']['args']['files'] = files
        sf_data['spec']['workload']['args']['threads'] = threads
        sf_data['spec']['workload']['args']['samples'] = samples
        sf_data['spec']['clustername'] = ceph_cluster.get_clustername()
        sf_data['spec']['test_user'] = f'{user}<{email}>'
        """ Calculating the size of the volume that need to be test, it should be at least twice in the size then the
             size of the files, and at least 100Gi.
             since the file_size is in Kb and the vol_size need to be in Gb, more calculation is needed.
        """
        vol_size = int(files * threads * file_size * 3)
        vol_size = int(vol_size / constants.GB2KB)
        if vol_size < 100:
            vol_size = 100
        sf_data['spec']['workload']['args']['storagesize'] = f"{vol_size}Gi"

        sf_obj = OCS(**sf_data)
        sf_obj.create()
        log.info(f'The smallfile yaml file is {sf_data}')

        # Initialaize the results doc file.
        results = {
            'user': sf_data['spec']['test_user'],
            'ocp_version': get_ocp_version(),
            'ocp_build': ocp_cluster.get_build(),
            'ocp_channel': ocp_cluster.get_channel(),
            'ocs_version': ceph_cluster.get_version(),
            'vendor': ocp_cluster.get_provider(),
            'cluster_name': sf_data['spec']['clustername'],
            'smallfile-res': {},
            'sample': samples,
            'global_options': {
                'files': files,
                'file_size': file_size,
                'threads': threads,
                'storageclass': sf_data['spec']['workload']['args']['storageclass'],
                'vol_size': sf_data['spec']['workload']['args']['storagesize']
            }
        }
        # Getting all test operations
        for op in sf_data['spec']['workload']['args']['operation']:
            results['smallfile-res'][op] = {}

        # wait for benchmark pods to get created - takes a while
        for bench_pod in TimeoutSampler(
            120, 3, get_pod_name_by_pattern, 'smallfile-client', 'my-ripsaw'
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
        timeout = 1800
        results['start_time'] = time.strftime('%Y-%m-%dT%H:%M:%SGMT', time.gmtime())
        while True:
            logs = bench_pod.exec_oc_cmd(
                f'logs {small_file_client_pod}',
                out_yaml_format=False
            )
            if "RUN STATUS DONE" in logs:
                results = self.analize_results(results, logs)
                results['end_time'] = time.strftime('%Y-%m-%dT%H:%M:%SGMT', time.gmtime())
                log.debug(f'The test results are : {results}')
                log.info("SmallFile Benchmark Completed Successfully")
                break

            if timeout < (time.time() - start_time):
                raise TimeoutError(f"Timed out waiting for benchmark to complete")
            time.sleep(30)
        assert not get_logs_with_errors()

        # push the results to our elasticsearch server
        es = Elasticsearch([{'host': sf_data['spec']['elasticsearch']['server'],
                             'port': sf_data['spec']['elasticsearch']['port']}])
        es.index(index='ripsaw-smallfile-results', doc_type='_doc', body=results)
