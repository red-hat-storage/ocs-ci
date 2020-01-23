"""
Test to exercise VDBench Workload
"""
import logging
import pytest
import time
import os

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler, ocsci_log_path
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest
from tests import helpers
from ocs_ci.ocs import machine

log = logging.getLogger(__name__)


# TODO: move this function to pod.py helper functions
def retrive_files_from_pod(pod_name, localpath, remotepath):
    """
    Download a file from pod

    Args:
        pod_name (str): Name of the pod
        localpath (str): Local file to download
        remotepath (str): Target path on the pod

    """
    cmd = f'cp {pod_name}:{remotepath} {os.path.expanduser(localpath)}'
    ocp_obj = OCP()
    ocp_obj.exec_oc_cmd(command=cmd)


@pytest.fixture(scope='function')
def label_nodes(request):
    """
    Fixture to label the node(s) that will run the application pod.
    That will be all workers node that do not run the OCS cluster.
    """
    def teardown():
        log.info('Clear label form worker (Application) nodes')
        # Getting all Application nodes
        app_nodes = machine.get_labeled_nodes(constants.APP_NODE_LABEL)
        helpers.remove_label_from_worker_node(app_nodes,
                                              constants.APP_NODE_LABEL)

    request.addfinalizer(teardown)

    # Getting all OCS nodes (to verify app pod wil not run on)
    ocs_nodes = machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
    # Add label to the worker nodes
    worker_nodes = helpers.get_worker_nodes()
    # Getting list of free nodes
    free_nodes = list(set(worker_nodes) - set(ocs_nodes))

    log.info('Adding the app-node label to Non-OCS workers')
    log.debug(f'The Workers nodes are : {worker_nodes}')
    log.debug(f'The OCS nodes are : {ocs_nodes}')
    log.debug(f'The free nodes are : {free_nodes}')

    assert free_nodes, \
        'Did not found any worker to run on, pleas deploy another worker'

    helpers.label_worker_node(free_nodes,
                              constants.APP_NODE_LABEL,
                              constants.VDBENCH_NODE_LABEL)

    return


@pytest.fixture(scope='function')
def ripsaw(request):
    """
    Fixture to deploy the ripsaw benchmarking operator
    """
    def teardown():
        log.info('cleanup the ripsaw operator')
        ripsaw.cleanup()

    request.addfinalizer(teardown)

    # Create RipSaw Operator
    # ripsaw = RipSaw()
    # TODO: replace beween the too lines when the vdbench will merge into
    #       ripsaw PR-#265
    ripsaw = RipSaw(repo='https://github.com/Avilir/ripsaw',
                    branch='vebench_for_testing')
    return ripsaw


class TestVDBenchWorkload(E2ETest):
    """
    Deploy Ripsaw operator and run VDBench workload

    The workload is using a template CRD yaml file that run 3 basic workloads :
        100% Read / 100% Write / Mix workload (75% read & 25% Write).

    This implementation run only on mount volume (CephFS / RBD) and not on
    block device (RAW). It create a tree of directories in in each directory it
    create files in defined size.
    The total number of files is calculating by : files * (width ** depth)
    """

    @pytest.mark.parametrize(
        argnames=['template', 'servers', 'threads', 'blocksize', 'fileio',
                  'samples', 'width', 'depth', 'files', 'file_size', 'runtime',
                  'pause'],
        argvalues=[
            pytest.param(*["VDBench-BCurve.yaml",
                           9, 4, ["4k"], "random",
                           1, 4, 3, 256, 5, 600, 5]),
            pytest.param(*["VDBench-BCurve.yaml",
                           9, 4, ["64k"], "random",
                           1, 4, 3, 256, 5, 600, 5]),
            pytest.param(*["VDBench-BCurve-FS.yaml",
                           9, 4, ["4k"], "random",
                           1, 4, 3, 256, 5, 600, 5]),
            pytest.param(*["VDBench-BCurve-FS.yaml",
                           9, 4, ["64k"], "random",
                           1, 4, 3, 256, 5, 600, 5]),
            pytest.param(*["VDBench-Basic.yaml",
                           9, 4, ["4k"], "random",
                           1, 4, 3, 256, 5, 600, 1],
                         marks=pytest.mark.workloads()),
        ],
    )


    def test_vdbench_workload(self, template, label_nodes, ripsaw, servers,
                              threads, blocksize, fileio, samples, width,
                              depth, files, file_size, runtime, pause
                              ):
        """
        Run VDBench Workload

        Args :
            template (str) : Name of yaml file that will used as a template
            label_nodes (fixture) : This fixture is labeling the worker(s)
                                    that will used for App. pod(s)
            ripsaw (fixture) : Fixture to deploy the ripsaw benchmarking operator
            servers (int) : Number of servers (pods) that will run the IO
            threads (int) : Number of threads that will run on each server
            blocksize (list - str): List of BlockSize - must add the 'K' to it
            fileio (str) : How to select file for the IO : random / sequential
            samples (int) : Number of time(s) to run each test
            width (int) : Width of directory tree to create
            depth (int) : Depth of directory tree to create
            files (int) : Number of files to create in each directory
            file_size (int) : File size (in MB) to create
            runtime (int) : Time (in Sec.) for each test iteration
            pause (int) : Time (in Min.) to pause between each test iteration.
        """
        log.info("Apply Operator CRD")

        crd = 'resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml'
        ripsaw.apply_crd(crd)

        log.info('Running vdbench benchmark')
        if template:
            template = os.path.join(constants.TEMPLATE_VDBENCH_DIR, template)
        else:
            template = constants.VDBENCH_BENCHMARK_YAML
        sf_data = templating.load_yaml(template)

        target_results = template + 'Results'
        """
            Setting up the parameters for this test
        """
        if servers:
            sf_data['spec']['workload']['args']['servers'] = servers
            target_results = target_results + '-' + str(servers)
        if threads:
            sf_data['spec']['workload']['args']['threads'] = threads
            target_results = target_results + '-' + str(threads)
        if fileio:
            sf_data['spec']['workload']['args']['fileio'] = fileio
            target_results = target_results + '-' + str(fileio)
        if samples:
            sf_data['spec']['workload']['args']['samples'] = samples
            target_results = target_results + '-' + str(samples)
        if width:
            sf_data['spec']['workload']['args']['width'] = width
            target_results = target_results + '-' + str(width)
        if depth:
            sf_data['spec']['workload']['args']['depth'] = depth
            target_results = target_results + '-' + str(depth)
        if files:
            sf_data['spec']['workload']['args']['files'] = files
            target_results = target_results + '-' + str(files)
        if file_size:
            sf_data['spec']['workload']['args']['file_size'] = file_size
            target_results = target_results + '-' + str(file_size)
        if runtime:
            sf_data['spec']['workload']['args']['runtime'] = runtime
            target_results = target_results + '-' + str(runtime)
        if pause:
            sf_data['spec']['workload']['args']['pause'] = pause
            target_results = target_results + '-' + str(pause)
        if len(blocksize) > 0:
            sf_data['spec']['workload']['args']['bs'] = blocksize
            target_results = target_results + '-' + '_'.join(blocksize)

        """
            Calculating the size of the volume that need to be test, it should
            be at least twice in the size then the size of the files, and at
            least 100Gi.
            since the file_size is in Kb and the vol_size need to be in Gb,
            more calculation is needed.
        """
        vol_size = int((files * (width ** depth)) * file_size * 1.3)
        log.info('number of files to create : {}'.format(
            int(files * (width ** depth)))
        )
        log.info(f'The size of all files is : {vol_size}MB')
        vol_size = int(vol_size / 1024)
        if vol_size < 100:
            vol_size = 100
        sf_data['spec']['workload']['args']['storagesize'] = f'{vol_size}Gi'

        log.info(sf_data)

        timeout = 86400  # 3600 (1H) * 24 (1D)  = one days

        sf_obj = OCS(**sf_data)
        sf_obj.create()
        # wait for benchmark pods to get created - takes a while
        for bench_pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, 'vdbench-client', 'my-ripsaw'
        ):
            try:
                if bench_pod[0] is not None:
                    vdbench_client_pod = bench_pod[0]
                    break
            except IndexError:
                log.info('Benchmark client pod not ready yet')

        bench_pod = OCP(kind='pod', namespace='my-ripsaw')
        log.info('Waiting for VDBench benchmark to Run')
        assert bench_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=vdbench_client_pod,
            sleep=30,
            timeout=600
        )
        start_time = time.time()
        while True:
            logs = bench_pod.exec_oc_cmd(
                f'logs {vdbench_client_pod}',
                out_yaml_format=False
            )
            if 'Test Run Finished' in logs:
                log.info('VdBench Benchmark Completed Successfully')
                break

            if timeout < (time.time() - start_time):
                raise TimeoutError('Timed out waiting for benchmark to complete')
            time.sleep(30)

        # Getting the results file from the benchmark pod and put it with the
        # test logs.
        # TODO: find the place of the actual test log and not in the parent
        #       logs path
        target_results = '{}/{}.tgz'.format(ocsci_log_path(), target_results)
        pod_results = constants.VDBENCH_RESULTS_FILE
        retrive_files_from_pod(vdbench_client_pod, target_results, pod_results)
