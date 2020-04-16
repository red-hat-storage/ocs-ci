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
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest
from tests import helpers
from ocs_ci.ocs import machine, node

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
def label_nodes(request, with_ocs):
    """
    Fixture to label the node(s) that will run the application pod.
    That will be all workers node that do not run the OCS cluster.
    """

    m_set = ''  # this will hold machine_set name that added

    def teardown():

        if with_ocs:
            return

        if m_set != '':
            log.info(f'Destroy {m_set}')
            machine.delete_custom_machineset(m_set)
        else:
            log.info('Clear label form worker (Application) nodes')
            # Getting all Application nodes
            app_nodes = machine.get_labeled_nodes(constants.APP_NODE_LABEL)
            log.debug(f'The application nodes are : {app_nodes}')
            helpers.remove_label_from_worker_node(
                app_nodes, constants.VDBENCH_NODE_LABEL
            )

    request.addfinalizer(teardown)

    if with_ocs:
        return

    # Add label to the worker nodes

    # Getting all OCS nodes (to verify app pod wil not run on)
    ocs_nodes = machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
    worker_nodes = helpers.get_worker_nodes()
    # Getting list of free nodes
    free_nodes = list(set(worker_nodes) - set(ocs_nodes))

    if not free_nodes:
        # No free nodes -  Creating new machineset for application pods
        log.info('Adding new machineset, with worker for application pod')
        m_set = machine.create_custom_machineset(label=constants.APP_NODE_LABEL)
        machine.wait_for_new_node_to_be_ready(m_set)

        free_nodes = machine.get_labeled_nodes(
            f'node-role.kubernetes.io/app={constants.APP_NODE_LABEL}'
        )

        # TODO: implement this for VMWare as well.

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

    @pytest.mark.parametrize('template', [
        pytest.param("VDBench-Basic.yaml", marks=pytest.mark.workloads()),
        pytest.param("VDBench-Basic-FS.yaml", marks=pytest.mark.workloads())]
    )
    @pytest.mark.parametrize('load', [15, 35, 70])
    @pytest.mark.parametrize(
        argnames=['with_ocs', 'servers', 'threads', 'blocksize', 'fileio',
                  'samples', 'width', 'depth', 'files', 'file_size',
                  'runtime', 'pause'],
        argvalues=[pytest.param(*[True, 0, 0, [], "", 1, 0, 0, 0, 0, 600, 5],
                                marks=pytest.mark.workloads())],
    )
    def test_vdbench_workload(
        self, template, with_ocs, load, label_nodes, ripsaw, servers, threads,
        blocksize, fileio, samples, width, depth, files, file_size, runtime,
        pause
    ):
        """
        Run VDBench Workload

        Args :
            template (str) : Name of yaml file that will used as a template
            with_ocs (bool) : This parameter will indicate if the test will
                              run on the same nodes as the OCS
            load (int) : load to run on the storage in percentage of the capacity.
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
        log.info(f'going to use {template} as template')
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

        log.info('Calculating Storage size....')
        ceph_cluster = CephCluster()
        total_capacity = ceph_cluster.get_ceph_capacity()
        assert (total_capacity > constants.VDBENCH_MIN_CAPACITY), \
            "Storage capacity is too low for performance testing"
        log.info(f'The Total usable capacity is {total_capacity}')

        if load:
            width = constants.VDBENCH_WIDTH
            depth = constants.VDBENCH_DEPTH
            file_size = constants.VDBENCH_FILE_SIZE
            capacity_per_pod = constants.VDBENCH_CAP_PER_POD
            total_dirs = width ** depth
            log.info(f'The total dirs in the tree {total_dirs}')
            log.info(f'Going to run with {load} % of the capacity load.')
            tested_capacity = round(total_capacity * 1024 * load / 100)
            log.info(f'Tested capacity is {tested_capacity} MB')
            servers = round(tested_capacity / capacity_per_pod)

            """
                To spread the application pods evenly on all workers or application nods and at least 2 app pods
                per node.
            """
            nodes = len(node.get_typed_nodes(node_type=constants.WORKER_MACHINE))
            if not with_ocs:
                nodes = len(machine.get_labeled_nodes(f'node-role.kubernetes.io/app={constants.APP_NODE_LABEL}'))
            log.info(f'Going to use {nodes} nodes for the test !')
            servers = round(servers / nodes) * nodes
            if servers < (nodes * 2):
                servers = nodes * 2

            files = round(tested_capacity / servers / total_dirs)
            total_files = round(files * servers * total_dirs)
            log.info(f'number of pods is {servers}')
            log.info(f'Going to create {total_files} files !')
            log.info(f'number of files in dir is {files}')

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
        if with_ocs:
            if sf_data['spec']['workload']['args']['pin_server']:
                del sf_data['spec']['workload']['args']['pin_server']

        """
            Calculating the size of the volume that need to be test, it should
            be at least twice in the size then the size of the files, and at
            least 100Gi.
            since the file_size is in Kb and the vol_size need to be in Gb,
            more calculation is needed.
        """
        vol_size = int((files * total_dirs) * file_size * 1.3)
        log.info('number of files to create : {}'.format(
            int(files * (width ** depth)))
        )
        log.info(f'The size of all files is : {vol_size}MB')
        vol_size = int(vol_size / 1024)
        if vol_size < 100:
            vol_size = 100
        sf_data['spec']['workload']['args']['storagesize'] = f'{vol_size}Gi'

        log.debug(f'output of configuration file is {sf_data}')

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
