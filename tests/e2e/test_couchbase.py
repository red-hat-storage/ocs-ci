"""
Module to perform COUCHBASE workload
"""
import logging
import pytest
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating, utils
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def ripsaw(request, storageclass_factory):
    # Create storage class
    log.info("Creating a Storage Class")
    storageclass_factory(sc_name='couchbase-storage')

    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)
    return ripsaw


@workloads
class TestCouchbaseWorkload(E2ETest):
    """
    Deploy Ycsb workload and run against Couchbase
    """
    def test_couchbase_workload_simple(self, ripsaw):
        """
        This is a basic ycsb workload
        """
        log.info("Deploying Couchbase")
        ripsaw.setup_couchbase()

        # Startup YCSB
        log.info("Deploying YCSB")
        run_cmd(
            f'oc config use-context my-ripsaw',
            shell=True,
            check=True,
            cwd=ripsaw.dir
        )
        # Deploy ycsb ripsaw operator
        log.info("Deploying Ycsb ripsaw operator")
        ripsaw.apply_crd(
            'resources/crds/'
            'ripsaw_v1alpha1_ripsaw_crd.yaml'
        )
        # Create ycsb benchmark
        log.info("Create resource file for ycsb")
        cb_data = templating.load_yaml_to_dict(constants.YCSB_BENCHMARK_YAML)
        cb_obj = OCS(**cb_data)
        cb_obj.apply(**cb_data)
        #
        # Wait for last workload test
        #
        workload_pattern = 'ycsb-bench-job-workload'
        last_workload = workload_pattern + 'c'
        for ycsbbench_pod in TimeoutSampler(
            60, 3, get_pod_name_by_pattern,
            last_workload,
            'my-ripsaw'
        ):
            try:
                if ycsbbench_pod[0] is not None:
                    ycsbbench_client_pod = ycsbbench_pod[0]
                    break
            except IndexError:
                log.info("Bench pod not ready yet")

        # Wait for workloadc to be complete
        log.info("Waiting for ycsb tests to complete")
        pod_obj = OCP(kind='pod')
        pod_obj.wait_for_resource(
            condition='Completed',
            resource_name=ycsbbench_client_pod,
            timeout=800,
            sleep=10,
        )
        suffix = ycsbbench_client_pod[len(last_workload):]
        for ycsb_test in ['a', 'b', 'c']:
            workload_pod = f'{workload_pattern}{ycsb_test}{suffix}'
            output = run_cmd(f'bin/oc logs {workload_pod}')
            couch_output = utils.parse_pgsql_logs(output)
            log.info(
                f'*******Couchbase log for {workload_pod}*********\n'
                f"{couch_output}"
            )
            throughput = couch_output['throughput']
            if not throughput:
                raise UnexpectedBehaviour(
                    f"{workload_pod} failed to run, "
                    "no throughput found"
                )
        log.info("YCSB test have completed successfully")
        log.info("Deleting ycsb bench benchmark")
        cb_obj.delete()
