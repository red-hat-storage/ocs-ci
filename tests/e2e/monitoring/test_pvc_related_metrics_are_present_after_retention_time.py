import logging
import pytest

from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.framework.testlib import tier4, E2ETest
from ocs_ci.ocs.monitoring import (
    check_pvcdata_collected_on_prometheus,
    add_retention_time_on_cluster_monitoring_pod,
    remove_retention_time_on_cluster_monitoring_pod
)
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern


logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def ripsaw(request, storageclass_factory):

    # Create storage class
    logger.info("Creating a Storage Class")
    storageclass_factory(sc_name='pgsql-workload')

    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)
    return ripsaw


@pytest.mark.polarion_id("OCS-577")
class TestRetentionTimeOnPrometheus(E2ETest):
    """
    Validate retention time on prometheus
    """

    @pytest.fixture()
    def pgsql_setup(self, request, ripsaw):
        """
        PGSQL test setup and teardown
        """

        # Deployment ripsaw and postgres database
        logger.info("Deploying postgres database")
        ripsaw.apply_crd(
            'resources/crds/'
            'ripsaw_v1alpha1_ripsaw_crd.yaml'
        )
        ripsaw.setup_postgresql()

        # Create pgbench benchmark
        logger.info("Create resource file for pgbench workload")
        pg_data = templating.load_yaml(constants.PGSQL_BENCHMARK_YAML)
        self.pg_obj = OCS(**pg_data)
        self.pg_obj.create()

        # Get the pvc list
        self.pvc_list = get_all_pvcs(namespace='my-ripsaw')
        self.pvc_name = self.pvc_list.get('items')[0].get('metadata').get('name')

        # Wait for pgbench pod to be created
        for pgbench_pod in TimeoutSampler(
            300, 3, get_pod_name_by_pattern, 'pgbench-1-dbs-client', 'my-ripsaw'
        ):
            try:
                if pgbench_pod[0] is not None:
                    self.pgbench_client_pod = pgbench_pod[0]
                    break
            except IndexError:
                logger.info("Bench pod not ready yet")

        def teardown():

            # Clean up pgbench benchmark
            logger.info("Deleting PG bench benchmark")
            self.pg_obj.delete()

        request.addfinalizer(teardown)

    @tier4
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_monitoring_after_retention_time_set_on_prometheus(self):
        """
        Test case to validate after retention time set on prometheus,
        the data/metric collected on pod is deleted after retention time met
        """
        # Set retention time for prometheus
        config_map = add_retention_time_on_cluster_monitoring_pod(retention='2h', wait=True)

        # Validate retention time is set on prometheus
        ocp_obj = ocp.OCP(kind='StatefulSet', namespace=defaults.OCS_MONITORING_NAMESPACE)
        prometheus_k8s_info = ocp_obj.get(resource_name='prometheus-k8s')
        condition = prometheus_k8s_info.get('spec').get('template').get('spec').get('containers')[0].get('args')
        assert bool(True for i in condition if i == '--storage.tsdb.retention.time=2h'), (
            f"Retention time is not set on prometheus {config_map}"
        )

        # Check for the created pvc metrics after rebooting the master nodes
        assert check_pvcdata_collected_on_prometheus(self.pvc_name), (
            f"On prometheus pod for created pvc {self.pvc_name} related data is not collected"
        )

        # Wait for pg_bench pod to initialized and complete
        logger.info("Waiting for pgbench_client to complete")
        pod_obj = ocp.OCP(kind='pod')
        pod_obj.wait_for_resource(
            condition='Completed',
            resource_name=self.pgbench_client_pod,
            timeout=800,
            sleep=10,
        )

        # Todo: Validation

        # Remove retention time set on prometheus
        assert remove_retention_time_on_cluster_monitoring_pod(config_map=config_map, wait=True), (
            "Failed to remove retention time from prometheus."
        )
