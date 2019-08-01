import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs import ocp
from ocs_ci.framework.testlib import tier4, ManageTest
from ocs_ci.utility import aws
from ocs_ci.ocs.cluster import CephCluster

logger = logging.getLogger(__name__)


@pytest.fixture()
def instances(request):
    """
    Get cluster instances

    Returns:
        list: The cluster instances dictionaries
    """
    def finalizer():
        """
        Make sure all instances are running
        """
        aws.start_instances(instances)

    request.addfinalizer(finalizer)

    instances = ocp.get_all_nodes()
    return instances


@tier4
class TestUngracefulShutdown(ManageTest):
    """
    Test ungraceful cluster shutdown
    """
    def test_ungraceful_shutdown(self, instances):
        """
        Test ungraceful cluster shutdown
        """

        aws.stop_instances(instances)
        aws.start_instances(instances)
        assert ocp.wait_for_nodes_ready(len(instances)), (
            "Not all nodes reached status Ready"
        )

        ceph_cluster = CephCluster()
        assert ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace']
        )
        ceph_cluster.cluster_health_check(timeout=60)
