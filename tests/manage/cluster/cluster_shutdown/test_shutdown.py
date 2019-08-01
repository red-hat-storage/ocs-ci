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


@pytest.fixture()
def vms():
    # TODO
    pass


@pytest.fixture()
def bm_machines():
    pass
    # TODO


class BaseClusterShutdown(ManageTest):
    """
    Base class for cluster shutdown related tests
    """
    def validate_cluster(self):
        assert ocp.wait_for_nodes_ready(len(instances)), (
            "Not all nodes reached status Ready"
        )

        ceph_cluster = CephCluster()
        assert ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace']
        )
        ceph_cluster.cluster_health_check(timeout=60)


@tier4
class TestUngracefulShutdown(BaseClusterShutdown):
    """
    Test ungraceful cluster shutdown
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    def test_ungraceful_shutdown_aws(self, instances):
        """
        Test ungraceful cluster shutdown - AWS
        """

        aws.stop_instances(instances)
        aws.start_instances(instances)
        self.validate_cluster()

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'VMWare',
        reason="Tests are not running on VMWare deployed cluster"
    )
    def test_ungraceful_shutdown_vmware(self, vms):
        """
        Test ungraceful cluster shutdown - VMWare
        """
        # TODO

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'BM',
        reason="Tests are not running on bare metal deployed cluster"
    )
    def test_ungraceful_shutdown_bm(self, bm_machines):
        """
        Test ungraceful cluster shutdown - Bare metal (RHHI.Next)
        """
        # TODO


@tier4
class GracefulShutdown(BaseClusterShutdown):
    pass
    # TODO
