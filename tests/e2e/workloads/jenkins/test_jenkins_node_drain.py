import logging
import pytest

from ocs_ci.framework.testlib import (
    E2ETest, workloads
)
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs import node

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def jenkins(request):

    jenkins = Jenkins()

    def teardown():
        jenkins.cleanup()
    request.addfinalizer(teardown)
    return jenkins


@workloads
@pytest.mark.polarion_id("OCS-2175")
class TestJenkinsNodeDrain(E2ETest):
    """
    Test running Jenkins and Node Reboot
    """
    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        # Deployment of jenkins
        jenkins.setup_jenkins()

    @pytest.mark.parametrize(
        argnames=['node_type'],
        argvalues=[
            pytest.param(
                *['worker'], marks=pytest.mark.polarion_id("OCS-2176")
            )
        ]
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_node_drain(self, jenkins, node_type):
        """
        Test jenkins workload
        """

        # Start Build
        jenkins.start_build()

        # Get relevant node
        node1 = jenkins.get_node(node_type=node_type)

        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([node1.name])

        # Make the node schedulable again
        node.schedule_nodes([node1.name])

        # Wait build reach Complete state
        jenkins.wait_for_build_status(status='Complete')
