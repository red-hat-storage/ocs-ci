import logging
import pytest

from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
)
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.ocs.constants import STATUS_COMPLETED

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def jenkins(request):

    jenkins = Jenkins()

    def teardown():
        jenkins.cleanup()
    request.addfinalizer(teardown)
    return jenkins


@workloads
@ignore_leftovers
class TestJenkinsNodeDrain(E2ETest):
    """
    Test running Jenkins and Node Drain
    """
    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        # Deployment of jenkins
        jenkins.create_ocs_jenkins_template()

    @pytest.mark.parametrize(
        argnames=['node_type', 'num_projects', 'num_of_builds'],
        argvalues=[
            pytest.param(
                *['worker', 4, 3], marks=pytest.mark.polarion_id("OCS-2177")
            ),
            pytest.param(
                *['master', 3, 6], marks=pytest.mark.polarion_id("OCS-2176")
            ),
        ]
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_drain_node(
        self, jenkins, node_type, num_projects, num_of_builds
    ):
        """

          Test jenkins workload
        """
        # Init number of projects
        jenkins.number_projects = num_projects

        # Create app jenkins
        jenkins.create_app_jenkins()

        # Create jenkins pvc
        jenkins.create_jenkins_pvc()

        # Create jenkins build config
        jenkins.create_jenkins_build_config()

        # Wait jenkins deploy pod reach to completed state
        jenkins.wait_for_jenkins_deploy_status(status=STATUS_COMPLETED)

        # Get relevant node
        node1 = jenkins.get_nodes(node_type=node_type, num_of_nodes=1)

        # Init number of builds per project
        jenkins.number_builds_per_project = num_of_builds

        # Start Builds
        jenkins.start_build()

        if len(node1) > 0:
            # Node maintenance - to gracefully terminate all pods on the node
            drain_nodes(node1)
            # Make the node schedulable again
            schedule_nodes(node1)

        # Wait build reach 'Complete' state
        jenkins.wait_for_build_to_complete()

        # Print table of builds
        jenkins.print_completed_builds_results()
