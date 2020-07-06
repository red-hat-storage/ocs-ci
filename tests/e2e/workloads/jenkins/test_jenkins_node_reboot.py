import logging
import pytest

from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
)
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.constants import STATUS_COMPLETED
from ocs_ci.ocs.node import get_node_objs


log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def jenkins(request):

    jenkins = Jenkins()

    def teardown():
        jenkins.cleanup()
    request.addfinalizer(teardown)
    return jenkins


@ignore_leftovers
@workloads
class TestJenkinsNodeReboot(E2ETest):
    """
    Test running Jenkins and Node Reboot
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
                *['worker', 4, 5], marks=pytest.mark.polarion_id("OCS-2178")
            ),
            pytest.param(
                *['master', 3, 6], marks=pytest.mark.polarion_id("OCS-2202")
            ),
        ]
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_reboot_node(
        self, jenkins, nodes, node_type, num_projects, num_of_builds
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

        # Init number of builds per project
        jenkins.number_builds_per_project = num_of_builds

        # Start Builds
        jenkins.start_build()

        # Get relevant node
        node1 = jenkins.get_nodes(node_type=node_type, num_of_nodes=1)

        # Reboot relevant node
        if len(node1) > 0:
            nodes.restart_nodes(get_node_objs(node1))

        # Wait build reach 'Complete' state
        jenkins.wait_for_build_status(status='Complete')

        # Print table of builds
        jenkins.print_completed_builds_results()
