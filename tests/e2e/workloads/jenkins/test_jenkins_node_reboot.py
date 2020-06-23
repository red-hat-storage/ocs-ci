import logging
import pytest

from ocs_ci.framework.testlib import (
    E2ETest, workloads
)
from ocs_ci.ocs.jenkins import Jenkins

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
        jenkins.setup_jenkins()

    @pytest.mark.parametrize(
        argnames=['node_type'],
        argvalues=[
            pytest.param(
                *['worker'], marks=pytest.mark.polarion_id("OCS-2178")
            ),
            pytest.param(
                *['master'], marks=pytest.mark.polarion_id("OCS-2202")
            )
        ]
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_reboot_node(self, jenkins, nodes, node_type):
        """
        Test jenkins workload
        """

        # Start Build
        jenkins.start_build()

        # Get relevant node
        node1 = jenkins.get_node(node_type=node_type)

        # Reboot relevant node
        nodes.restart_nodes([node1])

        # Wait build reach Complete state
        jenkins.wait_for_build_status(status='Complete')
