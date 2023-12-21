import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_vsphere_ipi,
    magenta_squad,
)
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.constants import STATUS_COMPLETED, MASTER_MACHINE, WORKER_MACHINE
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.node import get_node_objs

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def jenkins(request, nodes):

    jenkins = Jenkins()

    def teardown():
        jenkins.cleanup()
        # Make sure all VMs are up by the end of the test
        nodes.restart_nodes_by_stop_and_start_teardown()

    request.addfinalizer(teardown)
    return jenkins


@magenta_squad
@workloads
@ignore_leftovers
@skipif_vsphere_ipi
class TestJenkinsNodeReboot(E2ETest):
    """
    Test running Jenkins and Node Reboot
    """

    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

        # Deployment of jenkins
        jenkins.create_ocs_jenkins_template()

    @pytest.mark.parametrize(
        argnames=["node_type", "num_projects", "num_of_builds"],
        argvalues=[
            pytest.param(
                *[MASTER_MACHINE, 2, 15], marks=pytest.mark.polarion_id("OCS-2202")
            ),
            pytest.param(
                *[WORKER_MACHINE, 2, 15], marks=pytest.mark.polarion_id("OCS-2178")
            ),
        ],
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_node_reboot(
        self, jenkins, nodes, node_type, num_projects, num_of_builds
    ):
        """

        Test Node Reboot jenkins
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
        nodes_reboot = jenkins.get_node_name_where_jenkins_pod_not_hosted(
            node_type=node_type, num_of_nodes=1
        )

        # Init number of builds per project
        jenkins.number_builds_per_project = num_of_builds

        # Start Builds
        jenkins.start_build()

        if len(nodes_reboot) > 0:
            # Restart Node
            nodes.restart_nodes(get_node_objs(nodes_reboot))
        else:
            log.info("No node was reboot")

        # Wait build reach 'Complete' state
        jenkins.wait_for_build_to_complete()

        # Print table of builds
        jenkins.print_completed_builds_results()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=40)
