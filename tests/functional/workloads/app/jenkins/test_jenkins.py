import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.constants import STATUS_COMPLETED

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def jenkins(request):

    jenkins = Jenkins()

    def teardown():
        jenkins.cleanup()

    request.addfinalizer(teardown)
    return jenkins


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-2175")
class TestJenkinsWorkload(E2ETest):
    """
    Test running Jenkins
    """

    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        # Deployment of jenkins
        jenkins.create_ocs_jenkins_template()

    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_jenkins_workload_simple(self, jenkins, num_projects=5, num_of_builds=5):
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

        # Start  Builds
        jenkins.start_build()

        # Wait build reach 'Complete' state
        jenkins.wait_for_build_to_complete()

        # Print table of builds
        jenkins.print_completed_builds_results()
