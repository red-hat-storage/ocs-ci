import logging
import pytest

from ocs_ci.framework.testlib import (
    E2ETest, workloads
)
from ocs_ci.ocs.jenkins import Jenkins
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
    def test_jenkins_workload_simple(self, jenkins, num_projects=3, num_of_builds=5):
        """
        Test jenkins workload
        """
        # Create project names
        project_names = []
        for project_id in range(1, num_projects + 1):
            project_names.append('myjenkins-' + str(project_id))

        # Init project names in Jenkins class
        jenkins.project_names = project_names

        # Create app jenkins
        jenkins.create_app_jenkins()

        # Create jenkins pvc
        jenkins.create_jenkins_pvc()

        # Jenkins build config
        jenkins.create_jenkins_build_config()

        # wait_for_jenkins_deploy_status
        jenkins.wait_for_jenkins_deploy_status(status=STATUS_COMPLETED)

        # Number of builds per project
        jenkins.number_builds_per_project = num_of_builds

        # Start Builds
        jenkins.start_build()

        # Wait build reach Complete state
        jenkins.wait_for_build_status(status='Complete')
