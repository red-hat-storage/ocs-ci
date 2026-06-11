import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.constants import STATUS_COMPLETED

logger = logging.getLogger(__name__)


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
        logger.info("Setting up Jenkins environment")
        jenkins.create_ocs_jenkins_template()
        logger.info("Jenkins OCS template created")

    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_jenkins_workload_simple(self, jenkins, num_projects=5, num_of_builds=5):
        """
        Test jenkins workload
        """
        logger.test_step(f"Configure Jenkins with {num_projects} projects")
        jenkins.number_projects = num_projects
        logger.info(f"Number of projects set to: {num_projects}")

        logger.test_step("Create Jenkins application resources")
        jenkins.create_app_jenkins()
        logger.info("Jenkins application created")

        jenkins.create_jenkins_pvc()
        logger.info("Jenkins PVC created")

        jenkins.create_jenkins_build_config()
        logger.info("Jenkins build config created")

        logger.test_step("Wait for Jenkins deployment to complete")
        jenkins.wait_for_jenkins_deploy_status(status=STATUS_COMPLETED)
        logger.info(f"Jenkins deployment reached status: {STATUS_COMPLETED}")

        logger.test_step(f"Start {num_of_builds} builds per project")
        jenkins.number_builds_per_project = num_of_builds
        logger.info(f"Number of builds per project set to: {num_of_builds}")

        jenkins.start_build()
        logger.info(f"Started builds for {num_projects} projects")

        logger.test_step("Wait for all builds to complete")
        jenkins.wait_for_build_to_complete()
        logger.info("All builds completed successfully")

        logger.test_step("Display build results")
        jenkins.print_completed_builds_results()
        logger.info("Build results displayed")
