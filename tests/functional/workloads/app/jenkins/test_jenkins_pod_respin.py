import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.constants import STATUS_COMPLETED
from ocs_ci.helpers import disruption_helpers

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
class TestJenkinsPodRespin(E2ETest):
    """
    Test running Jenkins and with Ceph pods respin
    """

    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        logger.info(
            "Setting up Jenkins environment with Ceph pod respin test configuration"
        )
        self.sanity_helpers = Sanity()

        jenkins.create_ocs_jenkins_template()
        logger.info("Jenkins OCS template created")

    @pytest.mark.parametrize(
        argnames=["pod_name", "num_projects", "num_of_builds"],
        argvalues=[
            pytest.param(*["mon", 3, 4], marks=pytest.mark.polarion_id("OCS-2204")),
            pytest.param(*["osd", 4, 3], marks=pytest.mark.polarion_id("OCS-2179")),
            pytest.param(*["mgr", 3, 5], marks=pytest.mark.polarion_id("OCS-2205")),
        ],
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_respin_pod(
        self, jenkins, pod_name, num_projects, num_of_builds
    ):
        """
        Test jenkins workload
        """
        logger.test_step(
            f"Configure Jenkins: {num_projects} projects, {num_of_builds} builds/project"
        )
        jenkins.number_projects = num_projects
        logger.info(f"Number of projects: {num_projects}")

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
        jenkins.start_build()
        logger.info(f"Started builds for {num_projects} projects")

        logger.test_step(f"Respin Ceph {pod_name} pod during build execution")
        logger.info(f"Respinning Ceph {pod_name} pod")
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=f"{pod_name}")
        disruption.delete_resource()
        logger.info(f"Ceph {pod_name} pod respun successfully")

        logger.test_step("Wait for all builds to complete")
        jenkins.wait_for_build_to_complete()
        logger.info("All builds completed successfully after pod respin")

        logger.test_step("Display build results")
        jenkins.print_completed_builds_results()
        logger.info("Build results displayed")

        logger.test_step("Verify cluster and Ceph health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")
