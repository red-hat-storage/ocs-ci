import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.constants import STATUS_COMPLETED
from ocs_ci.helpers import disruption_helpers

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
class TestJenkinsPodRespin(E2ETest):
    """
    Test running Jenkins and with Ceph pods respin
    """

    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """

        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

        jenkins.create_ocs_jenkins_template()

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

        # Respin pod
        log.info(f"Respin pod {pod_name}")
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=f"{pod_name}")
        disruption.delete_resource()

        # Wait build reach 'Complete' state
        jenkins.wait_for_build_to_complete()

        # Print table of builds
        jenkins.print_completed_builds_results()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=40)
